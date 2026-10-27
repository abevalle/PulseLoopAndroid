package com.pulseloop.export.mqtt

import android.util.Log
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext

/**
 * One export pass: plan every type from Room, connect, publish, commit per acknowledged batch,
 * disconnect. Room is the queue — a pass that can't reach the broker commits nothing, so the
 * next pass re-plans the same readings from the same watermarks. Nothing is buffered that a
 * process death could lose.
 *
 * Per batch, in order: history messages (`…/<type>/history`, not retained — every reading), then
 * the retained state (`…/<type>`) if the batch holds a reading newer than the one already there.
 * A backfill replays old history in write order, so the state only moves forward in sample time.
 *
 * TODO(mqtt): deletions aren't exported. A reading deleted in the app after it was published
 *  stays in MQTT/HA. A tombstone message (e.g. `…/<type>/deleted` carrying the key and ts, driven
 *  by `measurement_deletions`) could be added later.
 */
class MqttExporter(
    private val source: ExportSource,
    private val state: ExportStateStore,
    private val transportFactory: () -> MqttTransport,
    private val appVersion: String,
    private val clock: () -> Long = System::currentTimeMillis,
    private val batchSize: Int = BATCH_SIZE,
    private val batchDelayMs: Long = BATCH_DELAY_MS,
) {
    sealed class PassResult {
        data object NoDevice : PassResult()
        data object Idle : PassResult()
        data class Published(val messages: Int) : PassResult()
        data class Failed(val error: MqttExportException) : PassResult()
    }

    private data class TypePlan(val type: ExportType, val batches: List<ExportPlanner.Batch>)

    private suspend fun plan(now: Long): List<TypePlan> {
        val floor = state.status().backfillCompletedAt?.let { now - LEDGER_RETENTION_MS }
        return ExportType.entries.map { type ->
            val candidates = source.records(type, state.watermark(type))
            val known = state.ledger(type.name, candidates.map { it.key })
            TypePlan(type, ExportPlanner.plan(candidates, known, floor, batchSize))
        }
    }

    private suspend fun discovery(device: ExportDevice, config: MqttConfig): List<OutgoingMessage> {
        val all = ExportType.entries.filter { it.hasEntity && source.hasData(it) }.mapNotNull {
            HaDiscovery.config(it, device, config.baseTopic, MqttExportSettings.DISCOVERY_PREFIX, appVersion)
        }
        val known = state.ledger(ExportStateStore.DISCOVERY, all.map { it.topic })
        return all.filter { known[it.topic] != ExportPlanner.hash(it.payload) }
    }

    /** Messages a pass would send right now, without connecting. Recorded for the settings screen. */
    suspend fun refreshPending(): Int {
        val count = if (source.device() == null) 0 else plan(clock()).sumOf { p -> p.batches.sumOf { it.toSend.size } }
        state.updateStatus { it.copy(pending = count) }
        return count
    }

    suspend fun runPass(config: MqttConfig): PassResult {
        val now = clock()
        val device = source.device() ?: return PassResult.NoDevice
        val plans = plan(now)
        val discovery = discovery(device, config)
        var remaining = plans.sumOf { p -> p.batches.sumOf { it.toSend.size } }
        state.updateStatus { it.copy(lastAttemptAt = now, pending = remaining) }

        if (remaining == 0 && discovery.isEmpty()) {
            // Only skip-only batches: advance the watermarks without touching the network.
            for (p in plans) for (b in p.batches) state.commit(p.type, emptyList(), b.watermark, null)
            finish(now, published = false)
            return PassResult.Idle
        }

        val transport = transportFactory()
        var sent = 0
        return try {
            transport.connect(config)
            if (discovery.isNotEmpty()) {
                transport.publish(discovery)
                state.commitDiscovery(discovery.map { LedgerWrite(it.topic, ExportPlanner.hash(it.payload), Long.MAX_VALUE) })
                sent += discovery.size
            }
            for (p in plans) {
                val historyTopic = Topics.history(config.baseTopic, device, p.type)
                val stateTopic = Topics.state(config.baseTopic, device, p.type)
                for (batch in p.batches) {
                    val latest = ExportPlanner.latestForState(batch.toSend, state.lastStateTs(p.type))
                    val messages = batch.toSend.map { OutgoingMessage(historyTopic, it.payload, retained = false) } +
                        listOfNotNull(latest?.let { OutgoingMessage(stateTopic, it.payload, retained = true) })
                    // Publish + commit as one unit: a worker stopped between the PUBACKs and the
                    // commit would otherwise re-send the whole batch on the next pass.
                    withContext(NonCancellable) {
                        if (messages.isNotEmpty()) transport.publish(messages)
                        state.commit(
                            p.type,
                            batch.toSend.map { LedgerWrite(it.key, ExportPlanner.hash(it.payload), it.sampleTs) },
                            batch.watermark,
                            latest?.sampleTs,
                        )
                    }
                    sent += messages.size
                    remaining -= batch.toSend.size
                    // Rate limit: a first-enable backfill is thousands of messages.
                    if (messages.isNotEmpty()) {
                        state.updateStatus { it.copy(pending = remaining) }
                        delay(batchDelayMs)
                    }
                }
            }
            finish(now, published = true)
            PassResult.Published(sent)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            val error = e as? MqttExportException ?: HiveMqTransport.classify(e)
            Log.w(TAG, "pass failed after $sent messages: ${error.kind}")
            state.updateStatus { it.copy(lastError = error.message, pending = remaining) }
            PassResult.Failed(error)
        } finally {
            withContext(NonCancellable) { transport.disconnect() }
        }
    }

    private suspend fun finish(now: Long, published: Boolean) {
        state.updateStatus {
            it.copy(
                lastPublishAt = if (published) clock() else it.lastPublishAt,
                lastError = null,
                pending = 0,
                // The first complete pass is the end of the initial backfill: from here on the
                // ledger can be pruned and the planner floor applies.
                backfillCompletedAt = it.backfillCompletedAt ?: now,
            )
        }
        state.prune(now - LEDGER_RETENTION_MS)
    }

    companion object {
        private const val TAG = "MqttExporter"
        const val BATCH_SIZE = 50
        const val BATCH_DELAY_MS = 250L

        /**
         * How long a reading's ledger line is kept. Rings hold about a week of history, so nothing
         * older can be re-sent by a sync; 45 days leaves a wide margin.
         */
        const val LEDGER_RETENTION_MS = 45L * 24 * 60 * 60 * 1000

        sealed class TestResult {
            data object Success : TestResult()
            data class Failure(val error: MqttExportException) : TestResult()
        }

        /** Connect and disconnect, with a distinct client id so a running pass isn't kicked off. */
        suspend fun testConnection(config: MqttConfig, transport: MqttTransport = HiveMqTransport()): TestResult =
            try {
                transport.connect(config.copy(clientId = config.clientId + "-test"))
                TestResult.Success
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                TestResult.Failure(e as? MqttExportException ?: HiveMqTransport.classify(e))
            } finally {
                withContext(NonCancellable) { transport.disconnect() }
            }
    }
}
