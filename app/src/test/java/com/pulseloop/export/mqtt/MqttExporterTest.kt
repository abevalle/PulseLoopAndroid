package com.pulseloop.export.mqtt

import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAckReturnCode
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.net.ConnectException
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.util.concurrent.CompletionException
import javax.net.ssl.SSLHandshakeException

class MqttExporterTest {

    private val device = ExportDevice("dev-1", "COLMI Ring 2", null, null)
    private val config = MqttConfig("broker", 1883, false, "pl", "u", "p", "pulseloop")

    private class FakeSource(val device: ExportDevice?) : ExportSource {
        val rows = mutableMapOf<ExportType, MutableList<ExportRecord>>()
        fun add(r: ExportRecord) { rows.getOrPut(r.type) { mutableListOf() } += r }
        override suspend fun device() = device
        override suspend fun records(type: ExportType, watermark: Long?) =
            rows[type].orEmpty().filter { watermark == null || it.cursor > watermark }
        override suspend fun hasData(type: ExportType) = rows[type].orEmpty().isNotEmpty()
    }

    private class FakeState : ExportStateStore {
        val watermarks = mutableMapOf<ExportType, Long>()
        val stateTs = mutableMapOf<ExportType, Long>()
        val ledger = mutableMapOf<Pair<String, String>, LedgerWrite>()
        val status = MutableStateFlow(ExportStatus())
        override suspend fun watermark(type: ExportType) = watermarks[type]
        override suspend fun lastStateTs(type: ExportType) = stateTs[type]
        override suspend fun ledger(ledgerType: String, keys: Collection<String>) =
            keys.mapNotNull { k -> ledger[ledgerType to k]?.let { k to it.hash } }.toMap()
        override suspend fun commit(type: ExportType, ledger: List<LedgerWrite>, watermark: Long, lastStateTs: Long?) {
            ledger.forEach { this.ledger[type.name to it.key] = it }
            watermarks[type] = maxOf(watermarks[type] ?: Long.MIN_VALUE, watermark)
            if (lastStateTs != null) stateTs[type] = lastStateTs
        }
        override suspend fun commitDiscovery(ledger: List<LedgerWrite>) {
            ledger.forEach { this.ledger[ExportStateStore.DISCOVERY to it.key] = it }
        }
        override suspend fun status() = status.value
        override fun statusFlow(): Flow<ExportStatus> = status
        override suspend fun updateStatus(transform: (ExportStatus) -> ExportStatus) { status.value = transform(status.value) }
        override suspend fun prune(cutoff: Long) { ledger.entries.removeIf { it.key.first != ExportStateStore.DISCOVERY && it.value.sampleTs < cutoff } }
        override suspend fun reset() { watermarks.clear(); stateTs.clear(); ledger.clear(); status.value = ExportStatus() }
        override suspend fun rewind() { watermarks.clear(); status.value = status.value.copy(backfillCompletedAt = null) }
    }

    /** Records acknowledged publishes; optionally fails the Nth publish call. */
    private class FakeTransport(var failConnect: MqttExportException? = null, var failOnPublishCall: Int? = null) : MqttTransport {
        val published = mutableListOf<OutgoingMessage>()
        var publishCalls = 0
        override suspend fun connect(config: MqttConfig) { failConnect?.let { throw it } }
        override suspend fun publish(messages: List<OutgoingMessage>) {
            publishCalls++
            if (publishCalls == failOnPublishCall) throw MqttExportException(FailureKind.TIMEOUT, "lost link")
            published += messages
        }
        override suspend fun disconnect() {}
    }

    private val now = 100L * 24 * 60 * 60 * 1000
    private fun hr(id: String, cursor: Long, sampleTs: Long, value: Int = 70) =
        ExportRecord(ExportType.HEART_RATE, id, sampleTs, cursor, """{"ts":$sampleTs,"value":$value}""")

    private fun exporter(source: ExportSource, state: ExportStateStore, transport: MqttTransport, batch: Int = 50) =
        MqttExporter(source, state, { transport }, "test", clock = { now }, batchSize = batch, batchDelayMs = 0)

    private fun FakeTransport.history() = published.filter { it.topic.endsWith("/history") }
    private fun FakeTransport.states(type: ExportType) = published.filter { it.topic == "pulseloop/colmi_ring_2/${type.topic}" }

    @Test
    fun `unreachable broker commits nothing and reports the error and the queue`() = runTest {
        val source = FakeSource(device).apply { add(hr("a", 1, now - 1000)); add(hr("b", 2, now - 500)) }
        val state = FakeState()
        val result = exporter(source, state, FakeTransport(failConnect = MqttExportException(FailureKind.REFUSED, null))).runPass(config)

        assertTrue(result is MqttExporter.PassResult.Failed)
        assertTrue(state.watermarks.isEmpty())
        assertTrue(state.ledger.isEmpty())
        assertEquals(2, state.status.value.pending)
        assertTrue(state.status.value.lastError!!.startsWith(FailureKind.REFUSED.label))
        assertNull(state.status.value.backfillCompletedAt)
    }

    @Test
    fun `a second pass, even from a new exporter, sends nothing twice`() = runTest {
        val source = FakeSource(device).apply { add(hr("a", 1, now - 1000)) }
        val state = FakeState()
        val first = FakeTransport()
        exporter(source, state, first).runPass(config)
        assertEquals(1, first.history().size)

        // The ring re-sends the same reading: @Upsert gives it a fresh createdAt.
        source.add(hr("a", 50, now - 1000))
        val second = FakeTransport()
        val result = exporter(source, state, second).runPass(config)
        assertEquals(MqttExporter.PassResult.Idle, result)
        assertTrue(second.published.isEmpty())
        assertEquals(50L, state.watermarks[ExportType.HEART_RATE])
    }

    @Test
    fun `a failure mid-backfill keeps the acknowledged batches and resends only the rest`() = runTest {
        val source = FakeSource(device).apply { (1..4L).forEach { add(hr("r$it", it, now - 10_000 + it)) } }
        val state = FakeState()
        // publish #1 = discovery, #2 = batch r1+r2, #3 = batch r3+r4 fails
        val failing = FakeTransport(failOnPublishCall = 3)
        exporter(source, state, failing, batch = 2).runPass(config)
        assertEquals(listOf("r1", "r2"), state.ledger.keys.filter { it.first == "HEART_RATE" }.map { it.second })
        assertEquals(2L, state.watermarks[ExportType.HEART_RATE])
        assertEquals(2, state.status.value.pending)

        val retry = FakeTransport()
        exporter(source, state, retry, batch = 2).runPass(config)
        assertEquals(2, retry.history().size)
        assertTrue(retry.published.none { it.topic.startsWith("homeassistant/") }) // discovery already acked
        assertNull(state.status.value.lastError)
        assertNotNull(state.status.value.backfillCompletedAt)
    }

    @Test
    fun `retained state ends on the newest reading even when backfill arrives out of order`() = runTest {
        // Written in this order (cursor), but the second row is the newest sample.
        val source = FakeSource(device).apply {
            add(hr("old1", 1, now - 9_000, 60)); add(hr("newest", 2, now - 1_000, 80))
            add(hr("old2", 3, now - 8_000, 61)); add(hr("old3", 4, now - 7_000, 62))
        }
        val state = FakeState()
        val t = FakeTransport()
        exporter(source, state, t, batch = 2).runPass(config)

        val states = t.states(ExportType.HEART_RATE)
        assertTrue(states.all { it.retained })
        assertTrue(t.history().none { it.retained })
        assertEquals(4, t.history().size)
        assertTrue(states.last().payload.contains("\"value\":80"))
        assertEquals(now - 1_000, state.stateTs[ExportType.HEART_RATE])
    }

    @Test
    fun `discovery covers only types with data, and is not republished unchanged`() = runTest {
        val source = FakeSource(device).apply { add(hr("a", 1, now - 1000)) }
        val state = FakeState()
        val t = FakeTransport()
        exporter(source, state, t).runPass(config)
        val configs = t.published.filter { it.topic.startsWith("homeassistant/") }
        assertEquals(listOf("homeassistant/sensor/pulseloop_dev_1/heart_rate/config"), configs.map { it.topic })
        assertTrue(configs.single().retained)

        source.add(hr("b", 2, now - 500))
        val t2 = FakeTransport()
        exporter(source, state, t2).runPass(config)
        assertTrue(t2.published.none { it.topic.startsWith("homeassistant/") })
    }

    @Test
    fun `a changed sleep session is republished under the same session id`() = runTest {
        fun night(cursor: Long, asleep: Int) =
            ExportRecord(ExportType.SLEEP, "s-1", now - 3_600_000, cursor, """{"session_id":"s-1","value":$asleep}""")
        val source = FakeSource(device).apply { add(night(1, 380)) }
        val state = FakeState()
        exporter(source, state, FakeTransport()).runPass(config)

        source.rows[ExportType.SLEEP] = mutableListOf(night(9, 410))
        val t = FakeTransport()
        exporter(source, state, t).runPass(config)
        assertEquals(1, t.history().size)
        assertTrue(t.history().single().payload.contains("410"))
    }

    @Test
    fun `imported history below the watermark is exported after a rewind, without resending what was sent`() = runTest {
        val source = FakeSource(device).apply { add(hr("live", 1_000, now - 1000)) }
        val state = FakeState()
        exporter(source, state, FakeTransport()).runPass(config)

        // Import from the upstream install: old rows keep their old createdAt, below the watermark,
        // and are far older than the ledger floor.
        source.add(hr("imported", 5, now - 90L * 24 * 60 * 60 * 1000))
        val ignored = FakeTransport()
        exporter(source, state, ignored).runPass(config)
        assertTrue(ignored.history().isEmpty())

        state.rewind()
        val t = FakeTransport()
        exporter(source, state, t).runPass(config)
        assertEquals(1, t.history().size)
        assertTrue(t.history().single().payload.contains("\"ts\":${now - 90L * 24 * 60 * 60 * 1000}"))
        // The retained state still holds the newest reading, not the imported one.
        assertTrue(t.states(ExportType.HEART_RATE).isEmpty())
    }

    @Test
    fun `no paired ring means no pass`() = runTest {
        val t = FakeTransport()
        assertEquals(MqttExporter.PassResult.NoDevice, exporter(FakeSource(null), FakeState(), t).runPass(config))
        assertTrue(t.published.isEmpty())
    }

    @Test
    fun `failures are told apart - auth, refused, timeout, dns, tls`() {
        assertEquals(FailureKind.AUTH, HiveMqTransport.returnCodeKind(Mqtt3ConnAckReturnCode.BAD_USER_NAME_OR_PASSWORD))
        assertEquals(FailureKind.NOT_AUTHORIZED, HiveMqTransport.returnCodeKind(Mqtt3ConnAckReturnCode.NOT_AUTHORIZED))
        assertEquals(FailureKind.REFUSED, HiveMqTransport.socketKind(CompletionException(ConnectException("Connection refused"))))
        assertEquals(FailureKind.TIMEOUT, HiveMqTransport.socketKind(RuntimeException(SocketTimeoutException("connect timed out"))))
        assertEquals(FailureKind.TIMEOUT, HiveMqTransport.socketKind(ConnectException("connection timed out: /10.0.0.2:1883")))
        assertEquals(FailureKind.UNKNOWN_HOST, HiveMqTransport.socketKind(UnknownHostException("nope.local")))
        assertEquals(FailureKind.TLS, HiveMqTransport.socketKind(SSLHandshakeException("bad cert")))
    }
}
