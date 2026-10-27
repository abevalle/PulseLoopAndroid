package com.pulseloop.export.mqtt

import kotlinx.coroutines.flow.Flow

/** One reading's ledger line: the payload hash last acknowledged by the broker. */
data class LedgerWrite(val key: String, val hash: String, val sampleTs: Long)

/** What the settings screen shows. */
data class ExportStatus(
    val lastPublishAt: Long? = null,
    val lastAttemptAt: Long? = null,
    val lastError: String? = null,
    /** Messages waiting as of the last pass or refresh. Room is the queue; this is a count. */
    val pending: Int = 0,
    val backfillCompletedAt: Long? = null,
)

/**
 * The export's own bookkeeping, kept out of `PulseLoopDatabase` so upstream schema migrations
 * never collide with it. [RoomExportStateStore] in production, an in-memory fake in tests.
 */
interface ExportStateStore {
    suspend fun watermark(type: ExportType): Long?
    suspend fun lastStateTs(type: ExportType): Long?

    /** Ledger hashes for [keys] of [ledgerType] — an [ExportType] name, or [DISCOVERY]. */
    suspend fun ledger(ledgerType: String, keys: Collection<String>): Map<String, String>

    /**
     * Atomically records an acknowledged batch: its ledger lines, the new watermark, and (when the
     * batch replaced the retained state) the state's sample time. Nothing is committed for a
     * batch the broker didn't acknowledge.
     */
    suspend fun commit(
        type: ExportType,
        ledger: List<LedgerWrite>,
        watermark: Long,
        lastStateTs: Long?,
    )

    suspend fun commitDiscovery(ledger: List<LedgerWrite>)

    suspend fun status(): ExportStatus
    fun statusFlow(): Flow<ExportStatus>
    suspend fun updateStatus(transform: (ExportStatus) -> ExportStatus)

    /** Drops ledger lines for readings taken before [cutoff]; discovery lines are kept. */
    suspend fun prune(cutoff: Long)

    /** Forgets everything — the "Re-export all history" action. */
    suspend fun reset()

    companion object {
        const val DISCOVERY = "discovery"
    }
}
