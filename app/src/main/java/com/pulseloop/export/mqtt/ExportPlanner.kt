package com.pulseloop.export.mqtt

import java.security.MessageDigest

/**
 * Decides what a pass sends. Pure: rows in, batches out — the exporter owns the network and the
 * store, so the "never twice, never skipped" rules are tested here without either.
 *
 * Why two mechanisms instead of one watermark:
 *  - The **watermark** is on the row's write time (`createdAt` / `updatedAt`), not the sample
 *    time, so history the ring delivers late is still picked up — the reason Health Connect's
 *    exporter does the same (`MeasurementDao.createdSince`).
 *  - But `@Upsert` re-stamps `createdAt` whenever the ring re-sends a day it still holds, so a
 *    watermark alone would republish that whole day on every sync. The **ledger** remembers the
 *    payload hash last sent for each reading's key; an unchanged reading is skipped, a changed
 *    one (a re-synced night, a corrected bucket) goes out again under the same key.
 *
 * Row ids are not usable as a watermark at all: they are UUIDs or `history:<kind>:<ts>` strings,
 * not a sequence.
 */
object ExportPlanner {

    /**
     * One unit of commit. Everything in [candidates] is settled once [toSend] is acknowledged:
     * [watermark] advances past all of them, skipped ones included, so an all-skipped run of rows
     * doesn't hold the watermark down.
     */
    data class Batch(
        val candidates: List<ExportRecord>,
        val toSend: List<ExportRecord>,
        val watermark: Long,
    )

    /**
     * @param known ledger hashes for the candidates' keys (missing = never sent).
     * @param floor after the first full backfill, a reading never sent whose sample time is older
     *   than this is treated as already handled — its ledger entry has been pruned, and the only
     *   way such a row reappears past the watermark is a re-upsert of something long exported.
     *   `null` during the initial backfill, so every stored reading goes out once.
     * @param batchSize max messages per batch. A batch never splits rows sharing one cursor
     *   value: the watermark is exclusive (`> wm`), so a split would strand the tail of the tie.
     */
    fun plan(
        candidates: List<ExportRecord>,
        known: Map<String, String>,
        floor: Long?,
        batchSize: Int,
    ): List<Batch> {
        require(batchSize > 0)
        // One record per key — the newest write wins (a key can't appear twice from one query
        // today, but a source that unions two queries mustn't double-send).
        val ordered = candidates
            .groupBy { it.key }
            .map { (_, sameKey) -> sameKey.maxWith(compareBy({ it.cursor }, { it.sampleTs })) }
            .sortedWith(compareBy({ it.cursor }, { it.sampleTs }, { it.key }))

        val batches = mutableListOf<Batch>()
        var current = mutableListOf<ExportRecord>()
        var sending = mutableListOf<ExportRecord>()
        // Bounds a batch of skipped rows too, so a long all-skipped run commits in pieces.
        val maxCandidates = batchSize * 10

        fun close() {
            if (current.isEmpty()) return
            batches += Batch(current, sending, current.last().cursor)
            current = mutableListOf()
            sending = mutableListOf()
        }

        for (record in ordered) {
            val full = sending.size >= batchSize || current.size >= maxCandidates
            if (full && record.cursor != current.last().cursor) close()
            current += record
            if (shouldSend(record, known, floor)) sending += record
        }
        close()
        return batches
    }

    fun shouldSend(record: ExportRecord, known: Map<String, String>, floor: Long?): Boolean {
        val previous = known[record.key]
        if (previous != null) return previous != hash(record.payload)
        return floor == null || record.sampleTs >= floor
    }

    /**
     * The record that should replace the retained state, if any: the newest by sample time among
     * [sent], and only if it is not older than what the state topic already holds — a backfill
     * batch of old readings must not overwrite today's value. Equal is allowed so a reading whose
     * value changed under the same timestamp (today's step total) updates the state.
     */
    fun latestForState(sent: List<ExportRecord>, lastStateTs: Long?): ExportRecord? {
        val newest = sent.maxWithOrNull(compareBy({ it.sampleTs }, { it.cursor })) ?: return null
        return if (lastStateTs == null || newest.sampleTs >= lastStateTs) newest else null
    }

    fun hash(payload: String): String =
        MessageDigest.getInstance("SHA-256").digest(payload.toByteArray(Charsets.UTF_8))
            .take(12).joinToString("") { "%02x".format(it) }
}
