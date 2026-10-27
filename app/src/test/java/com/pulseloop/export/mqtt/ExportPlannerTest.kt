package com.pulseloop.export.mqtt

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ExportPlannerTest {

    private fun rec(key: String, cursor: Long, sampleTs: Long = cursor, payload: String = "p-$key") =
        ExportRecord(ExportType.HEART_RATE, key, sampleTs, cursor, payload)

    @Test
    fun `new readings are all sent and the watermark lands on the last cursor`() {
        val batches = ExportPlanner.plan(listOf(rec("a", 1), rec("b", 2), rec("c", 3)), emptyMap(), null, 50)
        assertEquals(1, batches.size)
        assertEquals(listOf("a", "b", "c"), batches[0].toSend.map { it.key })
        assertEquals(3L, batches[0].watermark)
    }

    @Test
    fun `a re-synced reading with an unchanged payload is skipped but still moves the watermark`() {
        // The ring re-sent a day it still holds: @Upsert re-stamped createdAt, the value is the same.
        val r = rec("history:HEART_RATE:100", cursor = 900)
        val known = mapOf(r.key to ExportPlanner.hash(r.payload))
        val batches = ExportPlanner.plan(listOf(r), known, null, 50)
        assertTrue(batches.single().toSend.isEmpty())
        assertEquals(900L, batches.single().watermark)
    }

    @Test
    fun `a reading whose payload changed is sent again under the same key`() {
        val r = rec("sleep-1", cursor = 5, payload = """{"value":420}""")
        val known = mapOf("sleep-1" to ExportPlanner.hash("""{"value":400}"""))
        assertEquals(listOf("sleep-1"), ExportPlanner.plan(listOf(r), known, null, 50).single().toSend.map { it.key })
    }

    @Test
    fun `batches never split rows that share a cursor`() {
        // Watermark is exclusive (> wm): closing a batch between two rows at cursor 2 would commit
        // wm = 2 and the next pass would never select the second one.
        val rows = listOf(rec("a", 1), rec("b", 2), rec("c", 2), rec("d", 3))
        val batches = ExportPlanner.plan(rows, emptyMap(), null, batchSize = 2)
        assertEquals(listOf(listOf("a", "b", "c"), listOf("d")), batches.map { b -> b.toSend.map { it.key } })
        assertEquals(listOf(2L, 3L), batches.map { it.watermark })
    }

    @Test
    fun `batch size bounds the messages per batch`() {
        val rows = (1..7L).map { rec("k$it", it) }
        val batches = ExportPlanner.plan(rows, emptyMap(), null, batchSize = 3)
        assertEquals(listOf(3, 3, 1), batches.map { it.toSend.size })
        assertEquals(listOf(3L, 6L, 7L), batches.map { it.watermark })
    }

    @Test
    fun `initial backfill sends everything, later passes skip unknown readings below the floor`() {
        val old = rec("old", cursor = 10, sampleTs = 1_000)
        val recent = rec("recent", cursor = 11, sampleTs = 9_000)
        assertEquals(2, ExportPlanner.plan(listOf(old, recent), emptyMap(), floor = null, 50).single().toSend.size)
        assertEquals(listOf("recent"), ExportPlanner.plan(listOf(old, recent), emptyMap(), floor = 5_000, 50).single().toSend.map { it.key })
    }

    @Test
    fun `a known reading below the floor is still compared, not dropped`() {
        val r = rec("old-night", cursor = 10, sampleTs = 1_000, payload = "new")
        val known = mapOf("old-night" to ExportPlanner.hash("old"))
        assertEquals(1, ExportPlanner.plan(listOf(r), known, floor = 5_000, 50).single().toSend.size)
    }

    @Test
    fun `a key appearing twice is sent once, as its newest write`() {
        val batches = ExportPlanner.plan(listOf(rec("a", 1, payload = "v1"), rec("a", 4, payload = "v2")), emptyMap(), null, 50)
        assertEquals(listOf("v2"), batches.single().toSend.map { it.payload })
        assertEquals(4L, batches.single().watermark)
    }

    @Test
    fun `empty input plans nothing`() {
        assertTrue(ExportPlanner.plan(emptyList(), emptyMap(), null, 50).isEmpty())
    }

    @Test
    fun `retained state takes the newest sample, never an older backfilled one`() {
        val sent = listOf(rec("a", cursor = 1, sampleTs = 100), rec("b", cursor = 2, sampleTs = 300), rec("c", cursor = 3, sampleTs = 200))
        assertEquals("b", ExportPlanner.latestForState(sent, lastStateTs = null)?.key)
        assertEquals("b", ExportPlanner.latestForState(sent, lastStateTs = 300)?.key) // same ts, changed value
        assertNull(ExportPlanner.latestForState(sent, lastStateTs = 500))
        assertNull(ExportPlanner.latestForState(emptyList(), lastStateTs = null))
    }
}
