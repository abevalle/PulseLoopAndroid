package com.pulseloop.export.mqtt

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Test
import java.time.ZoneId
import java.time.ZonedDateTime

class PayloadFormatterTest {

    private val zone = ZoneId.of("America/Los_Angeles")
    private val fmt = PayloadFormatter(zone)
    private fun ms(y: Int, mo: Int, d: Int, h: Int, mi: Int) =
        ZonedDateTime.of(y, mo, d, h, mi, 0, 0, zone).toInstant().toEpochMilli()
    private fun parse(s: String): JsonObject = Json.parseToJsonElement(s).jsonObject

    @Test
    fun `timestamps carry the local offset, across a DST change`() {
        assertEquals("2026-10-26T08:05:00-07:00", fmt.iso(ms(2026, 10, 26, 8, 5)))
        assertEquals("2026-11-02T08:05:00-08:00", fmt.iso(ms(2026, 11, 2, 8, 5)))
    }

    @Test
    fun `measurement payload is ts, value, unit, with whole numbers as integers`() {
        val p = fmt.measurement(ms(2026, 10, 26, 8, 5), 72.0, "bpm", "history")
        assertEquals("""{"ts":"2026-10-26T08:05:00-07:00","value":72,"unit":"bpm","source":"history"}""", p)
        assertEquals("36.4", parse(fmt.measurement(0, 36.4, "°C", "history"))["value"]!!.jsonPrimitive.content)
    }

    @Test
    fun `steps bucket and day total`() {
        val bucket = parse(fmt.stepsBucket(ms(2026, 10, 26, 9, 15), 412, 300.0))
        assertEquals("2026-10-26T09:15:00-07:00", bucket["ts"]!!.jsonPrimitive.content)
        assertEquals("412", bucket["value"]!!.jsonPrimitive.content)
        assertEquals("15", bucket["duration_min"]!!.jsonPrimitive.content)

        val day = parse(fmt.stepsDaily(ms(2026, 10, 26, 0, 0), 8123, 5900.5))
        assertEquals("2026-10-26", day["date"]!!.jsonPrimitive.content)
        assertEquals("8123", day["value"]!!.jsonPrimitive.content)
        assertEquals("steps", day["unit"]!!.jsonPrimitive.content)
    }

    @Test
    fun `sleep reports time asleep with start, end and a stable stage breakdown`() {
        val start = ms(2026, 10, 25, 23, 30)
        val end = ms(2026, 10, 26, 7, 0)
        val p = fmt.sleep("s-1", start, end, 410, mapOf("rem" to 80, "deep" to 90, "light" to 240, "awake" to 40), 82)
        val json = parse(p)
        assertEquals("2026-10-26T07:00:00-07:00", json["ts"]!!.jsonPrimitive.content)
        assertEquals("410", json["value"]!!.jsonPrimitive.content)
        assertEquals("450", json["span_min"]!!.jsonPrimitive.content)
        assertEquals("s-1", json["session_id"]!!.jsonPrimitive.content)
        assertEquals(listOf("awake", "deep", "light", "rem"), json["stages"]!!.jsonObject.keys.toList())
        // Map order must not change the payload, or the ledger would see a "changed" night.
        assertEquals(p, fmt.sleep("s-1", start, end, 410, mapOf("awake" to 40, "light" to 240, "deep" to 90, "rem" to 80), 82))
    }

    @Test
    fun `payloads carry no write-time fields`() {
        val p = fmt.measurement(ms(2026, 10, 26, 8, 5), 72.0, "bpm", "history")
        assertFalse(p.contains("createdAt") || p.contains("updated"))
    }

    @Test
    fun `config toString never shows the password`() {
        val c = MqttConfig("10.0.0.2", 1883, false, "id", "user", "hunter2", "pulseloop")
        assertFalse(c.toString().contains("hunter2"))
    }
}
