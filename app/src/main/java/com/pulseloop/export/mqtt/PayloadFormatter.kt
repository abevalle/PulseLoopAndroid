package com.pulseloop.export.mqtt

import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

/**
 * JSON payloads for state and history messages: `{"ts": <ISO-8601 with offset>, "value": …,
 * "unit": …}` plus per-type extras. Pure, so the exact wire format is pinned by tests.
 *
 * `ts` is always the reading's own time, never the publish time. Payloads carry nothing that
 * changes when a row is merely re-written (no `updatedAt`, no `createdAt`): the ledger compares
 * payload hashes to decide whether a re-synced reading actually changed.
 */
class PayloadFormatter(private val zone: ZoneId = ZoneId.systemDefault()) {

    fun iso(epochMillis: Long): String = ISO.format(Instant.ofEpochMilli(epochMillis).atZone(zone))

    fun measurement(ts: Long, value: Double, unit: String, source: String): String = json {
        put("ts", iso(ts))
        putNumber("value", value)
        put("unit", unit)
        put("source", source)
    }

    fun battery(ts: Long, percent: Int): String = json {
        put("ts", iso(ts))
        put("value", percent)
        put("unit", "%")
    }

    fun stepsBucket(start: Long, steps: Int, distanceMeters: Double): String = json {
        put("ts", iso(start))
        put("value", steps)
        put("unit", "steps")
        put("duration_min", BUCKET_MINUTES)
        putNumber("distance_m", distanceMeters)
    }

    fun stepsDaily(dayStart: Long, steps: Int, distanceMeters: Double): String = json {
        put("ts", iso(dayStart))
        put("date", DATE.format(Instant.ofEpochMilli(dayStart).atZone(zone)))
        put("value", steps)
        put("unit", "steps")
        putNumber("distance_m", distanceMeters)
    }

    /**
     * A sleep session. `ts` is the session's end — when the night became a reading — and `value`
     * is time asleep (every stage but AWAKE, the app's own definition), not the start-to-end span.
     */
    fun sleep(
        sessionId: String,
        start: Long,
        end: Long,
        asleepMinutes: Int,
        stageMinutes: Map<String, Int>,
        score: Int?,
    ): String = json {
        put("ts", iso(end))
        put("value", asleepMinutes)
        put("unit", "min")
        put("session_id", sessionId)
        put("start", iso(start))
        put("end", iso(end))
        put("span_min", ((end - start) / 60_000L).toInt())
        put("stages", buildJsonObject { stageMinutes.toSortedMap().forEach { (k, v) -> put(k, v) } })
        if (score != null) put("score", score)
    }

    private fun json(block: JsonObjectBuilder.() -> Unit): String = buildJsonObject(block).toString()

    companion object {
        const val BUCKET_MINUTES = 15
        private val ISO: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX")
        private val DATE: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE

        /** Whole numbers go out as integers (`72`, not `72.0`); everything else as-is. */
        internal fun number(value: Double): JsonPrimitive =
            if (value % 1.0 == 0.0 && kotlin.math.abs(value) < 1e15) JsonPrimitive(value.toLong())
            else JsonPrimitive(value)

        private fun JsonObjectBuilder.putNumber(key: String, value: Double) {
            put(key, number(value))
        }
    }
}
