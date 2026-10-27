package com.pulseloop.export.mqtt

import com.pulseloop.ring.MeasurementKind

/**
 * One exported series. [topic] is the path segment under `<base>/<device>/`; [measurementKind]
 * is set for the types read from the `measurements` table.
 *
 * [STEPS] is the intraday 15-minute buckets (history only, no HA entity); [STEPS_DAILY] is the
 * day total that backs the "Steps today" entity. They are separate types so each has its own
 * watermark and its own retained state.
 */
enum class ExportType(
    val topic: String,
    val measurementKind: MeasurementKind? = null,
    val hasEntity: Boolean = true,
) {
    HEART_RATE("heart_rate", MeasurementKind.HEART_RATE),
    HRV("hrv", MeasurementKind.HRV),
    SPO2("spo2", MeasurementKind.SPO2),
    STRESS("stress", MeasurementKind.STRESS),
    SKIN_TEMPERATURE("skin_temperature", MeasurementKind.TEMPERATURE),
    STEPS("steps", hasEntity = false),
    STEPS_DAILY("steps_daily"),
    BATTERY("battery"),
    SLEEP("sleep");

    companion object {
        val measurementTypes: List<ExportType> = entries.filter { it.measurementKind != null }
    }
}

/**
 * One reading ready to publish.
 *
 * - [key] identifies the reading across passes (the ledger key): a measurement's row id, a
 *   bucket's start, a day, a sleep session id.
 * - [cursor] is the watermark axis — `createdAt` / `updatedAt` / insert time — never the sample
 *   time, so late-arriving ring history is still picked up.
 * - [sampleTs] is when the reading was taken; it decides which reading is "latest" for the
 *   retained state topic.
 */
data class ExportRecord(
    val type: ExportType,
    val key: String,
    val sampleTs: Long,
    val cursor: Long,
    val payload: String,
)
