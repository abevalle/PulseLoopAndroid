package com.pulseloop.export.mqtt

import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

/** The paired ring, as the export names it. */
data class ExportDevice(
    /** `devices.id` — stable across renames, so it anchors HA's unique_ids. */
    val id: String,
    /** What the ring advertises (`COLMI Ring 2`), falling back to the stored name. */
    val name: String,
    val model: String?,
    val firmware: String?,
)

/** One outgoing MQTT message. */
data class OutgoingMessage(val topic: String, val payload: String, val retained: Boolean)

/** Topic layout: `<base>/<device>/<type>` (retained state) and `…/<type>/history` (every reading). */
object Topics {
    /** `COLMI Ring 2` → `colmi_ring_2`. Never empty, never contains MQTT wildcards or `/`. */
    fun slug(text: String): String =
        text.lowercase().replace(Regex("[^a-z0-9]+"), "_").trim('_').ifEmpty { "ring" }

    fun state(base: String, device: ExportDevice, type: ExportType): String =
        "$base/${slug(device.name)}/${type.topic}"

    fun history(base: String, device: ExportDevice, type: ExportType): String =
        "${state(base, device, type)}/history"

    fun discovery(prefix: String, device: ExportDevice, type: ExportType): String =
        "$prefix/sensor/${nodeId(device)}/${type.topic}/config"

    fun nodeId(device: ExportDevice): String = "pulseloop_${slug(device.id)}"
}

/**
 * Home Assistant MQTT discovery configs, published retained under `homeassistant/sensor/…`.
 *
 * One HA device ("PulseLoop <ring name>") owns every sensor. Each sensor reads `value` out of the
 * JSON state payload, and the same payload is its attribute set, so the reading's own `ts` (and
 * sleep's stage breakdown) show up as attributes.
 *
 * No availability topic: the phone connects for a few seconds per sync, so a last-will "offline"
 * would mark every sensor unavailable for the other 29 minutes of each half hour.
 */
object HaDiscovery {

    private data class Spec(
        val name: String,
        val unit: String?,
        val deviceClass: String?,
        val stateClass: String,
        val icon: String?,
    )

    private fun spec(type: ExportType): Spec? = when (type) {
        ExportType.HEART_RATE -> Spec("Heart rate", "bpm", null, "measurement", "mdi:heart-pulse")
        ExportType.HRV -> Spec("Heart rate variability", "ms", "duration", "measurement", "mdi:heart-flash")
        ExportType.SPO2 -> Spec("Blood oxygen", "%", null, "measurement", "mdi:water-percent")
        ExportType.STRESS -> Spec("Stress", null, null, "measurement", "mdi:head-alert")
        ExportType.SKIN_TEMPERATURE -> Spec("Skin temperature", "°C", "temperature", "measurement", null)
        // A day total that resets at midnight: total_increasing lets HA's statistics treat the
        // drop as a reset. Under `measurement` the long-term stats would average a step counter.
        ExportType.STEPS_DAILY -> Spec("Steps today", "steps", null, "total_increasing", "mdi:shoe-print")
        ExportType.BATTERY -> Spec("Battery", "%", "battery", "measurement", null)
        ExportType.SLEEP -> Spec("Sleep", "min", "duration", "measurement", "mdi:sleep")
        ExportType.STEPS -> null
    }

    fun config(
        type: ExportType,
        device: ExportDevice,
        baseTopic: String,
        prefix: String,
        appVersion: String,
    ): OutgoingMessage? {
        val spec = spec(type) ?: return null
        val stateTopic = Topics.state(baseTopic, device, type)
        val payload = buildJsonObject {
            put("name", spec.name)
            put("unique_id", "${Topics.nodeId(device)}_${type.topic}")
            put("state_topic", stateTopic)
            put("value_template", "{{ value_json.value }}")
            put("json_attributes_topic", stateTopic)
            spec.unit?.let { put("unit_of_measurement", it) }
            spec.deviceClass?.let { put("device_class", it) }
            put("state_class", spec.stateClass)
            spec.icon?.let { put("icon", it) }
            put("device", buildJsonObject {
                put("identifiers", buildJsonArray { add(kotlinx.serialization.json.JsonPrimitive(Topics.nodeId(device))) })
                put("name", "PulseLoop ${device.name}")
                put("manufacturer", "PulseLoop")
                device.model?.let { put("model", it) }
                device.firmware?.let { put("sw_version", it) }
            })
            put("origin", buildJsonObject {
                put("name", "PulseLoop")
                put("sw", appVersion)
            })
        }
        return OutgoingMessage(Topics.discovery(prefix, device, type), payload.toString(), retained = true)
    }
}
