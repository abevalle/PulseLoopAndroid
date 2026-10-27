package com.pulseloop.export.mqtt

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class HaDiscoveryTest {

    private val device = ExportDevice(id = "3F2A-9b", name = "COLMI Ring 2", model = "Colmi Smart Health", firmware = "2.04")

    private fun config(type: ExportType): Pair<OutgoingMessage, JsonObject> {
        val msg = HaDiscovery.config(type, device, "pulseloop", "homeassistant", "2.7.0")!!
        return msg to Json.parseToJsonElement(msg.payload).jsonObject
    }
    private fun JsonObject.str(k: String) = this[k]?.jsonPrimitive?.content

    @Test
    fun `topics are slugged from the ring's advertised name`() {
        assertEquals("colmi_ring_2", Topics.slug("COLMI Ring 2"))
        assertEquals("ring", Topics.slug("///"))
        assertEquals("pulseloop/colmi_ring_2/heart_rate", Topics.state("pulseloop", device, ExportType.HEART_RATE))
        assertEquals("pulseloop/colmi_ring_2/heart_rate/history", Topics.history("pulseloop", device, ExportType.HEART_RATE))
    }

    @Test
    fun `heart rate config is retained, under the discovery prefix, reading value out of the json`() {
        val (msg, json) = config(ExportType.HEART_RATE)
        assertTrue(msg.retained)
        assertEquals("homeassistant/sensor/pulseloop_3f2a_9b/heart_rate/config", msg.topic)
        assertEquals("pulseloop/colmi_ring_2/heart_rate", json.str("state_topic"))
        assertEquals(json.str("state_topic"), json.str("json_attributes_topic"))
        assertEquals("{{ value_json.value }}", json.str("value_template"))
        assertEquals("bpm", json.str("unit_of_measurement"))
        assertEquals("measurement", json.str("state_class"))
        assertNull(json.str("device_class")) // HA has no heart-rate device class
        assertEquals("pulseloop_3f2a_9b_heart_rate", json.str("unique_id"))
    }

    @Test
    fun `every sensor belongs to one PulseLoop device`() {
        val devices = ExportType.entries.mapNotNull { HaDiscovery.config(it, device, "pulseloop", "homeassistant", "2.7.0") }
            .map { Json.parseToJsonElement(it.payload).jsonObject["device"]!!.jsonObject }
        assertEquals(1, devices.distinct().size)
        val d = devices.first()
        assertEquals("PulseLoop COLMI Ring 2", d.str("name"))
        assertEquals(listOf("pulseloop_3f2a_9b"), d["identifiers"]!!.jsonArray.map { it.jsonPrimitive.content })
        assertEquals("2.04", d.str("sw_version"))
    }

    @Test
    fun `device classes and units match what HA expects`() {
        assertEquals("temperature" to "°C", config(ExportType.SKIN_TEMPERATURE).second.let { it.str("device_class") to it.str("unit_of_measurement") })
        assertEquals("battery" to "%", config(ExportType.BATTERY).second.let { it.str("device_class") to it.str("unit_of_measurement") })
        assertEquals("duration" to "ms", config(ExportType.HRV).second.let { it.str("device_class") to it.str("unit_of_measurement") })
        assertEquals("duration" to "min", config(ExportType.SLEEP).second.let { it.str("device_class") to it.str("unit_of_measurement") })
        assertNull(config(ExportType.STRESS).second.str("unit_of_measurement"))
    }

    @Test
    fun `steps today is a resetting total, everything else a measurement`() {
        assertEquals("total_increasing", config(ExportType.STEPS_DAILY).second.str("state_class"))
        ExportType.entries.filter { it.hasEntity && it != ExportType.STEPS_DAILY }.forEach {
            assertEquals(it.name, "measurement", config(it).second.str("state_class"))
        }
    }

    @Test
    fun `intraday buckets get no entity`() {
        assertNull(HaDiscovery.config(ExportType.STEPS, device, "pulseloop", "homeassistant", "2.7.0"))
        assertFalse(ExportType.STEPS.hasEntity)
    }
}
