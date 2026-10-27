package com.pulseloop.export.mqtt

import com.hivemq.client.mqtt.MqttClient
import com.hivemq.client.mqtt.datatypes.MqttQos
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient
import com.hivemq.client.mqtt.mqtt3.exceptions.Mqtt3ConnAckException
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAckReturnCode
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withTimeout
import java.net.ConnectException
import java.net.NoRouteToHostException
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.util.concurrent.CompletionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import javax.net.ssl.SSLException

/** Why a connection or publish failed, in the terms the settings screen reports. */
enum class FailureKind(val label: String) {
    AUTH("Authentication failed — check the username and password"),
    NOT_AUTHORIZED("Not authorized — the broker rejected this user or client ID"),
    REFUSED("Connection refused — is the broker running on that host and port?"),
    TIMEOUT("Timed out — the broker is unreachable from this network"),
    UNKNOWN_HOST("Host not found"),
    TLS("TLS handshake failed — check TLS setting and port"),
    PROTOCOL("Broker rejected the connection"),
    OTHER("MQTT error"),
}

class MqttExportException(val kind: FailureKind, detail: String?, cause: Throwable? = null) :
    Exception(listOfNotNull(kind.label, detail?.takeIf { it.isNotBlank() }).joinToString(": "), cause)

/**
 * The network edge, behind an interface so the exporter's commit rules are testable with a fake.
 * [publish] returns only once the broker has acknowledged every message (QoS 1 PUBACK) — the
 * exporter commits a batch on that and nothing earlier.
 */
interface MqttTransport {
    suspend fun connect(config: MqttConfig)
    suspend fun publish(messages: List<OutgoingMessage>)
    suspend fun disconnect()
}

class HiveMqTransport(private val timeoutSeconds: Long = 10) : MqttTransport {
    private var client: Mqtt3AsyncClient? = null

    override suspend fun connect(config: MqttConfig) {
        val c = MqttClient.builder()
            .useMqttVersion3()
            .identifier(config.clientId)
            .serverHost(config.host)
            .serverPort(config.port)
            .transportConfig()
            .socketConnectTimeout(timeoutSeconds, TimeUnit.SECONDS)
            .mqttConnectTimeout(timeoutSeconds, TimeUnit.SECONDS)
            .applyTransportConfig()
            .let { if (config.tls) it.sslWithDefaultConfig() else it }
            .buildAsync()
        client = c
        val connect = c.connectWith().cleanSession(true).keepAlive(30)
        val send = if (config.username != null) {
            connect.simpleAuth()
                .username(config.username)
                .password((config.password ?: "").toByteArray(Charsets.UTF_8))
                .applySimpleAuth()
                .send()
        } else {
            connect.send()
        }
        guard { withTimeout(TimeUnit.SECONDS.toMillis(timeoutSeconds * 2)) { send.await() } }
    }

    override suspend fun publish(messages: List<OutgoingMessage>) {
        val c = client ?: throw MqttExportException(FailureKind.OTHER, "not connected")
        // Pipelined: send the whole batch, then wait for every PUBACK.
        val pending = messages.map { m ->
            c.publishWith().topic(m.topic).qos(MqttQos.AT_LEAST_ONCE).retain(m.retained)
                .payload(m.payload.toByteArray(Charsets.UTF_8)).send()
        }
        guard { withTimeout(TimeUnit.SECONDS.toMillis(timeoutSeconds * 3)) { pending.forEach { it.await() } } }
    }

    override suspend fun disconnect() {
        val c = client ?: return
        client = null
        runCatching { withTimeout(3_000) { c.disconnect().await() } }
    }

    private suspend fun <T> guard(block: suspend () -> T): T =
        try {
            block()
        } catch (e: MqttExportException) {
            throw e
        } catch (e: kotlinx.coroutines.CancellationException) {
            if (e is kotlinx.coroutines.TimeoutCancellationException) throw MqttExportException(FailureKind.TIMEOUT, null, e)
            throw e
        } catch (e: Exception) {
            throw classify(e)
        }

    companion object {
        /**
         * Maps whatever the client threw to a [FailureKind]. Walks the cause chain: HiveMQ wraps
         * socket errors in `ConnectionFailedException`, and futures wrap everything in
         * `CompletionException`.
         */
        fun classify(error: Throwable): MqttExportException {
            for (e in causes(error)) {
                if (e is Mqtt3ConnAckException) {
                    val kind = returnCodeKind(e.mqttMessage.returnCode)
                    return MqttExportException(kind, "broker said ${e.mqttMessage.returnCode}", error)
                }
            }
            return MqttExportException(socketKind(error), rootMessage(error), error)
        }

        fun returnCodeKind(code: Mqtt3ConnAckReturnCode): FailureKind = when (code) {
            Mqtt3ConnAckReturnCode.BAD_USER_NAME_OR_PASSWORD -> FailureKind.AUTH
            Mqtt3ConnAckReturnCode.NOT_AUTHORIZED -> FailureKind.NOT_AUTHORIZED
            Mqtt3ConnAckReturnCode.SERVER_UNAVAILABLE -> FailureKind.REFUSED
            else -> FailureKind.PROTOCOL
        }

        /** Socket-level classification, independent of HiveMQ's own exception types. */
        fun socketKind(error: Throwable): FailureKind {
            for (e in causes(error)) {
                // Netty's ConnectTimeoutException extends ConnectException — check it first.
                if (e is SocketTimeoutException || e is TimeoutException ||
                    e.javaClass.simpleName == "ConnectTimeoutException"
                ) return FailureKind.TIMEOUT
                if (e is NoRouteToHostException) return FailureKind.TIMEOUT
                if (e is UnknownHostException) return FailureKind.UNKNOWN_HOST
                if (e is SSLException) return FailureKind.TLS
                if (e is ConnectException) {
                    return if (e.message?.contains("timed out", ignoreCase = true) == true) FailureKind.TIMEOUT
                    else FailureKind.REFUSED
                }
            }
            return if (rootMessage(error)?.contains("timeout", ignoreCase = true) == true) FailureKind.TIMEOUT
            else FailureKind.OTHER
        }

        private fun causes(error: Throwable): Sequence<Throwable> =
            generateSequence(error) { it.cause?.takeIf { c -> c !== it } }.take(10)

        private fun rootMessage(error: Throwable): String? =
            causes(error).filterNot { it is CompletionException }.lastOrNull()?.message
    }
}
