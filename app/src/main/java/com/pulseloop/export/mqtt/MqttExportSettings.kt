package com.pulseloop.export.mqtt

import android.content.Context
import android.content.SharedPreferences
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKeys

/**
 * Broker connection settings for the MQTT export.
 *
 * Kept apart from [com.pulseloop.settings.ApiKeyStore] on purpose: this is a fork-only feature and
 * a separate file keeps upstream merges conflict-free.
 *
 * Non-secret fields live in plain SharedPreferences so [MqttExportWorker.enqueue] can check
 * [enabled] on the main thread (app foreground edge) without paying for a keystore unlock. The
 * username and password live in EncryptedSharedPreferences and are never logged — [MqttConfig]
 * redacts the password from its `toString`.
 */
class MqttExportSettings(context: Context) {
    private val app = context.applicationContext
    private val plain: SharedPreferences = app.getSharedPreferences(PLAIN_FILE, Context.MODE_PRIVATE)

    // Lazy: only the worker and the settings screen need the secrets.
    private val secure: SharedPreferences by lazy {
        EncryptedSharedPreferences.create(
            SECURE_FILE,
            MasterKeys.getOrCreate(MasterKeys.AES256_GCM_SPEC),
            app,
            EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
            EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM,
        )
    }

    var enabled: Boolean
        get() = plain.getBoolean(KEY_ENABLED, false)
        set(value) { plain.edit().putBoolean(KEY_ENABLED, value).apply() }

    var host: String
        get() = plain.getString(KEY_HOST, "") ?: ""
        set(value) { plain.edit().putString(KEY_HOST, value.trim()).apply() }

    var port: Int
        get() = plain.getInt(KEY_PORT, DEFAULT_PORT)
        set(value) { plain.edit().putInt(KEY_PORT, value).apply() }

    var tls: Boolean
        get() = plain.getBoolean(KEY_TLS, false)
        set(value) { plain.edit().putBoolean(KEY_TLS, value).apply() }

    /** Blank means "derive one": a stable per-install id, generated on first read. */
    var clientId: String
        get() = plain.getString(KEY_CLIENT_ID, null)?.takeIf { it.isNotBlank() } ?: generatedClientId()
        set(value) { plain.edit().putString(KEY_CLIENT_ID, value.trim()).apply() }

    var baseTopic: String
        get() = plain.getString(KEY_BASE_TOPIC, null)?.takeIf { it.isNotBlank() } ?: DEFAULT_BASE_TOPIC
        set(value) { plain.edit().putString(KEY_BASE_TOPIC, value.trim().trim('/')).apply() }

    var username: String
        get() = secure.getString(KEY_USERNAME, "") ?: ""
        set(value) { secure.edit().putString(KEY_USERNAME, value).apply() }

    var password: String
        get() = secure.getString(KEY_PASSWORD, "") ?: ""
        set(value) { secure.edit().putString(KEY_PASSWORD, value).apply() }

    val isConfigured: Boolean get() = host.isNotBlank() && port in 1..65535

    fun config(): MqttConfig = MqttConfig(
        host = host,
        port = port,
        tls = tls,
        clientId = clientId,
        username = username.takeIf { it.isNotEmpty() },
        password = password.takeIf { it.isNotEmpty() },
        baseTopic = baseTopic,
    )

    private fun generatedClientId(): String {
        val id = "pulseloop-" + java.util.UUID.randomUUID().toString().replace("-", "").take(8)
        plain.edit().putString(KEY_CLIENT_ID, id).apply()
        return id
    }

    companion object {
        const val DEFAULT_PORT = 1883
        const val DEFAULT_BASE_TOPIC = "pulseloop"
        const val DISCOVERY_PREFIX = "homeassistant"

        private const val PLAIN_FILE = "mqtt_export"
        private const val SECURE_FILE = "mqtt_export_secure"
        private const val KEY_ENABLED = "enabled"
        private const val KEY_HOST = "host"
        private const val KEY_PORT = "port"
        private const val KEY_TLS = "tls"
        private const val KEY_CLIENT_ID = "client_id"
        private const val KEY_BASE_TOPIC = "base_topic"
        private const val KEY_USERNAME = "username"
        private const val KEY_PASSWORD = "password"

        fun isEnabled(context: Context): Boolean =
            context.applicationContext.getSharedPreferences(PLAIN_FILE, Context.MODE_PRIVATE)
                .getBoolean(KEY_ENABLED, false)
    }
}

/** A snapshot of the connection settings. The password never appears in [toString]. */
data class MqttConfig(
    val host: String,
    val port: Int,
    val tls: Boolean,
    val clientId: String,
    val username: String?,
    val password: String?,
    val baseTopic: String,
) {
    override fun toString(): String =
        "MqttConfig(host=$host, port=$port, tls=$tls, clientId=$clientId, " +
            "username=${if (username == null) "none" else "set"}, password=${if (password == null) "none" else "***"}, " +
            "baseTopic=$baseTopic)"
}
