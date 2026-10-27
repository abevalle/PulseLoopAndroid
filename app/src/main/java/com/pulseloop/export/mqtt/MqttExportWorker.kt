package com.pulseloop.export.mqtt

import android.content.Context
import android.util.Log
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import com.pulseloop.BuildConfig
import com.pulseloop.data.PulseLoopDatabase
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.TimeUnit

/**
 * Runs an MQTT export pass. A plain one-time worker — no service, no wake lock — enqueued after
 * each ring sync (foreground and background) and on app foreground.
 *
 * - **Debounced**: 10 s initial delay + [ExistingWorkPolicy.REPLACE], so a burst of triggers is
 *   one pass. Replacing a running pass is safe: each batch commits as a non-cancellable unit, and
 *   the next pass resumes from the watermarks.
 * - **Network**: requires a connection, so a trigger fired offline waits for one.
 * - **Broker unreachable** (off home Wi-Fi, broker down): the pass commits nothing and returns
 *   retry, with exponential backoff from 5 minutes. Any new trigger replaces the backoff and tries
 *   again soon, so rejoining home Wi-Fi and opening the app (or the next 30-minute sync) catches up.
 */
class MqttExportWorker(context: Context, params: WorkerParameters) : CoroutineWorker(context, params) {

    override suspend fun doWork(): Result {
        val settings = MqttExportSettings(applicationContext)
        if (!settings.enabled || !settings.isConfigured) return Result.success()
        val exporter = exporter(applicationContext)
        val result = passMutex.withLock { exporter.runPass(settings.config()) }
        return when (result) {
            is MqttExporter.PassResult.Failed ->
                if (runAttemptCount < MAX_ATTEMPTS) Result.retry() else Result.success()
            is MqttExporter.PassResult.Published -> {
                Log.i(TAG, "published ${result.messages} messages")
                Result.success()
            }
            else -> Result.success()
        }
    }

    companion object {
        private const val TAG = "MqttExportWorker"
        private const val WORK_NAME = "mqtt_export"
        private const val MAX_ATTEMPTS = 6

        /** Serialises passes in this process (a worker and a settings-screen "Export now"). */
        val passMutex = Mutex()

        fun exporter(context: Context): MqttExporter = MqttExporter(
            source = RoomExportSource(PulseLoopDatabase.getInstance(context)),
            state = RoomExportStateStore(MqttExportDatabase.get(context)),
            transportFactory = { HiveMqTransport() },
            appVersion = BuildConfig.VERSION_NAME,
        )

        /** Cheap to call from anywhere: a no-op unless the export is switched on. */
        fun enqueue(context: Context, delaySeconds: Long = 10) {
            if (!MqttExportSettings.isEnabled(context)) return
            val request = OneTimeWorkRequestBuilder<MqttExportWorker>()
                .setInitialDelay(delaySeconds, TimeUnit.SECONDS)
                .setConstraints(Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build())
                .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 5, TimeUnit.MINUTES)
                .build()
            WorkManager.getInstance(context).enqueueUniqueWork(WORK_NAME, ExistingWorkPolicy.REPLACE, request)
        }

        fun cancel(context: Context) {
            WorkManager.getInstance(context).cancelUniqueWork(WORK_NAME)
        }
    }
}
