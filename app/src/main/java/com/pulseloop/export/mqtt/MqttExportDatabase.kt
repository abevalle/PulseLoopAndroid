package com.pulseloop.export.mqtt

import android.content.Context
import androidx.room.Dao
import androidx.room.Database
import androidx.room.Entity
import androidx.room.PrimaryKey
import androidx.room.Query
import androidx.room.Room
import androidx.room.RoomDatabase
import androidx.room.Transaction
import androidx.room.Upsert
import androidx.room.withTransaction
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

@Entity(tableName = "mqtt_ledger", primaryKeys = ["type", "key"])
data class MqttLedgerRow(
    val type: String,
    val key: String,
    val hash: String,
    val sampleTs: Long,
)

@Entity(tableName = "mqtt_cursor")
data class MqttCursorRow(
    @PrimaryKey val type: String,
    val watermark: Long? = null,
    val lastStateTs: Long? = null,
)

@Entity(tableName = "mqtt_status")
data class MqttStatusRow(
    @PrimaryKey val id: Int = 0,
    val lastPublishAt: Long? = null,
    val lastAttemptAt: Long? = null,
    val lastError: String? = null,
    val pending: Int = 0,
    val backfillCompletedAt: Long? = null,
)

@Dao
interface MqttExportDao {
    @Query("SELECT * FROM mqtt_cursor WHERE type = :type LIMIT 1")
    suspend fun cursor(type: String): MqttCursorRow?

    @Upsert
    suspend fun upsertCursor(row: MqttCursorRow)

    @Query("SELECT * FROM mqtt_ledger WHERE type = :type AND `key` IN (:keys)")
    suspend fun ledger(type: String, keys: List<String>): List<MqttLedgerRow>

    @Upsert
    suspend fun upsertLedger(rows: List<MqttLedgerRow>)

    @Query("DELETE FROM mqtt_ledger WHERE sampleTs < :cutoff AND type != :keepType")
    suspend fun prune(cutoff: Long, keepType: String)

    @Query("SELECT * FROM mqtt_status WHERE id = 0 LIMIT 1")
    suspend fun status(): MqttStatusRow?

    @Query("SELECT * FROM mqtt_status WHERE id = 0 LIMIT 1")
    fun statusFlow(): Flow<MqttStatusRow?>

    @Upsert
    suspend fun upsertStatus(row: MqttStatusRow)

    @Query("DELETE FROM mqtt_ledger")
    suspend fun clearLedger()

    @Query("DELETE FROM mqtt_cursor")
    suspend fun clearCursors()

    @Query("UPDATE mqtt_cursor SET watermark = NULL")
    suspend fun clearWatermarks()

    @Query("UPDATE mqtt_status SET backfillCompletedAt = NULL")
    suspend fun clearBackfill()

    @Transaction
    suspend fun rewind() {
        clearWatermarks()
        clearBackfill()
    }

    @Transaction
    suspend fun reset() {
        clearLedger()
        clearCursors()
        upsertStatus(MqttStatusRow())
    }
}

/**
 * The MQTT export's own database (`mqtt_export.db`). Separate from `PulseLoopDatabase` so this
 * fork never adds a table or a migration to upstream's schema.
 */
@Database(
    entities = [MqttLedgerRow::class, MqttCursorRow::class, MqttStatusRow::class],
    version = 1,
    exportSchema = false,
)
abstract class MqttExportDatabase : RoomDatabase() {
    abstract fun dao(): MqttExportDao

    companion object {
        @Volatile private var instance: MqttExportDatabase? = null

        fun get(context: Context): MqttExportDatabase =
            instance ?: synchronized(this) {
                instance ?: Room.databaseBuilder(
                    context.applicationContext, MqttExportDatabase::class.java, "mqtt_export.db",
                ).build().also { instance = it }
            }
    }
}

class RoomExportStateStore(private val db: MqttExportDatabase) : ExportStateStore {
    private val dao = db.dao()

    override suspend fun watermark(type: ExportType): Long? = dao.cursor(type.name)?.watermark
    override suspend fun lastStateTs(type: ExportType): Long? = dao.cursor(type.name)?.lastStateTs

    override suspend fun ledger(ledgerType: String, keys: Collection<String>): Map<String, String> =
        // SQLite caps bound variables at 999.
        keys.distinct().chunked(500).flatMap { dao.ledger(ledgerType, it) }.associate { it.key to it.hash }

    override suspend fun commit(type: ExportType, ledger: List<LedgerWrite>, watermark: Long, lastStateTs: Long?) {
        db.withTransaction {
            if (ledger.isNotEmpty()) dao.upsertLedger(ledger.map { MqttLedgerRow(type.name, it.key, it.hash, it.sampleTs) })
            val current = dao.cursor(type.name) ?: MqttCursorRow(type.name)
            dao.upsertCursor(current.copy(
                watermark = maxOf(current.watermark ?: Long.MIN_VALUE, watermark),
                lastStateTs = lastStateTs ?: current.lastStateTs,
            ))
        }
    }

    override suspend fun commitDiscovery(ledger: List<LedgerWrite>) {
        dao.upsertLedger(ledger.map { MqttLedgerRow(ExportStateStore.DISCOVERY, it.key, it.hash, it.sampleTs) })
    }

    override suspend fun status(): ExportStatus = (dao.status() ?: MqttStatusRow()).toStatus()

    override fun statusFlow(): Flow<ExportStatus> = dao.statusFlow().map { (it ?: MqttStatusRow()).toStatus() }

    override suspend fun updateStatus(transform: (ExportStatus) -> ExportStatus) {
        db.withTransaction {
            val next = transform(status())
            dao.upsertStatus(MqttStatusRow(0, next.lastPublishAt, next.lastAttemptAt, next.lastError, next.pending, next.backfillCompletedAt))
        }
    }

    override suspend fun prune(cutoff: Long) = dao.prune(cutoff, ExportStateStore.DISCOVERY)

    override suspend fun reset() = dao.reset()

    override suspend fun rewind() = dao.rewind()

    private fun MqttStatusRow.toStatus() =
        ExportStatus(lastPublishAt, lastAttemptAt, lastError, pending, backfillCompletedAt)
}
