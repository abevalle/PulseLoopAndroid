package com.pulseloop.export.mqtt

import com.pulseloop.data.PulseLoopDatabase
import com.pulseloop.ring.SleepStage
import com.pulseloop.service.asleepMinutes

/** Where readings come from. [RoomExportSource] in production, a fake in tests. */
interface ExportSource {
    suspend fun device(): ExportDevice?

    /** Every real (non-demo) reading of [type] written after [watermark]; `null` = all of them. */
    suspend fun records(type: ExportType, watermark: Long?): List<ExportRecord>

    /** Whether this ring has ever produced [type] — gates its Home Assistant entity. */
    suspend fun hasData(type: ExportType): Boolean
}

/**
 * Reads the app's own Room tables through the DAO queries Health Connect's exporter already uses,
 * so upstream's `Daos.kt` needs no changes. Demo/mock rows are excluded throughout.
 *
 * Entities are gated on stored rows rather than on the family's capability set: a capability list
 * is a static per-family constant (AGENTS.md, issue #67 — the R100 advertises stress it never
 * reports), so only data the ring actually produced gets a sensor.
 */
class RoomExportSource(
    private val db: PulseLoopDatabase,
    private val formatter: PayloadFormatter = PayloadFormatter(),
) : ExportSource {

    override suspend fun device(): ExportDevice? {
        val d = db.deviceDao().currentReal() ?: return null
        return ExportDevice(
            id = d.id,
            name = d.advertisedName?.takeIf { it.isNotBlank() } ?: d.name,
            model = d.wearableModelID ?: d.deviceTypeRaw,
            firmware = d.firmwareVersion,
        )
    }

    override suspend fun records(type: ExportType, watermark: Long?): List<ExportRecord> {
        val wm = watermark ?: Long.MIN_VALUE
        return when (type) {
            ExportType.STEPS -> stepBuckets(wm)
            ExportType.STEPS_DAILY -> db.activityDailyDao().updatedSince(wm).map {
                ExportRecord(type, it.date.toString(), it.date, it.updatedAt,
                    formatter.stepsDaily(it.date, it.steps, it.distanceMeters))
            }
            ExportType.BATTERY -> battery(wm)
            ExportType.SLEEP -> sleep(wm)
            else -> {
                val kind = type.measurementKind!!
                db.measurementDao().createdSince(kind.name, wm).map {
                    ExportRecord(type, it.id, it.timestamp, it.createdAt,
                        formatter.measurement(it.timestamp, it.value, it.unit.ifEmpty { kind.unit }, it.sourceRaw))
                }
            }
        }
    }

    override suspend fun hasData(type: ExportType): Boolean = when (type) {
        ExportType.STEPS, ExportType.STEPS_DAILY -> db.activityDailyDao().hasReal()
        ExportType.SLEEP -> db.sleepSessionDao().hasReal()
        ExportType.BATTERY -> db.batterySampleDao().samplesBetween(0, Long.MAX_VALUE, limit = 1).isNotEmpty()
        else -> db.measurementDao().hasReal(type.measurementKind!!.name)
    }

    /**
     * Buckets carry their own `updatedAt`, but no DAO selects on it, so the day rows are the index:
     * a bucket write always restates its day (`applyActivityBucketAtomic`), so every touched bucket
     * sits under a day whose `updatedAt` moved. Cursor = the day's `updatedAt`, which keeps the
     * watermark on one axis; the ledger stops untouched buckets of a touched day going out twice.
     */
    private suspend fun stepBuckets(wm: Long): List<ExportRecord> =
        db.activityDailyDao().updatedSince(wm).flatMap { day ->
            db.activityBucketDao().byDay(day.date).map {
                ExportRecord(ExportType.STEPS, it.startEpoch.toString(), it.startEpoch, day.updatedAt,
                    formatter.stepsBucket(it.startEpoch, it.steps, it.distanceMeters))
            }
        }

    /** Battery rows are insert-only and stamped with the insert time, so `timestamp` is the cursor. */
    private suspend fun battery(wm: Long): List<ExportRecord> {
        val out = mutableListOf<ExportRecord>()
        var from = if (wm == Long.MIN_VALUE) 0L else wm + 1
        while (true) {
            val page = db.batterySampleDao().samplesBetween(from, Long.MAX_VALUE, limit = PAGE)
            page.mapTo(out) { ExportRecord(ExportType.BATTERY, it.id, it.timestamp, it.timestamp, formatter.battery(it.timestamp, it.percent)) }
            if (page.size < PAGE) break
            // Rows sharing the page's last timestamp are re-read and collapsed by key in the planner.
            from = page.last().timestamp
        }
        return out
    }

    private suspend fun sleep(wm: Long): List<ExportRecord> =
        db.sleepSessionDao().updatedSince(wm).map { s ->
            val blocks = db.sleepStageBlockDao().forSession(s.id)
            val byStage = blocks.groupBy { SleepStage.entries.firstOrNull { st -> st.name == it.stageRaw } ?: SleepStage.UNKNOWN }
                .mapKeys { it.key.name.lowercase() }
                .mapValues { (_, b) -> b.sumOf { it.durationMinutes } }
            // Fall back to the stored total for a session with no blocks rather than reporting 0.
            val asleep = if (blocks.isEmpty()) s.totalMinutes else asleepMinutes(blocks)
            ExportRecord(ExportType.SLEEP, s.id, s.endAt, s.updatedAt,
                formatter.sleep(s.id, s.startAt, s.endAt, asleep, byStage, s.score))
        }

    private companion object {
        const val PAGE = 1000
    }
}
