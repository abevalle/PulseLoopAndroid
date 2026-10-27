package com.pulseloop.fork

import android.content.Context
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Card
import androidx.compose.material3.FilterChip
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import com.pulseloop.service.RingSyncWorker

/**
 * Fork: how often the background ring sync runs. Upstream hardcodes 30 minutes; this fork
 * defaults to 15 (WorkManager's minimum periodic interval) so Home Assistant gets readings sooner.
 */
object SyncIntervalPrefs {
    val OPTIONS = listOf(15, 30, 60)
    const val DEFAULT_MINUTES = 15
    private const val FILE = "fork_sync"
    private const val KEY = "interval_minutes"

    fun minutes(context: Context): Int =
        context.applicationContext.getSharedPreferences(FILE, Context.MODE_PRIVATE)
            .getInt(KEY, DEFAULT_MINUTES).takeIf { it in OPTIONS } ?: DEFAULT_MINUTES

    fun set(context: Context, minutes: Int) {
        require(minutes in OPTIONS)
        context.applicationContext.getSharedPreferences(FILE, Context.MODE_PRIVATE)
            .edit().putInt(KEY, minutes).apply()
    }

    /** Flex window: half the interval, never under WorkManager's 5-minute floor (30 → 15, as upstream). */
    fun flexMinutes(interval: Int): Int = maxOf(5, interval / 2)
}

@Composable
fun SyncIntervalCard() {
    val context = LocalContext.current
    var selected by remember { mutableIntStateOf(SyncIntervalPrefs.minutes(context)) }
    Card(Modifier.fillMaxWidth()) {
        Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Text("Background sync", style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
            Text(
                "How often the ring is synced while the app is closed. Shorter is fresher but costs " +
                    "ring and phone battery. Android may delay runs when the battery is low.",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                SyncIntervalPrefs.OPTIONS.forEach { m ->
                    FilterChip(
                        selected = selected == m,
                        onClick = {
                            selected = m
                            SyncIntervalPrefs.set(context, m)
                            RingSyncWorker.schedule(context.applicationContext)
                        },
                        label = { Text(if (m == 60) "1 hour" else "$m min") },
                    )
                }
            }
        }
    }
}
