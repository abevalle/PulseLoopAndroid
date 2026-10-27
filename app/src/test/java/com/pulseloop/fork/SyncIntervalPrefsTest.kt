package com.pulseloop.fork

import org.junit.Assert.assertEquals
import org.junit.Test

class SyncIntervalPrefsTest {
    @Test
    fun `flex is half the interval, floored at WorkManager's 5 minutes`() {
        assertEquals(7, SyncIntervalPrefs.flexMinutes(15))
        assertEquals(15, SyncIntervalPrefs.flexMinutes(30)) // upstream's 30/15 pair
        assertEquals(30, SyncIntervalPrefs.flexMinutes(60))
        assertEquals(5, SyncIntervalPrefs.flexMinutes(6))
    }

    @Test
    fun `default is the WorkManager minimum and upstream's 30 stays an option`() {
        assertEquals(15, SyncIntervalPrefs.DEFAULT_MINUTES)
        assertEquals(true, 30 in SyncIntervalPrefs.OPTIONS)
    }
}
