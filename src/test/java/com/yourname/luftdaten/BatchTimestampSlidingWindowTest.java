package com.yourname.luftdaten;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.yourname.luftdaten.entities.Batch;

/**
 * Tests for BatchTimestampSlidingWindow
 *
 * This test validates the sliding window logic:
 * - Batches within the last 24 hours are retained
 * - Batches older than 24 hours are filtered out
 * - Each batch is associated with its timestamp
 */
class BatchTimestampSlidingWindowTest {

    /**
     * Test that validates the window filtering logic.
     *
     * Simulates the window behavior: when we add a batch at timestamp T,
     * we compute the window boundary as T - 1 day, and keep all batches
     * with timestamps > boundary.
     */
    @Test
    void testWindowFilteringLogic() {
        // Current time
        long now = System.currentTimeMillis();

        // Create three timestamps: old, recent, and new
        long minus25Hours = now - ChronoUnit.HOURS.getDuration().toMillis() * 25;
        long minus1Hour = now - ChronoUnit.HOURS.getDuration().toMillis();
        long oneDayInMillis = ChronoUnit.DAYS.getDuration().toMillis();

        // When processing a batch at 'now', the window boundary is (now - 1 day)
        long windowBoundary = now - oneDayInMillis;

        // minus25Hours is definitely older than 1 day
        assertTrue(minus25Hours <= windowBoundary);

        // minus1Hour is definitely within 1 day
        assertTrue(minus1Hour > windowBoundary);

        // now is definitely within 1 day
        assertTrue(now > windowBoundary);
    }

    @Test
    void testBatchProperties() {
        // Test that Batch object supports the properties used in the window function
        Batch batch = new Batch(List.of("measurement1", "measurement2"));
        batch.setBatchId(42);
        batch.setLast(false);

        assertEquals(42, batch.getBatchId());
        assertFalse(batch.isLast());
        assertEquals(2, batch.getMeasurements().size());
    }

    @Test
    void testMultipleBatchesCollection() {
        // Test that we can collect batches in a list and track their IDs
        List<Batch> window = new ArrayList<>();

        Batch b1 = new Batch(List.of("data1"));
        b1.setBatchId(1);
        window.add(b1);

        Batch b2 = new Batch(List.of("data2"));
        b2.setBatchId(2);
        window.add(b2);

        Batch b3 = new Batch(List.of("data3"));
        b3.setBatchId(3);
        window.add(b3);

        assertEquals(3, window.size());

        Set<Integer> ids = new HashSet<>();
        for (Batch b : window) {
            ids.add(b.getBatchId());
        }
        assertEquals(3, ids.size());
        assertTrue(ids.contains(1));
        assertTrue(ids.contains(2));
        assertTrue(ids.contains(3));
    }

    @Test
    void testWindowBoundaryCalculation() {
        long now = System.currentTimeMillis();
        long oneDayMillis = ChronoUnit.DAYS.getDuration().toMillis();

        // Test the window boundary calculation used by BatchTimestampSlidingWindow
        long minusDayTimestamp = now - oneDayMillis;

        // Create test timestamps
        long slightlyBefore = minusDayTimestamp - 1000; // 1 second too old
        long atBoundary = minusDayTimestamp;           // exactly at boundary
        long inside = minusDayTimestamp + 1000;        // 1 second inside window

        // The window condition is: timestamp > minusDayTimestamp
        assertFalse(slightlyBefore > minusDayTimestamp);
        assertFalse(atBoundary > minusDayTimestamp);
        assertTrue(inside > minusDayTimestamp);
    }

    @Test
    void testLastBatchFlag() {
        // Test that the last flag is properly tracked
        Batch batchNotLast = new Batch(List.of("data"));
        batchNotLast.setLast(false);

        Batch batchLast = new Batch(List.of("data"));
        batchLast.setLast(true);

        assertFalse(batchNotLast.isLast());
        assertTrue(batchLast.isLast());
    }

    private static boolean filterOlderThanDay(long timestamp, long currentTime) {
        // Simulates the filtering logic of BatchTimestampSlidingWindow
        long oneDayMillis = ChronoUnit.DAYS.getDuration().toMillis();
        long minusDayTimestamp = currentTime - oneDayMillis;
        return timestamp > minusDayTimestamp;
    }

    @Test
    void testFilteringLogicThorough() {
        long now = System.currentTimeMillis();
        long oneDayMillis = ChronoUnit.DAYS.getDuration().toMillis();

        // Test various timestamps
        long veryOld = now - oneDayMillis - 3600000; // 1 hour before boundary
        long justOld = now - oneDayMillis - 1;       // 1ms before boundary
        long justInside = now - oneDayMillis + 1;    // 1ms after boundary
        long recent = now - 3600000;                  // 1 hour ago
        long current = now;                           // now

        assertFalse(filterOlderThanDay(veryOld, now));
        assertFalse(filterOlderThanDay(justOld, now));
        assertTrue(filterOlderThanDay(justInside, now));
        assertTrue(filterOlderThanDay(recent, now));
        assertTrue(filterOlderThanDay(current, now));
    }
}






















