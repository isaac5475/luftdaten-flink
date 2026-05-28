package com.yourname.luftdaten;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.junit.jupiter.api.Test;

import com.yourname.luftdaten.entities.SensorReading;

class SensorReadingParserTest {

    @Test
    void testParseValid() {
        String input = "21358;TYPE;13506;50.852;20.632;2020-01-01T00:00:01";
        SensorReading r = SensorReadingParser.parseReading(input);
        assertEquals(21358, r.getSensor_id());
        assertEquals(13506, r.getLocation());
        assertEquals(50.852, r.getLat());
        assertEquals(20.632, r.getLon());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        Instant timestamp = LocalDateTime.parse("2020-01-01T00:00:01", formatter).toInstant(java.time.ZoneOffset.UTC);
        assertEquals(timestamp, r.getTimestamp());
    }

    @Test
    void testParseFaulty() {
        String input = ";;;;;;";
        SensorReading r = SensorReadingParser.parseReading(input);
        assertNull(r.getSensor_id());
        assertNull(r.getLocation());
        assertNull(r.getLat());
        assertNull(r.getLon());
        assertNull(r.getTimestamp());
    }
}

