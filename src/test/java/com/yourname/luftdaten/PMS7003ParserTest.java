package com.yourname.luftdaten;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.junit.jupiter.api.Test;
import com.yourname.luftdaten.entities.PMS7003Reading;

class PMS7003ParserTest {
    @Test
    void testParse() {
        String input = "21358;PMS7003;13506;50.852;20.632;2020-01-01T00:00:01;20.20;17.80;9.60\n";

        PMS7003Reading reading = PMS7003Parser.parseReading(input);
        assertNotNull(reading);
        assertEquals(21358, reading.getSensor_id());
        assertEquals(13506, reading.getLocation());
        assertEquals(50.852, reading.getLat());
        assertEquals(20.632, reading.getLon());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        Instant timestamp = LocalDateTime.parse("2020-01-01T00:00:01", formatter).toInstant(java.time.ZoneOffset.UTC);
        assertEquals(timestamp, reading.getTimestamp());
        assertEquals(20.2, reading.getP1());
        assertEquals(17.8, reading.getP2());
        assertEquals(9.6, reading.getP0());
    }

    @Test
    void testParseFaulty() {
        String input = ";;;;;;";

        PMS7003Reading reading = PMS7003Parser.parseReading(input);
        assertNotNull(reading);
        assertNull(reading.getSensor_id());
        assertNull(reading.getLocation());
        assertNull(reading.getLat());
        assertNull(reading.getLon());
        assertNull(reading.getTimestamp());
        assertNull(reading.getP1());
        assertNull(reading.getP2());
        assertNull(reading.getP0());
    }
}