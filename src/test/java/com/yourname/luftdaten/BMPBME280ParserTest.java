package com.yourname.luftdaten;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.junit.jupiter.api.Test;

import com.yourname.luftdaten.entities.BME280Reading;
import com.yourname.luftdaten.entities.BMP280Reading;

class BMPBME280ParserTest {

    @Test
    void testBMP280Parse() {
        String input = "21358;BMP280;13506;50.852;20.632;2020-01-01T00:00:01;1013.25;150.0;1000.0;20.20";
        BMP280Reading r = BMP280Parser.parseReading(input);
        assertEquals(21358, r.getSensor_id());
        assertEquals(13506, r.getLocation());
        assertEquals(50.852, r.getLat());
        assertEquals(20.632, r.getLon());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        Instant timestamp = LocalDateTime.parse("2020-01-01T00:00:01", formatter).toInstant(java.time.ZoneOffset.UTC);
        assertEquals(timestamp, r.getTimestamp());
        assertEquals(1013.25, r.getPressure());
        assertEquals(150.0, r.getAltitude());
        assertEquals(1000.0, r.getPressureAtSeaLevel());
        assertEquals(20.2, r.getTemperature());
    }

    @Test
    void testBME280Parse() {
        String input = "21358;BME280;13506;50.852;20.632;2020-01-01T00:00:01;1013.25;150.0;1000.0;20.20;55.5";
        BME280Reading r = BME280Parser.parseReading(input);
        assertEquals(55.5, r.getHumidity());
    }

    @Test
    void testParseFaulty() {
        String input = ";;;;;";
        BMP280Reading r = BMP280Parser.parseReading(input);
        assertNull(r.getPressure());
        assertNull(r.getAltitude());
        assertNull(r.getPressureAtSeaLevel());
        assertNull(r.getTemperature());
    }
}

