package com.yourname.luftdaten;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.yourname.luftdaten.entities.SensorReading;

public class SensorReadingParser implements ReadingParser {
    public static SensorReading parseReading(String row) {
        String[] fields = row.split(";");
        SensorReading reading = new SensorReading();
        reading.setSensor_id(Integer.parseInt(fields[0]));
        reading.setLocation(Integer.parseInt(fields[2]));
        reading.setLat(Double.parseDouble(fields[3]));
        reading.setLon(Double.parseDouble(fields[4]));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        Instant timestamp = LocalDateTime.parse(fields[5], formatter).toInstant(java.time.ZoneOffset.UTC);
        reading.setTimestamp(timestamp);
        return reading;
    }
}
