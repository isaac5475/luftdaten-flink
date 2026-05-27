package com.yourname.luftdaten;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.yourname.luftdaten.entities.SensorReading;

public class SensorReadingParser implements ReadingParser {
    public static SensorReading parseReading(String row) {
        String[] fields = row.split(";");
        SensorReading reading = new SensorReading();
        reading.setSensor_id(tryGetIntegerValue(fields, 0));
        reading.setLocation(tryGetIntegerValue(fields, 2));
        reading.setLat(tryGetDoubleValue(fields, 3));
        reading.setLon(tryGetDoubleValue(fields, 4));
        Instant timestamp = tryGetInstantValue(fields, 5);
        reading.setTimestamp(timestamp);
        return reading;
    }

    private static Instant tryGetInstantValue(String[] fields, int idx) {
        if (fields.length > idx && !fields[idx].isEmpty()) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
            return LocalDateTime.parse(fields[idx], formatter).toInstant(java.time.ZoneOffset.UTC);
        } else {
            return null;
        }
    }

    protected static Integer tryGetIntegerValue(String[] fields, int idx) {
        try {
            if (fields.length > idx && !fields[idx].isEmpty()) {
                return Integer.parseInt(fields[idx]);
            }
        } catch (NumberFormatException ignore) {
        }
        return null;
    }

    protected static Double tryGetDoubleValue(String[] fields, int idx) {
        try {
            if (fields.length > idx && !fields[idx].isEmpty()) {
                return Double.parseDouble(fields[idx]);
            }
        } catch (NumberFormatException ignore) {
        }
        return null;
    }
}
