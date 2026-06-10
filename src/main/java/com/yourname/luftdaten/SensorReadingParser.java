package com.yourname.luftdaten;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import com.yourname.luftdaten.entities.SensorReading;

public class SensorReadingParser implements ReadingParser {
    public static SensorReading parseReading(String row) {
        String[] fields = row.split(FIELD_SEPARATOR);
        SensorReading reading = new SensorReading();
        reading.setSensor_id(tryGetIntegerValue(fields, 0));
        reading.setSensorType(tryGetStringValue(fields, 1));
        reading.setLocation(tryGetIntegerValue(fields, 2));
        reading.setLat(tryGetDoubleValue(fields, 3));
        reading.setLon(tryGetDoubleValue(fields, 4));
        Instant timestamp = tryGetInstantValue(fields, 5);
        reading.setTimestamp(timestamp);
        Long datagenTimestamp = Optional.ofNullable(tryGetLongValue(fields, fields.length - 1)).orElse(0L);
        reading.setDatagenTimestamp(datagenTimestamp);
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

    protected static String tryGetStringValue(String[] fields, int idx) {
            if (fields.length > idx && !fields[idx].isEmpty()) {
                return fields[idx];
            }
        return null;
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

    protected static Long tryGetLongValue(String[] fields, int idx) {
        try {
            if (fields.length > idx && !fields[idx].isEmpty()) {
                return Long.parseLong(fields[idx]);
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
