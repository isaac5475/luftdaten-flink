package com.yourname.luftdaten;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class BME280Parser implements ReadingParser {
    public static BME280Reading parseReading(String row) {
        String[] fields = row.split(";");
        BME280Reading bme280Reading = new BME280Reading();
        bme280Reading.setSensor_id(Integer.parseInt(fields[0]));
        bme280Reading.setLocation(Integer.parseInt(fields[2]));
        bme280Reading.setLat(Double.parseDouble(fields[3]));
        bme280Reading.setLon(Double.parseDouble(fields[4]));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        Instant timestamp = LocalDateTime.parse(fields[5], formatter).toInstant(java.time.ZoneOffset.UTC);
        bme280Reading.setTimestamp(timestamp);
        bme280Reading.setPressure(fields[6].isEmpty() ? null : Double.parseDouble(fields[6]));
        bme280Reading.setAltitude(fields[7].isEmpty() ? null : Double.parseDouble(fields[7]));
        bme280Reading.setPressureAtSeaLevel(fields[8].isEmpty() ? null : Double.parseDouble(fields[8]));
        bme280Reading.setTemperature(fields[9].isEmpty() ? null : Double.parseDouble(fields[9]));
        bme280Reading.setHumidity(fields[10].isEmpty() ? null : Double.parseDouble(fields[10]));
        return bme280Reading;
    }
}
