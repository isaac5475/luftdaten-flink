package com.yourname.luftdaten;

import com.yourname.luftdaten.entities.BMP280Reading;
import com.yourname.luftdaten.entities.SensorReading;

public class BMP280Parser implements ReadingParser {
    public static BMP280Reading parseReading(String row) {
        SensorReading sensorReading = SensorReadingParser.parseReading(row);
        BMP280Reading bmp280Reading = new BMP280Reading(sensorReading);
        String[] fields = row.split(";");
        bmp280Reading.setPressure(fields[6].isEmpty() ? null : Double.parseDouble(fields[6]));
        bmp280Reading.setAltitude(fields[7].isEmpty() ? null : Double.parseDouble(fields[7]));
        bmp280Reading.setPressureAtSeaLevel(fields[8].isEmpty() ? null : Double.parseDouble(fields[8]));
        bmp280Reading.setTemperature(fields[9].isEmpty() ? null : Double.parseDouble(fields[9]));
        return bmp280Reading;
    }
}
