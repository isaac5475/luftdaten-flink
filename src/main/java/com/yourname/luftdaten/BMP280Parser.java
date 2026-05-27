package com.yourname.luftdaten;

import static com.yourname.luftdaten.SensorReadingParser.tryGetDoubleValue;

import com.yourname.luftdaten.entities.BMP280Reading;
import com.yourname.luftdaten.entities.SensorReading;

public class BMP280Parser implements ReadingParser {
    public static BMP280Reading parseReading(String row) {
        SensorReading sensorReading = SensorReadingParser.parseReading(row);
        BMP280Reading bmp280Reading = new BMP280Reading(sensorReading);
        String[] fields = row.split(";");
        bmp280Reading.setPressure(tryGetDoubleValue(fields, 6));
        bmp280Reading.setAltitude(tryGetDoubleValue(fields, 7));
        bmp280Reading.setPressureAtSeaLevel(tryGetDoubleValue(fields, 8));
        bmp280Reading.setTemperature(tryGetDoubleValue(fields, 9));
        return bmp280Reading;
    }
}
