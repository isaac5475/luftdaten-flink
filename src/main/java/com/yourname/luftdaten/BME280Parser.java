package com.yourname.luftdaten;

import static com.yourname.luftdaten.SensorReadingParser.tryGetDoubleValue;

import com.yourname.luftdaten.entities.BME280Reading;
import com.yourname.luftdaten.entities.BMP280Reading;

public class BME280Parser implements ReadingParser {
    public static BME280Reading parseReading(String row) {
        BMP280Reading sensorReading = BMP280Parser.parseReading(row);
        BME280Reading bme280Reading = new BME280Reading(sensorReading);
        String[] fields = row.split(";");
        bme280Reading.setHumidity(tryGetDoubleValue(fields, 10));
        return bme280Reading;
    }
}
