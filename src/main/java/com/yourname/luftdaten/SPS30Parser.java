package com.yourname.luftdaten;

import static com.yourname.luftdaten.SensorReadingParser.tryGetDoubleValue;

import com.yourname.luftdaten.entities.BMP280Reading;
import com.yourname.luftdaten.entities.SPS30Reading;
import com.yourname.luftdaten.entities.SensorReading;

public class SPS30Parser implements ReadingParser {

    private SPS30Parser() {
        // private constructor to prevent instantiation
    }

    public static SPS30Reading parseReading(String row) {
        SensorReading sensorReading = SensorReadingParser.parseReading(row);
        SPS30Reading sps30Reading = new SPS30Reading(sensorReading);
        String[] fields = row.split(FIELD_SEPARATOR);
        sps30Reading.setP1(tryGetDoubleValue(fields, 6));
        sps30Reading.setP4(tryGetDoubleValue(fields, 7));
        sps30Reading.setP2(tryGetDoubleValue(fields, 8));
        sps30Reading.setP0(tryGetDoubleValue(fields, 9));
        sps30Reading.setN10(tryGetDoubleValue(fields, 10));
        sps30Reading.setN4(tryGetDoubleValue(fields, 11));
        sps30Reading.setN25(tryGetDoubleValue(fields, 12));
        sps30Reading.setN1(tryGetDoubleValue(fields, 13));
        sps30Reading.setN05(tryGetDoubleValue(fields, 14));
        sps30Reading.setTS(tryGetDoubleValue(fields, 15));
        return sps30Reading;
    }
}
