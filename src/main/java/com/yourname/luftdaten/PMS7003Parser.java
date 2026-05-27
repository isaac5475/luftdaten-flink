package com.yourname.luftdaten;

import static com.yourname.luftdaten.SensorReadingParser.tryGetDoubleValue;

import com.yourname.luftdaten.entities.PMS7003Reading;
import com.yourname.luftdaten.entities.SensorReading;

public class PMS7003Parser implements ReadingParser {

    private PMS7003Parser() {
        // private constructor to prevent instantiation
    }

    public static PMS7003Reading parseReading(String row) {
        SensorReading sensorReading = SensorReadingParser.parseReading(row);
        PMS7003Reading pms7003Reading = new PMS7003Reading(sensorReading);
        String[] fields = row.split(";");
        pms7003Reading.setP1(tryGetDoubleValue(fields, 6));
        pms7003Reading.setP2(tryGetDoubleValue(fields, 7));
        pms7003Reading.setP0(tryGetDoubleValue(fields, 8));
        return pms7003Reading;
    }
}
