package com.yourname.luftdaten;

import com.yourname.luftdaten.entities.SensorReading;

public interface ReadingParser {

    String FIELD_SEPARATOR = ";";

    static SensorReading parseReading(String row) {
        return null;
    }

}
