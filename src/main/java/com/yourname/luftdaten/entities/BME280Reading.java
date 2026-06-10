package com.yourname.luftdaten.entities;

import java.util.Optional;

public class BME280Reading extends BMP280Reading {

    private Double humidity;

    public BME280Reading() {
        super();
    }

    public BME280Reading(BMP280Reading sensorReading) {
        super(sensorReading);
    }

    public Double getHumidity() {
        return humidity;
    }

    public void setHumidity(Double humidity) {
        this.humidity = humidity;
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                getSensor_id(),
                Optional.ofNullable(getSensorType()).orElse(""),
                getLocation() == null ? "" : getLocation(),
                getLat() == null ? "" : getLat(),
                getLon() == null ? "" : getLon(),
                getTimestamp() == null ? "" : getTimestamp(),
                getPressure() ==  null ? "" : getPressure(),
                getAltitude() ==  null ? "" : getAltitude(),
                getPressureAtSeaLevel() ==   null ? "" : getPressureAtSeaLevel(),
                getTemperature() ==   null ? "" : getTemperature(),
                getHumidity() == null ? "" : getHumidity()
        );
    }

}
