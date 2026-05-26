package com.yourname.luftdaten.entities;

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
}
