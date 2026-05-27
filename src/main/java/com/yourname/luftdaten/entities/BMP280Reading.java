package com.yourname.luftdaten.entities;

public class BMP280Reading extends SensorReading {

    private Double pressure, pressureAtSeaLevel, altitude, temperature;

    public BMP280Reading() {
    }

    public BMP280Reading(SensorReading reading) {
        super(reading);
    }

    public BMP280Reading(BMP280Reading reading) {
        this((SensorReading) reading);
        setPressure(reading.getPressure());
        setTemperature(reading.getTemperature());
        setPressureAtSeaLevel(reading.getPressureAtSeaLevel());
        setAltitude(reading.getAltitude());
    }

    public Double getPressureAtSeaLevel() {
        return pressureAtSeaLevel;
    }

    public void setPressureAtSeaLevel(Double pressureAtSeaLevel) {
        this.pressureAtSeaLevel = pressureAtSeaLevel;
    }

    public Double getAltitude() {
        return altitude;
    }

    public void setAltitude(Double altitude) {
        this.altitude = altitude;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public Double getPressure() {
        return pressure;
    }

    public void setPressure(Double pressure) {
        this.pressure = pressure;
    }
}
