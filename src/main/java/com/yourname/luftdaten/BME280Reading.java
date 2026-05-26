package com.yourname.luftdaten;

public class BME280Reading extends SensorReading {

    private Double pressure, pressureAtSeaLevel, altitude, temperature, humidity;

    public BME280Reading() {
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

    public Double getHumidity() {
        return humidity;
    }

    public void setHumidity(Double humidity) {
        this.humidity = humidity;
    }

    public Double getPressure() {
        return pressure;
    }

    public void setPressure(Double pressure) {
        this.pressure = pressure;
    }
}
