package com.yourname.luftdaten.entities;

import java.time.Instant;

public class SensorReading {
    private Integer sensor_id;
    private String sensorType;
    private Integer location;
    private Double lat, lon;
    private Long timestamp;
    private long datagenTimestamp;

    public SensorReading() {
    }

    public SensorReading(SensorReading sensorReading) {
        this();
        setSensor_id(sensorReading.getSensor_id());
        setSensorType(sensorReading.getSensorType());
        setLocation(sensorReading.getLocation());
        setLat(sensorReading.getLat());
        setLon(sensorReading.getLon());
        setTimestamp(sensorReading.getTimestamp());
        setDatagenTimestamp(sensorReading.getDatagenTimestamp());
    }

    public Integer getSensor_id() {
        return sensor_id;
    }

    public void setSensor_id(Integer sensor_id) {
        this.sensor_id = sensor_id;
    }

    public Integer getLocation() {
        return location;
    }

    public void setLocation(Integer location) {
        this.location = location;
    }

    public Double getLat() {
        return lat;
    }

    public void setLat(Double lat) {
        this.lat = lat;
    }

    @Override
    public String toString() {
        return String.format("%s;%s;%s;%s;%s;%s;%s",
                sensor_id, sensorType, location, lat, lon, timestamp, datagenTimestamp);
    }

    public Double getLon() {
        return lon;
    }

    public void setLon(Double lon) {
        this.lon = lon;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public long getDatagenTimestamp() {
        return datagenTimestamp;
    }

    public void setDatagenTimestamp(long datagenTimestamp) {
        this.datagenTimestamp = datagenTimestamp;
    }

    public String getSensorType() {
        return sensorType;
    }

    public void setSensorType(String sensorType) {
        this.sensorType = sensorType;
    }
}
