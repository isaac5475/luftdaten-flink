package com.yourname.luftdaten.entities;

import java.time.Instant;

public class SensorReading {
    private Integer sensor_id;
    private Integer location;
    private Double lat, lon;
    private Instant timestamp;
    private long datagenTimestamp;

    public SensorReading() {
    }

    public SensorReading(SensorReading sensorReading) {
        this();
        setSensor_id(sensorReading.getSensor_id());
        setLocation(sensorReading.getLocation());
        setLat(sensorReading.getLat());
        setLon(sensorReading.getLon());
        setTimestamp(sensorReading.getTimestamp());
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

    public Double getLon() {
        return lon;
    }

    public void setLon(Double lon) {
        this.lon = lon;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public long getDatagenTimestamp() {
        return datagenTimestamp;
    }

    public void setDatagenTimestamp(long datagenTimestamp) {
        this.datagenTimestamp = datagenTimestamp;
    }
}
