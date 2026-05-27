package com.yourname.luftdaten.entities;

import java.time.Instant;

public class PMS7003Reading extends SensorReading {
    private Double p1;
    private Double p2;
    private Double p0;

    public PMS7003Reading() {
    }

    public PMS7003Reading(SensorReading sensorReading) {
        super(sensorReading);
    }

    public Double getP1() {
        return p1;
    }

    public void setP1(Double p1) {
        this.p1 = p1;
    }

    public Double getP2() {
        return p2;
    }

    public void setP2(Double p2) {
        this.p2 = p2;
    }

    public Double getP0() {
        return p0;
    }

    public void setP0(Double p0) {
        this.p0 = p0;
    }
}
