package com.yourname.luftdaten;

import java.io.Serializable;

public class Alert implements Serializable {
    int sensorId;
    long datagen_timestamp = Long.MIN_VALUE;

    public Alert(int sensorId, long datagen_timestamp, double p2) {
        this.sensorId = sensorId;
        this.datagen_timestamp = datagen_timestamp;
        this.p2 = p2;
    }

    public Alert() {
    }

    double p2;

    public int getSensorId() {
        return sensorId;
    }

    public void setSensorId(int sensorId) {
        this.sensorId = sensorId;
    }

    public long getDatagen_timestamp() {
        return datagen_timestamp;
    }

    public void setDatagen_timestamp(long datagen_timestamp) {
        this.datagen_timestamp = datagen_timestamp;
    }

    public double getP2() {
        return p2;
    }

    public void setP2(double p2) {
        this.p2 = p2;
    }
}
