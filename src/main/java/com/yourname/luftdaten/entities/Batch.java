package com.yourname.luftdaten.entities;

import java.util.List;

public class Batch {
    private boolean last = false;
    private int batchId;
    List<String> measurements;

    public Batch(List<String> measurements) {
        this.measurements = measurements;
    }

    public boolean isLast() {
        return last;
    }

    public void setLast(boolean last) {
        this.last = last;
    }

    public int getBatchId() {
        return batchId;
    }

    public void setBatchId(int batchId) {
        this.batchId = batchId;
    }

    public List<String> getMeasurements() {
        return measurements;
    }
}
