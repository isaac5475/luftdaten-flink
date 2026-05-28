package com.yourname.luftdaten.entities;

import java.util.ArrayList;
import java.util.List;

public class Batch<T> {
    private boolean last = false;
    private int batchId;
    List<T> measurements;

    public Batch(List<T> measurements) {
        this.measurements = new ArrayList<>(measurements);
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

    public List<T> getMeasurements() {
        return measurements;
    }
}
