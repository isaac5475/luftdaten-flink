package com.yourname.luftdaten;

public class AvgAccumulatorMaxValueAndDatagenTimestamp extends AvgAccumulatorMaxDatagenTimestamp {
    public double maxValue = Double.MIN_VALUE;

    @Override
    public void merge(AvgAccumulatorMaxDatagenTimestamp b) {
        super.merge(b);
        if (b instanceof AvgAccumulatorMaxValueAndDatagenTimestamp) {
            this.maxValue = Math.max(this.maxValue, ((AvgAccumulatorMaxValueAndDatagenTimestamp) b).maxValue);
        }
    }
}

