package com.yourname.luftdaten;

public class AvgAccumulatorMaxDatagenTimestamp {
        public double sum = 0.0;
        public long count = 0L;
        public long maxDatagenTimestamp = Long.MIN_VALUE;

        public void merge(AvgAccumulatorMaxDatagenTimestamp b) {
                this.sum += b.sum;
                this.count += b.count;
                this.maxDatagenTimestamp = Math.max(this.maxDatagenTimestamp, b.maxDatagenTimestamp);
        }
}
