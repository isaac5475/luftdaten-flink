package com.yourname.luftdaten;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import com.yourname.luftdaten.entities.BME280Reading;

/**
 * AggregateAverage that consumes BME280Reading and produces the average temperature (Double).
 */
public class AggregateAverageTempAndMaxDatagenTimestamp implements AggregateFunction<BME280Reading, AggregateAverageTempAndMaxDatagenTimestamp.AvgAccumulator, Tuple2<Double, Long>> {

    // nested static accumulator class (allowed because it's nested)
    public static class AvgAccumulator {
        public double sum = 0.0;
        public long count = 0L;
        public long maxDatagenTimestamp = Long.MIN_VALUE;
    }

    @Override
    public AvgAccumulator createAccumulator() {
        return new AvgAccumulator();
    }

    @Override
    public AvgAccumulator add(BME280Reading value, AvgAccumulator acc) {
        if (value == null) {
            return acc;
        }
        Double temp = value.getTemperature();
        if (temp != null) {
            acc.sum += temp;
            acc.count += 1;
            acc.maxDatagenTimestamp = Math.max(acc.maxDatagenTimestamp, value.getDatagenTimestamp());
        }
        return acc;
    }

    @Override
    public Tuple2<Double, Long> getResult(AvgAccumulator acc) {
        return new Tuple2<>(acc.count == 0 ? Double.NaN : acc.sum / acc.count, acc.maxDatagenTimestamp);
    }

    @Override
    public AvgAccumulator merge(AvgAccumulator a, AvgAccumulator b) {
        a.sum += b.sum;
        a.count += b.count;
        a.maxDatagenTimestamp = Math.max(a.maxDatagenTimestamp, b.maxDatagenTimestamp);
        return a;
    }
}