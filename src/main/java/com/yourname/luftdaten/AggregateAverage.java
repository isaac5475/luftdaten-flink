package com.yourname.luftdaten;

import org.apache.flink.api.common.functions.AggregateFunction;
import com.yourname.luftdaten.entities.BMP280Reading;

/**
 * AggregateAverage that consumes BME280Reading and produces the average temperature (Double).
 */
public class AggregateAverage implements AggregateFunction<BMP280Reading, AggregateAverage.AvgAccumulator, Double> {

    // nested static accumulator class (allowed because it's nested)
    public static class AvgAccumulator {
        public double sum = 0.0;
        public long count = 0L;
    }

    @Override
    public AvgAccumulator createAccumulator() {
        return new AvgAccumulator();
    }

    @Override
    public AvgAccumulator add(BMP280Reading value, AvgAccumulator acc) {
        if (value == null) {
            return acc;
        }
        Double temp = value.getTemperature();
        if (temp != null) {
            acc.sum += temp;
            acc.count += 1;
        }
        return acc;
    }

    @Override
    public Double getResult(AvgAccumulator acc) {
        return acc.count == 0 ? Double.NaN : acc.sum / acc.count;
    }

    @Override
    public AvgAccumulator merge(AvgAccumulator a, AvgAccumulator b) {
        a.sum += b.sum;
        a.count += b.count;
        return a;
    }
}