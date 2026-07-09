package com.yourname.luftdaten;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import com.yourname.luftdaten.entities.BME280Reading;

/**
 * AggregateAverage that consumes BME280Reading and produces the average temperature (Double).
 */
public class AggregateAverageTempAndMaxDatagenTimestamp implements AggregateFunction<BME280Reading, AvgAccumulatorMaxDatagenTimestamp, Tuple2<Double, Long>> {


    @Override
    public AvgAccumulatorMaxDatagenTimestamp createAccumulator() {
        return new AvgAccumulatorMaxDatagenTimestamp();
    }

    @Override
    public AvgAccumulatorMaxDatagenTimestamp add(BME280Reading value, AvgAccumulatorMaxDatagenTimestamp acc) {
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
    public Tuple2<Double, Long> getResult(AvgAccumulatorMaxDatagenTimestamp acc) {
        return new Tuple2<>(acc.count == 0 ? Double.NaN : acc.sum / acc.count, acc.maxDatagenTimestamp);
    }

    @Override
    public AvgAccumulatorMaxDatagenTimestamp merge(AvgAccumulatorMaxDatagenTimestamp a, AvgAccumulatorMaxDatagenTimestamp b) {
        a.sum += b.sum;
        a.count += b.count;
        a.maxDatagenTimestamp = Math.max(a.maxDatagenTimestamp, b.maxDatagenTimestamp);
        return a;
    }
}