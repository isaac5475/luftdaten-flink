package com.yourname.luftdaten;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import com.yourname.luftdaten.entities.SensorReading;

public abstract class AverageProcessingWithDatagenTimestampFunction<T extends SensorReading> implements AggregateFunction<T, AvgAccumulatorMaxDatagenTimestamp, Tuple2<Double, Long>> {

    abstract Double getValue(T value);

    @Override
    public AvgAccumulatorMaxDatagenTimestamp createAccumulator() {
        return new AvgAccumulatorMaxDatagenTimestamp();
    }

    @Override
    public AvgAccumulatorMaxDatagenTimestamp add(T value, AvgAccumulatorMaxDatagenTimestamp accumulator) {
        accumulator.maxDatagenTimestamp = Math.max(accumulator.maxDatagenTimestamp, value.getDatagenTimestamp());
        accumulator.sum += getValue(value);
        accumulator.count += 1;
        return accumulator;
    }

    @Override
    public Tuple2<Double, Long> getResult(AvgAccumulatorMaxDatagenTimestamp acc) {
        Double avg = acc.count == 0 ? Double.NaN : acc.sum / acc.count;
        return Tuple2.of(avg, acc.maxDatagenTimestamp);
    }

    @Override
    public AvgAccumulatorMaxDatagenTimestamp merge(AvgAccumulatorMaxDatagenTimestamp a, AvgAccumulatorMaxDatagenTimestamp b) {
        a.merge(b);
        return a;
    }
}
