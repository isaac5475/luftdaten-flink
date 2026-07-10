package com.yourname.luftdaten;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import com.yourname.luftdaten.entities.SensorReading;

public abstract class AverageAndMaxWithDatagenTimestampFunction<T extends SensorReading> implements AggregateFunction<T, AvgAccumulatorMaxValueAndDatagenTimestamp, Tuple3<Double, Double, Long>> {

    protected abstract Double getValue(T value);

    @Override
    public AvgAccumulatorMaxValueAndDatagenTimestamp createAccumulator() {
        return new AvgAccumulatorMaxValueAndDatagenTimestamp();
    }

    @Override
    public AvgAccumulatorMaxValueAndDatagenTimestamp add(T value, AvgAccumulatorMaxValueAndDatagenTimestamp accumulator) {
        accumulator.maxDatagenTimestamp = Math.max(accumulator.maxDatagenTimestamp, value.getDatagenTimestamp());
        accumulator.sum += getValue(value);
        accumulator.maxValue = Math.max(accumulator.maxValue, getValue(value));
        accumulator.count += 1;
        return accumulator;
    }

    @Override
    public Tuple3<Double, Double, Long> getResult(AvgAccumulatorMaxValueAndDatagenTimestamp acc) {
        Double avg = acc.count == 0 ? Double.NaN : acc.sum / acc.count;
        return Tuple3.of(avg, acc.maxValue, acc.maxDatagenTimestamp);
    }

    @Override
    public AvgAccumulatorMaxValueAndDatagenTimestamp merge(AvgAccumulatorMaxValueAndDatagenTimestamp a, AvgAccumulatorMaxValueAndDatagenTimestamp b) {
        a.merge(b);
        return a;
    }
}


