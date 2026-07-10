package com.yourname.luftdaten;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple5;

import com.yourname.luftdaten.entities.SPS30Reading;

// Tuple5: (avgP2, avgN1, avgN05, avgTS, maxDatagenTimestamp)
public class CrossSpectrumAggregateFunction
        implements AggregateFunction<SPS30Reading, CrossSpectrumAggregateFunction.Accumulator, Tuple5<Double, Double, Double, Double, Long>> {

    public static class Accumulator {
        double sumP2 = 0, sumN1 = 0, sumN05 = 0, sumTS = 0;
        long maxDatagenTimestamp = Long.MIN_VALUE;
        long count = 0;
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator add(SPS30Reading r, Accumulator acc) {
        if (r.getP2() == null || r.getN1() == null || r.getN05() == null || r.getTS() == null)
            return acc;
        acc.sumP2  += r.getP2();
        acc.sumN1  += r.getN1();
        acc.sumN05 += r.getN05();
        acc.sumTS  += r.getTS();
        acc.maxDatagenTimestamp = Math.max(acc.maxDatagenTimestamp, r.getDatagenTimestamp());
        acc.count++;
        return acc;
    }

    @Override
    public Tuple5<Double, Double, Double, Double, Long> getResult(Accumulator acc) {
        if (acc.count == 0) return Tuple5.of(0.0, 0.0, 0.0, 0.0, 0L);
        return Tuple5.of(
                acc.sumP2  / acc.count,
                acc.sumN1  / acc.count,
                acc.sumN05 / acc.count,
                acc.sumTS  / acc.count,
                acc.maxDatagenTimestamp
        );
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        a.sumP2  += b.sumP2;
        a.sumN1  += b.sumN1;
        a.sumN05 += b.sumN05;
        a.sumTS  += b.sumTS;
        a.maxDatagenTimestamp = Math.max(a.maxDatagenTimestamp, b.maxDatagenTimestamp);
        a.count  += b.count;
        return a;
    }
}