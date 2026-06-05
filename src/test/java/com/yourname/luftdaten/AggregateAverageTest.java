package com.yourname.luftdaten;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.yourname.luftdaten.entities.BME280Reading;

class AggregateAverageTest {

    @Test
    void testAddAndGetResult() {
        AggregateAverage agg = new AggregateAverage();
        AggregateAverage.AvgAccumulator acc = agg.createAccumulator();

        BME280Reading r1 = new BME280Reading();
        r1.setTemperature(10.0);
        agg.add(r1, acc);

        BME280Reading r2 = new BME280Reading();
        r2.setTemperature(20.0);
        agg.add(r2, acc);

        Double result = agg.getResult(acc);
        assertEquals(15.0, result);
    }

    @Test
    void testAddNullsAndMerge() {
        AggregateAverage agg = new AggregateAverage();
        AggregateAverage.AvgAccumulator a = agg.createAccumulator();
        AggregateAverage.AvgAccumulator b = agg.createAccumulator();

        // add a value to b
        BME280Reading r = new BME280Reading();
        r.setTemperature(5.0);
        agg.add(r, b);

        // merge b into a
        AggregateAverage.AvgAccumulator merged = agg.merge(a, b);
        assertTrue(merged.count > 0);
        assertEquals(5.0, agg.getResult(merged));
    }

    @Test
    void testEmptyAccumulatorProducesNaN() {
        AggregateAverage agg = new AggregateAverage();
        AggregateAverage.AvgAccumulator acc = agg.createAccumulator();
        Double res = agg.getResult(acc);
        assertTrue(res.isNaN());
    }
}

