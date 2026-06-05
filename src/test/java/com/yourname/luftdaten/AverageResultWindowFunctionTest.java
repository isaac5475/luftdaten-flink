package com.yourname.luftdaten;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class AverageResultWindowFunctionTest {

    static class ListCollector implements Collector<String> {
        final List<String> out = new ArrayList<>();

        @Override
        public void collect(String record) {
            out.add(record);
        }

        @Override
        public void close() {
        }
    }

    @Test
    void testProcessFormatsOutput() throws Exception {
        AverageResultWindowFunction fn = new AverageResultWindowFunction();
        @SuppressWarnings("unchecked")
        ProcessWindowFunction<Double, String, Integer, TimeWindow>.Context ctx = (ProcessWindowFunction.Context) mock(ProcessWindowFunction.Context.class);
        when(ctx.window()).thenReturn(new TimeWindow(1000L, 2000L));

        List<Double> elems = List.of(12.345);
        ListCollector collector = new ListCollector();

        fn.process(42, ctx, elems, collector);

        assertEquals(1, collector.out.size());
        assertEquals("sensor=42 window=[1000,2000) avg=12.345", collector.out.get(0));
    }
}

