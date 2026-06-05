package com.yourname.luftdaten;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AverageResultWithTimestampWindowFunction extends ProcessWindowFunction<Tuple2<Double, Long>, Tuple2<String, Long>, Integer, TimeWindow> {
    @Override
    public void process(Integer key, ProcessWindowFunction<Tuple2<Double, Long>, Tuple2<String, Long>, Integer, TimeWindow>.Context context, Iterable<Tuple2<Double, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
        Tuple2<Double, Long> tuple = elements.iterator().hasNext() ? elements.iterator().next() : new Tuple2<>(Double.NaN, 0L);
        out.collect(new Tuple2<>(String.format("sensor=%d window=[%d,%d) avg=%.3f",
                key, context.window().getStart(), context.window().getEnd(), tuple.f0), tuple.f1));
    }
}
