package com.yourname.luftdaten;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AverageResultWindowFunction extends ProcessWindowFunction<Double, String, Integer, TimeWindow> {
    @Override
    public void process(Integer key, ProcessWindowFunction<Double, String, Integer, TimeWindow>.Context context, Iterable<Double> elements, Collector<String> out) throws Exception {
       Double avg = elements.iterator().hasNext() ? elements.iterator().next() : Double.NaN;
        out.collect(String.format("sensor=%d window=[%d,%d) avg=%.3f",
                key, context.window().getStart(), context.window().getEnd(), avg));    
    }
}
