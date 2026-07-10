package com.yourname.luftdaten;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import com.yourname.luftdaten.entities.SPS30Reading;

public class SlidingWPM2AlertSPS30 {
    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.

        if (args.length < 4) {
            System.err.println("Usage: DataStreamJob <host producing input> <port producing input> <sink host> <port sink>");
            return;
        }

        String sourceHost = args[0], sinkHost = args[2];
        int sourcePort = Integer.parseInt(args[1]), sinkPort = Integer.parseInt(args[3]);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.socketTextStream(sourceHost, sourcePort);
        DataStream<String> filteredLines = stream.filter(line -> !line.startsWith("sensor_id"));
        DataStream<SPS30Reading> correctReadings = filteredLines.map(SPS30Parser::parseReading).filter(reading -> reading.getSensor_id() != null && reading.getP0() != null);
        DataStream<SPS30Reading> withTimestamps = correctReadings.assignTimestampsAndWatermarks(WatermarkStrategy.<SPS30Reading>forMonotonousTimestamps().withTimestampAssigner((reading, timestamp) -> reading.getTimestamp().toEpochMilli()));

        withTimestamps.keyBy(SPS30Reading::getSensor_id).window(SlidingEventTimeWindows.of(Duration.ofHours(1), Duration.ofMinutes(10)))
                .process(new ProcessWindowFunction<SPS30Reading, Tuple2<Double, Alert>, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, ProcessWindowFunction<SPS30Reading, Tuple2<Double, Alert>, Integer, TimeWindow>.Context context, Iterable<SPS30Reading> elements, Collector<Tuple2<Double, Alert>> out) {
                        List<SPS30Reading> list = StreamSupport
                                .stream(elements.spliterator(), false)
                                .collect(Collectors.toList());

                        double avg = list.stream().mapToDouble(SPS30Reading::getP2).average().orElse(0);
                        long maxTimestamp = list.stream().mapToLong(SPS30Reading::getDatagenTimestamp).max().orElse(Long.MIN_VALUE);
                        list.stream().filter(r -> r.getP2() > 1.5 * avg).map(spike -> new Alert(spike.getSensor_id(), maxTimestamp, spike.getP2())).forEach(a -> out.collect(Tuple2.of(avg, a)));
                    }
                })
                .map(r -> String.format("Sensor: %d, Avg: %.2f, Alerts(%.2f),%d\n", r.f1.getSensorId(), r.f0, r.f1.getP2(), r.f1.getDatagen_timestamp()))
                .writeToSocket(sinkHost, sinkPort, new SimpleStringSchema());
        // Execute program, beginning computation.
        env.execute("Sliding window (1hr, 10min) to find spikes (3 times of window's average) of PM2 levels for SPS30 readings and writing results to socket");
    }
}
