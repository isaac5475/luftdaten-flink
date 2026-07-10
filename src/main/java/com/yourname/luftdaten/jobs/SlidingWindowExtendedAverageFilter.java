package com.yourname.luftdaten.jobs;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import com.yourname.luftdaten.CrossSpectrumAggregateFunction;
import com.yourname.luftdaten.SPS30Parser;
import com.yourname.luftdaten.entities.SPS30Reading;

public class SlidingWindowExtendedAverageFilter {

    private static final double P2_THRESHOLD  = 18.0;
    private static final double N1_THRESHOLD  = 120.0;
    private static final double N05_THRESHOLD = 140.0;
    private static final double TS_THRESHOLD  = 0.49;

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: CrossSpectrumAnomalySPS30 <srcHost> <srcPort> <sinkHost> <sinkPort>");
            return;
        }

        String sourceHost = args[0], sinkHost = args[2];
        int sourcePort = Integer.parseInt(args[1]), sinkPort = Integer.parseInt(args[3]);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SPS30Reading> withTimestamps = env.socketTextStream(sourceHost, sourcePort)
                .filter(line -> !line.startsWith("sensor_id"))
                .map(SPS30Parser::parseReading)
                .filter(r -> r.getSensor_id() != null && r.getP2() != null
                        && r.getN1() != null && r.getN05() != null && r.getTS() != null)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SPS30Reading>forMonotonousTimestamps()
                                .withTimestampAssigner((r, ts) -> r.getTimestamp().toEpochMilli()));

        withTimestamps
                .keyBy(SPS30Reading::getSensor_id)
                .window(SlidingEventTimeWindows.of(Duration.ofHours(1), Duration.ofMinutes(10)))
                .aggregate(new CrossSpectrumAggregateFunction())
                .filter(r -> r.f0 > P2_THRESHOLD
                        && r.f1 > N1_THRESHOLD
                        && r.f2 > N05_THRESHOLD
                        && r.f3 < TS_THRESHOLD)
                .map((Tuple5<Double, Double, Double, Double, Long> r) ->
                        String.format("AvgP2: %.2f, AvgN1: %.2f, AvgN05: %.2f, AvgTS: %.3f,%d\n",
                                r.f0, r.f1, r.f2, r.f3, r.f4))
                .writeToSocket(sinkHost, sinkPort, new SimpleStringSchema());

        env.execute("Cross-spectrum anomaly detection (P2+N1+N05+TS) sliding window (1hr, 10min) for SPS30");
    }
}