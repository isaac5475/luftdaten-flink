package com.yourname.luftdaten.jobs;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import com.yourname.luftdaten.AverageProcessingWithDatagenTimestampFunction;
import com.yourname.luftdaten.SPS30Parser;
import com.yourname.luftdaten.entities.SPS30Reading;

//  Technically speaking, this is a sliding window with filter

public class SlidingWindowFilterSPS30 {

    private static final double THRESHOLD = 150;

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
        DataStream<SPS30Reading> correctReadings = filteredLines.map(SPS30Parser::parseReading).filter(reading -> reading.getSensor_id() != null && reading.getN05() != null);
        DataStream<SPS30Reading> withTimestamps = correctReadings.assignTimestampsAndWatermarks(WatermarkStrategy.<SPS30Reading>forMonotonousTimestamps().withTimestampAssigner((reading, timestamp) -> reading.getTimestamp().toEpochMilli()));

        withTimestamps.keyBy(SPS30Reading::getSensor_id).window(SlidingEventTimeWindows.of(Duration.ofMinutes(10), Duration.ofMinutes(1))).aggregate(new AverageProcessingWithDatagenTimestampFunction<>() {
                    @Override
                    protected Double getValue(SPS30Reading value) {
                        return value.getN05();
                    }
                }).filter(r -> r.f0 > THRESHOLD).map(r -> String.format("N05: %.2f,%d", r.f0, r.f1))
                .writeToSocket(sinkHost, sinkPort, new SimpleStringSchema());

        // Execute program, beginning computation.
        env.execute("Sliding window (10min, 1min) to find spikes (greater than 150) of N05 levels for SPS30 readings and writing results to socket");
    }
}
