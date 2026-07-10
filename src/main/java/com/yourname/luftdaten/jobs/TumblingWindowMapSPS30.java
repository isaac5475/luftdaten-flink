package com.yourname.luftdaten.jobs;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import com.yourname.luftdaten.AverageProcessingWithDatagenTimestampFunction;
import com.yourname.luftdaten.SPS30Parser;
import com.yourname.luftdaten.entities.SPS30Reading;

public class TumblingWindowMapSPS30 {
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

        withTimestamps.keyBy(SPS30Reading::getSensor_id).window(TumblingEventTimeWindows.of(Duration.ofMinutes(1))).aggregate(new AverageProcessingWithDatagenTimestampFunction<SPS30Reading>() {
                    @Override
                    protected Double getValue(SPS30Reading value) {
                        return value.getP2();
                    }
                }).map(r -> String.format("Average PM2.5: %.2f,%d\n", r.f0, r.f1))
        .writeToSocket(sinkHost, sinkPort, new SimpleStringSchema());

        // Execute program, beginning computation.
        env.execute("Tumbling window over seconds to compute average PM2 count for SPS30 readings and writing results to socket");
    }
}
