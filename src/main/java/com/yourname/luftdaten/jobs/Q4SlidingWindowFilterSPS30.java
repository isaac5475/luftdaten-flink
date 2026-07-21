package com.yourname.luftdaten.jobs;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import com.yourname.luftdaten.AverageProcessingWithDatagenTimestampFunction;
import com.yourname.luftdaten.SPS30Parser;
import com.yourname.luftdaten.entities.SPS30Reading;

//  Technically speaking, this is a sliding window with filter

public class Q4SlidingWindowFilterSPS30 {

    private static final double THRESHOLD = 150;

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.

        if (args.length < 4) {
            System.err.println("Usage: DataStreamJob <input kafka host:port> <topic> <host sink> <port sink>");
            return;
        }

        String bootstrapServers = args[0];
        String topic = args[1];
        String sinkHost = args[2];
        int sinkPort = Integer.parseInt(args[3]);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("luftdaten-benchmark")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");
        DataStream<SPS30Reading> correctReadings = stream.map(SPS30Parser::parseReading).filter(reading -> reading.getSensor_id() != null && reading.getN05() != null);
        DataStream<SPS30Reading> withTimestamps = correctReadings.assignTimestampsAndWatermarks(WatermarkStrategy.<SPS30Reading>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((reading, timestamp) -> reading.getTimestamp()).withIdleness(Duration.ofSeconds(10)));

        withTimestamps.keyBy(SPS30Reading::getSensor_id).window(SlidingEventTimeWindows.of(Duration.ofHours(1), Duration.ofMinutes(10))).aggregate(new AverageProcessingWithDatagenTimestampFunction<>() {
                    @Override
                    protected Double getValue(SPS30Reading value) {
                        return value.getN05();
                    }
                }).filter(r -> r.f0 > THRESHOLD).map(r -> String.format("N05: %.2f,%d\n", r.f0, r.f1))
                .writeToSocket(sinkHost, sinkPort, new SimpleStringSchema());

        // Execute program, beginning computation.
        env.execute("Sliding window (1hr, 10min) to find spikes (greater than 150) of N05 levels for SPS30 readings and writing results to socket");
    }
}
