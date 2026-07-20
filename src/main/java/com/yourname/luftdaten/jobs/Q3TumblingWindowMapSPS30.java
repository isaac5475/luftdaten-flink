package com.yourname.luftdaten.jobs;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import com.yourname.luftdaten.AverageProcessingWithDatagenTimestampFunction;
import com.yourname.luftdaten.SPS30Parser;
import com.yourname.luftdaten.entities.SPS30Reading;

public class Q3TumblingWindowMapSPS30 {
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
                .setGroupId("luftdaten-" + System.currentTimeMillis())  // новая группа на каждый прогон
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");
        DataStream<SPS30Reading> correctReadings = stream.map(SPS30Parser::parseReading).filter(reading -> reading.getSensor_id() != null && reading.getP2() != null);
        DataStream<SPS30Reading> withTimestamps = correctReadings.assignTimestampsAndWatermarks(WatermarkStrategy.<SPS30Reading>forMonotonousTimestamps().withTimestampAssigner((reading, timestamp) -> reading.getTimestamp()));

        withTimestamps.keyBy(SPS30Reading::getSensor_id).window(TumblingEventTimeWindows.of(Duration.ofMinutes(1))).aggregate(new AverageProcessingWithDatagenTimestampFunction<>() {
                    @Override
                    protected Double getValue(SPS30Reading value) {
                        return value.getP2();
                    }
                }).map(r -> String.format("Average PM2.5: %.2f,%d\n", r.f0, r.f1))
                .writeToSocket(sinkHost, sinkPort, new SimpleStringSchema());

        // Execute program, beginning computation.
        env.execute("Tumbling window with size 1 minute to compute average PM2 count for SPS30 readings and writing results to socket");
    }
}
