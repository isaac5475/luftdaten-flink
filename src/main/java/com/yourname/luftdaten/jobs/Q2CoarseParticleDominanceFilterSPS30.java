
package com.yourname.luftdaten.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.yourname.luftdaten.SPS30Parser;
import com.yourname.luftdaten.entities.SPS30Reading;

public class Q2CoarseParticleDominanceFilterSPS30 {

    private static final double THRESHOLD = 0.9;

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
        DataStream<SPS30Reading> correctReadings = stream.map(SPS30Parser::parseReading).filter(reading -> reading.getSensor_id() != null && reading.getP0() != null && reading.getP1() != null);
        DataStream<SPS30Reading> withTimestamps = correctReadings.assignTimestampsAndWatermarks(WatermarkStrategy.<SPS30Reading>forMonotonousTimestamps().withTimestampAssigner((reading, timestamp) -> reading.getTimestamp()));

        withTimestamps.filter(reading -> {
                    if (reading.getP1() == 0) {
                        return false;
                    }
                    double ratio = reading.getP0() / reading.getP1();
                    return ratio < THRESHOLD;
                })
                .map(reading -> String.format("Sensor %d, P0: %.2f, P1: %.2f, P0/P1: %.2f,%d\n", reading.getSensor_id(), reading.getP0(), reading.getP1(), reading.getP0() / reading.getP1(), reading.getDatagenTimestamp()))
                .writeToSocket(sinkHost, sinkPort, new SimpleStringSchema());

        // Execute program, beginning computation.
        env.execute("Coarse Particle Dominance filter outputs SPS30 readings so that P0 / P1 is less than 0.9 and writes results to socket");
    }
}
