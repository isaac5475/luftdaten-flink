
package com.yourname.luftdaten.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.yourname.luftdaten.AQICalculator;
import com.yourname.luftdaten.AQICategory;
import com.yourname.luftdaten.SPS30Parser;
import com.yourname.luftdaten.entities.SPS30Reading;

public class Q1AQIHazardLevelStatelessFilterSPS30 {
    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.

        if (args.length < 4) {
            System.err.println("Usage: DataStreamJob <kafka host:port> <port producing input> <topic> <host sink> <port sink>");
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
        DataStream<SPS30Reading> correctReadings = stream.map(SPS30Parser::parseReading).filter(reading -> reading.getSensor_id() != null && reading.getP1() != null && reading.getP2() != null);
        DataStream<SPS30Reading> withTimestamps = correctReadings.assignTimestampsAndWatermarks(WatermarkStrategy.<SPS30Reading>forMonotonousTimestamps().withTimestampAssigner((reading, timestamp) -> reading.getTimestamp()));

        withTimestamps.map(reading -> Tuple2.of(AQICalculator.aqi(reading.getP1(), reading.getP2()), reading)).returns(TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<>() {
                }))
                .filter(t -> AQICategory.of(t.f0).isAtLeast(AQICategory.MODERATE))
                .map(reading -> String.format("Category: %s, AQI: %d, P1: %.2f, P2: %.2f, Timestamp: %s,%d\n", AQICategory.of(reading.f0), reading.f0, reading.f1.getP1(), reading.f1.getP2(), reading.f1.getTimestamp().toString(), reading.f1.getDatagenTimestamp()))
                .writeToSocket(sinkHost, sinkPort, new SimpleStringSchema());

        // Execute program, beginning computation.
        env.execute("Air Quality Index filter and categorisation for SPS30 readings and writing results to socket");
    }
}
