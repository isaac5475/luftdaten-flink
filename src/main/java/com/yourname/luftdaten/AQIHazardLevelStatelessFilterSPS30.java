
package com.yourname.luftdaten;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.yourname.luftdaten.entities.SPS30Reading;

public class AQIHazardLevelStatelessFilterSPS30 {
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

        withTimestamps.map(reading -> Tuple2.of(AQICalculator.aqi(reading.getP1(), reading.getP2()), reading)).returns(TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<Integer, SPS30Reading>>() {
                }))
                .filter(t -> AQICategory.of(t.f0).isAtLeast(AQICategory.MODERATE))
                .map(reading -> String.format("Category: %s, AQI: %d, P1: %.2f, P2: %.2f, Timestamp: %s,%d\n", AQICategory.of(reading.f0), reading.f0, reading.f1.getP1(), reading.f1.getP2(), reading.f1.getTimestamp().toString(), reading.f1.getDatagenTimestamp()))
                .writeToSocket(sinkHost, sinkPort, new SimpleStringSchema());

        // Execute program, beginning computation.
        env.execute("Air Quality Index filter and categorisation for SPS30 readings and writing results to socket");
    }
}
