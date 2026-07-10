
package com.yourname.luftdaten;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.yourname.luftdaten.entities.SPS30Reading;

public class CoarseParticleDominanceFilterSPS30 {

    private static final double THRESHOLD = 0.9;

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

        withTimestamps.filter(reading -> {
                    double ratio = reading.getP0() / reading.getP1();
                    if (reading.getP1() == 0) {
                        return false;
                    }
                    return ratio < THRESHOLD;
                })
                .map(reading -> String.format("Sensor %d, P1: %.2f, P2: %.2f, P2/P1: %.2f,%d\n", reading.getSensor_id(), reading.getP1(), reading.getP2(), reading.getP2() / reading.getP1(), reading.getDatagenTimestamp()))
//                .print();
                .writeToSocket(sinkHost, sinkPort, new SimpleStringSchema());

        // Execute program, beginning computation.
        env.execute("Coarse Particle Dominance filter outputs SPS30 readings so that P1 / P0 is less than 0.1 and writes results to socket");
    }
}
