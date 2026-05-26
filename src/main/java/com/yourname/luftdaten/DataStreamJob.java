/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yourname.luftdaten;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {
    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("src/main/resources/2020-01-01_bme280_sensor_141.csv")).build();
        DataStream<String> lines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
        DataStream<String> filteredLines = lines.filter(line -> !line.startsWith("sensor_id"));
        DataStream<BME280Reading> readings = filteredLines.map(line -> {
            String[] fields = line.split(";");
            BME280Reading bme280Reading = new BME280Reading();
            bme280Reading.setSensor_id(Integer.parseInt(fields[0]));
            bme280Reading.setLocation(Integer.parseInt(fields[2]));
            bme280Reading.setLat(Double.parseDouble(fields[3]));
            bme280Reading.setLon(Double.parseDouble(fields[4]));
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
            Instant timestamp = LocalDateTime.parse(fields[5], formatter).toInstant(java.time.ZoneOffset.UTC);
            bme280Reading.setTimestamp(timestamp);
            bme280Reading.setPressure(fields[6].isEmpty() ? null : Double.parseDouble(fields[6]));
            bme280Reading.setAltitude(fields[7].isEmpty() ? null : Double.parseDouble(fields[7]));
            bme280Reading.setPressureAtSeaLevel(fields[8].isEmpty() ? null : Double.parseDouble(fields[8]));
            bme280Reading.setTemperature(fields[9].isEmpty() ? null : Double.parseDouble(fields[9]));
            bme280Reading.setHumidity(fields[10].isEmpty() ? null : Double.parseDouble(fields[10]));
            return bme280Reading;
        });
        DataStream<BME280Reading> withTimestamps = readings.assignTimestampsAndWatermarks(WatermarkStrategy.<BME280Reading>forMonotonousTimestamps().withTimestampAssigner((reading, timestamp) -> reading.getTimestamp().toEpochMilli()));
        DataStream<String> tempAverageHourly = withTimestamps
                .keyBy(BME280Reading::getSensor_id)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
//                        .aggregate()
                .aggregate(new AggregateAverage(), new AverageResultWindowFunction());
        tempAverageHourly.print();
//        readings.map(reading -> String.format("[%s] Sensor %d, temperature %fC, humidity %f", reading.getTimestamp().toString(), reading.getSensor_id(), reading.getTemperature(), reading.getHumidity())).print();
        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.fromSequence(1, 10);
         *
         * then, transform the resulting DataStream<Long> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.window()
         * 	.process()
         *
         * and many more.
         * Have a look at the programming guide:
         *
         * https://nightlies.apache.org/flink/flink-docs-stable/
         *
         */

        // Execute program, beginning computation.
        env.execute("BME 280 basic parsing and map");
    }
}
