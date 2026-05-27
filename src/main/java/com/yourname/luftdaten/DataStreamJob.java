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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import com.yourname.luftdaten.entities.BME280Reading;

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

        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("src/main/resources/2020-01_bme280.csv")).build();
        DataStream<String> lines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
        DataStream<String> filteredLines = lines.filter(line -> !line.startsWith("sensor_id"));
        DataStream<BME280Reading> correctReadings = filteredLines
                .map(BME280Parser::parseReading)
                .filter(reading -> reading.getSensor_id() != null && reading.getTemperature() != null);
        DataStream<BME280Reading> withTimestamps = correctReadings.assignTimestampsAndWatermarks(WatermarkStrategy.<BME280Reading>forMonotonousTimestamps().withTimestampAssigner((reading, timestamp) -> reading.getTimestamp().toEpochMilli()));
        DataStream<String> tempAverageHourly = withTimestamps
                .keyBy(BME280Reading::getSensor_id)
                .window(TumblingEventTimeWindows.of(Duration.ofDays(1)))
                .aggregate(new AggregateAverage(), new AverageResultWindowFunction());
        tempAverageHourly.print();

        // Execute program, beginning computation.
        env.execute("BME 280 basic parsing and map");
    }
}
