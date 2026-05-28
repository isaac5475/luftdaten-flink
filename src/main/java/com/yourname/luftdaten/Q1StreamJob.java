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

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.yourname.luftdaten.entities.Batch;
import com.yourname.luftdaten.entities.PMS7003Reading;

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
public class Q1StreamJob {
    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.

        if (args.length < 1) {
            System.err.println("Usage: DataStreamJob <input path of the CSV file>");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<String> lines = FileUtils.readLines(new File(args[0]));
        BatchSource source = new BatchSource(lines, 100);
        DataStream<Batch> batches = env.addSource(source);
//        DataStream<String> filteredLines = batches.filter(line -> !line.startsWith("sensor_id"));
//        DataStream<PMS7003Reading> validReadings = filteredLines
//                .map(PMS7003Parser::parseReading)
//                .filter(reading -> reading.getSensor_id() != null && reading.getP0() != null && reading.getP1() != null && reading.getP2() != null);
//        validReadings
//                .keyBy(PMS7003Reading::getSensor_id)   // TODO aggregate by city, not each sensor individually
//                .window()
//        DataStream<PMS7003Reading> withTimestamps = validReadings.assignTimestampsAndWatermarks(WatermarkStrategy.<PMS7003Reading>forMonotonousTimestamps().withTimestampAssigner((reading, timestamp) -> reading.getTimestamp().toEpochMilli()));

        // Execute program, beginning computation.
        env.execute("Q1 Stream Job");
    }
}
