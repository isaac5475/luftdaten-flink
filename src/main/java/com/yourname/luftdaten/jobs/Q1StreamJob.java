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

package com.yourname.luftdaten.jobs;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.yourname.luftdaten.BatchSource;
import com.yourname.luftdaten.PMS7003Parser;
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
        BatchSource source = new BatchSource(args[0], 100);
        DataStream<Batch<String>> batches = env.addSource(source);
        DataStream<Batch<String>> filteredBatches = batches.map(batch -> {
            if (!batch.getMeasurements().isEmpty()) {
                String firstMeasurement = batch.getMeasurements().get(0);
                if (firstMeasurement.startsWith("sensor_id")) {
                    batch.getMeasurements().remove(0);
                }
            }
            return batch;
        }).returns(new TypeHint<Batch<String>>() {});
        DataStream<Batch<PMS7003Reading>> readings = filteredBatches.map(batch -> {
            List<PMS7003Reading> parsedMeasurements = batch.getMeasurements().stream()
                    .map(PMS7003Parser::parseReading).collect(Collectors.toCollection(ArrayList::new));
            Batch<PMS7003Reading> newBatch = new Batch<>(parsedMeasurements);
            newBatch.setLast(batch.isLast());
            newBatch.setBatchId(batch.getBatchId());
            return newBatch;
        }).returns(new TypeHint<Batch<PMS7003Reading>>() {});
        readings.print();

        // Execute program, beginning computation.
        env.execute("Q1 Stream Job");
    }
}
