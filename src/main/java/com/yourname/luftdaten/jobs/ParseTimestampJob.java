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

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import com.yourname.luftdaten.BME280Parser;
import com.yourname.luftdaten.ReadingParser;
import com.yourname.luftdaten.SensorReadingParser;
import com.yourname.luftdaten.entities.BME280Reading;
import com.yourname.luftdaten.entities.SensorReading;

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
public class ParseTimestampJob {
    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.

        if (args.length < 1) {
            System.err.println("Usage: DataStreamJob <input path of the CSV file>");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(args[0])).build();
        DataStream<String> lines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source")
                .filter(s -> !s.startsWith("sensor_id"))
                .map(s -> Tuple2.of(s, SensorReadingParser.parseReading(s))).returns(TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<>() {
                }))
                .map(tuple -> String.format("%s,%s", tuple.f0.replace(";",","), tuple.f1.getTimestamp()));
        FileSink<String> fileSink = FileSink
                .forRowFormat(new Path("tmp/luftdaten-output"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(15).toMillis())   // roll after 15m
                                .withInactivityInterval(Duration.ofMinutes(5).toMillis())  // roll if inactive 5m
                                .withMaxPartSize(1024 * 1024 * 1024)                        // 1024 MB
                                .build()
                )
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("part")
                        .withPartSuffix(".csv")
                        .build())
                .build();
        lines.sinkTo(fileSink);

        // Execute program, beginning computation.
        env.execute("Replace field split symbol ';' to ',' and add event-time timestamp in epoch milliseconds as last column");
    }
}
