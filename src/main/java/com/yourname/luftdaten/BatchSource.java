package com.yourname.luftdaten;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import com.yourname.luftdaten.entities.Batch;

public class BatchSource implements SourceFunction<Batch<String>> {

    private static final int TIMESTAMP_FIELD_IDX = 5;
    
    private int batchSize = 100;
    private final String filePath;
    private long count = 1L;
    private volatile boolean isRunning = false;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    public BatchSource(String filePath, int batchSize) throws IOException {
        this(filePath);
        this.batchSize = batchSize;
    }

    public BatchSource(String filePath) throws IOException {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<Batch<String>> ctx) throws Exception {
        long timestamp = Long.MIN_VALUE;
        isRunning = true;

        List<String> lst = new ArrayList<>();
        boolean firstRow = true;
        List<String> lines = FileUtils.readLines(new File(filePath));
        Iterator<String> iterator = lines.iterator();
        int batchId = 0;
        while (iterator.hasNext()) {
            String measurement = iterator.next();
            if (!isRunning) {
                break;
            }

            if (firstRow) {
                firstRow = false;
                continue; // skip header
            }
            lst.add(measurement);
            String timestampField = measurement.split(";")[TIMESTAMP_FIELD_IDX];
            Instant recordTimestamp = LocalDateTime.parse(timestampField, formatter).toInstant(java.time.ZoneOffset.UTC);
            timestamp = Math.max(timestamp, recordTimestamp.toEpochMilli());
            if (count % batchSize == 0) {
                Collections.shuffle(lst);
                Batch<String> batch = new Batch<>(new ArrayList<>(lst));
                batch.setLast(!iterator.hasNext());
                batch.setBatchId(batchId++);
                ctx.collectWithTimestamp(batch, timestamp);
                ctx.emitWatermark(new Watermark(timestamp));
                lst = new ArrayList<>();
            }
            count++;
        }
        if (isRunning && !lst.isEmpty()) {
            Batch<String> batch = new Batch<>(new ArrayList<>(lst));
            batch.setLast(true);
            batch.setBatchId(batchId);
            ctx.collectWithTimestamp(batch, timestamp);
            ctx.emitWatermark(new Watermark(timestamp));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
