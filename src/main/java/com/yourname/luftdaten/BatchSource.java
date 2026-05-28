package com.yourname.luftdaten;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import com.yourname.luftdaten.entities.Batch;

public class BatchSource implements SourceFunction<Batch> {

    private static final int TIMESTAMP_FIELD_IDX = 5;
    
    private int batchSize = 100;
    private final Iterable<String> measurementsIterator;
    private long count = 1L;
    private volatile boolean isRunning = false;
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    public BatchSource(Iterable<String> measurementsIterator, int batchSize) throws IOException {
        this(measurementsIterator);
        this.batchSize = batchSize;
    }

    public BatchSource(Iterable<String> measurementsIterator) throws IOException {
        this.measurementsIterator = measurementsIterator;

    }

    @Override
    public void run(SourceContext<Batch> ctx) throws Exception {
        long timestamp = Long.MIN_VALUE;
        isRunning = true;

        java.util.List<String> lst = new ArrayList<>();
        boolean firstRow = true;
        Iterator<String> iterator = measurementsIterator.iterator();
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
                Batch batch = new Batch(lst);
                batch.setLast(!iterator.hasNext());
                batch.setBatchId(batchId++);
                ctx.collectWithTimestamp(batch, timestamp);
                ctx.emitWatermark(new Watermark(timestamp));
                lst = new ArrayList<>();
            }
            count++;
        }
        if (isRunning && !lst.isEmpty()) {
            Batch batch = new Batch(lst);
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
