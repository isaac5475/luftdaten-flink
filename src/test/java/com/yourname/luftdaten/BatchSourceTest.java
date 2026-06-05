package com.yourname.luftdaten;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.yourname.luftdaten.entities.Batch;

class BatchSourceTest {

        private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        private final LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
        private final LocalDateTime nowPlus1 = now.plusSeconds(1);
        private final LocalDateTime nowPlus2 = now.plusSeconds(2);
        private final LocalDateTime nowPlus3 = now.plusSeconds(3);
        private final LocalDateTime nowPlus4 = now.plusSeconds(4);

        @Test
        void testBatchSource() throws Exception {
                List<String> measurements = List.of(
                        "header values",
                        ";;;;;" + formatter.format(now),
                        ";;;;;" + formatter.format(nowPlus1),
                        ";;;;;" + formatter.format(nowPlus2),
                        ";;;;;" + formatter.format(nowPlus3),
                        ";;;;;" + formatter.format(nowPlus4)
                );

                Path tempCsv = writeTempCsv(measurements);

                try {
                        SourceContext<Batch<String>> ctx = (SourceContext<Batch<String>>) mock(SourceContext.class);
                        BatchSource sut = new BatchSource(tempCsv.toString(), 2);

                        ArgumentCaptor<Batch> batchCaptor = ArgumentCaptor.forClass(Batch.class);
                        ArgumentCaptor<Long> timestampCaptor = ArgumentCaptor.forClass(Long.class);
                        ArgumentCaptor<Watermark> watermarkCaptor = ArgumentCaptor.forClass(Watermark.class);

                        sut.run(ctx);

                        verify(ctx, times(3)).collectWithTimestamp(batchCaptor.capture(), timestampCaptor.capture());
                        verify(ctx, times(3)).emitWatermark(watermarkCaptor.capture());

                        List<Long> ts = timestampCaptor.getAllValues();
                        assertEquals(3, ts.size());
                        assertEquals(nowPlus1.toInstant(ZoneOffset.UTC).toEpochMilli(), ts.get(0));
                        assertEquals(nowPlus3.toInstant(ZoneOffset.UTC).toEpochMilli(), ts.get(1));
                        assertEquals(nowPlus4.toInstant(ZoneOffset.UTC).toEpochMilli(), ts.get(2));

                        List<Watermark> wm = watermarkCaptor.getAllValues();
                        assertEquals(nowPlus1.toInstant(ZoneOffset.UTC).toEpochMilli(), wm.get(0).getTimestamp());
                        assertEquals(nowPlus3.toInstant(ZoneOffset.UTC).toEpochMilli(), wm.get(1).getTimestamp());
                        assertEquals(nowPlus4.toInstant(ZoneOffset.UTC).toEpochMilli(), wm.get(2).getTimestamp());

                        List<Batch> batches = batchCaptor.getAllValues();
                        assertEquals(3, batches.size());
                        assertFalse(batches.get(0).isLast());
                        assertFalse(batches.get(1).isLast());
                        assertTrue(batches.get(2).isLast());

                        assertEquals(new HashSet<>(measurements.subList(1, 3)), new HashSet<>(batches.get(0).getMeasurements()));
                        assertEquals(new HashSet<>(measurements.subList(3, 5)), new HashSet<>(batches.get(1).getMeasurements()));
                        assertEquals(new HashSet<>(measurements.subList(5, 6)), new HashSet<>(batches.get(2).getMeasurements()));

                        assertEquals(0, batches.get(0).getBatchId());
                        assertEquals(1, batches.get(1).getBatchId());
                        assertEquals(2, batches.get(2).getBatchId());
                } finally {
                        Files.deleteIfExists(tempCsv);
                }
        }

        private Path writeTempCsv(List<String> lines) throws IOException {
                Path tempFile = Files.createTempFile("batch-source-test", ".csv");
                Files.write(tempFile, lines);
                return tempFile;
        }
}