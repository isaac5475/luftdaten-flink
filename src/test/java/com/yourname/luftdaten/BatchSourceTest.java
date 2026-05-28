package com.yourname.luftdaten;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

import com.yourname.luftdaten.entities.Batch;

class BatchSourceTest {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
        LocalDateTime nowPlus1 = this.now.plusSeconds(1);
        LocalDateTime nowPlus2 = this.now.plusSeconds(2);
        LocalDateTime nowPlus3 = this.now.plusSeconds(3);
        LocalDateTime nowPlus4 = this.now.plusSeconds(4);

        List<String> measurements;

        {
                measurements = List.of(
                        "header values",
                        ";;;;;" + formatter.format(now),
                        ";;;;;" + formatter.format(nowPlus1),
                        ";;;;;" + formatter.format(nowPlus2),
                        ";;;;;" + formatter.format(nowPlus3),
                        ";;;;;" + formatter.format(nowPlus4)
                );
        }

        @Test
        void testBatchSource() throws Exception {
                SourceContext<Batch> ctx = (SourceContext<Batch>) mock(SourceContext.class);
                BatchSource sut = new BatchSource(measurements, 2);
                ArgumentCaptor<Batch> batchCaptor = ArgumentCaptor.forClass(Batch.class);
                ArgumentCaptor<Long> timestampCaptor = ArgumentCaptor.forClass(Long.class);
                ArgumentCaptor<Watermark> watermarkArgumentCaptor = ArgumentCaptor.forClass(Watermark.class);
                sut.run(ctx);
                verify(ctx, times(3)).collectWithTimestamp(batchCaptor.capture(), timestampCaptor.capture());
                verify(ctx, times(3)).emitWatermark(watermarkArgumentCaptor.capture());

                List<Long> ts = timestampCaptor.getAllValues();
                assertEquals(3, ts.size());
                assertEquals(nowPlus1.toInstant(java.time.ZoneOffset.UTC).toEpochMilli(), ts.get(0));
                assertEquals(nowPlus3.toInstant(java.time.ZoneOffset.UTC).toEpochMilli(), ts.get(1));
                assertEquals(nowPlus4.toInstant(java.time.ZoneOffset.UTC).toEpochMilli(), ts.get(2));

                List<Watermark> wm = watermarkArgumentCaptor.getAllValues();
                assertEquals(nowPlus1.toInstant(java.time.ZoneOffset.UTC).toEpochMilli(), wm.get(0).getTimestamp());
                assertEquals(nowPlus3.toInstant(java.time.ZoneOffset.UTC).toEpochMilli(), wm.get(1).getTimestamp());
                assertEquals(nowPlus4.toInstant(java.time.ZoneOffset.UTC).toEpochMilli(), wm.get(2).getTimestamp());

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

        }
}