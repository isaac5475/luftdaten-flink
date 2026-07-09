package com.yourname.luftdaten;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.yourname.luftdaten.entities.Batch;

public class BatchTimestampSlidingWindow extends org.apache.flink.streaming.api.functions.KeyedProcessFunction<Long, Batch, List<Batch>> {
    private ListState<Tuple2<Batch, Long>> batchState;

    @Override
    public void open(OpenContext openContext) throws Exception {
        ListStateDescriptor<Tuple2<Batch, Long>> batchStateDescriptor = new ListStateDescriptor<>("batchState", TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<>() {
        }));
        batchState = getRuntimeContext().getListState(batchStateDescriptor);
    }

    @Override
    public void processElement(Batch value, KeyedProcessFunction<Long, Batch, List<Batch>>.Context ctx, Collector<List<Batch>> out) throws Exception {
        long currentWatermark = ctx.timerService().currentWatermark();
        long minusDayTimestamp = currentWatermark - ChronoUnit.DAYS.getDuration().toMillis();
        List<Tuple2<Batch, Long>> newBatches = new ArrayList<>();
        for (Tuple2<Batch, Long> tuple : batchState.get()) {
            if (tuple.f1 >  minusDayTimestamp) {
                newBatches.add(tuple);
            }
        }
        newBatches.add(Tuple2.of(value, currentWatermark));
        batchState.update(newBatches);
        out.collect(newBatches.stream().map(t -> t.f0).collect(Collectors.toList()));
    }
}
