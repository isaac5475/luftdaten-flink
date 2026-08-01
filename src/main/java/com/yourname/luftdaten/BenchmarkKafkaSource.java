package com.yourname.luftdaten;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Shared Kafka source for every benchmark query (Q1-Q5), so all five read the
 * stream on identical terms and the offset semantics live in exactly one place.
 *
 * <h2>Why the starting offsets matter so much here</h2>
 *
 * Every query previously used {@code OffsetsInitializer.earliest()}. Combined
 * with {@code upgradeMode: stateless} in k8s/FlinkDeployment.yaml, that made
 * autoscaler-enabled runs measure the wrong thing entirely: the Flink
 * autoscaler applies a new parallelism by restarting the job, and a stateless
 * restart with {@code earliest()} rewinds the source to offset 0 and reprocesses
 * the WHOLE topic from the beginning. Measured consequences in the archived
 * runs: Q1 with the autoscaler on emitted 11,404,973 sink records against
 * 7,343,897 for the identical autoscaler-off run (+55% duplicates), reported
 * end-to-end latencies up to 639 seconds, and a "peak backlog" of 28.9M records
 * versus 4.2M without the autoscaler. None of those numbers described the
 * system's burst behaviour — they described the same records being replayed two
 * to four times.
 *
 * <p>{@code committedOffsets(EARLIEST)} fixes that with no other change:
 * checkpointing is enabled (30s interval), so the Kafka source commits offsets
 * to the consumer group on every checkpoint. A restart therefore resumes from
 * the last committed position, while a genuinely fresh run — the harness
 * deletes the consumer group and recreates the topic before every query — falls
 * back to EARLIEST and still reads the full stream exactly once.
 *
 * <p>Residual caveat, unchanged by this class: under {@code stateless} upgrade
 * mode a rescale still discards operator state, so windows in flight at the
 * rescale are lost for Q3/Q4/Q5. Removing that too requires
 * {@code upgradeMode: last-state} plus checkpoint storage shared between the
 * old and new pods (the current {@code file:///opt/flink/checkpoints} path is
 * pod-local, so last-state cannot restore as configured today). Up to ~30s of
 * records may also be reprocessed after a restart, bounded by the checkpoint
 * interval.
 *
 * <p>Set {@code KAFKA_STARTING_OFFSETS=earliest} to restore the old behaviour
 * when reproducing a pre-fix run.
 */
public final class BenchmarkKafkaSource {

    /** Fixed group id: survives autoscaler restarts, so offsets are not lost. */
    public static final String GROUP_ID = "luftdaten-benchmark";

    private BenchmarkKafkaSource() {
    }

    public static KafkaSource<String> create(String bootstrapServers, String topic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(startingOffsets())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    private static OffsetsInitializer startingOffsets() {
        String mode = System.getenv("KAFKA_STARTING_OFFSETS");
        if (mode != null && mode.equalsIgnoreCase("earliest")) {
            // Pre-fix behaviour, kept only for reproducing archived runs.
            return OffsetsInitializer.earliest();
        }
        return OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
    }
}
