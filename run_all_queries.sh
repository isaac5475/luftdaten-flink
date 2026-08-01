#!/bin/bash
# run_all_queries.sh - Benchmark orchestrator for all 5 SPS30 queries
#
# Runs each query sequentially (they must stay sequential - one query at a time
# owns the cluster, and run.sh's flock enforces that) with per-query result
# directories, a suite manifest, and configuration tracking for reproducibility.
#
# Usage:
#   ./run_all_queries.sh [--sleep SECONDS] [--skip-build] [--config-name TAG]
#                        [--isolate] [--queries Q1,Q3] [--allow-dirty]
#
# Examples:
#   # Full suite at the configured datagen duration + drain
#   ./run_all_queries.sh --config-name "half_15m1_autoscaler_off"
#
#   # Faster iteration
#   ./run_all_queries.sh --sleep 180 --skip-build --config-name "iteration_test"
#
#   # Flink-only measurement: scale the NebulaStream pods to zero for the suite
#   ./run_all_queries.sh --isolate --config-name "half_15m1_autoscaler_on"
#
# What changed and why (the previous version's costs, measured on the
# 2026-07-29 suite: 4114s wall clock for 3300s of observation window):
#   * Results now go straight into a per-query directory via run.sh --run-dir.
#     Before, artifacts were swept out of the shared benchmark_timeline/ and
#     plots/ directories with `find -newer <a yaml file>`, which silently
#     depended on file mtimes: a file written slightly too early was missed, and
#     any unrelated file touched during the run (an editor swap file, for
#     instance) was moved into the results.
#   * Cluster prerequisites (minikube, metrics-server, cert-manager, the Flink
#     operator, Strimzi, the Kafka CR, the python venv) are checked ONCE for the
#     suite instead of once per query (~13s/query, and far worse on a cold
#     cluster where a single query could burn a 600s Kafka-ready wait).
#   * The per-query latency log is left in its own directory rather than being
#     copied through a shared ./latency-logs/ that nothing ever cleared - that
#     shared directory is how Q4 of the half-dataset suite ended up containing
#     1.2 GB of NUL bytes followed by Q3's records.
#   * A completion gate refuses to finalise a query directory that has no
#     latency data, so a failed query is never archived as if it had succeeded.
#   * A suite-level manifest records what ran, on which commit, with which
#     load configuration, plus sha256 of every artifact.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CUSTOM_SLEEP=""
SKIP_BUILD=false
ISOLATE=false
ALLOW_DIRTY=false
CONFIG_NAME=""
QUERY_FILTER=""
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sleep)        CUSTOM_SLEEP="$2"; shift 2 ;;
        --skip-build)   SKIP_BUILD=true; shift ;;
        --config-name)  CONFIG_NAME="$2"; shift 2 ;;
        --isolate)      ISOLATE=true; shift ;;
        --queries)      QUERY_FILTER="$2"; shift 2 ;;
        --allow-dirty)  ALLOW_DIRTY=true; shift ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: ./run_all_queries.sh [--sleep SECONDS] [--skip-build] [--config-name TAG] [--isolate] [--queries Q1,Q3] [--allow-dirty]"
            exit 1 ;;
    esac
done

source .env

FLINK_DEPLOYMENT_YAML="$SCRIPT_DIR/k8s/FlinkDeployment.yaml"
DATAGEN_CONFIG_YAML="$SCRIPT_DIR/k8s/DatagenConfig.yaml"

if ! grep -q "entryClass" "$FLINK_DEPLOYMENT_YAML"; then
    echo "ERROR: $FLINK_DEPLOYMENT_YAML has no 'entryClass' field under spec.job."
    exit 1
fi

# ---------------------------------------------------------------------------
# Derive a self-describing default label from the configuration that will
# actually run, so a results directory can never again claim a load it did not
# receive. (The archive currently contains a directory called
# "..._quarter_7m6_600s_..." that was measured at the HALF rate with the
# autoscaler off - a 2x error that took reconstructing Kafka offsets to catch.)
# ---------------------------------------------------------------------------
dg() { awk -v key="$1" '
    /config\.kafka\.yaml:/ {inblock=1; next}
    /^  [a-z._]+\.yaml:/  {if (inblock) exit}
    inblock && $1 == key":" {gsub(/#.*/,"",$2); print $2+0; exit}
' "$DATAGEN_CONFIG_YAML"; }

DATA_RATE=$(dg data_rate)
PERIOD_MS=$(dg period)
DURATION=$(dg duration)
AUTOSCALER=$(awk -F'"' '/job.autoscaler.enabled:/ {print $2; exit}' "$FLINK_DEPLOYMENT_YAML")
PARTITIONS=$(awk '/partitions:/ {print $2; exit}' k8s/topic.yaml)
AS_LABEL=$([ "$AUTOSCALER" = "true" ] && echo "autoscaler_on" || echo "autoscaler_off")

if [ -z "$CONFIG_NAME" ]; then
    RATE_LABEL=$(awk -v r="$DATA_RATE" 'BEGIN{printf "%.2fm", r/1000000}')
    CONFIG_NAME="rate_${RATE_LABEL}_period_$((PERIOD_MS/1000))s_dur_${DURATION}s_kafka_${PARTITIONS}p_${AS_LABEL}"
    echo "No --config-name given; derived from the live configuration: $CONFIG_NAME"
fi

GIT_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo unknown)
GIT_DIRTY=$(test -n "$(git status --porcelain 2>/dev/null)" && echo true || echo false)
if [ "$GIT_DIRTY" = true ] && [ "$ALLOW_DIRTY" = false ]; then
    echo "ERROR: the working tree is dirty, so this suite would not be reproducible from git."
    echo "       Commit first, or pass --allow-dirty (the diff is then stored in the suite manifest)."
    echo
    git status --porcelain | head -20
    exit 1
fi

JOBS=(
    "com.yourname.luftdaten.jobs.Q1AQIHazardLevelStatelessFilterSPS30:Q1AQIHazardLevelStatelessFilterSPS30"
    "com.yourname.luftdaten.jobs.Q2CoarseParticleDominanceFilterSPS30:Q2CoarseParticleDominanceFilterSPS30"
    "com.yourname.luftdaten.jobs.Q3TumblingWindowMapSPS30:Q3TumblingWindowMapSPS30"
    "com.yourname.luftdaten.jobs.Q4SlidingWindowFilterSPS30:Q4SlidingWindowFilterSPS30"
    "com.yourname.luftdaten.jobs.Q5SlidingWindowExtendedAverageFilter:Q5SlidingWindowExtendedAverageFilter"
)

if [ -n "$QUERY_FILTER" ]; then
    FILTERED=()
    IFS=',' read -ra WANTED <<< "$QUERY_FILTER"
    for entry in "${JOBS[@]}"; do
        for want in "${WANTED[@]}"; do
            case "${entry##*:}" in "$want"*) FILTERED+=("$entry") ;; esac
        done
    done
    JOBS=("${FILTERED[@]}")
    [ ${#JOBS[@]} -eq 0 ] && { echo "ERROR: --queries '$QUERY_FILTER' matched nothing."; exit 1; }
fi

RESULTS_DIR="benchmark_results/${CONFIG_NAME}_${TIMESTAMP}"
[ -d "$RESULTS_DIR" ] && { echo "ERROR: $RESULTS_DIR already exists."; exit 1; }
mkdir -p "$RESULTS_DIR"

echo "============================================================"
echo "Flink Benchmark Suite"
echo "============================================================"
echo "Label        : $CONFIG_NAME"
echo "Queries      : ${#JOBS[@]}"
echo "Load         : data_rate=$DATA_RATE/period, period=${PERIOD_MS}ms, duration=${DURATION}s"
echo "Autoscaler   : ${AUTOSCALER:-unset}   Kafka partitions: $PARTITIONS"
echo "Commit       : ${GIT_COMMIT:0:12} (dirty: $GIT_DIRTY)"
echo "Results      : $RESULTS_DIR"
[ -n "$CUSTOM_SLEEP" ] && echo "Window       : ${CUSTOM_SLEEP}s (overridden)" || echo "Window       : datagen duration + drain margin (derived by run.sh)"
echo "Isolate NS   : $ISOLATE"
echo ""

# ---------------------------------------------------------------------------
# Optional isolation of the NebulaStream side. Its pods were resident during
# past Flink measurements (19 pods vs 8), holding ~870 MiB. They were idle, so
# the perturbation was small, but a Flink-only baseline should be Flink-only.
# ---------------------------------------------------------------------------
NS_RESTORE=()
if [ "$ISOLATE" = true ]; then
    echo "Scaling NebulaStream-side workloads to zero for this suite..."
    for res in deployment/nebula-worker deployment/nebula-cli deployment/rtracker-nebula; do
        if kubectl get "$res" >/dev/null 2>&1; then
            REPLICAS=$(kubectl get "$res" -o jsonpath='{.spec.replicas}')
            NS_RESTORE+=("$res=$REPLICAS")
            kubectl scale "$res" --replicas=0
        fi
    done
    if kubectl get statefulset kafka-tcp-bridge -n kafka >/dev/null 2>&1; then
        REPLICAS=$(kubectl get statefulset kafka-tcp-bridge -n kafka -o jsonpath='{.spec.replicas}')
        NS_RESTORE+=("statefulset/kafka-tcp-bridge:kafka=$REPLICAS")
        kubectl scale statefulset kafka-tcp-bridge -n kafka --replicas=0
    fi
    echo "  will restore: ${NS_RESTORE[*]:-nothing}"
fi

restore_isolation() {
    [ ${#NS_RESTORE[@]} -eq 0 ] && return 0
    echo "Restoring NebulaStream-side replica counts..."
    for entry in "${NS_RESTORE[@]}"; do
        target="${entry%%=*}"; count="${entry##*=}"
        if [[ "$target" == *":kafka" ]]; then
            kubectl scale "${target%%:kafka}" -n kafka --replicas="$count" || true
        else
            kubectl scale "$target" --replicas="$count" || true
        fi
    done
}
trap restore_isolation EXIT

# ---------------------------------------------------------------------------
# Suite loop
# ---------------------------------------------------------------------------
QUERY_NUM=1
TOTAL_QUERIES=${#JOBS[@]}
FIRST_JOB=true
SUITE_START=$(date +%s)
declare -a SUMMARY_LINES

for entry in "${JOBS[@]}"; do
    ENTRY_CLASS="${entry%%:*}"
    QUERY_NAME="${entry##*:}"
    QUERY_RESULT_DIR="$RESULTS_DIR/$QUERY_NAME"
    mkdir -p "$QUERY_RESULT_DIR"

    echo "------------------------------------------------------------"
    echo "[$(date +'%H:%M:%S')] Query $QUERY_NUM/$TOTAL_QUERIES: $QUERY_NAME"
    echo "------------------------------------------------------------"

    sed -i "s|entryClass:.*|entryClass: ${ENTRY_CLASS}|" "$FLINK_DEPLOYMENT_YAML"

    RUN_ARGS=(--run-dir "$QUERY_RESULT_DIR")
    if [ "$FIRST_JOB" = true ]; then
        # First query does the one-time work: image build/load (unless skipped)
        # and the cluster prerequisite checks.
        [ "$SKIP_BUILD" = true ] && RUN_ARGS+=(--skip-build)
        # The burst pattern (data_rate/period/duration/fraction/distribution)
        # is the same DatagenConfig for every query in the suite, so it is
        # captured once here rather than once per query, straight into the
        # suite root (not a per-query directory) since it describes the whole
        # suite's load, not any single query's run.
        RUN_ARGS+=(--plot-pattern-out "$RESULTS_DIR/burst_pattern.png")
    else
        RUN_ARGS+=(--skip-build --skip-infra)
    fi

    QUERY_START=$(date +%s)
    if [ -n "$CUSTOM_SLEEP" ]; then
        SLEEP_SECONDS="$CUSTOM_SLEEP" "$SCRIPT_DIR/run.sh" "${RUN_ARGS[@]}" || {
            echo "!! $QUERY_NAME FAILED — continuing with the rest of the suite."
            SUMMARY_LINES+=("$QUERY_NAME FAILED")
            QUERY_NUM=$((QUERY_NUM + 1)); FIRST_JOB=false; continue
        }
    else
        # Unset SLEEP_SECONDS so run.sh derives the window from the datagen
        # duration instead of a hardcoded number that has to be kept in sync.
        env -u SLEEP_SECONDS "$SCRIPT_DIR/run.sh" "${RUN_ARGS[@]}" || {
            echo "!! $QUERY_NAME FAILED — continuing with the rest of the suite."
            SUMMARY_LINES+=("$QUERY_NAME FAILED")
            QUERY_NUM=$((QUERY_NUM + 1)); FIRST_JOB=false; continue
        }
    fi
    QUERY_SECONDS=$(( $(date +%s) - QUERY_START ))
    FIRST_JOB=false

    # --- completion gate -----------------------------------------------------
    LATENCY_LOG="$QUERY_RESULT_DIR/latency_output.log"
    if [ ! -s "$LATENCY_LOG" ]; then
        echo "!! $QUERY_NAME produced no latency data — marking the directory INCOMPLETE."
        touch "$QUERY_RESULT_DIR/INCOMPLETE"
        SUMMARY_LINES+=("$QUERY_NAME INCOMPLETE (no latency data) ${QUERY_SECONDS}s")
        QUERY_NUM=$((QUERY_NUM + 1))
        continue
    fi

    LINES=$(wc -l < "$LATENCY_LOG")
    SPAN=$(awk -F',' 'NR==1{f=$NF} {l=$NF} END{printf "%.0f", (l-f)/1000}' "$LATENCY_LOG")

    # --- per-query timeline plot --------------------------------------------
    PY=venv/bin/python3; [ -x "$PY" ] || PY=python3
    "$PY" plot_benchmark_timeline.py "$QUERY_RESULT_DIR" \
        -o "$QUERY_RESULT_DIR/timeline_${QUERY_NAME}.png" \
        --title "$QUERY_NAME — $CONFIG_NAME" >/dev/null 2>&1 \
        || echo "  (timeline plot failed for $QUERY_NAME)"

    # --- archive the big log -------------------------------------------------
    # 0.8-4 GB of text per query compresses ~8x. Done after the measurement, so
    # it costs nothing that matters, and every analysis script reads .gz.
    if command -v gzip >/dev/null 2>&1; then
        gzip -f "$LATENCY_LOG"
        echo "  latency log gzipped ($(du -h "${LATENCY_LOG}.gz" | cut -f1))"
    fi

    SUMMARY_LINES+=("$QUERY_NAME ok ${QUERY_SECONDS}s lines=${LINES} data_span=${SPAN}s")
    echo "✓ $QUERY_NAME: ${LINES} latency rows spanning ${SPAN}s, cycle ${QUERY_SECONDS}s"
    QUERY_NUM=$((QUERY_NUM + 1))
done

SUITE_SECONDS=$(( $(date +%s) - SUITE_START ))

# ---------------------------------------------------------------------------
# Suite manifest: what ran, on what, with checksums.
# ---------------------------------------------------------------------------
MANIFEST="$RESULTS_DIR/SUITE_MANIFEST.md"
{
    echo "# Benchmark suite: $CONFIG_NAME"
    echo
    echo "- started: $(date -u -d "@$SUITE_START" +"%Y-%m-%dT%H:%M:%SZ")"
    echo "- wall clock: ${SUITE_SECONDS}s"
    echo "- git commit: $GIT_COMMIT (dirty: $GIT_DIRTY)"
    echo "- autoscaler: ${AUTOSCALER:-unset}"
    echo "- datagen: data_rate=$DATA_RATE per ${PERIOD_MS}ms period, duration=${DURATION}s"
    echo "- kafka partitions: $PARTITIONS"
    echo "- NebulaStream isolated: $ISOLATE"
    echo "- host: $(hostname)"
    [ -s "$RESULTS_DIR/burst_pattern.png" ] && echo "- burst pattern: burst_pattern.png (same load pattern for every query above)"
    echo
    echo "## Queries"
    echo
    for line in "${SUMMARY_LINES[@]}"; do echo "- $line"; done
    echo
    echo "## Analysis"
    echo
    echo '```'
    echo "python3 analyze_all_results.py $RESULTS_DIR"
    echo '```'
} > "$MANIFEST"

if [ "$GIT_DIRTY" = true ]; then
    git diff > "$RESULTS_DIR/uncommitted.diff" 2>/dev/null || true
fi

# Checksums: the one artifact that would have caught the 92%-NUL latency log.
( cd "$RESULTS_DIR" && find . -type f ! -name MANIFEST.sha256 -exec sha256sum {} + > MANIFEST.sha256 )

echo ""
echo "============================================================"
echo "Suite finished in ${SUITE_SECONDS}s"
echo "Results:  $RESULTS_DIR"
echo "Manifest: $MANIFEST"
echo "Analyse:  python3 analyze_all_results.py $RESULTS_DIR"
echo "============================================================"
