#!/bin/bash
# run_all_queries.sh - Benchmark orchestrator for all 5 SPS30 queries
#
# Runs each query sequentially with proper Kafka topic cleanup and
# configuration tracking for thesis reproducibility.
#
# Usage:
#   ./run_all_queries.sh [--sleep SECONDS] [--skip-build] [--config-name TAG]
#
# Examples:
#   # Full 600s per query, recommended for thesis runs
#   ./run_all_queries.sh --config-name "quarter_dataset_bursty_600s"
#
#   # Faster iteration (300s per query)
#   ./run_all_queries.sh --sleep 300 --skip-build --config-name "iteration_test"

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CUSTOM_SLEEP=""
SKIP_BUILD=false
CONFIG_NAME="benchmark_run"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sleep)
            CUSTOM_SLEEP="$2"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --config-name)
            CONFIG_NAME="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: ./run_all_queries.sh [--sleep SECONDS] [--skip-build] [--config-name TAG]"
            exit 1
            ;;
    esac
done

# Load base env
source .env
if [ -n "$CUSTOM_SLEEP" ]; then
    SLEEP_SECONDS="$CUSTOM_SLEEP"
fi

echo "============================================================"
echo "Flink Benchmark Suite - All 5 Queries"
echo "============================================================"
echo "Config: $CONFIG_NAME"
echo "Sleep per query: $SLEEP_SECONDS seconds"
echo "Timestamp: $TIMESTAMP"
echo "Skip build: $SKIP_BUILD"
echo ""

# List of queries to run (in order, matching thesis design)
JOBS=(
    "com.yourname.luftdaten.jobs.Q1AQIHazardLevelStatelessFilterSPS30:Q1AQIHazardLevelStatelessFilterSPS30"
    "com.yourname.luftdaten.jobs.Q2CoarseParticleDominanceFilterSPS30:Q2CoarseParticleDominanceFilterSPS30"
    "com.yourname.luftdaten.jobs.Q3TumblingWindowMapSPS30:Q3TumblingWindowMapSPS30"
    "com.yourname.luftdaten.jobs.Q4SlidingWindowFilterSPS30:Q4SlidingWindowFilterSPS30"
    "com.yourname.luftdaten.jobs.Q5SlidingWindowExtendedAverageFilter:Q5SlidingWindowExtendedAverageFilter"
)

FLINK_DEPLOYMENT_YAML="$SCRIPT_DIR/k8s/FlinkDeployment.yaml"

if ! grep -q "entryClass" "$FLINK_DEPLOYMENT_YAML"; then
    echo "ERROR: $FLINK_DEPLOYMENT_YAML has no 'entryClass' field under spec.job."
    echo "Add a line like 'entryClass: null' under spec.job so this script can patch it."
    exit 1
fi

# Create result directory
ALL_RESULTS_DIR="benchmark_results"
mkdir -p "$ALL_RESULTS_DIR"
RESULTS_DIR="$ALL_RESULTS_DIR/benchmark_results_${CONFIG_NAME}_${TIMESTAMP}"

if [ -d "$RESULTS_DIR" ]; then
    echo "ERROR: $RESULTS_DIR already exists — pick a different label or remove it first."
    exit 1
fi
mkdir -p "$RESULTS_DIR"

echo "Results will be saved to: $RESULTS_DIR"
echo ""

# Copy run_all_queries.sh metadata for reproducibility
cp "$0" "$RESULTS_DIR/run_all_queries.sh"

# Run each query
QUERY_NUM=1
TOTAL_QUERIES=${#JOBS[@]}
FIRST_JOB=true

for entry in "${JOBS[@]}"; do
    ENTRY_CLASS="${entry%%:*}"
    QUERY_NAME="${entry##*:}"

    echo "[$(date +'%H:%M:%S')] Running query $QUERY_NUM/$TOTAL_QUERIES: $QUERY_NAME"
    echo "Entry class: $ENTRY_CLASS"
    echo "Runtime: $SLEEP_SECONDS seconds"
    echo ""

    # Patch entryClass in place
    sed -i "s|entryClass:.*|entryClass: ${ENTRY_CLASS}|" "$FLINK_DEPLOYMENT_YAML"

    # Export overrides for this run
    export SLEEP_SECONDS

    # Run the query
    BUILD_OPT=""
    if [ "$FIRST_JOB" = true ] && [ "$SKIP_BUILD" = false ]; then
        # Full build on first job only
        "$SCRIPT_DIR/run.sh"
    else
        # Skip build for subsequent jobs (same jar, just different entryClass)
        "$SCRIPT_DIR/run.sh" --skip-build
    fi
    FIRST_JOB=false

    # Move results to per-query directory
    QUERY_RESULT_DIR="$RESULTS_DIR/$QUERY_NAME"
    mkdir -p "$QUERY_RESULT_DIR"

    # Grab only files newer than when this iteration started
    find benchmark_timeline -maxdepth 1 -type f -newer "$FLINK_DEPLOYMENT_YAML" -exec mv {} "$QUERY_RESULT_DIR/" \; 2>/dev/null || true
    [ -f latency-logs/latency_output.log ] && cp latency-logs/latency_output.log "$QUERY_RESULT_DIR/latency_output.log"
    find plots -maxdepth 1 -type f -newer "$FLINK_DEPLOYMENT_YAML" -exec mv {} "$QUERY_RESULT_DIR/" \; 2>/dev/null || true

    # Copy per-query metadata
    mkdir -p "$QUERY_RESULT_DIR/run_metadata"
    [ -d run_metadata ] && cp run_metadata/*.json "$QUERY_RESULT_DIR/run_metadata/" 2>/dev/null || true
    rm -rf run_metadata

    echo "✓ $QUERY_NAME completed. Results saved to $QUERY_RESULT_DIR"
    echo ""
    QUERY_NUM=$((QUERY_NUM + 1))
done

echo "============================================================"
echo "All queries completed successfully!"
echo "Results directory: $RESULTS_DIR"
echo "Analyze with: python3 analyze_all_results.py $RESULTS_DIR"
echo "============================================================"
