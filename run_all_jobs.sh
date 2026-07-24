#!/bin/bash
# run_all_jobs.sh
#
# Runs all 5 Flink benchmark jobs sequentially, each as a clean, isolated
# run.sh invocation. Assumes all 5 job classes are already compiled into
# the SAME jar (luftdaten-flink:local) — switching jobs is done by patching
# spec.job.entryClass in k8s/FlinkDeployment.yaml before each run, not by
# recompiling/rebuilding the image. run.sh itself still handles rebuilding
# the image if you've actually changed code, but for just switching which
# job runs, no rebuild is needed.
#
# Usage:
#   ./run_all_jobs.sh [run_label]
#
# run_label names the results folder for this whole run, e.g.:
#   ./run_all_jobs.sh all_dataset_bursty_period_10min_data_rate_30mln_no_scaling_triggered
# -> results under benchmark_results_all_dataset_bursty_period_10min_data_rate_30mln_no_scaling_triggered/
#
# If omitted, falls back to benchmark_results_<timestamp>/.
#
# IMPORTANT: adjust the JOBS array below to match your actual fully-qualified
# class names if they differ from what's listed here.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLINK_DEPLOYMENT_YAML="$SCRIPT_DIR/k8s/FlinkDeployment.yaml"

# entryClass:label pairs — label is used to tag output files so results from
# different jobs don't overwrite each other.
JOBS=(
    "com.yourname.luftdaten.jobs.Q1AQIHazardLevelStatelessFilterSPS30:Q1_aqi_stateless"
    "com.yourname.luftdaten.jobs.Q2CoarseParticleDominanceFilterSPS30:Q2_coarse_particle_stateless"
    "com.yourname.luftdaten.jobs.Q3TumblingWindowMapSPS30:Q3_tumbling_avg"
    "com.yourname.luftdaten.jobs.Q4SlidingWindowFilterSPS30:Q4_sliding_spike"
    "com.yourname.luftdaten.jobs.Q5SlidingWindowExtendedAverageFilter:Q5_cross_spectrum"
)

if ! grep -q "entryClass" "$FLINK_DEPLOYMENT_YAML"; then
    echo "ERROR: $FLINK_DEPLOYMENT_YAML has no 'entryClass' field under spec.job."
    echo "Add a line like 'entryClass: null' under spec.job so this script can patch it."
    exit 1
fi

# Usage:
#   ./run_all_jobs.sh [run_label] [--skip-build|-s]
#
# --skip-build / -s skips rebuilding the image even for the first job (e.g.
# when you know the jar/image on this Minikube is already up to date and
# only want to re-run the same code with a fresh dataset/config).
#
# run_label (any non-flag argument) names the results folder, e.g.:
#   ./run_all_jobs.sh all_dataset_bursty_period_10min_data_rate_30mln_no_scaling_triggered
# Falls back to a plain timestamp if omitted.
SKIP_BUILD=false
RUN_LABEL=""
for arg in "$@"; do
    case "$arg" in
        --skip-build|-s)
            SKIP_BUILD=true
            ;;
        *)
            RUN_LABEL="$arg"
            ;;
    esac
done

if [ -n "$RUN_LABEL" ]; then
    RESULTS_ROOT="benchmark_results_${RUN_LABEL}"
else
    RESULTS_ROOT="benchmark_results_$(date +%Y%m%d_%H%M%S)"
fi

if [ -d "$RESULTS_ROOT" ]; then
    echo "ERROR: $RESULTS_ROOT already exists — pick a different label or remove it first."
    exit 1
fi
mkdir -p "$RESULTS_ROOT"

FIRST_JOB=true
for entry in "${JOBS[@]}"; do
    ENTRY_CLASS="${entry%%:*}"
    LABEL="${entry##*:}"

    echo "=============================================="
    echo " Running job: $LABEL"
    echo " entryClass:  $ENTRY_CLASS"
    echo "=============================================="

    # Patch entryClass in place. Matches "entryClass: <anything or nothing>"
    # under spec.job, whatever its current value (null, a previous class, etc).
    sed -i "s|entryClass:.*|entryClass: ${ENTRY_CLASS}|" "$FLINK_DEPLOYMENT_YAML"

    # run.sh does the full cycle: (re)deploy Flink with the patched entryClass,
    # recreate the Kafka topic + consumer group, start datagen, wait, log the
    # timeline, collect rtracker results, tear down. It flocks itself, so
    # running it sequentially here is safe — this call blocks until done.
    #
    # Build the image fully on the first job only — all 5 job classes live
    # in the same jar, so once it's built once with today's code, switching
    # entryClass for jobs 2-5 doesn't require rebuilding/reloading it again.
    # If --skip-build/-s was passed to run_all_jobs.sh itself, skip it even
    # for the first job too.
    #
    # Note: `[ A && B ]` is invalid — single test brackets don't support
    # `&&` internally. Use `[[ ... && ... ]]` (bash-specific double
    # brackets) or two separate `[ ]` tests joined by shell `&&`.
    if [ "$FIRST_JOB" = true ] && [ "$SKIP_BUILD" = false ]; then
        "$SCRIPT_DIR/run.sh"
    else
        "$SCRIPT_DIR/run.sh" --skip-build
    fi
    FIRST_JOB=false

    # Move this job's output (timeline CSVs, resource CSVs, latency logs,
    # plots) into a labeled subfolder so the next job's run.sh doesn't
    # overwrite it — run.sh always writes to benchmark_timeline/, latency-logs/
    # and plots/ using only a timestamp, with no job label of its own.
    JOB_DIR="$RESULTS_ROOT/$LABEL"
    mkdir -p "$JOB_DIR"

    # Grab only files newer than when this iteration started, so we don't
    # accidentally sweep up a previous job's leftovers.
    find benchmark_timeline -maxdepth 1 -type f -newer "$FLINK_DEPLOYMENT_YAML" -exec mv {} "$JOB_DIR/" \; 2>/dev/null || true
    [ -f latency-logs/latency_output.log ] && cp latency-logs/latency_output.log "$JOB_DIR/latency_output.log"
    find plots -maxdepth 1 -type f -newer "$FLINK_DEPLOYMENT_YAML" -exec mv {} "$JOB_DIR/" \; 2>/dev/null || true

    echo "Finished $LABEL. Results saved under: $JOB_DIR"
    echo ""
done

echo "=============================================="
echo "All 5 jobs completed."
echo "Results: $RESULTS_ROOT/"
echo "=============================================="
