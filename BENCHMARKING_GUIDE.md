# Flink Benchmark Suite - Operational Guide

## Overview

The benchmark suite has been enhanced to support comprehensive, reproducible testing of all 5 SPS30 queries with strict timing control and full configuration tracking for thesis reproducibility.

## Quick Start

### Run All Queries (600s per query)
```bash
./run_all_queries.sh --config-name "quarter_dataset_7m6_tuples_sec"
```

### Run All Queries Faster (300s per query, for iteration)
```bash
./run_all_queries.sh --sleep 300 --skip-build --config-name "iteration_test"
```

### Analyze Results
```bash
python3 analyze_all_results.py benchmark_results_quarter_dataset_7m6_tuples_sec_20260729_143000
```

## Features

### 1. Comprehensive Timing Tracking
- **Start/end timestamps** (UTC with millisecond precision)
- **Actual vs. requested duration** comparison
- **Variance calculation** for timing validation
- **Metadata JSON** file per run for reproducibility

### 2. Kafka Topic Cleanup
The `run.sh` script automatically:
1. Deletes the old `bid` topic before each query
2. Recreates it from `k8s/topic.yaml`
3. Clears the consumer group to ensure fresh offsets
4. Waits for the topic to be ready before deploying Flink

This ensures **no cross-contamination** between query runs.

### 3. Results Organization
```
benchmark_results_<config>_<timestamp>/
├── Q1AQIHazardLevelStatelessFilterSPS30/
│   ├── latency_output.log            # Raw latency measurements
│   ├── run_metadata/
│   │   └── run_<timestamp>.json      # Timing & config for this query
│   ├── <timestamp>.csv               # Kafka lag timeline
│   ├── resources_<timestamp>.csv     # CPU/memory samples
│   ├── parallelism_<timestamp>.csv   # Parallelism over time
│   └── latency_plot_*.png
├── Q2CoarseParticleDominanceFilterSPS30/
│   └── [same structure]
...
```

## Configuration Parameters

### Data Generation
Edit `k8s/DatagenConfig.yaml` before running:

```yaml
data_rate: 76566253        # tuples/sec (7.66M for 1/4 dataset)
period: 300000             # zipf period in ms (5 min)
duration: 600              # total runtime in sec (10 min)

pattern:
  - type: burst            # bursty traffic pattern
  - fraction: 0.7          # 70% of load in bursts
  - distribution: 0.3      # 30% uniformly distributed

zipf:
  period_ms: 1000          # zipf period (should match `period`)
  s: 1.4                   # zipf shape parameter (higher = more skewed)
  stochastic: true
```

### Timing Control
Edit `.env`:
```bash
SLEEP_SECONDS=600          # observation window per query
```

Or override at runtime:
```bash
./run_all_queries.sh --sleep 300
```

## Timing Validation

Each run produces `run_metadata/run_<timestamp>.json`:

```json
{
  "start_time_utc": "2026-07-29T14:30:00.123Z",
  "end_time_utc": "2026-07-29T14:40:05.456Z",
  "actual_duration_seconds": 605,
  "requested_duration_seconds": 600,
  "timing_variance_percent": 0.83,
  "kafka_topic": "bid",
  "kafka_group": "luftdaten-benchmark"
}
```

**Variance interpretation:**
- **±5%** ✓ Acceptable (normal system variance)
- **±10%** ⚠ Investigate (possible CPU/memory pressure)
- **>10%** ✗ Check system load (minikube, docker, other processes)

## Expected Runtime

For a full 5-query suite with 600s per query:
- **Flink cluster setup + Kafka prep:** ~3 min (once per suite)
- **Per-query runtime:** 10 min + 2 min cleanup
- **Total time:** ~60 minutes for 5 queries

To speed up iteration:
```bash
./run_all_queries.sh --sleep 120 --skip-build
```
This reduces total time to ~20 minutes.

## Analyzing Results

### Quick Summary
```bash
python3 analyze_all_results.py benchmark_results_<dir>
```

Output:
```
TIMING VALIDATION
─────────────────────────────
  Q1AQIHazardLevelStatelessFilterSPS30    | 603s (req: 600s, var:  +0.5%) ✓
  Q2CoarseParticleDominanceFilterSPS30    | 602s (req: 600s, var:  +0.3%) ✓
  Q3TumblingWindowMapSPS30                | 604s (req: 600s, var:  +0.7%) ✓
  Q4SlidingWindowFilterSPS30              | 601s (req: 600s, var:  +0.2%) ✓
  Q5SlidingWindowExtendedAverageFilter    | 610s (req: 600s, var:  +1.7%) ⚠

LATENCY STATISTICS (milliseconds)
──────────────────────────────────────────────────────
Query                                   | Samples |      Min |      Med |     Mean |      Max |        σ
─────────────────────────────────────────────────────
Q1AQIHazardLevelStatelessFilterSPS30    |  100000 |     12.10 |     22.00 |    29.10 |  1158.75 |    28.86
Q2CoarseParticleDominanceFilterSPS30    |  100000 |      0.01 |     12.14 |    15.76 |   654.62 |    17.60
Q3TumblingWindowMapSPS30                |  100000 |      5.20 |     18.50 |    24.30 |   890.40 |    32.15
Q4SlidingWindowFilterSPS30              |  100000 |      8.90 |     25.80 |    38.20 |  1250.30 |    45.60
Q5SlidingWindowExtendedAverageFilter    |   85000 |    157.60 |    208.58 |   373.72 |  9996.03 |   760.42
```

### Deep Dive: Latency Distribution
```bash
# Extract Q1 latencies for further analysis
grep -h "^" benchmark_results_*/Q1*/latency_output.log | cut -d',' -f3 | sort -n > q1_latencies.txt

# Compute percentiles
awk '{sum+=$1; sumsq+=$1*$1; a[NR]=$1} 
     END {
       for(i=1;i<=NR;i++) if(i<=int(NR*0.05)) min=a[i]; 
       for(i=1;i<=NR;i++) if(i>int(NR*0.95) && !p95) p95=a[i];
       print "p50:", a[int(NR*0.5)], "p95:", p95, "p99:", a[int(NR*0.99)]
     }' q1_latencies.txt
```

### Kafka Lag Analysis
```bash
# Extract kafka lag events from timeline
awk -F',' '$2 == "kafka_lag" {print $1, $NF}' benchmark_results_*/*/*.csv | sort | uniq

# Visualize lag progression (if you have gnuplot/matplotlib)
# See latency-plotter.py for timeline visualization
```

## Troubleshooting

### Issue: Run takes >10 minutes per query
**Check:**
- `docker ps` — is Minikube container responding?
- `kubectl top nodes` — CPU/memory pressure?
- `kubectl top pods -A` — any pod using >50% CPU?

**Solution:**
```bash
# Restart minikube if sluggish
minikube stop && minikube start --driver=docker --cpus=12 --memory=12g

# Monitor during next run
watch -n 1 'kubectl top pods -n kafka && echo "---" && kubectl top nodes'
```

### Issue: "Kafka topic not ready after 60s"
**Cause:** Strimzi operator is slow on this system.

**Solution:**
```bash
# Check operator status
kubectl get deploy -n kafka
kubectl logs -n kafka -l name=strimzi-cluster-operator | tail -20

# Give it more time (edit run.sh if chronic):
kubectl wait kafkatopic/"$KAFKA_TOPIC" -n kafka --for=condition=Ready --timeout=120s
```

### Issue: Q5 latencies are missing or sparse
**Cause:** Complex windowed query may crash or fall behind under load. See [[open-items]] memory for Q5 status.

**Check:**
```bash
# Watch the Flink job manager during the run
kubectl logs -f deployment/luftdaten-job | grep -E "ERROR|Exception"

# Check if the job is running
kubectl get pods -l app=luftdaten-job -o wide
```

## Reproducibility Checklist

Before running benchmarks for thesis handover:

- [ ] Git commit current config (run.sh, .env, k8s/*.yaml)
- [ ] Tag the commit with the data rate: `git tag -a "quarter_dataset_7m6tuples_300s_period_600s_duration_kafka_8p" -m "Q1-Q5 on SPS30 1/4 dataset"`
- [ ] Run the benchmark suite
- [ ] Verify timing variance is <5% for all queries
- [ ] Save the results directory to external storage/cloud
- [ ] Document any anomalies in run_notes.txt

## Git Integration

Tag your runs for reproducibility:
```bash
# Tag the config
git tag -a "benchmark_$(date +%Y%m%d_%H%M%S)_all_queries" -m "Full Q1-Q5 suite, 600s per query"

# Tag with data rate info (preferred)
git tag -a "quarter_dataset_bursty_7m6tuples_600s_kafka_8p" -m "Q1-Q5, 7.66M tuples/sec, 600s runtime"

# Push for safe keeping
git push origin --tags
```

When you need to reproduce:
```bash
git checkout quarter_dataset_bursty_7m6tuples_600s_kafka_8p
./run_all_queries.sh --config-name "reproduction_q1_q5"
```

## References

- **run.sh** — single-query orchestrator (called by run_all_queries.sh)
- **run_all_queries.sh** — suite orchestrator (new, runs all 5 queries)
- **analyze_all_results.py** — timing validation & latency analysis
- **utils/timeline_logging.sh** — Kafka lag + pod events logger
- **utils/resource_logging.sh** — CPU/memory tracker
- **utils/parallelism_logging.sh** — Flink parallelism tracker
- **latency-plotter.py** — visualization script (existing)
