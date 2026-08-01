# Flink Benchmark Suite — Operational Guide

How to run the SPS30 query benchmarks, what the harness collects, and how to read the output.

> Read `SUPERVISOR_REPORT.md` first if you are interpreting results. It documents the load
> shape the generator actually produces, which findings in the archive are invalid, and why.

---

## Quick start

```bash
# Full suite. The observation window is derived from the datagen duration —
# do not pass --sleep unless you deliberately want a shorter run.
./run_all_queries.sh --config-name "half_15m1_autoscaler_off" --isolate

# Faster iteration (180s window per query, no image rebuild)
./run_all_queries.sh --sleep 180 --skip-build --config-name "iteration_test"

# One or two queries only
./run_all_queries.sh --queries Q1,Q3 --skip-build --config-name "q1_q3_check"

# Analyse, and compare two suites side by side
python3 analyze_all_results.py benchmark_results/<run>
python3 analyze_all_results.py benchmark_results/<off> benchmark_results/<on> --compare

# One figure per query: input rate, backlog, parallelism, backpressure, CPU, memory, latency
python3 plot_benchmark_timeline.py benchmark_results/<run>/<Q_label> -o out.png
```

`--isolate` scales the NebulaStream-side workloads to zero for the duration of the suite and
restores them afterwards, so a Flink-only measurement really is Flink-only. Without it the
harness prints how many foreign pods were resident and records the count in the metadata.

The suite refuses to start from a dirty working tree (`--allow-dirty` overrides and stores the
diff with the results).

---

## What the harness collects

Per query, inside `benchmark_results/<label>_<timestamp>/<QueryName>/`:

| file | content |
|---|---|
| `latency_output.log[.gz]` | one line per output record: `<latency_ms>,<payload…>,<datagen_emit_ms>,<rtracker_recv_ms>` |
| `<timestamp>.csv` | Kafka per-partition offsets/lag + the Kubernetes event stream for the FlinkDeployment |
| `resources_<timestamp>.csv` | `kubectl top pods` samples (CPU, memory) |
| `parallelism_<timestamp>.csv` | applied parallelism and status per vertex |
| `backpressure_<timestamp>.csv` | busy / backpressured / idle ms-per-second per vertex |
| `throughput_<timestamp>.csv` | records in/out per second per vertex, plus source `pendingRecords` |
| `checkpoints_<timestamp>.csv` | completed/failed counts, latest duration and state size |
| `datagen_<timestamp>.log` | the generator's own log: realized schedule and end-of-run record counts |
| `run_metadata/run_<timestamp>.json` | timings, load configuration, delivered record count, git commit |
| `config_snapshot/` | verbatim copies of the manifests and `.env` used |
| `timeline_<QueryName>.png` | the multi-panel figure |

Plus, per suite: `SUITE_MANIFEST.md`, `MANIFEST.sha256`.

**Parsing the latency log:** the latency is field **0**; the receive timestamp is the **last**
field. The payload in between contains commas by construction (`Category: MODERATE, AQI: 56,
…`), so never index a fixed positive position. Getting this wrong is what made the old analysis
tool silently report nothing at all.

```bash
# correct
zcat latency_output.log.gz | awk -F',' '{print $1}'          # latency ms
zcat latency_output.log.gz | awk -F',' '{print $NF}'         # receive timestamp ms
# Kafka lag from a timeline CSV: field 6 of the kafka_lag rows
awk -F',' '$2=="kafka_lag" && $3 ~ /^[0-9]+$/ {print $1, $6}' <timestamp>.csv
```

---

## The measurement window

The window is anchored to **the first record that actually reaches Kafka**, not to the creation
of the datagen Job — the generator parses a 3.77 GB / 30.2 M-row CSV into memory before it emits
anything, and its own `duration` timer only starts after that. The run then ends when output
stops (rtracker's log size stable for `LOG_STABLE_SECONDS`, default 45 s), bounded by
`DRAIN_TIMEOUT` (default 240 s).

Tunables (environment variables, all optional):

| variable | default | meaning |
|---|---|---|
| `SLEEP_SECONDS` | derived | override the window length explicitly |
| `DRAIN_MARGIN` | 90 | added to the datagen duration when deriving the window |
| `DRAIN_TIMEOUT` | 240 | give up waiting for output to stop |
| `LOG_STABLE_SECONDS` | 45 | how long the latency log must not grow before the run ends |
| `KAFKA_EXEC_TIMEOUT` | 20 | per-poll timeout for the broker CLI (3 s lost 8–66 % of samples) |
| `FLINK_WAIT_TIMEOUT` | 420 | fail if the job does not reach RUNNING |

**Do not** gate on Kafka lag reaching zero: it returns to zero between bursts, and it is a
checkpoint-commit sawtooth rather than a true backlog. Use `pendingRecords` from
`throughput_*.csv` when you need the real queue depth.

---

## Load configuration

`k8s/DatagenConfig.yaml` (the `config.kafka.yaml` block is the one the Job reads):

```yaml
data_rate: 15132506   # records PER PERIOD (not per second)
period: 300000        # length of one burst→baseline cycle, ms
duration: 600         # total generation time, s (starts after the CSV load)
chunk_size: 1900000   # unused on this code path — see SUPERVISOR_REPORT.md
pattern:
  - type: burst
  - fraction: 0.7     # share of the period's records inside the burst
  - distribution: 0.3 # share of the period the burst occupies
```

Effective mean rate = `data_rate / (period/1000)` = 50,444 rec/s here. With
`fraction: 0.7` / `distribution: 0.3` the realized profile per 300 s period is 30 s at
151,332/s, 60 s at 100,888/s, then the baseline.

Two things the harness now checks before every run and records in the metadata:

1. **Dataset wrap.** Records emitted = `data_rate × duration×1000/period` must stay ≤ 30,265,013
   (the CSV's row count). Beyond that the generator wraps, event time jumps ~2 years backwards,
   and every windowed query silently drops everything after the wrap.
2. **Silent tail.** With the generator's stock arithmetic, `distribution < fraction` leaves the
   last `1 − distribution − (1 − fraction)` of every period emitting nothing (40 % here).
   `k8s/DatagenJob.yaml` sets `DATAGEN_SPREAD_BASELINE=1` to spread the baseline over the whole
   period instead; set it to `"0"` to reproduce a run made before that existed.

---

## Reading a timeline figure

Panels share one time axis. Grey bands mark stretches where the generator produced nothing;
dotted vertical lines mark the job reaching RUNNING (start, and every autoscaler rescale).

* **Input rate** — reconstructed from Kafka high-watermark offsets, 10 s smoothed, against the
  configured mean. This is the ground truth for "what load was applied".
* **Backlog** — source `pendingRecords` where available, otherwise broker-reported consumer lag
  (labelled as the sawtooth it is).
* **Parallelism** — per vertex. Note the sink: `writeToSocket` pins it at 1, so the autoscaler
  cannot scale it however much it scales the source.
* **Busy / backpressure** — the panel that explains the latency curve. High busy = compute
  bound; high backpressure = waiting on something downstream.
* **CPU / memory** — separate panels on purpose. `kubectl top` refreshes only every ~53 s, so
  a "peak" here is a ~53 s average and cannot be pinned to a single burst.
* **Latency** — p50 and p95 per bucket. The line **breaks** where no records arrived; a
  continuous line across a gap would imply traffic that never happened.

Latency percentiles over a whole run mix a cold start, bursts of different intensity and an idle
drain. `analyze_all_results.py` therefore reports them per input phase as well — quote the phase
numbers, not just the aggregate.

---

## Troubleshooting

**A query produced no latency data.** The suite marks the directory `INCOMPLETE` and continues.
Check `datagen_<timestamp>.log` for the record counts and `kubectl logs deployment/luftdaten-job`.

**`analyze_all_results.py` warns about lag-poll loss > 5 %.** The broker CLI is timing out;
raise `KAFKA_EXEC_TIMEOUT`. Rate and lag curves under-sample the bursts when this happens.

**A latency log is flagged CORRUPT.** Two rtracker pods shared one log file. Fixed
(`strategy: Recreate`, pod-local `emptyDir`, single-pod barrier in `run.sh`), but any affected
query has to be re-run — `tidy_benchmark_results.py` quarantines the file and explains why.

**Run takes much longer than expected.** `kubectl top nodes`, `kubectl top pods -A`. Note that
`minikube start --cpus/--memory` is silently ignored on an existing cluster: check the real
limits with `docker inspect minikube --format '{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}'`.

---

## Reproducibility checklist

- [ ] working tree committed (the suite enforces this)
- [ ] `k8s/DatagenConfig.yaml` and `k8s/FlinkDeployment.yaml` reflect the intended experiment
- [ ] run the suite; confirm each query reports `delivered ≈ configured` within 1 %
- [ ] `python3 analyze_all_results.py <dir>` shows no integrity warnings
- [ ] `python3 tidy_benchmark_results.py --apply` to index and checksum the archive
- [ ] copy the results directory off this machine (the archive is not in git)

---

## Files

| file | role |
|---|---|
| `run.sh` | one query end to end |
| `run_all_queries.sh` | the suite, per-query result directories, manifest |
| `analyze_all_results.py` | coverage, latency (overall and per phase), load, elasticity, integrity |
| `plot_benchmark_timeline.py` | the multi-panel figure |
| `latency-plotter.py` | single-run full-resolution latency plot |
| `tidy_benchmark_results.py` | normalise, index and checksum the stored archive |
| `utils/timeline_logging.sh` | Kafka offsets/lag + Kubernetes events |
| `utils/resource_logging.sh` | `kubectl top` sampling |
| `utils/flink_metrics_logging.sh` + `utils/flink_metrics_poll.py` | parallelism, backpressure, throughput, checkpoints |
| `utils/parallelism_logging.sh` | superseded by `flink_metrics_logging.sh`; kept for older invocations |
| `infra/datagen_baseline_spread.patch` | the datagen patch, for re-applying after a submodule update |
