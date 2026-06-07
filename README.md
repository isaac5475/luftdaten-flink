# Luftdaten Flink container setup

This repository provides a Docker Compose environment for an end-to-end Luftdaten stream processing demo.

It uses two git submodules:

- `infra/datagen_parallel` for data generation
- `infra/latency-tracker` for latency tracking

Both submodules are wired into the top-level Compose stack and use the configuration files under `./config`.

## End-to-end flow

1. `datagen` reads `config/datagen/config.yaml`
2. The generated records are annotated with a timestamp
3. Flink ingests and processes the stream
4. The result is written to the latency tracker sink
5. `rtracker` calculates the latency and writes the output to `./latency-logs`

The relevant config files are:

- `config/datagen/config.yaml`
- `config/datagen/config.mqtt.yaml`
- `config/datagen/config.kafka.yaml`
- `config/rtracker/rtracker-config.yaml`

## Setup

Before starting the stack, create a `.env` file in the repository root.

Use `.env.example` as the starting point:

```bash
cp .env.example .env
```

Adjust the values in `.env` if needed for your local setup.

## Build the job jar

From the repository root:

```bash
mvn -Dmaven.test.skip=true clean package
```

The compose setup expects this jar by default:

- `target/luftdaten-flink-0.1.jar`

If your build creates a different jar name, update `JOB_JAR` in `docker-compose.yml`.

## Start the stack

```bash
docker compose up --build
```

The Compose stack starts:

- Flink JobManager and TaskManager
- `datagen` for stream input
- `rtracker` for latency measurement
- `job-submitter`, which waits for the services to be ready and submits the job automatically on startup

## Notes on configuration

- The job submitter uses the `.env` values for source and sink host/port settings.
- `datagen` is exposed on the source port defined in `.env`.
- `rtracker` listens on the sink port defined in `.env` and writes results to `./latency-logs`.
- If you switch the data source or broker backend, use the matching file in `config/datagen/`.

## Flink UI

When the stack is running, open:

- http://localhost:8081

## Stop everything

```bash
docker compose down
```


## Generate latency plot
After stopping the stack, you can generate a latency plot from the logs:

### Create virtual environment and install dependencies

```bash
python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt
```

### Run the plotting script

```bash
python3 ./latency-plotter.py latency-logs/latency_output.log latency.png
```