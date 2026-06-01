# Luftdaten Flink container setup

This repository now includes a Docker Compose setup that starts a local Flink cluster and auto-submits `DailyTemperatureBME280StreamJobDataGen` when the stack comes up.

## What it does

- starts a Flink JobManager and TaskManager
- waits for optional socket source and sink containers to be reachable
- submits the job automatically on `docker compose up`
- mounts the Maven build output from `./target`

## Build the job jar

From the repository root:

```bash
mvn -Dmaven.test.skip=true clean package
```

The compose setup expects this jar by default:

- `target/luftdaten-flink-0.1.jar`

If your build creates a different jar name, update `JOB_JAR` in `docker-compose.yml`.

## Start the Flink cluster and auto-submit the job

```bash
docker compose up --build
```

The submitter container uses these defaults:

- `SOURCE_HOST=source`
- `SOURCE_PORT=9000`
- `SINK_HOST=sink`
- `SINK_PORT=9001`

You can override them when starting Compose:

```bash
SOURCE_HOST=my-source SOURCE_PORT=9000 SINK_HOST=my-sink SINK_PORT=9001 docker compose up --build
```

## Using separate source and sink containers

Attach your source and sink containers to the shared Docker network created by Compose:

- network name: `luftdaten-net`

Example pattern:

```bash
docker run -d --name source --network luftdaten-net ...
docker run -d --name sink --network luftdaten-net ...
```

For a quick local test, you can use `socat` containers that talk over TCP:

```bash
docker run -d --name source --network luftdaten-net \
  -v "$(pwd)/src/main/resources/2020-01_bme280.csv:/data.csv:ro" \
  alpine:3.20 sh -c 'apk add --no-cache socat >/dev/null && while true; do socat -u FILE:/data.csv TCP-LISTEN:9000,reuseaddr,fork; sleep 1; done'

docker run -d --name sink --network luftdaten-net \
  alpine:3.20 sh -c 'apk add --no-cache socat >/dev/null && socat -u TCP-LISTEN:9001,reuseaddr,fork -'
```

As long as the containers are named or reachable as `source` and `sink` (or you override the env vars above), the submitter will wait for both sockets before submitting the job.

## Flink UI

When the stack is running, open:

- http://localhost:8081

## Stop everything

```bash
docker compose down
```



