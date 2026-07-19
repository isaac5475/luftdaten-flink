FROM flink:1.19.0-scala_2.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY submit-job.sh /opt/submitter/submit-job.sh
RUN chmod +x /opt/submitter/submit-job.sh

USER flink
ENTRYPOINT ["/opt/submitter/submit-job.sh"]
