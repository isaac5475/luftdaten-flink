FROM flink:1.19.0-scala_2.12

RUN mkdir -p /opt/flink/usrlib
COPY target/luftdaten-flink-0.1.jar /opt/flink/usrlib/