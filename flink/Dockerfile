FROM flink:2.2.0-java17

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends curl \
  && rm -rf /var/lib/apt/lists/*

# add optional existing plugins
RUN mkdir -p /opt/flink/plugins/s3-fs-presto \
    && cp /opt/flink/opt/flink-s3-fs-presto-2.2.0.jar \
       /opt/flink/plugins/s3-fs-presto/

# Cluster libs for Artifactory parity (downloaded by scripts/download_jars.py)
COPY jars/*.jar /opt/flink/lib/

USER flink
