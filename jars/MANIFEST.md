# Cluster JARs (Artifactory shopping list)

Download with:

```bash
python scripts/download_jars.py
```

These jars are copied into `/opt/flink/lib` by [`flink/Dockerfile`](../flink/Dockerfile).
**Do not** fat-jar them into the job — keep them as cluster libs so the list matches on-prem.

| File | Maven coordinates | Why |
|------|-------------------|-----|
| `iceberg-flink-runtime-2.2-1.12.0.jar` | `org.apache.iceberg:iceberg-flink-runtime-2.2:1.12.0` | Iceberg Flink runtime, JDBC catalog, **DynamicIcebergSink** |
| `iceberg-aws-bundle-1.12.0.jar` | `org.apache.iceberg:iceberg-aws-bundle:1.12.0` | `S3FileIO` for MinIO / S3 |
| `flink-sql-connector-kafka-5.0.0-2.2.jar` | `org.apache.flink:flink-sql-connector-kafka:5.0.0-2.2` | Kafka source (uber jar includes DataStream connector) |
| `postgresql-42.7.4.jar` | `org.postgresql:postgresql:42.7.4` | JDBC catalog driver |
| `hadoop-client-api-3.3.6.jar` | `org.apache.hadoop:hadoop-client-api:3.3.6` | `org.apache.hadoop.conf.Configuration` for CatalogLoader |
| `hadoop-client-runtime-3.3.6.jar` | `org.apache.hadoop:hadoop-client-runtime:3.3.6` | Hadoop runtime classes used by client-api |

## Versions pinned with this lab

- Flink **2.2.0**
- Iceberg **1.12.0**
- Kafka connector **5.0.0-2.2**

## Separate from this list

- Application job jar built from [`job/`](../job/) (`flink-iceberg-lab-*.jar`) — deploy as a Flink job, not as a permanent cluster lib.

## If something is still missing at runtime

Check JobManager / TaskManager logs for `ClassNotFoundException` / `NoClassDefFoundError`.
Add the missing jar here and to `scripts/download_jars.py`, re-download, rebuild the Flink image.
