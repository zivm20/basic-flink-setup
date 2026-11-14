# Local Flink 2 + Iceberg Dynamic Sink Lab

Experiment with **Flink 2.2** + **Iceberg DynamicIcebergSink** (JDBC catalog + S3/MinIO) so you can learn which jars to upload to on-prem Artifactory.

This repo does **not** start MinIO, Postgres, or Trino — point the env files at the ones you already run.

## What runs here

| Compose file | Services |
|--------------|----------|
| `docker-compose.kafka.yml` | Kafka (KRaft) + topic init |
| `docker-compose.flink.yml` | Job build, Flink JM/TM, job submitter |

Scripts:

- `scripts/download_jars.py` — cluster libs → `jars/` (Artifactory list)
- `scripts/produce_events.py` — JSON into Kafka
- `scripts/query_trino.py` — query **your** Trino (no wait loop)

Pinned versions: Flink **2.2.0**, Iceberg **1.12.0**, Kafka connector **5.0.0-2.2**.

## 1. Configure env files

Edit (or copy examples):

| File | Purpose |
|------|---------|
| `env/local.kafka.stack.env` | Kafka compose ports / topic |
| `env/local.flink.stack.env` | Flink UI port |
| `env/local.flink.env` | **Job** settings inside Flink containers |
| `env/local.kafka.env` | Producer on the host |
| `env/trino.env` | Copy from `env/trino.env.example` |
| `env/onprem.flink.env.example` | Same job vs on-prem Kafka/JDBC/S3 |

Important: `env/local.flink.env` must use hosts reachable **from Docker**:

- Kafka / MinIO / Postgres on the host → `host.docker.internal` (Docker Desktop)
- Match **warehouse**, JDBC DB, and credentials to the catalog your Trino already uses

## 2. Download cluster jars

```bash
python scripts/download_jars.py
```

See [`jars/MANIFEST.md`](jars/MANIFEST.md) for Maven coordinates and why each jar exists.

## 3. Start Kafka, then Flink

```bash
docker compose -f docker-compose.kafka.yml --env-file env/local.kafka.stack.env up -d

docker compose -f docker-compose.flink.yml --env-file env/local.flink.stack.env up --build
```

- Flink UI: http://localhost:8081  
- Iceberg commits happen on **checkpoints** (~10s). Wait at least one checkpoint before querying Trino.

## 4. Produce events

```bash
pip install -r scripts/requirements.txt
python scripts/produce_events.py --env-file env/local.kafka.env
```

Messages alternate `dataset=orders` and `dataset=clicks` so the dynamic sink creates two tables under `ICEBERG_NAMESPACE` (default `lake`). After 10 messages, `orders` gains a `priority` field (schema evolution).

## 5. Query your Trino

```bash
cp env/trino.env.example env/trino.env
# edit env/trino.env

python scripts/query_trino.py --env-file env/trino.env
python scripts/query_trino.py --env-file env/trino.env --sql "SELECT * FROM lake.orders LIMIT 10"
```

## On-prem / Artifactory

1. Upload everything listed in `jars/MANIFEST.md` to Artifactory and install onto Flink `/lib` (or your usual lib path).
2. Build/deploy the thin job jar from `job/target/flink-iceberg-lab-*.jar`.
3. Skip `docker-compose.kafka.yml` if a broker already exists; point `KAFKA_BOOTSTRAP_SERVERS` (and JDBC/S3) via something like `env/onprem.flink.env`.

Kafka compose stays separate on purpose so you can reuse it later without bundling it into Flink.

## Layout

```
docker-compose.kafka.yml
docker-compose.flink.yml
env/
flink/Dockerfile
flink/flink-conf.yaml
jars/MANIFEST.md
job/                 # Java DynamicIcebergSink job
scripts/
```
