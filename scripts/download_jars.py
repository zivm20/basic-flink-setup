#!/usr/bin/env python3
"""Download Flink cluster JARs into ./jars for local Docker and Artifactory upload."""

from __future__ import annotations

import hashlib
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JARS_DIR = ROOT / "jars"
MAVEN = "https://repo1.maven.org/maven2"

FLINK_VERSION = "2.2.0"
ICEBERG_VERSION = "1.12.0"
KAFKA_CONNECTOR_VERSION = "5.0.0-2.2"
POSTGRES_VERSION = "42.7.4"
HADOOP_VERSION = "3.3.6"

# (relative maven path under repo1, filename)
JARS: list[tuple[str, str]] = [
    (
        f"org/apache/iceberg/iceberg-flink-runtime-2.2/{ICEBERG_VERSION}/"
        f"iceberg-flink-runtime-2.2-{ICEBERG_VERSION}.jar",
        f"iceberg-flink-runtime-2.2-{ICEBERG_VERSION}.jar",
    ),
    (
        f"org/apache/iceberg/iceberg-aws-bundle/{ICEBERG_VERSION}/"
        f"iceberg-aws-bundle-{ICEBERG_VERSION}.jar",
        f"iceberg-aws-bundle-{ICEBERG_VERSION}.jar",
    ),
    (
        f"org/apache/flink/flink-sql-connector-kafka/{KAFKA_CONNECTOR_VERSION}/"
        f"flink-sql-connector-kafka-{KAFKA_CONNECTOR_VERSION}.jar",
        f"flink-sql-connector-kafka-{KAFKA_CONNECTOR_VERSION}.jar",
    ),
    (
        f"org/postgresql/postgresql/{POSTGRES_VERSION}/postgresql-{POSTGRES_VERSION}.jar",
        f"postgresql-{POSTGRES_VERSION}.jar",
    ),
    # Hadoop Configuration is required by Iceberg CatalogLoader
    (
        f"org/apache/hadoop/hadoop-client-api/{HADOOP_VERSION}/"
        f"hadoop-client-api-{HADOOP_VERSION}.jar",
        f"hadoop-client-api-{HADOOP_VERSION}.jar",
    ),
    (
        f"org/apache/hadoop/hadoop-client-runtime/{HADOOP_VERSION}/"
        f"hadoop-client-runtime-{HADOOP_VERSION}.jar",
        f"hadoop-client-runtime-{HADOOP_VERSION}.jar",
    ),
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def download(rel_path: str, filename: str) -> Path:
    JARS_DIR.mkdir(parents=True, exist_ok=True)
    dest = JARS_DIR / filename
    url = f"{MAVEN}/{rel_path}"
    if dest.exists() and dest.stat().st_size > 0:
        print(f"OK  (cached) {filename}")
        return dest
    print(f"GET {url}")
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    try:
        urllib.request.urlretrieve(url, tmp)
        tmp.replace(dest)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise
    print(f"OK  {filename}  sha256={sha256_file(dest)[:16]}...")
    return dest


def main() -> int:
    print(f"Downloading into {JARS_DIR}")
    print(
        f"Flink={FLINK_VERSION} Iceberg={ICEBERG_VERSION} "
        f"KafkaConnector={KAFKA_CONNECTOR_VERSION}"
    )
    for rel, name in JARS:
        try:
            download(rel, name)
        except Exception as exc:
            print(f"FAILED {name}: {exc}", file=sys.stderr)
            return 1
    print("Done. See jars/MANIFEST.md for Artifactory upload notes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
