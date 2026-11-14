#!/usr/bin/env python3
"""Run a query against an existing Trino cluster (no wait/retry loop)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from envfile import load_env_file, require

try:
    from trino.dbapi import connect
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency. Install with: pip install trino\n" + str(exc)
    ) from exc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        default="env/trino.env",
        help="Path to Trino env file (copy from env/trino.env.example)",
    )
    parser.add_argument(
        "--sql",
        default=None,
        help="SQL to run (overrides TRINO_QUERY)",
    )
    args = parser.parse_args()

    env = load_env_file(args.env_file)
    host = require(env, "TRINO_HOST")
    port = int(env.get("TRINO_PORT", "8080"))
    user = env.get("TRINO_USER", "trino")
    catalog = require(env, "TRINO_CATALOG")
    schema = env.get("TRINO_SCHEMA", "lake")
    http_scheme = env.get("TRINO_HTTP_SCHEME", "http")
    sql = args.sql or env.get("TRINO_QUERY") or f"SHOW TABLES FROM {catalog}.{schema}"

    print(f"Connecting to {http_scheme}://{host}:{port} as {user}")
    print(f"catalog={catalog} schema={schema}")
    print(f"SQL: {sql}")

    try:
        conn = connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema,
            http_scheme=http_scheme,
        )
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        description = cur.description or []
        columns = [col[0] for col in description]
        if columns:
            print(" | ".join(columns))
            print("-" * 40)
        for row in rows:
            print(" | ".join("" if v is None else str(v) for v in row))
        print(f"({len(rows)} rows)")
        cur.close()
        conn.close()
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
