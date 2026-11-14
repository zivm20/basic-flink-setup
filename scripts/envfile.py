"""Minimal KEY=VALUE env-file loader (no external deps)."""

from __future__ import annotations

from pathlib import Path


def load_env_file(path: str | Path) -> dict[str, str]:
    result: dict[str, str] = {}
    file_path = Path(path)
    if not file_path.is_file():
        raise FileNotFoundError(f"Env file not found: {file_path}")

    for raw in file_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        result[key] = value
    return result


def require(env: dict[str, str], key: str) -> str:
    value = env.get(key, "").strip()
    if not value:
        raise SystemExit(f"Missing required key in env file: {key}")
    return value
