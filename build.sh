ls .venv || ( python -m venv .venv && .venv/Scripts/activate && pip install -r scripts/requirements.txt)

python scripts/download_jars.py

docker compose -f docker-compose.kafka.yml --env-file env/local.kafka.stack.env up -d
docker compose -f docker-compose.flink.yml --env-file env/local.flink.stack.env up --build -d


