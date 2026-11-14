docker compose -f docker-compose.flink-submit.yml --env-file env/local.flink.stack.env up --build -d

python scripts/produce_events.py
sleep 5
python query_trino.py
