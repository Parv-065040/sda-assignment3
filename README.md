# Streaming E-commerce Analytics Pipeline (Production-Grade)

Phase 0 of the production upgrade: the entire pipeline (Kafka, Zookeeper,
MySQL, Grafana, the producer, and the consumer) now runs as one Dockerized
stack, with retry logic, logging, and persistent storage replacing the
manual, fragile setup used in Assignments 1-3.

## What changed from the assignment version

- **Everything is containerized.** `producer.py` and `consumer.py` now run
  as their own Docker services (`producer/`, `consumer/`), not scripts you
  run manually on your host machine. This is what eliminates the
  "which Python, is the port free, is Kafka up" class of problems entirely.
- **Named volumes** for MySQL, Kafka, and Grafana data. Recreating a
  container (which happened several times during the assignments due to
  Windows/Docker quirks) no longer wipes your tables or dashboard config.
- **Retry with exponential backoff** on every external connection (Kafka,
  MySQL) in both scripts. A container starting in the "wrong" order, or a
  brief restart, no longer crashes the whole pipeline.
- **Structured logging** (console + a `.log` file per service) instead of
  `print()`.
- **Auto-created database schema** via `db/mysql-init.sql`, run
  automatically the first time the MySQL container starts.
- **Credentials moved to `.env`** (never committed -- see `.env.example`
  for the template).
- **3 partitions per Kafka topic** instead of 1, laying the groundwork for
  Phase 1's multi-instance consumer scaling.
- Two new tables (`alerts`, `customer_segments`) are already created,
  ready for Phase 2 (alerting) and Phase 3 (RFM segmentation).

## Project structure

```
pipeline/
├── docker-compose.yml       # the whole stack
├── .env.example             # copy to .env and fill in real values
├── .gitignore
├── db/
│   └── mysql-init.sql       # auto-creates all 5 tables on first run
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── producer.py
│   ├── generate_sample_data.py
│   └── sample_data/         # generated .jsonl files, baked into the image
└── consumer/
    ├── Dockerfile
    ├── requirements.txt
    └── consumer.py
```

## How to run

1. Copy the env template and fill in real values:
   ```
   cp .env.example .env
   ```
   At minimum, set `MYSQL_ROOT_PASSWORD`, `MYSQL_PASSWORD` (same value),
   and `GF_SECURITY_ADMIN_PASSWORD`. Leave the SMTP fields for Phase 2.

2. Build and start everything:
   ```
   docker-compose up -d --build
   ```

3. Check everything came up:
   ```
   docker-compose ps
   ```
   All 6 services should show as running.

4. Watch the logs of any service:
   ```
   docker-compose logs -f consumer
   docker-compose logs -f producer
   ```

5. Open Grafana at `http://localhost:3000` (login with the admin
   credentials from your `.env`) and re-add the MySQL data source (host:
   `mysql:3306`) and rebuild the dashboard panels from the Assignment 3
   report -- or restore them from a dashboard JSON export if you have one.

## Scaling the consumer (Phase 1 preview)

Since Kafka topics now have 3 partitions, you can run multiple consumer
instances that will automatically split the work between them:
```
docker-compose up -d --scale consumer=2
```

## Stopping / resetting

```
docker-compose down          # stops everything, keeps data (volumes)
docker-compose down -v       # stops everything AND wipes all data
```
