# Fintech Realtime Monitor

Infrastructure stack for real-time fintech monitoring: Kafka, Postgres, and Kestra.

## Quick Start

1. Copy `.env.example` to `.env` and set your values:
   ```bash
   cp .env.example .env
   ```

2. Start services:
   ```bash
   docker compose up -d
   ```

## Services & Ports

| Service | Port | Description |
|---------|------|-------------|
| Kafka | 9092 (internal), 29092 (host) | Message broker |
| Postgres | 5433 | Database |
| Kestra | 8080 | Workflow orchestration UI |

## Notes

- Kafka uses KRaft mode (no ZooKeeper)
- Kestra starts only after Postgres and Kafka are healthy
