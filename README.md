ETL Project to collect papers from ArXiv and load into PostGreSQL

## Setup

```
docker compose up -d
uv run python infra/db/init_db.py
```
