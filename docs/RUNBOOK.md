# MASX-GSGI — Runbook

## 1. Local Development Setup

### Prerequisites
- Python 3.12+
- Docker + Docker Compose
- Git

### Quick Start

```bash
# Clone and enter
git clone <repo-url> && cd MASX-GSGI

# Copy env file
cp .env.example .env
# Edit .env with your database credentials

# Start Postgres (with pgvector)
docker compose up -d db

# Wait for DB health check
docker compose exec db pg_isready -U gsgi

# Install Python dependencies
pip install -e ".[dev]"

# Run migrations
alembic upgrade head

# Run tests
pytest -v -m "not slow"

# Run pipeline (Tier A, cheapest)
python -m apps.orchestrator.main --tier A
```

### Running Individual Workers

```bash
# Ingest only
python -m apps.ingest_worker.main

# Cluster only (requires run_id from previous ingest)
python -m apps.cluster_worker.main run_20260212_040000_abc12345

# Summarize only
python -m apps.summary_worker.main run_20260212_040000_abc12345

# Score + alert
python -m apps.score_alert_worker.main run_20260212_040000_abc12345
```

## 2. Railway Deployment

### Service Definitions

| Service Name | Type | Schedule | Command |
|-------------|------|----------|---------|
| `gsgi-orchestrator` | Cron | `0 4 * * *` (daily 4am UTC) | `python -m apps.orchestrator.main` |
| `gsgi-ingest` | Cron | (triggered by orchestrator) | `python -m apps.ingest_worker.main` |
| `gsgi-cluster` | Cron | (triggered by orchestrator) | `python -m apps.cluster_worker.main $RUN_ID` |
| `gsgi-summary` | Cron | (triggered by orchestrator) | `python -m apps.summary_worker.main $RUN_ID` |

### Environment Variables (Railway)

```
DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/dbname
DATABASE_URL_SYNC=postgresql://user:pass@host:5432/dbname
OPENAI_API_KEY=sk-...
PIPELINE_TIER=B
MAX_CONCURRENT_FETCHES=50
PER_DOMAIN_CONCURRENCY=3
LOG_LEVEL=INFO
LOG_FORMAT=json
RAILWAY_ENVIRONMENT=production
```

### Deployment Steps

1. Create a new Railway project
2. Add PostgreSQL service (with pgvector extension enabled)
3. For each worker, create a new service:
   - Set build command: `pip install -e .`
   - Set start command: see table above
   - Set schedule: `0 4 * * *` for orchestrator
4. Configure environment variables
5. Deploy

### Railway Private Networking

If running multiple services, use internal DNS:
```
# Instead of external DB URL:
DATABASE_URL=postgresql+asyncpg://user:pass@postgres.railway.internal:5432/dbname
```

## 3. Operations

### Checking Run Status

```sql
-- Latest runs
SELECT run_id, status, total_entries, processed_entries, failed_entries,
       started_at, completed_at, metrics
FROM processing_runs
ORDER BY created_at DESC
LIMIT 10;

-- Job status breakdown for a run
SELECT status, COUNT(*) as count
FROM feed_entry_jobs
WHERE run_id = 'run_20260212_...'
GROUP BY status;

-- Failure reasons
SELECT failure_reason, COUNT(*) as count
FROM feed_entry_jobs
WHERE run_id = 'run_20260212_...' AND status = 'failed'
GROUP BY failure_reason
ORDER BY count DESC;
```

### Checking Clusters

```sql
-- Clusters per flashpoint
SELECT fp.title, nc.cluster_id, nc.article_count, nc.summary,
       nc.top_domains, nc.languages
FROM news_clusters nc
JOIN flash_point fp ON fp.id = nc.flashpoint_id
ORDER BY fp.title, nc.cluster_id;
```

### Manual Re-Run

```bash
# Re-run for specific entries (will skip already-processed via idempotent claims)
python -m apps.orchestrator.main --tier B
```

### Clearing a Failed Run

```sql
-- Delete failed run's jobs (allows re-processing)
DELETE FROM feed_entry_jobs WHERE run_id = 'run_xxx' AND status = 'failed';
-- Then re-run
```

## 4. Health Checks

### Critical Metrics to Monitor

| Metric | Warning Threshold | Critical Threshold |
|--------|------------------|-------------------|
| Total processing time | > 2 hours | > 4 hours |
| Failed entries % | > 10% | > 25% |
| Extraction failure rate | > 15% | > 30% |
| Domain circuit breakers open | > 5 domains | > 15 domains |
| Clusters with 1 article | > 80% of clusters | > 95% |

### Log Queries (JSON structured logs)

```bash
# Find failures for a specific flashpoint
grep '"flashpoint_id":"abc..."' logs.json | grep '"level":"error"'

# Per-domain failure stats
grep '"stage":"fetch"' logs.json | jq '.domain' | sort | uniq -c | sort -rn
```

## 5. Disaster Recovery

### Database Backup
Supabase handles automatic backups. Manual backup:
```bash
pg_dump $DATABASE_URL_SYNC > backup_$(date +%Y%m%d).sql
```

### Data Recovery
All sidecar tables can be safely dropped and recreated:
```bash
alembic downgrade base
alembic upgrade head
# Then re-run pipeline
```

The `feed_entries` and `flash_point` tables are never modified structurally.

## 6. Scaling

### Horizontal Scaling
- Increase `MAX_CONCURRENT_FETCHES` (up to ~200 for Railway)
- Run multiple ingest workers for different flashpoint batches
- Use Redis for distributed job claims (future enhancement)

### Vertical Scaling
- Increase Railway service memory for embedding model loading
- Use GPU-enabled Railway services for faster embedding (if needed)
