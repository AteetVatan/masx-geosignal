# MASX-GSGI

**Global Security & Geopolitical Intelligence** — Daily high-throughput news ingestion + NLP pipeline.

## Overview

Processes **10,000+ multilingual news URLs/day**:
1. **Extracts** article text via 4-method extraction ensemble
2. **Deduplicates** via SHA-256 + MinHash LSH
3. **Embeds** using sentence-transformers (pgvector)
4. **Clusters per flashpoint_id** using kNN + Union-Find
5. **Summarizes** clusters (local extractive or OpenAI Batch API)
6. **Writes** to `news_clusters` output table

## Quick Start

```bash
# Setup
cp .env.example .env  # Edit with your DB credentials
docker compose up -d db
pip install -e ".[dev]"
alembic upgrade head

# Run
python -m apps.orchestrator.main --tier A
```

## Architecture

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for full system design.

## Pipeline Tiers

| Tier | Features | Monthly Cost |
|------|----------|-------------|
| **A** | Fetch + Extract + Dedupe + Metadata | ~$6/mo |
| **B** | + Embeddings + Clustering + Local Summaries | ~$6/mo |
| **C** | + OpenAI Batch Summaries + Premium Pass | ~$11/mo |

## Project Structure

```
├── apps/
│   ├── orchestrator/     # Daily run coordinator (cron)
│   ├── ingest_worker/    # Fetch + extract + dedupe + embed
│   ├── cluster_worker/   # kNN clustering per flashpoint
│   ├── summary_worker/   # Summarize clusters → news_clusters
│   └── score_alert_worker/ # Hotspot scoring + alerts
├── core/
│   ├── config/           # Pydantic Settings
│   ├── db/               # SQLAlchemy models + repos
│   └── pipeline/         # Extract, fetch, dedupe, embed, cluster, summarize, score
├── tests/                # Unit + integration tests
├── docs/                 # Architecture, runbook, cost model, troubleshooting
├── alembic/              # Database migrations
├── Dockerfile
├── docker-compose.yml
└── pyproject.toml
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — System design, data flow, module boundaries
- [Runbook](docs/RUNBOOK.md) — Deployment, operations, health checks
- [Cost Model](docs/COST_MODEL.md) — Per-tier cost breakdowns
- [Troubleshooting](docs/TROUBLESHOOTING.md) — Common issues + solutions

## Testing

```bash
pytest -v -m "not slow"   # Unit tests only
pytest -v                  # All tests
pytest --cov=core --cov=apps  # With coverage
```

## License

Proprietary — All rights reserved.
