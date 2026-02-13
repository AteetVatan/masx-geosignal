# MASX-GSGI — Setup Guide

> Step-by-step instructions to get the pipeline running from a fresh clone.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Clone & Environment Variables](#2-clone--environment-variables)
3. [Option A — Local Python Setup](#3-option-a--local-python-setup)
4. [Option B — Docker Setup](#4-option-b--docker-setup)
5. [Database Setup](#5-database-setup)
6. [Seed Debug Data](#6-seed-debug-data)
7. [Running the Pipeline](#7-running-the-pipeline)
8. [Running Tests](#8-running-tests)
9. [Debugging the Pipeline](#9-debugging-the-pipeline)
10. [Verify Output](#10-verify-output)
11. [Architecture Reference](#11-architecture-reference)
12. [Railway Deployment](#12-railway-deployment)
13. [Next Steps](#13-next-steps)

---

## 1. Prerequisites

| Requirement | Version | Check Command |
|-------------|---------|---------------|
| **Python** | ≥ 3.12 | `python --version` |
| **pip** | latest | `pip --version` |
| **Git** | any | `git --version` |
| **Docker** *(Option B only)* | ≥ 24.0 | `docker --version` |
| **Docker Compose** *(Option B only)* | ≥ 2.20 | `docker compose version` |
| **PostgreSQL client** *(optional, for debugging)* | any | `psql --version` |

### Supabase (Production Database)

The project connects to an existing **Supabase Postgres** instance. The upstream project (`ai-global-signal-grid`) creates **date-partitioned tables** that this pipeline reads from and enriches:

- `flash_point_YYYYMMDD` — Flashpoints (read-only input)
- `feed_entries_YYYYMMDD` — News entries (read + write enrichment fields)
- `news_clusters_YYYYMMDD` — Cluster output (created by this pipeline)

You'll need:

- Your Supabase **project URL** and **database password**
- The connection string from Supabase Dashboard → Settings → Database → Connection string
- The **pgvector** extension enabled (Supabase Pro plan or above)

> **Note**: If you're starting without Supabase, you can use a local Postgres container instead (see [Option B](#4-option-b--docker-setup)), then seed dummy data with the [debug seeder](#6-seed-debug-data).

---

## 2. Clone & Environment Variables

### 2.1 Clone the Repository

```bash
git clone <your-repo-url> MASX-GSGI
cd MASX-GSGI
```

### 2.2 Create Your `.env` File

```bash
# Unix / macOS
cp .env.example .env

# Windows (PowerShell)
Copy-Item .env.example .env
```

### 2.3 Edit `.env` — Fill in Required Values

Open `.env` in your editor and update these **required** fields:

```env
# ── Database (REQUIRED) ──────────────────────────────
# Replace YOUR_PROJECT_REF and YOUR_DB_PASSWORD with your Supabase credentials
DATABASE_URL=postgresql+asyncpg://postgres.YOUR_PROJECT_REF:YOUR_DB_PASSWORD@aws-0-eu-central-1.pooler.supabase.com:6543/postgres
DATABASE_URL_SYNC=postgresql://postgres.YOUR_PROJECT_REF:YOUR_DB_PASSWORD@aws-0-eu-central-1.pooler.supabase.com:6543/postgres
```

> **Important**: Both `DATABASE_URL` (async, used by the pipeline) and `DATABASE_URL_SYNC` (sync, used by Alembic migrations) must point to the same database. Only the protocol prefix differs (`postgresql+asyncpg://` vs `postgresql://`).

> **pgBouncer**: Supabase uses pgBouncer on port `6543`. The engine is pre-configured to disable prepared statement caching for compatibility — no extra setup needed.

Optional but recommended for Tier C:

```env
# ── LLM (only needed for Tier C) ─────────────────────
LLM_API_KEY=your-together-ai-api-key-here
```

All other values have sensible defaults. You can leave them as-is to start.

---

## 3. Option A — Local Python Setup

Best for development and debugging. Requires Python 3.12+ installed on your system.

### 3.1 Create a Virtual Environment

```bash
# Unix / macOS
python3.12 -m venv .venv
source .venv/bin/activate

# Windows (PowerShell)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3.2 Install Dependencies

```bash
# Core + dev dependencies
pip install -e ".[dev]"
```

This installs:
- **Core**: httpx, SQLAlchemy, asyncpg, trafilatura, sentence-transformers, transformers (NER + local summarization), pycountry (geo-entities), datasketch, openai (used as universal LLM client), structlog, blingfire (fast sentence segmentation), tomli_w (TOML serialization), optimum + onnxruntime (ONNX inference), etc.
- **Dev**: pytest, ruff, mypy, pre-commit, etc.

> **First install may take 3–5 minutes** as `sentence-transformers` downloads PyTorch and the NER/summarization models are downloaded on first use.

### 3.3 (Optional) Export ONNX Model

For ~2× faster local summarization on CPU, export the DistilBART model to ONNX format:

```bash
python scripts/export_onnx.py
# → creates models/distilbart-cnn-onnx/
# → auto-detected on next pipeline run
```

> This is optional. Without it, the local summarizer falls back to vanilla PyTorch. The ONNX model only needs to be exported once.

### 3.4 (Optional) Install Extras

```bash
# Browser automation (for JS-heavy sites)
pip install -e ".[browser]"
playwright install chromium

# Offline title translation (argostranslate)
pip install -e ".[translation]"
```

### 3.5 Verify Installation

```bash
python -c "from core.config import get_settings; s = get_settings(); print(f'Tier: {s.pipeline_tier.value}, DB: {s.database_url[:30]}...')"
```

Expected output:
```
Tier: A, DB: postgresql+asyncpg://post...
```

### 3.6 Verify Database Connection

```bash
python _check_db.py
```

This script tests:
- Import chain (settings, engine, table resolver)
- Database connectivity through pgBouncer
- Table resolution for today's date-partitioned tables
- Output table creation (`ensure_output_table`)
- Sidecar table existence

---

## 4. Option B — Docker Setup

Best for isolated, reproducible environments. No Python installation required.

### 4.1 Start the Database

```bash
docker compose up -d db
```

This starts a **PostgreSQL 16 + pgvector** container on port `5432`.

Wait for it to be healthy:

```bash
docker compose exec db pg_isready -U gsgi
```

Expected output: `accepting connections`

### 4.2 Run Migrations (Docker)

```bash
docker compose run --rm migrate
```

### 4.3 Run the Pipeline (Docker)

```bash
docker compose up pipeline
```

### 4.4 Run Tests (Docker)

```bash
docker compose run --rm test
```

> **Note**: The Docker setup uses a local Postgres container, not your Supabase instance. To use Supabase with Docker, update the environment variables in `docker-compose.yml` to point to your Supabase connection string.

---

## 5. Database Setup

### 5.1 Understanding the Table Architecture

The project works with **two kinds of tables**:

#### Date-Partitioned Tables (managed by upstream project)

These tables are created by the `ai-global-signal-grid` upstream project. Each day gets its own set of tables with a `_YYYYMMDD` suffix:

| Table Pattern | Created By | MASX-GSGI Access |
|--------------|-----------|-------------------|
| `flash_point_YYYYMMDD` | Upstream | **Read-only** — flashpoint definitions |
| `feed_entries_YYYYMMDD` | Upstream | **Read + Write** — reads initial data, writes enrichment fields back |
| `news_clusters_YYYYMMDD` | **This project** | **Write** — created on demand via `ensure_output_table` |

##### Enrichment fields written back to `feed_entries_YYYYMMDD`:

| Field | Filled By | Description |
|-------|-----------|-------------|
| `title_en` | `core/pipeline/translate.py` | English translation of title |
| `hostname` | `core/pipeline/translate.py` | Extracted from URL |
| `content` | `core/pipeline/extract.py` | Full article text (also serves as "processed" marker) |

| `summary` | `core/pipeline/summarize.py` | Filled during summarization stage |
| `entities` | `core/pipeline/ner.py` | NER output (PERSON, ORG, LOC, GPE, etc. + meta) |
| `geo_entities` | `core/pipeline/geo.py` | Country-resolved locations with ISO codes |
| `images` | IngestService | Extracted from HTML (og:image, twitter:image, etc.) |

##### How `content IS NULL` works:

An entry is considered **unprocessed** when its `content` field is `NULL`. The pipeline picks up entries where `flashpoint_id IS NOT NULL AND content IS NULL`, processes them through the enrichment pipeline, and writes text back to `content` — marking them as processed.

#### Sidecar Tables (managed by Alembic)

These are static tables created by this project's migrations:

| Table | Purpose |
|-------|---------|
| `processing_runs` | Tracks daily pipeline runs and their status/metrics |
| `feed_entry_jobs` | Per-entry state machine (queued → extracted → clustered → …) |
| `feed_entry_vectors` | Stores embeddings in pgvector with HNSW index |
| `feed_entry_topics` | IPTC Media Topic classifications |
| `cluster_members` | Links entries to internal cluster UUIDs |

### 5.2 Enable pgvector Extension

If using **Supabase**, enable pgvector from the Dashboard:

1. Go to **Database → Extensions**
2. Search for `vector`
3. Toggle it **ON**

Or via SQL:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

### 5.3 Run Alembic Migrations

```bash
# Make sure your .env has the correct DATABASE_URL_SYNC
alembic upgrade head
```

### 5.4 Verify Migration

```bash
# Check migration status
alembic current
```

Expected output:
```
001_initial (head)
```

Or verify tables exist via SQL:

```sql
SELECT tablename FROM pg_tables
WHERE schemaname = 'public'
AND tablename IN ('processing_runs', 'feed_entry_jobs', 'feed_entry_vectors',
                   'feed_entry_topics', 'cluster_members');
```

Should return 5 rows.

### 5.5 Rollback (if needed)

```bash
# Undo all migrations (drops sidecar tables only, never touches existing tables)
alembic downgrade base
```

---

## 6. Seed Debug Data

For development and testing, you can create dummy `flash_point` and `feed_entries` tables populated with realistic geopolitical data.

### 6.1 Seed with Today's Date

```bash
python scripts/seed_debug_data.py
```

### 6.2 Seed with a Specific Date

```bash
python scripts/seed_debug_data.py --date 2026-02-12
```

### 6.3 Drop and Reseed

```bash
python scripts/seed_debug_data.py --date 2026-02-12 --drop
```

### What Gets Created

The seeder creates:

| Table | Records | Description |
|-------|---------|-------------|
| `flash_point_YYYYMMDD` | 5 flashpoints | Russia-Ukraine, Israel-Palestine, South China Sea, Sudan, Amazon |
| `feed_entries_YYYYMMDD` | 19 entries | Multilingual articles (EN, PT, RU, AR, UK, FR) across all flashpoints |

All feed entries have `content IS NULL` so the pipeline will pick them up as unprocessed.

### Sample Output

```
╔══════════════════════════════════════════════╗
║  MASX-GSGI Debug Data Seeder                ║
╠══════════════════════════════════════════════╣
║  Date:    2026-02-12                         ║
║  FP tbl:  flash_point_20260212             ║
║  FE tbl:  feed_entries_20260212            ║
╚══════════════════════════════════════════════╝

  ✓ Created table: flash_point_20260212
  ✓ Created table: feed_entries_20260212
  ✓ Inserted 5 flashpoints
  ✓ Inserted 19 feed entries

  Languages: {'en': 13, 'pt': 2, 'ru': 1, 'ar': 1, 'uk': 1, 'fr': 1}
  Unprocessed (content IS NULL): 19
  ✓ Ready for pipeline! Run with --date 2026-02-12
```

---

## 7. Running the Pipeline

### 7.1 Choose Your Tier

| Tier | Cost | What It Does |
|------|------|-------------|
| **A** (default) | ~$0.19/day | Fetch + Extract + Dedupe + NER + Geo-entities + Translation |
| **B** | ~$0.21/day | + Embeddings + Clustering + Local extractive summaries |
| **C** | ~$0.22/day | + Two-stage summarization (DistilBART local pre-summary → LLM cluster synthesis) |

Set the tier in `.env`:
```env
PIPELINE_TIER=A   # or B or C
```

Or pass it as a CLI flag (overrides `.env`):

```bash
python -m apps.orchestrator.main --tier B
```

### 7.2 The `--date` Flag

All pipeline commands accept a `--date` flag to target a specific date partition:

```bash
# Process today's tables (default)
python -m apps.orchestrator.main

# Process a specific date's tables
python -m apps.orchestrator.main --date 2026-02-12

# Process the debug data you just seeded
python -m apps.orchestrator.main --date 2026-02-12 --tier A
```

The `--date` flag resolves to the corresponding tables:
- `flash_point_20260212`
- `feed_entries_20260212`
- `news_clusters_20260212` (created on demand)

### 7.3 Run the Full Pipeline (Orchestrator)

The orchestrator runs all stages sequentially: ingest → cluster → summarize → score.

```bash
python -m apps.orchestrator.main --date 2026-02-12
```

**What happens:**
1. Resolves date-partitioned table names for the target date
2. Creates a `processing_run` record with a unique `run_id`
3. Selects up to 10,000 unprocessed entries (`content IS NULL`)
4. Claims a job for each entry (idempotent — safe to re-run)
5. **Ingests**: Fetches HTML → Extracts text → Translates title → Extracts hostname → Runs NER → Resolves geo-entities → Deduplicates → Writes enrichment back
6. **Embeds**: Computes embeddings with sentence-transformers (Tier B/C)
7. **Clusters**: Groups entries per `flashpoint_id` using kNN + Union-Find (Tier B/C)
8. **Summarizes**: Writes `news_clusters` rows with summaries and metadata (Tier B/C)
9. **Scores**: Computes hotspot scores and flags top clusters
10. Records pipeline metrics and exits

### 7.4 Run Individual Workers (Advanced)

Each stage can also be run independently. All workers accept the `--date` flag:

```bash
# 1. Ingest only
python -m apps.ingest_worker.main --date 2026-02-12

# 2. Cluster a specific run
python -m apps.cluster_worker.main run_20260212_040000_abc12345 --date 2026-02-12

# 3. Summarize a specific run
python -m apps.summary_worker.main run_20260212_040000_abc12345 --date 2026-02-12

# 4. Score + alert a specific run
python -m apps.score_alert_worker.main run_20260212_040000_abc12345 --date 2026-02-12
```

### 7.5 Using Registered CLI Commands

After `pip install -e .`, you also get named CLI commands:

```bash
gsgi-orchestrator --tier A --date 2026-02-12
gsgi-ingest --date 2026-02-12
gsgi-cluster run_20260212_040000_abc12345 --date 2026-02-12
gsgi-summarize run_20260212_040000_abc12345 --date 2026-02-12
gsgi-score run_20260212_040000_abc12345 --date 2026-02-12
```

---

## 8. Running Tests

### 8.1 Unit Tests (No Database Required)

```bash
pytest -v -m "not integration and not e2e and not slow"
```

### 8.2 All Tests

```bash
pytest -v
```

### 8.3 With Coverage Report

```bash
pytest --cov=core --cov=apps --cov-report=term-missing
```

### 8.4 Run a Specific Test File

```bash
pytest tests/test_extract.py -v
pytest tests/test_cluster.py -v
pytest tests/test_dedupe.py -v
```

### 8.5 Linting & Formatting

```bash
# Check for issues
ruff check .

# Auto-fix issues
ruff check --fix .

# Check formatting
ruff format --check .

# Auto-format
ruff format .
```

### 8.6 Type Checking

```bash
mypy core/ apps/ --ignore-missing-imports
```

---

## 9. Debugging the Pipeline

When something goes wrong — or you just want to understand the data flow — follow these steps to isolate the issue.

### 9.1 Enable Verbose Logging

Switch to human-readable debug output before investigating:

```env
# In your .env file
LOG_LEVEL=DEBUG
LOG_FORMAT=console
```

- **`DEBUG`** shows every fetch, extraction, NER, geo-resolution, and deduplication decision.
- **`console`** uses `rich` for colorized, structured output instead of raw JSON.

> **Tip**: Reset to `LOG_LEVEL=INFO` and `LOG_FORMAT=json` after debugging to avoid noise.

### 9.2 Validate Configuration

Ensure Pydantic Settings can load your `.env` correctly:

```bash
python -c "
from core.config import get_settings
s = get_settings()
print(f'Tier:       {s.pipeline_tier.value}')
print(f'DB URL:     {s.database_url[:40]}...')
print(f'Log:        {s.log_level} / {s.log_format}')
print(f'Clustering: {s.tier_has_clustering}')
print(f'LLM:        {s.tier_has_llm}')
print(f'Concurrent: {s.max_concurrent_fetches}')
"
```

| Output | Meaning |
|--------|---------|
| `ValidationError` | Missing required env var (e.g., `DATABASE_URL`) |
| Wrong tier shown | `PIPELINE_TIER` not set or `.env` not being read |
| Clustering `False` when expected `True` | Tier is `A` — set to `B` or `C` |

### 9.3 Validate Database Connection

Use the built-in checker:

```bash
python _check_db.py
```

Or test manually:

```bash
python -c "
import asyncio
from core.db.engine import get_async_session

async def check():
    factory = get_async_session()
    async with factory() as session:
        result = await session.execute(__import__('sqlalchemy').text('SELECT 1'))
        print('DB connection OK:', result.scalar())

        tables = await session.execute(__import__('sqlalchemy').text(
            \"\"\"SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public' ORDER BY table_name\"\"\"
        ))
        for row in tables:
            print(f'  📋 {row[0]}')

asyncio.run(check())
"
```

**Common failures:**
- `Connection refused` → Docker DB not running or wrong `DATABASE_URL`
- Missing sidecar tables → Run `alembic upgrade head`
- No date-partitioned tables → Upstream hasn't populated them, or use the [debug seeder](#6-seed-debug-data)

### 9.4 Verify Date-Partitioned Tables Exist

The pipeline dynamically resolves `feed_entries_YYYYMMDD` tables. Test the resolver:

```bash
python -c "
import asyncio
from core.db.engine import get_async_session
from core.db.table_resolver import TableContext

async def check():
    factory = get_async_session()
    async with factory() as session:
        ctx = await TableContext.create(session)
        print(f'Target date:    {ctx.target_date}')
        print(f'feed_entries:   {ctx.feed_entries}')
        print(f'flash_point:    {ctx.flash_point}')
        print(f'news_clusters:  {ctx.news_clusters}')

asyncio.run(check())
"
```

**If no tables are found:** Seed debug data for a specific date:
```bash
python scripts/seed_debug_data.py --date 2026-02-12
```

### 9.5 Run Pipeline with Minimum Tier First

Always start debugging with **Tier A** (cheapest — fetch + extract + dedupe only):

```bash
python -m apps.orchestrator.main --tier A --date 2026-02-12
```

#### Key log events to watch:

| Log Event | Meaning | 🟢 Good | 🔴 Bad |
|-----------|---------|---------|--------|
| `pipeline_starting` | Config loaded | `tier=A` visible | Missing |
| `tables_resolved` | Found date tables | Shows table names | "No tables found" |
| `entries_selected` | Found entries | `total > 0` | `total = 0` |
| `jobs_claimed` | Created job records | `claimed ≈ total` | `claimed = 0` |
| `fetch_complete` | URL fetched | `status=200` | `status=403/429/timeout` |
| `extraction_complete` | Text extracted | `method` + `char_count` | `no_text` |
| `pipeline_completed` | Finished | Metrics shown | Never appears |
| `pipeline_failed` | Crashed | — | Exception shown |

Once Tier A works, escalate to `--tier B` (adds embeddings + clustering + summaries).

### 9.6 Diagnose Fetch Failures

If many entries fail at the fetch stage, query the job table:

```sql
-- Failure breakdown for a specific run
SELECT failure_reason, COUNT(*) as cnt
FROM feed_entry_jobs
WHERE run_id = '<your_run_id>' AND status = 'failed'
GROUP BY failure_reason
ORDER BY cnt DESC;

-- Which domains are failing?
SELECT fe.domain, COUNT(*) as failures
FROM feed_entry_jobs fej
JOIN "feed_entries_20260212" fe ON fe.id = fej.feed_entry_id
WHERE fej.status = 'failed'
GROUP BY fe.domain
ORDER BY failures DESC
LIMIT 20;
```

**Remedies:**

| Symptom | Fix |
|---------|-----|
| Timeouts | Increase `FETCH_TIMEOUT_SECONDS=45` |
| 403 Forbidden | Site blocks bots — enable `PLAYWRIGHT_ENABLED=true` |
| 429 Rate Limited | Lower `PER_DOMAIN_CONCURRENCY=1` |
| Circuit breaker tripping | Wait 5 min cooldown, or lower failure threshold |
| DNS resolution failures | Check network / firewall |

### 9.7 Diagnose Extraction Failures

If fetches succeed but extraction returns empty text, test manually:

```bash
python -c "
import asyncio
from core.pipeline.fetch import AsyncFetcher
from core.pipeline.extract import extract_article

async def test():
    async with AsyncFetcher() as fetcher:
        html, status, _ = await fetcher.fetch('https://www.reuters.com/world/')
        print(f'Fetched: status={status}, HTML length={len(html or \'\')}')
        if html:
            result = extract_article(html, 'https://www.reuters.com/world/')
            print(f'Extracted: {len(result.text) if result else 0} chars')
            print(f'Method: {result.method if result else \"none\"}')

asyncio.run(test())
"
```

The extraction ensemble tries 4 methods in order: **Trafilatura → readability-lxml → jusText → BoilerPy3**. If all fail:
- The site likely requires JavaScript rendering → enable `PLAYWRIGHT_ENABLED=true`
- The extracted text may be too short → lower `MIN_CONTENT_LENGTH=100`

### 9.8 Diagnose Deduplication

```sql
SELECT
    COUNT(*) FILTER (WHERE is_duplicate) as duplicates,
    COUNT(*) FILTER (WHERE NOT is_duplicate) as unique_entries,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_duplicate)
          / NULLIF(COUNT(*), 0), 1) as dup_pct
FROM feed_entry_jobs
WHERE run_id = '<your_run_id>';
```

- **>50% duplicates** is common for multi-source news feeds (working as designed)
- **Too aggressive?** → Raise `MINHASH_THRESHOLD=0.9` (fewer near-dupes caught)
- **Missing duplicates?** → Lower `MINHASH_THRESHOLD=0.7`

### 9.9 Diagnose Embedding & Clustering (Tier B/C)

**Embedding issues:**
- OOM during embedding → Lower `EMBEDDING_BATCH_SIZE=32` or use smaller model `all-MiniLM-L6-v2`
- No vectors stored → Check `SELECT COUNT(*) FROM feed_entry_vectors;`

**Clustering issues:**

```sql
-- Cluster size distribution
SELECT cluster_uuid, COUNT(*) as members
FROM cluster_members
WHERE flashpoint_id = '<fp_id>'
GROUP BY cluster_uuid
ORDER BY members DESC;
```

| Symptom | Fix |
|---------|-----|
| All articles in one giant cluster | Raise `CLUSTER_COSINE_THRESHOLD=0.75` |
| All singletons (no grouping) | Lower `CLUSTER_COSINE_THRESHOLD=0.50` |
| Bad embeddings | Articles may be very short (<100 chars) → lower quality vectors |
| Empty `news_clusters` | Ensure tier ≥ B; check that `flashpoint_id` is not NULL on entries |

### 9.10 Run Individual Workers in Isolation

If the orchestrator fails, isolate which stage is breaking by running workers individually:

```bash
# 1. Ingest only (fetch + extract + dedupe + NER + geo)
python -m apps.ingest_worker.main --date 2026-02-12

# 2. Cluster only (needs run_id from step 1 — check processing_runs table)
python -m apps.cluster_worker.main <run_id> --date 2026-02-12

# 3. Summarize only
python -m apps.summary_worker.main <run_id> --date 2026-02-12

# 4. Score + alert
python -m apps.score_alert_worker.main <run_id> --date 2026-02-12
```

To find the `run_id` from a previous ingest:

```sql
SELECT run_id, status, started_at FROM processing_runs
ORDER BY created_at DESC LIMIT 5;
```

### 9.11 Inspect a Failed Run

```sql
-- Run status and error
SELECT run_id, status, error_message, metrics
FROM processing_runs
WHERE status = 'failed'
ORDER BY created_at DESC
LIMIT 5;

-- Job-level detail for that run
SELECT status, failure_reason, extraction_method, COUNT(*)
FROM feed_entry_jobs
WHERE run_id = '<run_id>'
GROUP BY status, failure_reason, extraction_method
ORDER BY count DESC;
```

### 9.12 Re-Run After Fixing an Issue

The pipeline is **idempotent** — jobs are claimed via unique constraints, so re-running is safe:

```bash
# Simply re-run the orchestrator with the same date
python -m apps.orchestrator.main --tier A --date 2026-02-12
```

To force re-processing of failed entries:

```sql
-- Delete failed jobs so they can be re-claimed
DELETE FROM feed_entry_jobs
WHERE run_id = '<run_id>' AND status = 'failed';
```

To completely start fresh with debug data:

```bash
python scripts/seed_debug_data.py --date 2026-02-12 --drop
python -m apps.orchestrator.main --tier A --date 2026-02-12
```

### 9.13 Skip URL Fetching (Test Enrichment Only)

If you want to test NER, geo-resolution, and translation without fetching live URLs:

```bash
# Seed with pre-filled sample content
python scripts/seed_debug_data.py --date 2026-02-12 --drop --with-content
```

This fills the `content` field with realistic article text so the pipeline skips fetching and goes straight to enrichment stages.

> **Note**: Entries with `content IS NOT NULL` are treated as "already processed" by the orchestrator's unprocessed filter. The `--with-content` mode is designed for testing downstream enrichment modules directly.

### 9.14 Debug Checklist

| # | Check | Command / Query |
|---|-------|-----------------|
| 1 | Python 3.12+? | `python --version` |
| 2 | `.env` valid? | `python -c "from core.config import get_settings; get_settings()"` |
| 3 | DB reachable? | `python _check_db.py` |
| 4 | Sidecar tables exist? | `alembic current` → should show `head` |
| 5 | Date tables exist? | `python scripts/seed_debug_data.py --date YYYY-MM-DD` |
| 6 | Feed data present? | `SELECT COUNT(*) FROM "feed_entries_YYYYMMDD" WHERE content IS NULL;` |
| 7 | Run succeeded? | `SELECT run_id, status FROM processing_runs ORDER BY created_at DESC LIMIT 5;` |
| 8 | Jobs completed? | `SELECT status, COUNT(*) FROM feed_entry_jobs WHERE run_id='...' GROUP BY status;` |
| 9 | Why did jobs fail? | `SELECT failure_reason, COUNT(*) FROM feed_entry_jobs WHERE status='failed' GROUP BY failure_reason;` |
| 10 | Vectors stored? | `SELECT COUNT(*) FROM feed_entry_vectors;` |
| 11 | Clusters created? | `SELECT COUNT(*) FROM cluster_members;` |
| 12 | Output written? | `SELECT COUNT(*) FROM "news_clusters_YYYYMMDD";` |

---

## 10. Verify Output

After a successful pipeline run, verify the output. Remember that **all queries against date-partitioned tables must use the physical table name** (e.g., `feed_entries_20260212`, not `feed_entries`).

### 9.1 Check Run Status

```sql
SELECT run_id, status, pipeline_tier, target_date,
       started_at, completed_at, metrics
FROM processing_runs
ORDER BY created_at DESC
LIMIT 5;
```

### 9.2 Check Job Breakdown

```sql
SELECT status, COUNT(*) as count
FROM feed_entry_jobs
WHERE run_id = '<your_run_id>'
GROUP BY status
ORDER BY count DESC;
```

### 9.3 Check Enrichment Fields

```sql
-- Verify enrichment was written back to feed_entries
SELECT id,
       LEFT(title, 50) as title,
       LEFT(title_en, 50) as title_en,
       hostname,
       LENGTH(content) as content_len,
       entities IS NOT NULL as has_entities,
       geo_entities IS NOT NULL as has_geo,
       ARRAY_LENGTH(images, 1) as image_count
FROM "feed_entries_20260212"
WHERE content IS NOT NULL
LIMIT 10;
```

### 9.4 Check NER Entities

```sql
-- View extracted entities for an entry
SELECT id,
       LEFT(title, 60) as title,
       entities->'PERSON' as persons,
       entities->'ORG' as orgs,
       entities->'LOC' as locations,
       entities->'meta'->>'score' as ner_confidence
FROM "feed_entries_20260212"
WHERE entities IS NOT NULL
LIMIT 5;
```

### 9.5 Check Geo-Entities

```sql
-- View resolved geo-entities
SELECT id,
       LEFT(title, 60) as title,
       jsonb_array_elements(geo_entities)->>'name' as country,
       jsonb_array_elements(geo_entities)->>'alpha2' as code,
       jsonb_array_elements(geo_entities)->>'count' as mentions
FROM "feed_entries_20260212"
WHERE geo_entities IS NOT NULL AND jsonb_array_length(geo_entities) > 0
LIMIT 10;
```

### 9.6 Check Clusters Created (Tier B/C)

```sql
SELECT flashpoint_id, cluster_id, article_count,
       LEFT(summary, 100) as summary_preview,
       top_domains, languages
FROM "news_clusters_20260212"
ORDER BY article_count DESC
LIMIT 10;
```

### 9.7 Check for Common Issues

```sql
-- Top failure reasons
SELECT failure_reason, COUNT(*) FROM feed_entry_jobs
WHERE status = 'failed'
GROUP BY failure_reason ORDER BY count DESC;

-- Extraction methods used
SELECT extraction_method, COUNT(*) FROM feed_entry_jobs
WHERE extraction_method IS NOT NULL
GROUP BY extraction_method ORDER BY count DESC;

-- Duplicate rate
SELECT
    COUNT(*) FILTER (WHERE is_duplicate) as duplicates,
    COUNT(*) FILTER (WHERE NOT is_duplicate) as unique_entries,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_duplicate) / NULLIF(COUNT(*), 0), 1) as dup_pct
FROM feed_entry_jobs
WHERE run_id = '<your_run_id>';
```

---

## 11. Architecture Reference

### Data Flow

```
ai-global-signal-grid (upstream)       MASX-GSGI (this project)
────────────────────────────────       ────────────────────────────
flash_point_YYYYMMDD ──────────────►  FlashPointRepo (READ ONLY)
                                            │
feed_entries_YYYYMMDD ─────────────►  FeedEntryRepo
  (id, url, title, seendate,               │
   domain, language, sourcecountry,        ▼
   description, image)              ┌─────────────────────┐
                                    │   Enrichment Pipeline │
                                    ├─────────────────────┤
                                    │ 1. Fetch HTML        │
                                    │ 2. Extract text      │
                                    │ 3. Detect language   │
                                    │ 4. Translate title   │
                                    │ 5. Extract hostname  │
                                    │ 6. NER → entities    │
                                    │ 7. Geo → geo_entities│
                                    │ 8. Deduplicate       │
                                    │ 9. Compress          │
                                    │10. Embed (Tier B/C)  │
                                    └─────────┬───────────┘
                                              │
                                              ▼
                                    feed_entries_YYYYMMDD (WRITE BACK)
                                      (title_en, hostname, content,
                                       compressed_content, summary,
                                       entities, geo_entities, images)
                                              │
                                              ▼
                                    news_clusters_YYYYMMDD (OUTPUT)
                                      (flashpoint_id, cluster_id,
                                       summary, article_count,
                                       top_domains, languages,
                                       urls, images)
```

### Pipeline Modules

| Module | Purpose |
|--------|---------|
| `core/pipeline/fetch.py` | Async HTTP fetching with domain-level circuit breakers |
| `core/pipeline/extract.py` | Ensemble article text extraction (Trafilatura, readability, jusText, BoilerPy3) |
| `core/pipeline/lang.py` | Language detection using fastText LID model |
| `core/pipeline/translate.py` | Title translation to English (argostranslate) + hostname extraction |
| `core/pipeline/ner.py` | Named Entity Recognition using `distilbert-base-multilingual-cased-ner-hrl` |
| `core/pipeline/geo.py` | Geo-entity resolution — LOC/GPE entities → ISO country codes via pycountry |
| `core/pipeline/dedupe.py` | Content deduplication (exact hash + MinHash LSH near-duplicate) |
| `core/pipeline/embed.py` | Sentence embeddings via `all-MiniLM-L6-v2` (Tier B/C) |
| `core/pipeline/topics.py` | IPTC Media Topic classification via ONNX model |
| `core/pipeline/cluster.py` | kNN graph + Union-Find clustering (Tier B/C) |
| `core/pipeline/local_summarizer.py` | DistilBART local pre-summarization with ONNX support + ProcessPoolExecutor (8 workers) |
| `core/pipeline/toml_serde.py` | Token-efficient TOML serialization for LLM I/O |
| `core/pipeline/summarize.py` | Two-stage: local DistilBART pre-summary → LLM cluster synthesis (Tier C) |
| `core/pipeline/score.py` | Hotspot scoring algorithm |
| `core/pipeline/alerts.py` | Alert dispatch (webhook/Slack stubs) |

### Database Layer

| Module | Purpose |
|--------|---------|
| `core/db/engine.py` | Async SQLAlchemy engine with pgBouncer-compatible settings |
| `core/db/models.py` | ORM models for sidecar tables only (not date-partitioned) |
| `core/db/repositories.py` | CRUD repos — raw SQL for partitioned tables, ORM for sidecar tables |
| `core/db/table_resolver.py` | Dynamic table name resolution (`TableContext`) + `ensure_output_table` |

### Key Design Decisions

1. **Raw SQL for partitioned tables**: ORM models are not used for `feed_entries_YYYYMMDD`, `flash_point_YYYYMMDD`, or `news_clusters_YYYYMMDD` because the table names are dynamic. All queries use `text()` with `CAST(:param AS jsonb)` syntax (not `::jsonb`) for asyncpg compatibility.

2. **`CAST()` not `::` for parameterized JSONB**: asyncpg converts named parameters (`:param`) to positional (`$N`), which mangles the `::jsonb` cast syntax. Always use `CAST(:param AS jsonb)` in parameterized queries.

3. **`content IS NULL` as "unprocessed" marker**: The pipeline uses the nullity of the `content` field to determine whether an entry has been processed. This matches the upstream convention.

4. **pgBouncer compatibility**: The engine disables prepared statement caching (`statement_cache_size=0`) and uses unnamed prepared statements (`prepared_statement_name_func=lambda: ""`) for Supabase's pgBouncer.

---

## 12. Railway Deployment

### 11.1 Create Railway Project

1. Go to [railway.app](https://railway.app) and create a new project
2. Connect your GitHub repository

### 11.2 Add Environment Variables

In Railway Dashboard → Variables, add:

```
DATABASE_URL=postgresql+asyncpg://user:pass@host:6543/postgres
DATABASE_URL_SYNC=postgresql://user:pass@host:6543/postgres
LLM_API_KEY=your-key-here       # Only for Tier C
LLM_BASE_URL=https://api.together.xyz/v1
LLM_MODEL=meta-llama/Llama-3.2-3B-Instruct-Turbo
PIPELINE_TIER=B
MAX_CONCURRENT_FETCHES=50
LOCAL_SUMMARIZER_WORKERS=8      # CPUs for DistilBART (default: 8)
LOG_LEVEL=INFO
LOG_FORMAT=json
RAILWAY_ENVIRONMENT=production
```

### 11.3 Configure Cron Service

1. Create a new service in Railway
2. Set **Start Command**: `python -m apps.orchestrator.main`
3. Set **Cron Schedule**: `0 4 * * *` (daily at 4:00 AM UTC)
4. Set **Restart Policy**: Never (cron jobs should terminate)

> The orchestrator auto-detects today's date for table resolution. No `--date` flag needed in production.

### 11.4 Run Initial Migration

In Railway console or via a one-off command:

```bash
alembic upgrade head
```

### 11.5 Monitor

Check Railway logs for structured JSON output with `run_id`, stage timings, and error details.

---

## 13. Next Steps

After your first successful pipeline run:

1. **Check enrichment**: Verify that `title_en`, `entities`, `geo_entities`, `hostname`, and `content` are populated in the feed entries
2. **Tune extraction**: Check which domains fail and why (see failure reasons query above)
3. **Adjust clustering**: Modify `CLUSTER_COSINE_THRESHOLD` if clusters are too large or too small
4. **Upgrade tier**: Move from A → B → C as you validate output quality
5. **Enable Playwright**: Set `PLAYWRIGHT_ENABLED=true` + install browser for JS-heavy sites
6. **Install translation**: Run `pip install -e ".[translation]"` for offline title translation via argostranslate
7. **Review costs**: See [`docs/COST_MODEL.md`](COST_MODEL.md) for per-tier breakdowns
8. **Troubleshoot**: See [`docs/TROUBLESHOOTING.md`](TROUBLESHOOTING.md) for common issues

---

## Quick Reference

| Task | Command |
|------|---------|
| Install dependencies | `pip install -e ".[dev]"` |
| Export ONNX model (optional, 2× faster) | `python scripts/export_onnx.py` |
| Run migrations | `alembic upgrade head` |
| Seed debug data | `python scripts/seed_debug_data.py --date 2026-02-12` |
| Reseed (drop + recreate) | `python scripts/seed_debug_data.py --date 2026-02-12 --drop` |
| Verify DB connection | `python _check_db.py` |
| Run pipeline (Tier A) | `python -m apps.orchestrator.main --tier A --date 2026-02-12` |
| Run pipeline (Tier B) | `python -m apps.orchestrator.main --tier B --date 2026-02-12` |
| Run ingest only | `python -m apps.ingest_worker.main --date 2026-02-12` |
| Run tests | `pytest -v` |
| Lint | `ruff check .` |
| Format | `ruff format .` |
| Type check | `mypy core/ apps/` |
| Rollback DB | `alembic downgrade base` |
| Docker start | `docker compose up -d db` |
| Docker migrate | `docker compose run --rm migrate` |
| Docker test | `docker compose run --rm test` |
