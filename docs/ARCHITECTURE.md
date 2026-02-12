# MASX-GSGI — Architecture

> Global Security & Geopolitical Intelligence: Daily News Ingestion + NLP Pipeline

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATOR (Cron)                          │
│  Runs daily · Creates processing_run · Dispatches stages           │
└──────────┬──────────────┬──────────────┬──────────────┬────────────┘
           │              │              │              │
           ▼              ▼              ▼              ▼
   ┌──────────────┐ ┌───────────┐ ┌───────────┐ ┌──────────────┐
   │ INGEST       │ │ CLUSTER   │ │ SUMMARY   │ │ SCORE/ALERT  │
   │ WORKER       │ │ WORKER    │ │ WORKER    │ │ WORKER       │
   │              │ │           │ │           │ │              │
   │ fetch →      │ │ kNN graph │ │ local     │ │ hotspot      │
   │ extract →    │ │ union-find│ │ extractive│ │ scoring      │
   │ dedupe →     │ │ per FP_ID │ │ or Batch  │ │ alerts       │
   │ embed        │ │           │ │ LLM       │ │              │
   └──────┬───────┘ └─────┬─────┘ └─────┬─────┘ └──────┬───────┘
          │               │             │               │
          ▼               ▼             ▼               ▼
   ┌──────────────────────────────────────────────────────────────┐
   │                    POSTGRES (Supabase)                       │
   │                                                              │
   │  feed_entries        ←  existing (read + update content)     │
   │  flash_point         ←  existing (read-only)                 │
   │  news_clusters       ←  existing (write final output)        │
   │                                                              │
   │  processing_runs     ←  NEW (run tracking)                   │
   │  feed_entry_jobs     ←  NEW (per-entry state machine)        │
   │  feed_entry_vectors  ←  NEW (pgvector embeddings)            │
   │  feed_entry_topics   ←  NEW (IPTC classifications)           │
   │  cluster_members     ←  NEW (internal cluster links)         │
   └──────────────────────────────────────────────────────────────┘
```

## Module Boundaries

### 1. Core Layer (`core/`)

| Module | Responsibility |
|--------|---------------|
| `core/config/` | Pydantic Settings, logging setup |
| `core/db/` | SQLAlchemy engine, ORM models, repositories |
| `core/pipeline/extract.py` | 4-method extraction ensemble + heuristics |
| `core/pipeline/fetch.py` | Async HTTP with concurrency/circuit breakers |
| `core/pipeline/dedupe.py` | SHA-256 + MinHash LSH deduplication |
| `core/pipeline/lang.py` | fastText language identification |
| `core/pipeline/embed.py` | sentence-transformers embeddings |
| `core/pipeline/cluster.py` | kNN graph + Union-Find clustering |
| `core/pipeline/summarize.py` | Extractive + OpenAI Batch API summaries |
| `core/pipeline/topics.py` | ONNX IPTC topic classification |
| `core/pipeline/score.py` | Hotspot scoring (4-component weighted) |
| `core/pipeline/alerts.py` | Webhook / Slack / email dispatch |

### 2. Apps Layer (`apps/`)

| Service | Type | Schedule |
|---------|------|----------|
| `orchestrator` | Cron job | Daily 04:00 UTC |
| `ingest_worker` | Cron / standalone | With orchestrator |
| `cluster_worker` | Cron / standalone | After ingestion |
| `summary_worker` | Cron / standalone | After clustering |
| `score_alert_worker` | Cron / standalone | After summarization |

## Job State Machine

Each `feed_entry` passes through these states per run:

```
QUEUED → FETCHING → EXTRACTED → DEDUPED → EMBEDDED → CLUSTERED → SUMMARIZED → SCORED
                                    ↓
                            SKIPPED_DUPLICATE
                                    
     Any state → FAILED (with failure_reason)
```

## Data Flow

```
feed_entries (input)
    ↓
    ├── fetch raw HTML (httpx async)
    ├── extract text (trafilatura → readability → jusText → boilerpy3)
    ├── detect language (fastText)
    ├── dedupe (SHA-256 → MinHash LSH)
    ├── embed (sentence-transformers → pgvector)
    ├── cluster per flashpoint_id (kNN + Union-Find)
    ├── summarize per cluster (local or OpenAI Batch)
    └── score + alert (hotspot formula)
         ↓
news_clusters (output)
```

## Clustering Design (Critical)

**Invariant**: Clusters are formed **strictly per flashpoint_id**.

1. For each `flashpoint_id`, gather all non-duplicate embedded entries
2. Build full cosine similarity matrix from normalized embeddings
3. For each entry, find k nearest neighbors above threshold
4. Use Union-Find to find connected components
5. Sort components by size (DESC)
6. Assign dense-rank `cluster_id` (1, 2, 3, ...)
7. Write to `cluster_members` and then aggregate to `news_clusters`

## Tiered Pipeline Architecture

| Feature | Tier A (CPU-only) | Tier B (Balanced) | Tier C (Best) |
|---------|:--:|:--:|:--:|
| Fetch + Extract | ✅ | ✅ | ✅ |
| Dedupe | ✅ | ✅ | ✅ |
| Language Detection | ✅ | ✅ | ✅ |
| Metadata Storage | ✅ | ✅ | ✅ |
| Embeddings | ❌ | ✅ | ✅ |
| Clustering | ❌ | ✅ | ✅ |
| Local Summaries | ✅ (extractive) | ✅ | ✅ (fallback) |
| LLM Batch Summaries | ❌ | ❌ | ✅ |
| Premium LLM Pass | ❌ | ❌ | ✅ (top 10%) |
| Topic Classification | Week 3 | Week 3 | Week 3 |
| Hotspot Scoring | Week 3 | Week 3 | Week 3 |

## Extraction Ensemble

```
                    Raw HTML
                       ↓
              ┌─── Trafilatura (favor_recall) ───┐
              │        ↓ (if < min_length)       │
              │   readability-lxml               │
              │        ↓ (if < min_length)       │
              │   jusText                        │
              │        ↓ (if < min_length)       │
              │   BoilerPy3                      │
              │        ↓ (if all fail)           │
              │   Detect reason:                 │
              │     js_required? → Playwright    │
              │     paywall? → mark & skip       │
              │     consent? → Playwright        │
              └──────────────────────────────────┘
```

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.12 |
| Async HTTP | httpx (HTTP/2) |
| Database | SQLAlchemy 2.0 + asyncpg |
| Migrations | Alembic |
| Vector DB | pgvector (HNSW index) |
| Config | Pydantic Settings |
| Embedding | sentence-transformers |
| Language ID | fastText LID |
| Topics | ONNX IPTC classifier |
| Dedupe | datasketch MinHash LSH |
| LLM | OpenAI Batch API |
| Logging | structlog (JSON) |
| CLI | Click |
| Deploy | Railway (cron services) |
