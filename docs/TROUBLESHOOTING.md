# MASX-GSGI — Troubleshooting Guide

## Common Issues

### 1. Extraction Failures

**Symptom**: High percentage of `failed` jobs with `no_text` reason.

**Causes**:
- Target site requires JavaScript rendering
- Site has cookie consent wall
- Content behind paywall
- HTML structure not recognized by extractors

**Solutions**:
```bash
# Check failure reasons
SELECT failure_reason, COUNT(*) FROM feed_entry_jobs
WHERE status = 'failed' GROUP BY failure_reason ORDER BY count DESC;

# Check which domains are problematic
SELECT fe.domain, COUNT(*) as failures
FROM feed_entry_jobs fej
JOIN feed_entries fe ON fe.id = fej.feed_entry_id
WHERE fej.status = 'failed'
GROUP BY fe.domain ORDER BY failures DESC LIMIT 20;
```

- For JS-heavy sites: Enable `PLAYWRIGHT_ENABLED=true`
- For consent walls: Playwright can auto-dismiss (when implemented)
- For paywalls: These are expected failures; mark as `paywall` and skip

### 2. High Duplicate Rate

**Symptom**: >50% of entries marked as `skipped_duplicate`.

**Investigation**:
```sql
-- Check dedupe stats per run
SELECT
    COUNT(*) FILTER (WHERE is_duplicate) as duplicates,
    COUNT(*) FILTER (WHERE NOT is_duplicate) as unique,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_duplicate) / COUNT(*), 1) as dup_pct
FROM feed_entry_jobs WHERE run_id = 'run_...';
```

**Solutions**:
- This is actually a cost saver — working as designed
- If too aggressive: increase `MINHASH_THRESHOLD` (e.g., 0.9 → fewer near-dupes)
- Check if feed source is sending genuinely different articles

### 3. Clustering Issues

**Symptom**: All articles end up in one cluster, or all singletons.

**Diagnosis**:
```sql
-- Cluster size distribution
SELECT cm.cluster_uuid, COUNT(*) as members
FROM cluster_members cm
WHERE cm.flashpoint_id = 'xxx'
GROUP BY cm.cluster_uuid ORDER BY members DESC;
```

**Solutions**:
- **All in one cluster**: Increase `CLUSTER_COSINE_THRESHOLD` (e.g., 0.65 → 0.75)
- **All singletons**: Decrease `CLUSTER_COSINE_THRESHOLD` (e.g., 0.65 → 0.50)
- **Bad embeddings**: Check if articles are very short (<100 chars) → lower quality embeddings
- **Wrong model**: Try a different embedding model (e.g., `all-mpnet-base-v2`)

### 4. Database Connection Issues

**Symptom**: `asyncpg.exceptions.TooManyConnectionsError`

**Solutions**:
- Check `pool_size` in engine config (default: 20)
- Supabase free tier limits: upgrade to Pro
- Use connection pooler (Supabase provides PgBouncer):
  ```
  DATABASE_URL=postgresql+asyncpg://...pooler.supabase.com:6543/postgres
  ```

### 5. Memory Issues on Railway

**Symptom**: Service killed with OOM during embedding computation.

**Solutions**:
- Reduce embedding batch size: `EMBEDDING_BATCH_SIZE=32` (default: 64)
- Use a smaller model: `EMBEDDING_MODEL=all-MiniLM-L6-v2` (384 dims, ~90MB)
- Increase Railway service memory allocation
- Process entries in smaller chunks

### 6. OpenAI Batch API Issues

**Symptom**: Batch job stuck or results not arriving.

**Investigation**:
```python
import openai
client = openai.OpenAI()

# Check batch status
batch = client.batches.retrieve("batch_xxx")
print(batch.status, batch.request_counts)
```

**Solutions**:
- Batch API can take up to 24 hours — this is expected
- Check for malformed JSONL in the batch input file
- Verify API key has batch permissions
- Fallback to local extractive summary (always available)

### 7. Alembic Migration Failures

**Symptom**: `alembic upgrade head` fails.

**Common causes**:
```bash
# pgvector not enabled
psql $DATABASE_URL_SYNC -c "CREATE EXTENSION IF NOT EXISTS vector;"

# Enum already exists (re-running migration)
psql $DATABASE_URL_SYNC -c "DROP TYPE IF EXISTS run_status CASCADE;"
psql $DATABASE_URL_SYNC -c "DROP TYPE IF EXISTS job_status CASCADE;"
psql $DATABASE_URL_SYNC -c "DROP TYPE IF EXISTS failure_reason CASCADE;"

# Then retry
alembic upgrade head
```

### 8. Circuit Breaker Blocking Domain

**Symptom**: Domain entries all failing with "Domain blocked (circuit breaker)".

**Investigation**:
Check logs for the domain's failure history. Circuit breaker opens after 5 consecutive failures and auto-resets after 5 minutes.

**Solutions**:
- Wait for cooldown (5 min)
- Check if the domain is actually down
- Investigate rate-limiting by the domain
- Adjust `DomainCircuitBreaker.threshold` if too aggressive

### 9. Slow Pipeline Execution

**Symptom**: Pipeline takes >2 hours for 10k entries.

**Investigation**:
```sql
-- Timing stats per stage
SELECT extraction_method, 
       AVG(fetch_duration_ms) as avg_fetch,
       AVG(extract_duration_ms) as avg_extract
FROM feed_entry_jobs 
WHERE run_id = 'run_...' AND status != 'failed'
GROUP BY extraction_method;
```

**Solutions**:
- Increase `MAX_CONCURRENT_FETCHES` (50 → 100)
- Decrease `REQUEST_DELAY_SECONDS` (0.25 → 0.1)
- Reduce `FETCH_TIMEOUT_SECONDS` (30 → 15)
- Skip slow domains (use circuit breaker)

### 10. Empty news_clusters Table

**Symptom**: Pipeline completes but `news_clusters` has no rows.

**Checklist**:
1. Are entries being extracted? Check `feed_entry_jobs` for `extracted` status
2. Are entries being embedded? Check `feed_entry_vectors` table
3. Is clustering running? Check for `clustered` status in jobs
4. Is summarization running? Check pipeline tier ≥ B for clustering
5. Do entries have `flashpoint_id`? NULL flashpoint = skipped

```sql
-- Quick health check
SELECT 
    (SELECT COUNT(*) FROM feed_entry_jobs WHERE status = 'extracted') as extracted,
    (SELECT COUNT(*) FROM feed_entry_vectors) as embedded,
    (SELECT COUNT(*) FROM cluster_members) as clustered,
    (SELECT COUNT(*) FROM news_clusters) as summarized;
```

## Performance Tuning Guide

| Parameter | Default | Faster | Careful |
|-----------|---------|--------|---------|
| `MAX_CONCURRENT_FETCHES` | 50 | 200 | May trigger rate limits |
| `PER_DOMAIN_CONCURRENCY` | 3 | 10 | May get blocked |
| `FETCH_TIMEOUT_SECONDS` | 30 | 15 | May miss slow sites |
| `REQUEST_DELAY_SECONDS` | 0.25 | 0.05 | May trigger 429s |
| `MINHASH_THRESHOLD` | 0.8 | 0.9 | Fewer near-dupes caught |
| `CLUSTER_COSINE_THRESHOLD` | 0.65 | 0.5 | Larger clusters |
| `CLUSTER_KNN_K` | 10 | 20 | More connected graph |
