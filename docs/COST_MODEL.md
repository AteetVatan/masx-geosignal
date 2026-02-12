# MASX-GSGI — Cost Model

## Daily Pipeline Costs (10,000 URLs/day)

### Tier A — CPU-Only (Cheapest)

| Component | Unit Cost | Daily Volume | Daily Cost |
|-----------|----------|-------------|------------|
| Railway Cron (512MB, ~30min) | ~$0.000463/min | ~30 min | **$0.014** |
| Supabase Postgres (Pro) | Included | — | **$0** (within plan) |
| Network egress | ~$0.09/GB | ~2 GB HTML | **$0.18** |
| **Total Tier A** | | | **~$0.19/day** |

### Tier B — Balanced (With Embeddings + Clustering)

| Component | Unit Cost | Daily Volume | Daily Cost |
|-----------|----------|-------------|------------|
| Railway Cron (1GB, ~60min) | ~$0.000463/min | ~60 min | **$0.028** |
| Embedding computation (CPU) | ~0ms additional Railway | 10k entries | **$0** (included in runtime) |
| pgvector storage | Included in Supabase | 10k × 384 dims | **$0** |
| Network egress | ~$0.09/GB | ~2 GB | **$0.18** |
| **Total Tier B** | | | **~$0.21/day** |

### Tier C — Best Quality (With LLM)

| Component | Unit Cost | Daily Volume | Daily Cost |
|-----------|----------|-------------|------------|
| Railway Cron (1GB, ~60min) | ~$0.000463/min | ~60 min | **$0.028** |
| OpenAI Batch API (gpt-4o-mini) | $0.075/1M input + $0.30/1M output (50% discount) | ~500 clusters × 2K tokens | **~$0.12** |
| Premium LLM pass (top 10%) | $0.075/1M input | ~50 clusters × 3K tokens | **~$0.02** |
| **Total Tier C** | | | **~$0.35/day** |

## Monthly Estimates

| Tier | Daily | Monthly (30 days) | Annual |
|------|-------|------------------|--------|
| A (CPU-only) | $0.19 | **$5.70** | **$68** |
| B (Balanced) | $0.21 | **$6.30** | **$76** |
| C (Best quality) | $0.35 | **$10.50** | **$126** |

## Cost Optimization Strategies

### Already Implemented

1. **Dedupe-first**: Skip embeddings/clustering for duplicate content (~20-30% savings)
2. **Cluster-first, then summarize**: 1 summary per cluster vs 1 per article (~80% LLM savings)
3. **Translate summaries only**: Not full article content (~90% translation savings)
4. **OpenAI Batch API**: 50% discount vs real-time API
5. **Local embeddings**: sentence-transformers on CPU = $0 per embedding
6. **Tiered pipeline**: Only pay for what you need

### Future Optimizations

1. **Content caching**: Cache extracted HTML for re-processing (avoid re-fetching)
2. **Incremental embeddings**: Only embed new/changed content
3. **Batch size tuning**: Larger batches = fewer API calls
4. **Model distillation**: Replace gpt-4o-mini with fine-tuned smaller model
5. **Spot instances**: Use preemptible Railway services for batch work

## Cost Breakdown by Component

```
Tier A:  ████████████████████████████ Compute(7%) + Network(93%)
Tier B:  ████████████████████████████ Compute(13%) + Network(86%) + Storage(1%)
Tier C:  ████████████████████████████ Compute(8%) + Network(51%) + LLM(34%) + Storage(7%)
```

## Comparison with Alternatives

| Approach | Monthly Cost (10k/day) | Quality |
|----------|----------------------|---------|
| **Our Tier A** | **$5.70** | Good (extractive) |
| **Our Tier C** | **$10.50** | Excellent |
| Real-time GPT-4o per article | ~$300 | Overkill |
| AWS Comprehend per article | ~$150 | Good |
| Google Cloud NL per article | ~$100 | Good |

The pipeline is **30-50x cheaper** than naive per-article LLM approaches.
