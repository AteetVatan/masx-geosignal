"""Quick check: do our sidecar tables exist now?"""
import psycopg2
from core.config import get_settings

url = get_settings().database_url_sync
conn = psycopg2.connect(url)
conn.autocommit = True
cur = conn.cursor()

# Check only our sidecar tables
sidecar = [
    "processing_runs",
    "feed_entry_jobs",
    "feed_entry_vectors",
    "feed_entry_topics",
    "cluster_members",
]
for name in sidecar:
    cur.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name=%s)",
        (name,),
    )
    exists = cur.fetchone()[0]
    print(f"  {'✓' if exists else '✗'} {name}")

# Check enums
cur.execute(
    "SELECT typname FROM pg_type "
    "WHERE typname IN ('run_status', 'job_status', 'failure_reason') ORDER BY typname"
)
enums = [r[0] for r in cur.fetchall()]
print(f"\nEnums: {enums}")

# Check alembic version
cur.execute("SELECT version_num FROM alembic_version")
print(f"Alembic: {[r[0] for r in cur.fetchall()]}")

cur.close()
conn.close()
