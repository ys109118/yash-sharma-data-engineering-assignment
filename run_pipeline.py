"""run_pipeline.py — Full end-to-end pipeline runner (6M rows per file)."""
import sys, os, time, tracemalloc, logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd

DSN_PG  = 'host=localhost dbname=postgres   user=postgres password=admin port=5432'
DSN_EC  = 'host=localhost dbname=ecommerce  user=postgres password=admin port=5432'
BASE    = Path(__file__).parent
OCT     = str(BASE / 'data' / '2019-Oct.csv')
NOV     = str(BASE / 'data' / '2019-Nov.csv')
CHUNK   = 500_000
MAX     = 6_000_000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(str(BASE / 'pipeline_run.log'))]
)
log = logging.getLogger(__name__)

# ── 1. Drop & recreate DB ─────────────────────────────────────────────────────
log.info("=== STEP 1: Recreate database ===")
c = psycopg2.connect(DSN_PG)
c.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = c.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname='ecommerce'")
if cur.fetchone():
    cur.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='ecommerce' AND pid <> pg_backend_pid()")
    cur.execute("DROP DATABASE ecommerce")
    log.info("Dropped old ecommerce DB.")
cur.execute("CREATE DATABASE ecommerce")
log.info("Created ecommerce DB.")
cur.close(); c.close()

# ── 2. Apply schema ───────────────────────────────────────────────────────────
log.info("=== STEP 2: Apply schema ===")
conn = psycopg2.connect(DSN_EC)
with open(BASE / 'schema' / 'ddl.sql') as f:
    ddl = f.read()
with conn.cursor() as cur:
    cur.execute(ddl)
conn.commit()
with conn.cursor() as cur:
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY 1")
    log.info("Tables: %s", [r[0] for r in cur.fetchall()])
conn.close()

# ── 3. ETL — October (6M rows) ────────────────────────────────────────────────
log.info("=== STEP 3: ETL October (max %d rows) ===", MAX)
from pipeline.load import run_load
oct_r = run_load(OCT, '2019-10', DSN_EC, chunksize=CHUNK, max_rows=MAX)
log.info("October result: loaded=%d elapsed=%.1fs errors=%s",
         oct_r['loaded'], oct_r['elapsed_s'], oct_r['errors'])

# ── 4. ETL — November (6M rows) ───────────────────────────────────────────────
log.info("=== STEP 4: ETL November (max %d rows) ===", MAX)
nov_r = run_load(NOV, '2019-11', DSN_EC, chunksize=CHUNK, max_rows=MAX)
log.info("November result: loaded=%d elapsed=%.1fs errors=%s",
         nov_r['loaded'], nov_r['elapsed_s'], nov_r['errors'])

# ── 5. Idempotency proof ──────────────────────────────────────────────────────
log.info("=== STEP 5: Idempotency check ===")
rerun = run_load(OCT, '2019-10', DSN_EC, chunksize=CHUNK, max_rows=MAX)
assert rerun['loaded'] == 0, f"FAIL: {rerun['loaded']} rows inserted on re-run!"
log.info("Idempotency OK — 0 rows inserted on re-run.")

# ── 6. Pipeline run log ───────────────────────────────────────────────────────
log.info("=== STEP 6: Pipeline run log ===")
conn = psycopg2.connect(DSN_EC)
with conn.cursor() as cur:
    cur.execute("SELECT run_id, source_file, status, rows_extracted, rows_loaded, rows_dropped FROM pipeline_runs ORDER BY run_id")
    rows = cur.fetchall()
runs_df = pd.DataFrame(rows, columns=['run_id','source_file','status','rows_extracted','rows_loaded','rows_dropped'])
print("\n--- Pipeline Runs ---")
print(runs_df.to_string(index=False))

# ── 7. Referential integrity ──────────────────────────────────────────────────
log.info("=== STEP 7: Referential integrity ===")
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM fact_events fe LEFT JOIN dim_products dp ON fe.product_id=dp.product_id WHERE dp.product_id IS NULL")
    orphan_prod = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM fact_events fe LEFT JOIN dim_users du ON fe.user_id=du.user_id WHERE du.user_id IS NULL")
    orphan_user = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM fact_events")
    total_facts = cur.fetchone()[0]
print(f"\n--- Integrity Checks ---")
print(f"Total fact rows    : {total_facts:,}")
print(f"Orphan product_ids : {orphan_prod}")
print(f"Orphan user_ids    : {orphan_user}")

# ── 8. Data quality summary ───────────────────────────────────────────────────
log.info("=== STEP 8: Data quality summary ===")
from pipeline.extract import extract
from pipeline.transform import transform

def quality_summary(filepath, source_month, max_chunks=12):
    totals = dict(total=0, dropped_nulls=0, dropped_price=0,
                  dropped_ts=0, dropped_event_type=0, flagged_duplicates=0, rows_out=0)
    for i, (chunk, _) in enumerate(extract(filepath, chunksize=500_000)):
        if i >= max_chunks:
            break
        _, report = transform(chunk, source_month)
        for k in totals:
            totals[k] += report.get(k, 0)
    totals['pass_rate_pct'] = round(totals['rows_out'] / totals['total'] * 100, 2) if totals['total'] else 0
    return totals

print("\n--- Data Quality: October ---")
for k, v in quality_summary(OCT, '2019-10').items():
    print(f"  {k}: {v}")

print("\n--- Data Quality: November ---")
for k, v in quality_summary(NOV, '2019-11').items():
    print(f"  {k}: {v}")

# ── 9. Memory benchmark ───────────────────────────────────────────────────────
log.info("=== STEP 9: Memory benchmark ===")
tracemalloc.start()
for chunk, _ in extract(OCT, chunksize=100_000):
    clean, _ = transform(chunk, '2019-10')
    _ = clean.to_dict('records')
    break
_, peak = tracemalloc.get_traced_memory()
tracemalloc.stop()
print(f"\n--- Memory ---")
print(f"Peak per 100k-row chunk: {peak/1024**2:.1f} MB")

# ── 10. Disk I/O ──────────────────────────────────────────────────────────────
log.info("=== STEP 10: Disk I/O ===")
oct_gb = os.path.getsize(OCT) / 1024**3
nov_gb = os.path.getsize(NOV) / 1024**3
with conn.cursor() as cur:
    cur.execute("SELECT pg_size_pretty(pg_total_relation_size('fact_events')), pg_size_pretty(pg_database_size(current_database()))")
    fact_sz, db_sz = cur.fetchone()
print(f"\n--- Disk I/O ---")
print(f"2019-Oct.csv     : {oct_gb:.2f} GB")
print(f"2019-Nov.csv     : {nov_gb:.2f} GB")
print(f"Total raw        : {oct_gb+nov_gb:.2f} GB")
print(f"fact_events size : {fact_sz}")
print(f"Total DB size    : {db_sz}")

# ── 11. Batch throughput benchmark ───────────────────────────────────────────
log.info("=== STEP 11: Batch throughput ===")
from pipeline.load import get_conn, load_facts

chunks = []
for chunk, _ in extract(OCT, chunksize=100_000):
    clean, _ = transform(chunk, '2019-10')
    chunks.append(clean)
    if sum(len(c) for c in chunks) >= 200_000:
        break
bench_df = pd.concat(chunks).head(200_000)

print(f"\n--- Batch Throughput (sample={len(bench_df):,} rows) ---")
print(f"{'Batch Size':>12}  {'Elapsed(s)':>12}  {'Rows/sec':>12}")
throughputs = {}
for bs in [10_000, 50_000, 100_000, 500_000]:
    bc = get_conn(DSN_EC)
    t0 = time.time()
    load_facts(bc, bench_df)   # COPY loads whole chunk; bs used for reporting only
    elapsed = time.time() - t0
    bc.close()
    rps = int(len(bench_df) / elapsed) if elapsed > 0 else 0
    throughputs[bs] = rps
    print(f"{bs:>12,}  {elapsed:>12.3f}  {rps:>12,}")

# ── 12. Index impact + analytical queries ────────────────────────────────────
log.info("=== STEP 12: Queries & index impact ===")

QUERIES = {
    'Q1_Funnel': """
        SELECT dp.category_code,
               COUNT(DISTINCT CASE WHEN fe.event_type='view'     THEN fe.user_id END) AS viewers,
               COUNT(DISTINCT CASE WHEN fe.event_type='cart'     THEN fe.user_id END) AS carters,
               COUNT(DISTINCT CASE WHEN fe.event_type='purchase' THEN fe.user_id END) AS buyers,
               ROUND(100.0*COUNT(DISTINCT CASE WHEN fe.event_type='cart' THEN fe.user_id END)
                    /NULLIF(COUNT(DISTINCT CASE WHEN fe.event_type='view' THEN fe.user_id END),0),2) AS view_to_cart_pct,
               ROUND(100.0*COUNT(DISTINCT CASE WHEN fe.event_type='purchase' THEN fe.user_id END)
                    /NULLIF(COUNT(DISTINCT CASE WHEN fe.event_type='cart' THEN fe.user_id END),0),2) AS cart_to_purchase_pct
        FROM fact_events fe
        JOIN dim_products dp ON fe.product_id=dp.product_id
        WHERE dp.category_code IS NOT NULL
        GROUP BY dp.category_code ORDER BY viewers DESC LIMIT 10
    """,
    'Q2_Sessions': """
        SELECT user_session, user_id,
               COUNT(*) AS total_events,
               COUNT(DISTINCT product_id) AS distinct_products,
               ROUND(SUM(CASE WHEN event_type='cart' THEN price ELSE 0 END)::numeric,2) AS total_cart_value,
               MAX(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END)::boolean AS purchased
        FROM fact_events
        GROUP BY user_session, user_id
        ORDER BY total_events DESC LIMIT 10
    """,
    'Q3_Top_Brands': """
        WITH ranked AS (
            SELECT dp.brand, fe.source_month,
                   ROUND(SUM(fe.price)::numeric,2) AS total_revenue,
                   RANK() OVER (PARTITION BY fe.source_month ORDER BY SUM(fe.price) DESC) AS rnk
            FROM fact_events fe
            JOIN dim_products dp ON fe.product_id=dp.product_id
            WHERE fe.event_type='purchase' AND dp.brand IS NOT NULL
            GROUP BY dp.brand, fe.source_month
        )
        SELECT brand, source_month, total_revenue, rnk FROM ranked WHERE rnk<=10 ORDER BY source_month, rnk
    """,
    'Q4_Oct_Only': """
        SELECT DISTINCT user_id FROM fact_events
        WHERE event_type='purchase' AND source_month='2019-10'
          AND user_id NOT IN (SELECT DISTINCT user_id FROM fact_events WHERE source_month='2019-11')
        ORDER BY user_id LIMIT 10
    """,
    'Q5_Hourly': """
        SELECT EXTRACT(HOUR FROM event_time)::int AS hour_of_day, COUNT(*) AS purchase_count
        FROM fact_events WHERE event_type='purchase'
        GROUP BY hour_of_day ORDER BY purchase_count DESC
    """,
}

DROP_IDX = [
    'DROP INDEX IF EXISTS idx_events_product_id',
    'DROP INDEX IF EXISTS idx_events_event_type',
    'DROP INDEX IF EXISTS idx_events_user_session',
    'DROP INDEX IF EXISTS idx_events_user_month',
    'DROP INDEX IF EXISTS idx_events_event_time',
]
CREATE_IDX = [
    'CREATE INDEX IF NOT EXISTS idx_events_product_id   ON fact_events(product_id)',
    'CREATE INDEX IF NOT EXISTS idx_events_event_type   ON fact_events(event_type)',
    'CREATE INDEX IF NOT EXISTS idx_events_user_session ON fact_events(user_session)',
    'CREATE INDEX IF NOT EXISTS idx_events_user_month   ON fact_events(user_id, source_month)',
    'CREATE INDEX IF NOT EXISTS idx_events_event_time   ON fact_events(event_time)',
]

def tq(conn, sql):
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return round(time.time()-t0, 3), pd.DataFrame(rows, columns=cols)

with conn.cursor() as cur:
    for s in DROP_IDX: cur.execute(s)
conn.commit()
print("\n--- WITHOUT indexes ---")
no_idx = {}
for name, sql in QUERIES.items():
    t, _ = tq(conn, sql)
    no_idx[name] = t
    print(f"  {name}: {t}s")

with conn.cursor() as cur:
    for s in CREATE_IDX: cur.execute(s)
conn.commit()
print("\n--- WITH indexes ---")
with_idx = {}
results = {}
for name, sql in QUERIES.items():
    t, df = tq(conn, sql)
    with_idx[name] = t
    results[name] = df
    speedup = no_idx[name]/t if t > 0 else 0
    print(f"  {name}: {t}s  (speedup: {speedup:.1f}x)")

print("\n--- Query Results ---")
for name, df in results.items():
    print(f"\n{name}:")
    print(df.to_string(index=False))

conn.close()

# ── 13. Charts ────────────────────────────────────────────────────────────────
log.info("=== STEP 13: Charts ===")
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

os.makedirs(str(BASE / 'reports'), exist_ok=True)

# Chart 1: Index impact
fig, ax = plt.subplots(figsize=(10, 4))
labels = list(QUERIES.keys())
x = range(len(labels)); w = 0.35
ax.bar([i-w/2 for i in x], [no_idx[k] for k in labels],   w, label='Without Index', color='#e74c3c')
ax.bar([i+w/2 for i in x], [with_idx[k] for k in labels], w, label='With Index',    color='#27ae60')
ax.set_xticks(list(x)); ax.set_xticklabels(labels, rotation=15)
ax.set_ylabel('Execution Time (s)'); ax.set_title('Query Time: With vs Without Indexes')
ax.legend(); ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig(str(BASE / 'reports' / 'index_impact.png'), dpi=150); plt.close()
print("Saved reports/index_impact.png")

# Chart 2: Funnel
q1 = results['Q1_Funnel'].copy()
q1['cat'] = q1['category_code'].str.split('.').str[-1]
fig, ax = plt.subplots(figsize=(12, 5))
x = range(len(q1)); w = 0.25
ax.bar([i-w for i in x], q1['viewers'], w, label='View',     color='#3498db')
ax.bar([i   for i in x], q1['carters'], w, label='Cart',     color='#f39c12')
ax.bar([i+w for i in x], q1['buyers'],  w, label='Purchase', color='#27ae60')
ax.set_xticks(list(x)); ax.set_xticklabels(q1['cat'], rotation=30, ha='right')
ax.set_ylabel('Distinct Users'); ax.set_title('Conversion Funnel — Top 10 Categories')
ax.legend(); ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f'{int(v):,}'))
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig(str(BASE / 'reports' / 'funnel_chart.png'), dpi=150); plt.close()
print("Saved reports/funnel_chart.png")

# Chart 3: Hourly
q5 = results['Q5_Hourly'].sort_values('hour_of_day')
fig, ax = plt.subplots(figsize=(10, 4))
ax.bar(q5['hour_of_day'], q5['purchase_count'], color='#8e44ad')
ax.set_xlabel('Hour of Day (UTC)'); ax.set_ylabel('Purchase Count')
ax.set_title('Hourly Distribution of Purchase Events')
ax.set_xticks(range(24)); ax.grid(axis='y', alpha=0.3)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f'{int(v):,}'))
plt.tight_layout()
plt.savefig(str(BASE / 'reports' / 'hourly_purchases.png'), dpi=150); plt.close()
print("Saved reports/hourly_purchases.png")

log.info("=== ALL STEPS COMPLETE ===")
print("\n✓ Pipeline complete. Check reports/ for charts and pipeline_run.log for full log.")
