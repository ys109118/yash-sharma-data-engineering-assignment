"""load.py — Batch-load transformed data into PostgreSQL with idempotency."""
import io
import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def get_conn(dsn):
    return psycopg2.connect(dsn)


def init_schema(conn):
    ddl_path = Path(__file__).parent.parent / 'schema' / 'ddl.sql'
    with open(ddl_path) as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info("Schema initialised.")


def _upsert(conn, table, cols, rows):
    if not rows:
        return
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s ON CONFLICT DO NOTHING"
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=10_000)
    conn.commit()


def load_dimensions(conn, df):
    # dim_users
    users = [(int(x),) for x in df["user_id"].unique()]
    _upsert(conn, "dim_users", ["user_id"], users)

    # dim_categories
    cats = df[["category_id", "category_code"]].drop_duplicates("category_id")
    cat_rows = [
        (int(r.category_id), r.category_code if pd.notna(r.category_code) else None)
        for r in cats.itertuples(index=False)
    ]
    _upsert(conn, "dim_categories", ["category_id", "category_code"], cat_rows)

    # dim_products
    prods = (
        df.sort_values("event_time")
          .drop_duplicates("product_id", keep="last")
        [["product_id", "category_id", "category_code", "brand", "price"]]
    )
    prod_rows = [
        (int(r.product_id), int(r.category_id),
         r.category_code if pd.notna(r.category_code) else None,
         r.brand if pd.notna(r.brand) else None,
         float(r.price))
        for r in prods.itertuples(index=False)
    ]
    _upsert(conn, "dim_products",
            ["product_id", "category_id", "category_code", "brand", "price"],
            prod_rows)


def load_facts(conn, df):
    """COPY chunk into temp table then INSERT … ON CONFLICT DO NOTHING."""
    cols = ["event_time", "event_type", "product_id", "user_id",
            "user_session", "price", "source_month"]
    buf = io.StringIO()
    df[cols].to_csv(buf, index=False, header=False)
    buf.seek(0)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE tmp_facts (
                event_time   TIMESTAMP,
                event_type   VARCHAR(20),
                product_id   BIGINT,
                user_id      BIGINT,
                user_session VARCHAR(64),
                price        NUMERIC(10,2),
                source_month CHAR(7)
            )
        """)
        cur.copy_expert("COPY tmp_facts FROM STDIN WITH CSV", buf)
        cur.execute("""
            WITH ins AS (
                INSERT INTO fact_events
                    (event_time, event_type, product_id, user_id, user_session, price, source_month)
                SELECT event_time, event_type, product_id, user_id, user_session, price, source_month
                FROM tmp_facts
                ON CONFLICT ON CONSTRAINT uq_event DO NOTHING
                RETURNING 1
            )
            SELECT COUNT(*) FROM ins
        """)
        loaded = cur.fetchone()[0]
        cur.execute("DROP TABLE tmp_facts")
    conn.commit()
    return loaded


def log_run_start(conn, source_file):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs (source_file, started_at, status) "
            "VALUES (%s, %s, 'running') RETURNING run_id",
            (source_file, datetime.utcnow()),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def log_run_end(conn, run_id, extracted, transformed, loaded, dropped, errors=None):
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE pipeline_runs
               SET finished_at=%s, rows_extracted=%s, rows_transformed=%s,
                   rows_loaded=%s, rows_dropped=%s, errors=%s, status=%s
               WHERE run_id=%s""",
            (datetime.utcnow(), extracted, transformed, loaded, dropped,
             errors, "success" if not errors else "error", run_id),
        )
    conn.commit()


def run_load(filepath, source_month, dsn, chunksize=500_000, max_rows=6_000_000):
    """ETL for one CSV file, capped at max_rows loaded."""
    from pipeline.extract import extract
    from pipeline.transform import transform

    conn = get_conn(dsn)
    init_schema(conn)
    run_id = log_run_start(conn, filepath)

    total_extracted = total_transformed = total_loaded = total_dropped = 0
    errors = []
    t0 = time.time()

    try:
        for chunk, _ in extract(filepath, chunksize=chunksize):
            total_extracted += len(chunk)
            clean, report = transform(chunk, source_month)

            dropped_this = report["total"] - report["rows_out"]
            total_dropped += dropped_this
            total_transformed += report["rows_out"]

            # Cap to max_rows
            remaining = max_rows - total_loaded
            if remaining <= 0:
                break
            if len(clean) > remaining:
                clean = clean.iloc[:remaining]

            load_dimensions(conn, clean)
            loaded = load_facts(conn, clean)
            total_loaded += loaded

            logger.info("Chunk | extracted=%d clean=%d loaded=%d total_loaded=%d",
                        len(chunk), len(clean), loaded, total_loaded)

            if total_loaded >= max_rows:
                logger.info("Reached %d row cap. Stopping.", max_rows)
                break

    except Exception as e:
        errors.append(str(e))
        logger.error("Pipeline error: %s", e, exc_info=True)
    finally:
        elapsed = time.time() - t0
        log_run_end(conn, run_id, total_extracted, total_transformed,
                    total_loaded, total_dropped,
                    "; ".join(errors) if errors else None)
        conn.close()

    logger.info("Done in %.1fs | loaded=%d dropped=%d", elapsed, total_loaded, total_dropped)
    return {
        "run_id": run_id, "elapsed_s": round(elapsed, 2),
        "extracted": total_extracted, "transformed": total_transformed,
        "loaded": total_loaded, "dropped": total_dropped,
        "errors": errors,
    }
