"""transform.py — Clean, validate, and shape raw chunks for loading."""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

PRICE_MAX = 50_000.0   # threshold: prices above this are flagged as anomalous
VALID_EVENT_TYPES = {"view", "cart", "purchase"}
VALID_DATE_RANGE = (pd.Timestamp("2019-10-01"), pd.Timestamp("2019-11-30 23:59:59"))


def transform(chunk: pd.DataFrame, source_month: str) -> tuple[pd.DataFrame, dict]:
    """
    Clean a raw chunk and return (clean_df, quality_report).
    quality_report keys: total, dropped_nulls, dropped_price, dropped_ts,
                         dropped_event_type, flagged_duplicates
    """
    report = {
        "total": len(chunk),
        "dropped_nulls": 0,
        "dropped_price": 0,
        "dropped_ts": 0,
        "dropped_event_type": 0,
        "flagged_duplicates": 0,
    }

    df = chunk.copy()

    # --- Parse timestamps ---
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
    df["event_time"] = df["event_time"].dt.tz_localize(None)  # store as naive UTC

    bad_ts = df["event_time"].isna()
    out_of_range = (
        (df["event_time"] < VALID_DATE_RANGE[0]) |
        (df["event_time"] > VALID_DATE_RANGE[1])
    )
    ts_mask = bad_ts | out_of_range
    report["dropped_ts"] = int(ts_mask.sum())
    df = df[~ts_mask]

    # --- NOT NULL columns ---
    required = ["event_time", "event_type", "product_id", "category_id",
                "price", "user_id", "user_session"]
    null_mask = df[required].isnull().any(axis=1)
    report["dropped_nulls"] = int(null_mask.sum())
    df = df[~null_mask]

    # --- Valid event types ---
    bad_et = ~df["event_type"].isin(VALID_EVENT_TYPES)
    report["dropped_event_type"] = int(bad_et.sum())
    df = df[~bad_et]

    # --- Price validation ---
    bad_price = (df["price"] <= 0) | (df["price"] > PRICE_MAX)
    report["dropped_price"] = int(bad_price.sum())
    df = df[~bad_price]

    # --- Deduplication (same user, product, event_type, timestamp, session) ---
    dup_cols = ["event_time", "event_type", "product_id", "user_id", "user_session"]
    before = len(df)
    df = df.drop_duplicates(subset=dup_cols)
    report["flagged_duplicates"] = before - len(df)

    # --- Derive source_month ---
    df["source_month"] = source_month

    # --- Normalise types ---
    df["product_id"] = df["product_id"].astype("int64")
    df["category_id"] = df["category_id"].astype("int64")
    df["user_id"]     = df["user_id"].astype("int64")
    df["price"]       = df["price"].astype("float64").round(2)

    # Nullable string columns — keep as-is (NaN → None handled at load time)
    df["category_code"] = df["category_code"].where(df["category_code"].notna(), None)
    df["brand"]         = df["brand"].where(df["brand"].notna(), None)

    report["rows_out"] = len(df)
    return df, report
