"""extract.py — Read raw CSV files into DataFrames with error handling."""
import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

EXPECTED_COLS = {
    "event_time", "event_type", "product_id", "category_id",
    "category_code", "brand", "price", "user_id", "user_session",
}

DTYPES = {
    "event_type":     "str",
    "product_id":     "Int64",
    "category_id":    "Int64",
    "category_code":  "str",
    "brand":          "str",
    "price":          "float64",
    "user_id":        "Int64",
    "user_session":   "str",
}


def extract(filepath: str | Path, chunksize: int = 100_000):
    """
    Yield DataFrames chunk-by-chunk from a CSV file.
    Skips malformed rows (bad_lines='skip').
    Returns (chunk_df, rows_read) per chunk.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"CSV not found: {filepath}")

    reader = pd.read_csv(
        filepath,
        dtype=DTYPES,
        on_bad_lines="skip",
        chunksize=chunksize,
        engine="python",
    )

    total = 0
    for chunk in reader:
        missing = EXPECTED_COLS - set(chunk.columns)
        if missing:
            logger.warning("Missing columns in chunk: %s", missing)
            continue
        total += len(chunk)
        yield chunk, total

    logger.info("Extraction complete: %d rows read from %s", total, filepath.name)
