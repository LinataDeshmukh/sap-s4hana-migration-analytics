# =============================================================================
# ingestion/loaders/parquet_loader.py
#
# PURPOSE:
#   Generic loader for any Parquet file → MySQL bronze table.
#   Scans bronze_data/ folder automatically for all .parquet files.
#
#   Parquet is the cleanest format to load because:
#     - Schema is embedded in the file (column names and types are defined)
#     - No encoding issues (binary format, not text)
#     - Highly compressed — large files load faster than equivalent CSV
#     - Null values are natively supported (no "None" string issues)
#
#   However we still load everything as VARCHAR in MySQL because:
#     - Bronze layer preserves raw data — no type casting at this stage
#     - Type enforcement happens in the silver layer (dbt)
#     - Avoids MySQL type mismatch errors from messy data
#
# HOW TO RUN:
#   python ingestion/loaders/parquet_loader.py
#
# OUR PARQUET FILES:
#   bronze_storage_bins.parquet           → ~52,000 rows
#   bronze_vendor_delta.parquet           → ~2,500 rows
#   bronze_material_master_delta.parquet  → ~3,000 rows
#   bronze_material_costing.parquet       → ~37,100 rows
#
# FAILURE HANDLING:
#   Scenario 1 — File not found:
#       Logs FAILED to bronze_load_audit. Moves to next file.
#   Scenario 2 — File unreadable / corrupt Parquet:
#       Logs FAILED to bronze_load_audit. Moves to next file.
#   Scenario 3 — Individual row insert fails:
#       Skips that row, logs to bronze_failed_rows with raw data.
#       Final status = PARTIAL if any rows failed.
#   Scenario 4 — MySQL connection drops mid-load:
#       Commits every 1000 rows so partial progress is always saved.
# =============================================================================

import pandas as pd
import numpy as np
import os
import sys
import json
import logging
from datetime import datetime

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
from config.settings import BRONZE_DIR
from ingestion.db_connection import (
    get_connection,
    create_database_if_not_exists,
    create_audit_tables,
    log_audit_start,
    log_audit_complete,
    log_failed_row,
)

# ── Force UTF-8 on Windows console ───────────────────────────────────────────
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── Logger ────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

file_handler = logging.FileHandler(
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "logs", "ingestion.log"
    ),
    encoding="utf-8"
)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)


# =============================================================================
# SINGLE FILE LOADER
# =============================================================================

def load_parquet(file_name, table_name, batch_id):
    """
    Loads a single Parquet file into a MySQL bronze table.

    Args:
        file_name  (str): Parquet filename inside bronze_data/ folder
                          e.g. "bronze_material_costing.parquet"
        table_name (str): Target MySQL table name
                          e.g. "bronze_material_costing"
        batch_id   (str): Shared batch ID for this pipeline run

    Returns:
        dict: Summary with table, status, rows_read, rows_inserted,
              rows_failed, duration_seconds
    """
    file_path  = os.path.join(BRONZE_DIR, file_name)
    started_at = datetime.now()

    # ── Dedicated audit connection ────────────────────────────────────────────
    audit_conn   = get_connection()
    audit_cursor = audit_conn.cursor()

    audit_id = log_audit_start(
        audit_cursor, table_name, file_name, batch_id, started_at
    )
    audit_conn.commit()

    # ── Scenario 1: File does not exist ──────────────────────────────────────
    if not os.path.exists(file_path):
        error_msg = f"File not found: {file_path}"
        logger.error(f"[FAILED] {table_name} - {error_msg}")

        log_audit_complete(
            audit_cursor, audit_id,
            status="FAILED", rows_read=0, rows_inserted=0,
            rows_failed=0, error_message=error_msg,
            completed_at=datetime.now(), duration_seconds=0,
        )
        audit_conn.commit()
        audit_cursor.close()
        audit_conn.close()

        return {
            "table": table_name, "status": "FAILED",
            "rows_read": 0, "rows_inserted": 0,
            "rows_failed": 0, "error": error_msg
        }

    logger.info(f"[LOADING] {table_name} <- {file_name}")

    # ── Scenario 2: File exists but cannot be read ────────────────────────────
    try:
        # Read Parquet file using pyarrow engine
        # Parquet natively preserves column names and null values
        df = pd.read_parquet(file_path, engine="pyarrow")

        # Log the native schema from Parquet for debugging reference
        # This shows what types the source system used
        logger.info(f"  Native schema:")
        for col, dtype in df.dtypes.items():
            logger.info(f"    {col}: {dtype}")

        # Convert all columns to string for bronze layer storage.
        # We intentionally discard Parquet type info here because:
        #   - Bronze stores raw values as-is
        #   - Type casting and validation happens in silver (dbt)
        #   - Avoids MySQL rejecting values that don't fit strict types
        df = df.astype(str)

        # Convert "None" and "nan" strings → actual None → MySQL NULL
        # astype(str) converts Python None → "None" and NaN → "nan"
        # so we need to reverse that for proper NULL storage
        df = df.replace({"None": None, "nan": None, "NaN": None,
                         "<NA>": None, "NaT": None, "": None})

        # Clean column names — Parquet columns are usually clean already
        # but we standardize just in case
        df.columns = [
            str(c).strip().lower()
                   .replace(" ", "_")
                   .replace("-", "_")
                   .replace(".", "_")
            for c in df.columns
        ]

        logger.info(f"  Rows read:   {len(df):,}")
        logger.info(f"  Columns:     {list(df.columns)}")

    except Exception as e:
        error_msg = f"Could not read Parquet file: {e}"
        logger.error(f"[FAILED] {table_name} - {error_msg}")

        log_audit_complete(
            audit_cursor, audit_id,
            status="FAILED", rows_read=0, rows_inserted=0,
            rows_failed=0, error_message=error_msg,
            completed_at=datetime.now(), duration_seconds=0,
        )
        audit_conn.commit()
        audit_cursor.close()
        audit_conn.close()

        return {
            "table": table_name, "status": "FAILED",
            "rows_read": 0, "rows_inserted": 0,
            "rows_failed": 0, "error": error_msg
        }

    rows_read = len(df)

    # ── Add audit columns ─────────────────────────────────────────────────────
    df["_ingestion_timestamp"] = datetime.now()
    df["_source_file"]         = file_name
    df["_batch_id"]            = batch_id

    # ── Build CREATE TABLE dynamically ────────────────────────────────────────
    col_definitions = ["load_id INT AUTO_INCREMENT PRIMARY KEY"]
    for col in df.columns:
        if col == "_ingestion_timestamp":
            col_definitions.append(f"`{col}` DATETIME")
        elif col in ("_source_file", "_batch_id"):
            col_definitions.append(f"`{col}` VARCHAR(200)")
        else:
            col_definitions.append(f"`{col}` VARCHAR(500)")

    create_sql = (
        f"CREATE TABLE IF NOT EXISTS `{table_name}` "
        f"({', '.join(col_definitions)}) "
        f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    )

    # ── Connect and set up table ──────────────────────────────────────────────
    try:
        create_database_if_not_exists()
        conn   = get_connection()
        cursor = conn.cursor()

        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
        cursor.execute(create_sql)
        conn.commit()
        logger.info(f"  Table `{table_name}` ready.")

    except Exception as e:
        error_msg = f"Table setup failed: {e}"
        logger.error(f"[FAILED] {table_name} - {error_msg}")

        log_audit_complete(
            audit_cursor, audit_id,
            status="FAILED", rows_read=rows_read, rows_inserted=0,
            rows_failed=0, error_message=error_msg,
            completed_at=datetime.now(),
            duration_seconds=(datetime.now() - started_at).total_seconds(),
        )
        audit_conn.commit()
        audit_cursor.close()
        audit_conn.close()

        return {
            "table": table_name, "status": "FAILED",
            "rows_read": rows_read, "rows_inserted": 0,
            "rows_failed": 0, "error": error_msg
        }

    # ── Build INSERT statement ────────────────────────────────────────────────
    insert_cols  = list(df.columns)
    placeholders = ", ".join(["%s"] * len(insert_cols))
    col_names    = ", ".join([f"`{c}`" for c in insert_cols])
    insert_sql   = (
        f"INSERT INTO `{table_name}` ({col_names}) "
        f"VALUES ({placeholders})"
    )

    # ── Insert rows with row-level failure handling ───────────────────────────
    rows_inserted = 0
    rows_failed   = 0

    for idx, row in df.iterrows():
        try:
            values = []
            for col in insert_cols:
                val = row[col]
                if val is None:
                    values.append(None)
                elif isinstance(val, float) and np.isnan(val):
                    values.append(None)
                else:
                    values.append(str(val))

            cursor.execute(insert_sql, values)
            rows_inserted += 1

            if rows_inserted % 1000 == 0:
                conn.commit()
                logger.info(f"  Progress: {rows_inserted:,} rows inserted...")

        except Exception as e:
            rows_failed += 1
            raw_data = json.dumps(
                {k: str(v) for k, v in row.items()},
                ensure_ascii=False
            )
            log_failed_row(
                audit_cursor,
                batch_id      = batch_id,
                table_name    = table_name,
                source_file   = file_name,
                row_number    = idx,
                error_message = str(e),
                raw_data      = raw_data,
            )
            audit_conn.commit()

    conn.commit()

    # ── Determine final status ────────────────────────────────────────────────
    if rows_failed == 0:
        status = "SUCCESS"
    elif rows_inserted == 0:
        status = "FAILED"
    else:
        status = "PARTIAL"

    completed_at     = datetime.now()
    duration_seconds = (completed_at - started_at).total_seconds()

    log_audit_complete(
        audit_cursor, audit_id,
        status           = status,
        rows_read        = rows_read,
        rows_inserted    = rows_inserted,
        rows_failed      = rows_failed,
        error_message    = None,
        completed_at     = completed_at,
        duration_seconds = duration_seconds,
    )
    audit_conn.commit()

    logger.info("-" * 50)
    logger.info(f"[{status}] {table_name}")
    logger.info(f"  Rows read:     {rows_read:,}")
    logger.info(f"  Rows inserted: {rows_inserted:,}")
    logger.info(f"  Rows failed:   {rows_failed:,}")
    logger.info(f"  Duration:      {duration_seconds:.1f}s")
    logger.info("-" * 50)

    cursor.close()
    conn.close()
    audit_cursor.close()
    audit_conn.close()

    return {
        "table":            table_name,
        "status":           status,
        "rows_read":        rows_read,
        "rows_inserted":    rows_inserted,
        "rows_failed":      rows_failed,
        "duration_seconds": duration_seconds,
    }


# =============================================================================
# BULK LOADER
# =============================================================================

def load_all_parquets(batch_id):
    """
    Scans bronze_data/ and loads every .parquet file found.

    Args:
        batch_id (str): Shared batch ID for this pipeline run

    Returns:
        list of dicts: One result dict per file loaded
    """
    parquet_files = sorted([
        f for f in os.listdir(BRONZE_DIR)
        if f.endswith(".parquet")
    ])

    if not parquet_files:
        logger.warning(f"No Parquet files found in {BRONZE_DIR}")
        return []

    logger.info(f"Found {len(parquet_files)} Parquet files:")
    for f in parquet_files:
        logger.info(f"  {f}")

    results = []
    for i, file_name in enumerate(parquet_files, start=1):
        table_name = file_name.replace(".parquet", "")
        logger.info(f"\n[{i}/{len(parquet_files)}] {file_name} -> {table_name}")
        result = load_parquet(file_name, table_name, batch_id)
        results.append(result)

    return results


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":

    batch_id = datetime.now().strftime("BATCH_%Y%m%d_%H%M%S")

    print("=" * 65)
    print("SAP Migration - Bronze Parquet Loader")
    print(f"Batch ID : {batch_id}")
    print("=" * 65)

    create_database_if_not_exists()
    create_audit_tables()

    results = load_all_parquets(batch_id)

    print("\n" + "=" * 65)
    print("SUMMARY")
    print("=" * 65)
    print(f"{'Table':<45} {'Status':<10} {'Rows':>8} {'Failed':>6}")
    print("-" * 65)

    total_inserted = 0
    total_failed   = 0
    counts         = {"SUCCESS": 0, "PARTIAL": 0, "FAILED": 0}

    for r in results:
        print(
            f"{r['table']:<45} "
            f"{r['status']:<10} "
            f"{r.get('rows_inserted', 0):>8,} "
            f"{r.get('rows_failed', 0):>6,}"
        )
        total_inserted += r.get("rows_inserted", 0)
        total_failed   += r.get("rows_failed", 0)
        counts[r["status"]] = counts.get(r["status"], 0) + 1

    print("-" * 65)
    print(f"{'TOTAL':<45} {'':10} {total_inserted:>8,} {total_failed:>6,}")
    print("=" * 65)
    print(f"SUCCESS: {counts['SUCCESS']}  PARTIAL: {counts['PARTIAL']}  FAILED: {counts['FAILED']}")
    print(f"Batch ID: {batch_id}")
    print("=" * 65)