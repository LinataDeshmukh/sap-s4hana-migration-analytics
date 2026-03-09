# =============================================================================
# ingestion/loaders/csv_loader.py
#
# PURPOSE:
#   Generic loader for any CSV file → MySQL bronze table.
#   Reads the file exactly as-is with no transformations.
#   Dynamically builds the CREATE TABLE and INSERT statements
#   from whatever columns actually exist in the file — so it
#   works for any CSV regardless of schema.
#
# HOW TO RUN:
#   python ingestion/loaders/csv_loader.py
#   This will scan bronze_data/ and load ALL CSV files found.
#
# FAILURE HANDLING:
#   Scenario 1 — File not found:
#       Logs FAILED to bronze_load_audit. Moves to next file.
#   Scenario 2 — File unreadable (encoding error, corrupt):
#       Logs FAILED to bronze_load_audit. Moves to next file.
#   Scenario 3 — Individual row insert fails:
#       Skips that row, logs it to bronze_failed_rows with the
#       raw data and error. Continues loading remaining rows.
#       Final status = PARTIAL if any rows failed.
#   Scenario 4 — MySQL connection drops mid-load:
#       Commits every 1000 rows so partial progress is saved.
#       Audit table shows exact row count committed before failure.
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
# Uses a named logger so each module has its own log namespace.
# Logs go to both console and the ingestion.log file.
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
# Loads one CSV file into one MySQL bronze table.
# Called by load_all_csvs() below.
# =============================================================================

def load_csv(file_name, table_name, batch_id):
    """
    Loads a single CSV file into a MySQL bronze table.

    Args:
        file_name  (str): CSV filename inside bronze_data/ folder
                          e.g. "bronze_material_master.csv"
        table_name (str): Target MySQL table name
                          e.g. "bronze_material_master"
        batch_id   (str): Shared batch ID for this pipeline run
                          e.g. "BATCH_20260308_185008"

    Returns:
        dict: Summary with table, status, rows_read, rows_inserted,
              rows_failed, duration_seconds
    """
    file_path  = os.path.join(BRONZE_DIR, file_name)
    started_at = datetime.now()

    # ── Open audit connection (separate from data insert connection) ──────────
    # Dedicated connection for audit logging so audit writes always succeed
    # even if the data insert connection has problems.
    audit_conn   = get_connection()
    audit_cursor = audit_conn.cursor()

    # Insert STARTED row into audit table immediately.
    # This ensures a record exists even if the load crashes halfway through.
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
            status           = "FAILED",
            rows_read        = 0,
            rows_inserted    = 0,
            rows_failed      = 0,
            error_message    = error_msg,
            completed_at     = datetime.now(),
            duration_seconds = 0,
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
        # Read everything as string — never want pandas to guess data types.
        # keep_default_na=False ensures empty strings stay as "" not NaN.
        df = pd.read_csv(
            file_path,
            dtype           = str,
            keep_default_na = False,
            encoding        = "utf-8",
        )

        # Convert empty strings to None → these become NULL in MySQL
        df = df.replace("", None)
        df = df.where(pd.notnull(df), None)

    except Exception as e:
        error_msg = f"Could not read CSV file: {e}"
        logger.error(f"[FAILED] {table_name} - {error_msg}")

        log_audit_complete(
            audit_cursor, audit_id,
            status           = "FAILED",
            rows_read        = 0,
            rows_inserted    = 0,
            rows_failed      = 0,
            error_message    = error_msg,
            completed_at     = datetime.now(),
            duration_seconds = 0,
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
    logger.info(f"  Rows read:   {rows_read:,}")
    logger.info(f"  Columns:     {list(df.columns)}")

    # ── Add audit columns to every row ────────────────────────────────────────
    # These columns are added by the pipeline (not from the source system).
    # They allow us to track exactly when, from where, and in which
    # batch each row was loaded — critical for debugging and reprocessing.
    df["_ingestion_timestamp"] = datetime.now()  # exact load time
    df["_source_file"]         = file_name        # which file this row came from
    df["_batch_id"]            = batch_id         # which pipeline run

    # ── Build CREATE TABLE dynamically from actual CSV columns ────────────────
    # All source columns → VARCHAR(500) to handle any messy value safely.
    # Audit columns      → their proper MySQL types.
    # Using backticks around column names handles reserved words and spaces.
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

    # ── Connect to MySQL and set up the table ─────────────────────────────────
    try:
        create_database_if_not_exists()
        conn   = get_connection()
        cursor = conn.cursor()

        # DROP and recreate the table on every load.
        # Bronze always reflects the latest extract — no duplicates from reruns.
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
        cursor.execute(create_sql)
        conn.commit()
        logger.info(f"  Table `{table_name}` ready.")

    except Exception as e:
        error_msg = f"Table setup failed: {e}"
        logger.error(f"[FAILED] {table_name} - {error_msg}")

        log_audit_complete(
            audit_cursor, audit_id,
            status           = "FAILED",
            rows_read        = rows_read,
            rows_inserted    = 0,
            rows_failed      = 0,
            error_message    = error_msg,
            completed_at     = datetime.now(),
            duration_seconds = (datetime.now() - started_at).total_seconds(),
        )
        audit_conn.commit()
        audit_cursor.close()
        audit_conn.close()

        return {
            "table": table_name, "status": "FAILED",
            "rows_read": rows_read, "rows_inserted": 0,
            "rows_failed": 0, "error": error_msg
        }

    # ── Build INSERT statement dynamically ────────────────────────────────────
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
            # Convert each value safely:
            # None       → MySQL NULL
            # float NaN  → MySQL NULL (pandas uses NaN for missing numbers)
            # Everything else → string (avoids all type mismatch errors)
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

            # Commit every 1000 rows.
            # If the connection drops at row 34,000 → rows 1-34,000 are safe.
            if rows_inserted % 1000 == 0:
                conn.commit()
                logger.info(f"  Progress: {rows_inserted:,} rows inserted...")

        except Exception as e:
            # ── Scenario 3: Individual row fails ─────────────────────────────
            # Don't crash — skip this row, log it, keep going.
            rows_failed += 1

            # Store the full raw row as JSON so nothing is permanently lost.
            # The team can investigate and reprocess from bronze_failed_rows.
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

    # Final commit for any remaining rows not yet committed
    conn.commit()

    # ── Determine final status ────────────────────────────────────────────────
    if rows_failed == 0:
        status = "SUCCESS"          # every row made it
    elif rows_inserted == 0:
        status = "FAILED"           # nothing made it
    else:
        status = "PARTIAL"          # some rows made it, some didn't

    completed_at     = datetime.now()
    duration_seconds = (completed_at - started_at).total_seconds()

    # ── Update audit table with final result ──────────────────────────────────
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

    # ── Log summary ───────────────────────────────────────────────────────────
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
# Scans bronze_data folder and loads ALL CSV files found automatically.
# No hardcoded filenames — any CSV in the folder gets loaded.
# Table name is derived from filename: bronze_material_master.csv
#                                    → bronze_material_master
# =============================================================================

def load_all_csvs(batch_id):
    """
    Scans bronze_data/ directory and loads every .csv file found.

    Args:
        batch_id (str): Shared batch ID for this pipeline run

    Returns:
        list of dicts: One result dict per file loaded
    """
    # Scan folder for all CSV files, sorted alphabetically
    csv_files = sorted([
        f for f in os.listdir(BRONZE_DIR)
        if f.endswith(".csv")
    ])

    if not csv_files:
        logger.warning(f"No CSV files found in {BRONZE_DIR}")
        return []

    logger.info(f"Found {len(csv_files)} CSV files:")
    for f in csv_files:
        logger.info(f"  {f}")

    results = []
    for i, file_name in enumerate(csv_files, start=1):
        # Derive table name automatically from filename
        table_name = file_name.replace(".csv", "")
        logger.info(f"\n[{i}/{len(csv_files)}] {file_name} -> {table_name}")
        result = load_csv(file_name, table_name, batch_id)
        results.append(result)

    return results


# =============================================================================
# MAIN — runs when you execute: python ingestion/loaders/csv_loader.py
# =============================================================================

if __name__ == "__main__":
    from datetime import datetime

    batch_id = datetime.now().strftime("BATCH_%Y%m%d_%H%M%S")

    print("=" * 65)
    print("SAP Migration - Bronze CSV Loader")
    print(f"Batch ID : {batch_id}")
    print("=" * 65)

    # Ensure database and audit tables exist before loading
    create_database_if_not_exists()
    create_audit_tables()

    # Load all CSVs found in bronze_data/
    results = load_all_csvs(batch_id)

    # ── Print final summary ───────────────────────────────────────────────────
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