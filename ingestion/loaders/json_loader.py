# =============================================================================
# ingestion/loaders/json_loader.py
#
# PURPOSE:
#   Generic loader for any JSON file → MySQL bronze table.
#   Scans bronze_data/ folder automatically for all .json files.
#   Handles two JSON structures automatically:
#
#   Structure 1 — Flat array (simple):
#       [{"id": "1", "name": "abc"}, ...]
#
#   Structure 2 — Nested with wrapper key (our files):
#       {
#         "apiVersion": "1.0",
#         "data": [{"profitCenterId": "PC001", "auditInfo": {"source": "FI"}}]
#       }
#
#   For Structure 2, the loader:
#     - Automatically finds the key that contains the data array
#     - Flattens nested objects using pd.json_normalize()
#       e.g. auditInfo.source → auditinfo_source
#
# HOW TO RUN:
#   python ingestion/loaders/json_loader.py
#
# OUR JSON FILES:
#   bronze_profit_centers.json      → nested, data key = "data"
#   bronze_source_list.json         → nested, data key = "data"
#   bronze_mrp_controllers.json     → nested, data key = "data"
#   bronze_warehouse_master.json    → nested, data key = "data"
#   bronze_sampling_procedures.json → nested, data key = "data"
#
# FAILURE HANDLING:
#   Scenario 1 — File not found:
#       Logs FAILED to bronze_load_audit. Moves to next file.
#   Scenario 2 — File unreadable / invalid JSON syntax:
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
# HELPER: DETECT AND EXTRACT DATA ARRAY FROM JSON
# =============================================================================

def extract_data_array(raw):
    """
    Detects the JSON structure and returns the data array.

    Structure 1 — root is already a list:
        [{"id": 1}, {"id": 2}]
        → returns the list directly

    Structure 2 — root is a dict with one key holding the data array:
        {"apiVersion": "1.0", "data": [{...}, {...}]}
        → finds the key whose value is a list and returns it

    Args:
        raw: parsed JSON object (list or dict)

    Returns:
        list: the data records array
        str:  the key name where data was found (for logging)

    Raises:
        ValueError: if no data array can be found
    """
    # Structure 1 — root is already a list
    if isinstance(raw, list):
        return raw, "root"

    # Structure 2 — root is a dict, find the key with the data array
    if isinstance(raw, dict):
        # Find all keys whose value is a non-empty list
        array_keys = [
            k for k, v in raw.items()
            if isinstance(v, list) and len(v) > 0
        ]

        if len(array_keys) == 1:
            # Only one array key found — that's our data
            key = array_keys[0]
            return raw[key], key

        elif len(array_keys) > 1:
            # Multiple array keys found — pick the largest one
            # (most likely the data array, not a small metadata list)
            key = max(array_keys, key=lambda k: len(raw[k]))
            logger.info(f"  Multiple array keys found: {array_keys}")
            logger.info(f"  Using largest: '{key}' ({len(raw[key])} records)")
            return raw[key], key

    raise ValueError(
        f"Cannot find data array in JSON. "
        f"Root type: {type(raw).__name__}. "
        f"Keys: {list(raw.keys()) if isinstance(raw, dict) else 'N/A'}"
    )


# =============================================================================
# SINGLE FILE LOADER
# =============================================================================

def load_json(file_name, table_name, batch_id):
    """
    Loads a single JSON file into a MySQL bronze table.

    Args:
        file_name  (str): JSON filename inside bronze_data/ folder
        table_name (str): Target MySQL table name
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

    # ── Scenario 2: File exists but cannot be read / invalid JSON ────────────
    try:
        # Read raw JSON — UTF-8 encoding handles all special characters
        with open(file_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        # Detect structure and extract the data array
        records, data_key = extract_data_array(raw)
        logger.info(f"  JSON structure: data found at key '{data_key}'")
        logger.info(f"  Records found:  {len(records):,}")

        # Flatten nested objects using json_normalize
        # Example: {"auditInfo": {"source": "FI"}}
        #       → {"auditinfo_source": "FI"}  (column safe name)
        # sep="." would give "auditInfo.source" which breaks SQL
        # We use "_" and lowercase everything for clean column names
        df = pd.json_normalize(records, sep="_")

        # Clean column names — lowercase, replace spaces and dots with underscores
        # This ensures all column names are valid MySQL identifiers
        df.columns = [
            str(c).strip().lower()
                   .replace(" ", "_")
                   .replace(".", "_")
                   .replace("-", "_")
            for c in df.columns
        ]

        # Convert all values to string — preserve raw messy data as-is
        df = df.astype(str)

        # Convert "None" and "nan" strings back to actual None → MySQL NULL
        # These appear because astype(str) converts Python None → "None"
        df = df.replace({"None": None, "nan": None, "NaN": None, "": None})

        logger.info(f"  Columns after flatten: {list(df.columns)}")

    except Exception as e:
        error_msg = f"Could not read JSON file: {e}"
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
    df["_data_key"]            = data_key   # which JSON key held the data array
    df["_batch_id"]            = batch_id

    # ── Build CREATE TABLE dynamically ────────────────────────────────────────
    col_definitions = ["load_id INT AUTO_INCREMENT PRIMARY KEY"]
    for col in df.columns:
        if col == "_ingestion_timestamp":
            col_definitions.append(f"`{col}` DATETIME")
        elif col in ("_source_file", "_data_key", "_batch_id"):
            col_definitions.append(f"`{col}` VARCHAR(200)")
        else:
            # VARCHAR(500) handles any flattened JSON value
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
# Scans bronze_data/ and loads ALL .json files found automatically.
# =============================================================================

def load_all_jsons(batch_id):
    """
    Scans bronze_data/ and loads every .json file found.

    Args:
        batch_id (str): Shared batch ID for this pipeline run

    Returns:
        list of dicts: One result dict per file loaded
    """
    json_files = sorted([
        f for f in os.listdir(BRONZE_DIR)
        if f.endswith(".json")
    ])

    if not json_files:
        logger.warning(f"No JSON files found in {BRONZE_DIR}")
        return []

    logger.info(f"Found {len(json_files)} JSON files:")
    for f in json_files:
        logger.info(f"  {f}")

    results = []
    for i, file_name in enumerate(json_files, start=1):
        table_name = file_name.replace(".json", "")
        logger.info(f"\n[{i}/{len(json_files)}] {file_name} -> {table_name}")
        result = load_json(file_name, table_name, batch_id)
        results.append(result)

    return results


# =============================================================================
# MAIN — runs when you execute: python ingestion/loaders/json_loader.py
# =============================================================================

if __name__ == "__main__":

    batch_id = datetime.now().strftime("BATCH_%Y%m%d_%H%M%S")

    print("=" * 65)
    print("SAP Migration - Bronze JSON Loader")
    print(f"Batch ID : {batch_id}")
    print("=" * 65)

    create_database_if_not_exists()
    create_audit_tables()

    results = load_all_jsons(batch_id)

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