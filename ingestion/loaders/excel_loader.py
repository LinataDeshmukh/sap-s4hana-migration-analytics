# =============================================================================
# ingestion/loaders/excel_loader.py
#
# PURPOSE:
#   Generic loader for any Excel (.xlsx) file → MySQL bronze table.
#   Scans bronze_data/ folder automatically for all .xlsx files.
#   Reads first sheet only (data sheet) — ignores instruction/reference sheets.
#   Dynamically builds CREATE TABLE and INSERT from actual columns in the file.
#
# HOW TO RUN:
#   python ingestion/loaders/excel_loader.py
#
# OUR EXCEL FILES:
#   bronze_material_plant_data_UDS.xlsx  → data on sheet 1
#   bronze_planning_calendar.xlsx        → data on sheet 1
#   bronze_maintenance_plans.xlsx        → data on sheet 1
#   bronze_quality_info_records.xlsx     → data on sheet 1
#   bronze_gl_accounts.xlsx              → data on sheet 1 (GL Accounts)
#                                          sheet 2 = Instructions (ignored)
#                                          sheet 3 = Valid Values (ignored)
#
# FAILURE HANDLING:
#   Scenario 1 — File not found:
#       Logs FAILED to bronze_load_audit. Moves to next file.
#   Scenario 2 — File unreadable (corrupt, password protected):
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
# Loads one Excel file into one MySQL bronze table.
# Called by load_all_excels() below.
# =============================================================================

def load_excel(file_name, table_name, batch_id, sheet_index=0):
    """
    Loads a single Excel (.xlsx) file into a MySQL bronze table.

    Args:
        file_name   (str): Excel filename inside bronze_data/ folder
                           e.g. "bronze_gl_accounts.xlsx"
        table_name  (str): Target MySQL table name
                           e.g. "bronze_gl_accounts"
        batch_id    (str): Shared batch ID for this pipeline run
        sheet_index (int): Which sheet to read. Default 0 = first sheet.
                           First sheet is always the data sheet in our files.

    Returns:
        dict: Summary with table, status, rows_read, rows_inserted,
              rows_failed, duration_seconds
    """
    file_path  = os.path.join(BRONZE_DIR, file_name)
    started_at = datetime.now()

    # ── Dedicated audit connection ────────────────────────────────────────────
    # Separate from data connection so audit writes always succeed
    # even if data inserts encounter problems.
    audit_conn   = get_connection()
    audit_cursor = audit_conn.cursor()

    # Log STARTED immediately so a record exists even if load crashes
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
        # Peek at sheet names first so we can log which sheet is being read
        # and warn if we are ignoring additional sheets
        xl         = pd.ExcelFile(file_path, engine="openpyxl")
        all_sheets = xl.sheet_names
        sheet_name = all_sheets[sheet_index]

        logger.info(f"  Total sheets:  {len(all_sheets)} {all_sheets}")
        logger.info(f"  Reading sheet: '{sheet_name}' (index {sheet_index})")

        if len(all_sheets) > 1:
            ignored = [s for s in all_sheets if s != sheet_name]
            logger.info(f"  Ignoring:      {ignored}")

        # Read the data sheet — everything as string to preserve raw values.
        # header=0 means first row contains column names.
        # keep_default_na=False prevents pandas from converting empty
        # cells to NaN automatically.
        df = pd.read_excel(
            file_path,
            sheet_name      = sheet_index,
            dtype           = str,
            engine          = "openpyxl",
            keep_default_na = False,
            header          = 0,
        )

        # Clean column names — Excel columns often have:
        #   - Leading/trailing spaces from manual editing
        #   - Mixed case (e.g. "GL Account" → "gl_account")
        #   - Spaces that would break SQL column names
        df.columns = [
            str(c).strip().replace(" ", "_").lower()
            for c in df.columns
        ]

        # Convert empty strings → None → MySQL NULL
        df = df.replace("", None)
        df = df.where(pd.notnull(df), None)

        # Drop completely empty rows — Excel files often have blank rows
        # at the bottom from formatting or accidental scrolling
        before = len(df)
        df     = df.dropna(how="all")
        after  = len(df)
        if before != after:
            logger.info(f"  Dropped {before - after} empty rows.")

        logger.info(f"  Rows read:   {len(df):,}")
        logger.info(f"  Columns:     {list(df.columns)}")

    except Exception as e:
        error_msg = f"Could not read Excel file: {e}"
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

    # ── Add audit columns ─────────────────────────────────────────────────────
    # Extra column _source_sheet tells us exactly which Excel sheet this
    # data came from — useful when files have multiple sheets
    df["_ingestion_timestamp"] = datetime.now()
    df["_source_file"]         = file_name
    df["_source_sheet"]        = sheet_name   # which sheet was read
    df["_batch_id"]            = batch_id

    # ── Build CREATE TABLE dynamically from actual Excel columns ──────────────
    col_definitions = ["load_id INT AUTO_INCREMENT PRIMARY KEY"]
    for col in df.columns:
        if col == "_ingestion_timestamp":
            col_definitions.append(f"`{col}` DATETIME")
        elif col in ("_source_file", "_source_sheet", "_batch_id"):
            col_definitions.append(f"`{col}` VARCHAR(200)")
        else:
            # VARCHAR(500) safely handles any cell value
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

        # DROP and recreate — bronze always reflects latest extract
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
            values = []
            for col in insert_cols:
                val = row[col]
                if val is None:
                    values.append(None)
                elif isinstance(val, float) and np.isnan(val):
                    # pandas NaN → MySQL NULL
                    values.append(None)
                else:
                    values.append(str(val))

            cursor.execute(insert_sql, values)
            rows_inserted += 1

            # Commit every 1000 rows to protect against connection drops
            if rows_inserted % 1000 == 0:
                conn.commit()
                logger.info(f"  Progress: {rows_inserted:,} rows inserted...")

        except Exception as e:
            # Scenario 3: Individual row fails — log it, keep going
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

    # ── Update audit table ────────────────────────────────────────────────────
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
# Scans bronze_data/ and loads ALL .xlsx files found automatically.
# Table name derived from filename: bronze_gl_accounts.xlsx
#                                 → bronze_gl_accounts
# =============================================================================

def load_all_excels(batch_id):
    """
    Scans bronze_data/ directory and loads every .xlsx file found.

    Args:
        batch_id (str): Shared batch ID for this pipeline run

    Returns:
        list of dicts: One result dict per file loaded
    """
    xlsx_files = sorted([
        f for f in os.listdir(BRONZE_DIR)
        if f.endswith(".xlsx")
    ])

    if not xlsx_files:
        logger.warning(f"No Excel files found in {BRONZE_DIR}")
        return []

    logger.info(f"Found {len(xlsx_files)} Excel files:")
    for f in xlsx_files:
        logger.info(f"  {f}")

    results = []
    for i, file_name in enumerate(xlsx_files, start=1):
        # Derive table name automatically from filename
        table_name = file_name.replace(".xlsx", "")
        logger.info(f"\n[{i}/{len(xlsx_files)}] {file_name} -> {table_name}")
        result = load_excel(file_name, table_name, batch_id)
        results.append(result)

    return results


# =============================================================================
# MAIN — runs when you execute: python ingestion/loaders/excel_loader.py
# =============================================================================

if __name__ == "__main__":

    batch_id = datetime.now().strftime("BATCH_%Y%m%d_%H%M%S")

    print("=" * 65)
    print("SAP Migration - Bronze Excel Loader")
    print(f"Batch ID : {batch_id}")
    print("=" * 65)

    # Ensure database and audit tables exist before loading
    create_database_if_not_exists()
    create_audit_tables()

    # Load all Excel files found in bronze_data/
    results = load_all_excels(batch_id)

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