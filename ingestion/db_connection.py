# =============================================================================
# ingestion/db_connection.py
#
# PURPOSE:
#   Central MySQL connection manager for the SAP Migration bronze layer.
#   Handles:
#     - Creating the bronze database if it doesn't exist
#     - Providing reusable MySQL connections to all loader scripts
#     - Creating the two audit tables that track every load attempt:
#         bronze_load_audit   → one row per file load (SUCCESS/PARTIAL/FAILED)
#         bronze_failed_rows  → one row per individual row that failed to insert
#
# USED BY:
#   All loader scripts (csv_loader, excel_loader, json_loader, xml_loader,
#   parquet_loader) and run_all_bronze.py
# =============================================================================

import mysql.connector
import logging
import os
import sys

# Allow imports from the project root (e.g. config/settings.py)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DB_CONFIG, LOG_DIR

# ── Ensure log directory exists ──────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)

# ── Force UTF-8 output on Windows console ────────────────────────────────────
# Windows terminal defaults to cp1252 encoding which crashes on special
# characters (German umlauts, French accents, Unicode symbols etc.)
# This ensures all console output is UTF-8 safe regardless of the data content.
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── Logger setup ─────────────────────────────────────────────────────────────
# Two handlers:
#   1. File handler  → writes to logs/ingestion.log (UTF-8, persists forever)
#   2. Console handler → prints to terminal during script execution
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

file_handler = logging.FileHandler(
    os.path.join(LOG_DIR, "ingestion.log"),
    encoding="utf-8"        # Always UTF-8 in log files regardless of OS
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)


# =============================================================================
# DATABASE CONNECTION
# =============================================================================

def get_connection(use_database=True):
    """
    Returns a live MySQL connection.

    Args:
        use_database (bool):
            True  → connect directly to precision_mfg_bronze (normal use)
            False → connect without specifying a database (used when
                    creating the database for the first time)

    Raises:
        mysql.connector.Error: if connection fails (wrong password,
        MySQL not running, wrong host/port etc.)
    """
    try:
        config = DB_CONFIG.copy()
        if not use_database:
            # Remove database key so MySQL doesn't reject the connection
            # because the DB doesn't exist yet
            config.pop("database")

        conn = mysql.connector.connect(**config)
        logger.info("MySQL connection established.")
        return conn

    except mysql.connector.Error as e:
        logger.error(f"Connection failed: {e}")
        raise


# =============================================================================
# DATABASE SETUP
# =============================================================================

def create_database_if_not_exists():
    """
    Creates the bronze database if it doesn't already exist.

    Uses utf8mb4 character set which supports:
      - Standard ASCII
      - European characters (umlauts, accents)
      - Asian characters
      - Emoji and special symbols
    This ensures no data is lost regardless of source system encoding.
    """
    conn   = get_connection(use_database=False)
    cursor = conn.cursor()

    cursor.execute(
        f"CREATE DATABASE IF NOT EXISTS `{DB_CONFIG['database']}` "
        f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    )
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Database '{DB_CONFIG['database']}' ready.")


def create_audit_tables():
    """
    Creates the two audit/monitoring tables used to track every load attempt.
    These tables are created once and persist across all pipeline runs.

    Table 1: bronze_load_audit
        Tracks every file load attempt with status, row counts, timing,
        and error messages. One row per file per pipeline run.
        Allows the team to answer: "Did everything load? What failed? How long?"

    Table 2: bronze_failed_rows
        Captures individual rows that failed to insert into MySQL.
        Stores the raw row data as JSON so nothing is lost.
        Allows the team to investigate and reprocess failed rows later.
    """
    conn   = get_connection()
    cursor = conn.cursor()

    # ── Audit table: tracks every file load attempt ───────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze_load_audit (
            audit_id            INT AUTO_INCREMENT PRIMARY KEY,

            -- Which file and table were being loaded
            table_name          VARCHAR(100)    NOT NULL,
            source_file         VARCHAR(200)    NOT NULL,

            -- Unique identifier for this pipeline run
            -- Format: BATCH_YYYYMMDD_HHMMSS
            -- Shared across all files loaded in the same run_all_bronze.py execution
            batch_id            VARCHAR(50)     NOT NULL,

            -- Final outcome of the load attempt
            -- SUCCESS  = all rows inserted, zero failures
            -- PARTIAL  = some rows inserted, some failed (check bronze_failed_rows)
            -- FAILED   = file could not be loaded at all (corrupt, missing, etc.)
            status              VARCHAR(10)     NOT NULL,

            -- Row count tracking
            rows_read           INT             DEFAULT 0,  -- rows found in source file
            rows_inserted       INT             DEFAULT 0,  -- rows successfully in MySQL
            rows_failed         INT             DEFAULT 0,  -- rows that failed to insert

            -- Error details (populated when status = FAILED or PARTIAL)
            error_message       TEXT,

            -- Timing information
            started_at          DATETIME        NOT NULL,
            completed_at        DATETIME,
            duration_seconds    DECIMAL(10,2)   -- how long the load took

        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """)

    # ── Failed rows table: captures individual row failures ───────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze_failed_rows (
            failed_id           INT AUTO_INCREMENT PRIMARY KEY,

            -- Links back to bronze_load_audit for context
            batch_id            VARCHAR(50)     NOT NULL,
            table_name          VARCHAR(100)    NOT NULL,
            source_file         VARCHAR(200)    NOT NULL,

            -- Where in the file this row appeared
            `row_number`        INT,

            -- What went wrong
            error_message       TEXT,

            -- The full raw row stored as a JSON string
            -- This means no data is ever permanently lost even if it fails to insert
            -- The team can investigate and manually reprocess if needed
            raw_data            MEDIUMTEXT,

            failed_at           DATETIME        NOT NULL

        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """)

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Audit tables ready: bronze_load_audit, bronze_failed_rows.")


# =============================================================================
# AUDIT LOGGING HELPERS
# =============================================================================

def log_audit_start(cursor, table_name, source_file, batch_id, started_at):
    """
    Inserts an initial STARTED row into bronze_load_audit when a file
    load begins. Returns the audit_id so it can be updated at completion.

    We insert at the START (not just at the end) so that if the pipeline
    crashes mid-load, there is still a record showing it was attempted.
    """
    cursor.execute("""
        INSERT INTO bronze_load_audit
            (table_name, source_file, batch_id, status, started_at)
        VALUES (%s, %s, %s, 'STARTED', %s)
    """, (table_name, source_file, batch_id, started_at))
    cursor.execute("SELECT LAST_INSERT_ID()")
    return cursor.fetchone()[0]     # return audit_id for later update


def log_audit_complete(cursor, audit_id, status, rows_read,
                        rows_inserted, rows_failed,
                        error_message, completed_at, duration_seconds):
    """
    Updates the bronze_load_audit row at the end of a load with
    final status, row counts, timing, and any error message.
    """
    cursor.execute("""
        UPDATE bronze_load_audit
        SET
            status           = %s,
            rows_read        = %s,
            rows_inserted    = %s,
            rows_failed      = %s,
            error_message    = %s,
            completed_at     = %s,
            duration_seconds = %s
        WHERE audit_id = %s
    """, (
        status, rows_read, rows_inserted, rows_failed,
        error_message, completed_at, duration_seconds, audit_id
    ))


def log_failed_row(cursor, batch_id, table_name,
                   source_file, row_number, error_message, raw_data):
    """
    Inserts one row into bronze_failed_rows for every row that
    failed to insert during a load.

    raw_data is stored as a JSON string so the complete original
    row is preserved and can be investigated or reprocessed later.
    """
    cursor.execute("""
        INSERT INTO bronze_failed_rows
            (batch_id, table_name, source_file,
             row_number, error_message, raw_data, failed_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
    """, (
        batch_id, table_name, source_file,
        row_number, str(error_message)[:500],   # cap error message at 500 chars
        str(raw_data)[:5000],                   # cap raw data at 5000 chars
    ))