# =============================================================================
# profiling/bronze_column_profiler.py
#
# PURPOSE:
#   Profiles every column of every bronze table in precision_mfg_bronze.
#   For each column it computes:
#     - Row counts (total, null, blank, populated)
#     - Distinct value counts
#     - Most common values (top 5)
#     - String length statistics (min, max, avg)
#     - Numeric detection (how many values look like numbers)
#     - Date detection (how many values look like dates)
#     - Sample values (first 5 distinct non-null values)
#
#   Results are written to MySQL table:
#     precision_mfg_bronze.bronze_column_profile
#
#   This output drives:
#     - Silver model cleaning rules (know exactly what to fix)
#     - dbt profiling models (domain-level summaries)
#     - Power BI data quality dashboard
#
# HOW TO RUN:
#   python profiling/bronze_column_profiler.py
#
# OUTPUT TABLE:
#   precision_mfg_bronze.bronze_column_profile
# =============================================================================

import sys
import os
import logging
import json
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DB_CONFIG, LOG_DIR
from ingestion.db_connection import get_connection

# ── Force UTF-8 on Windows console ───────────────────────────────────────────
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── Logger ────────────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter    = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
file_handler = logging.FileHandler(
    os.path.join(LOG_DIR, "profiling.log"), encoding="utf-8"
)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)


# =============================================================================
# STEP 1: CREATE OUTPUT TABLE
# =============================================================================

CREATE_PROFILE_TABLE = """
CREATE TABLE IF NOT EXISTS bronze_column_profile (
    profile_id          INT AUTO_INCREMENT PRIMARY KEY,

    -- Which table and column this profile is for
    table_name          VARCHAR(100)    NOT NULL,
    column_name         VARCHAR(100)    NOT NULL,
    column_position     INT,            -- position of column in table (1-based)

    -- Row count statistics
    total_rows          INT,            -- total rows in the table
    null_count          INT,            -- rows where this column is NULL
    blank_count         INT,            -- rows where this column is empty string
    populated_count     INT,            -- rows where column has a real value
    null_pct            DECIMAL(5,2),   -- % of rows that are NULL
    blank_pct           DECIMAL(5,2),   -- % of rows that are blank
    populated_pct       DECIMAL(5,2),   -- % of rows that are populated

    -- Distinct value statistics
    distinct_count      INT,            -- number of unique values
    distinct_pct        DECIMAL(5,2),   -- distinct / total * 100

    -- String length statistics
    min_length          INT,            -- shortest value length
    max_length          INT,            -- longest value length
    avg_length          DECIMAL(8,2),   -- average value length

    -- Value type detection
    numeric_count       INT,            -- how many values look like numbers
    numeric_pct         DECIMAL(5,2),   -- % of values that are numeric
    date_count          INT,            -- how many values look like dates
    date_pct            DECIMAL(5,2),   -- % of values that are dates

    -- Top values (stored as JSON string)
    -- e.g. [{"value": "KG", "count": 8400}, {"value": "EA", "count": 3200}]
    top_5_values        TEXT,

    -- Sample of distinct values (first 5 non-null unique values)
    sample_values       TEXT,

    -- Data quality flags derived from profiling
    has_nulls           TINYINT(1),     -- 1 if any nulls exist
    high_null_rate      TINYINT(1),     -- 1 if null_pct > 20%
    all_null            TINYINT(1),     -- 1 if 100% null
    is_constant         TINYINT(1),     -- 1 if only one distinct value
    high_cardinality    TINYINT(1),     -- 1 if distinct_pct > 90% (possible ID col)
    has_blanks          TINYINT(1),     -- 1 if any blank strings exist
    mixed_types         TINYINT(1),     -- 1 if column has both numeric and text values

    -- Domain classification
    domain              VARCHAR(50),    -- which SAP domain this table belongs to

    -- When this profile was run
    profiled_at         DATETIME        NOT NULL,
    profile_batch_id    VARCHAR(50)     NOT NULL

) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
"""

# =============================================================================
# TABLE TO DOMAIN MAPPING
# Maps each bronze table to its SAP functional domain
# =============================================================================

TABLE_DOMAIN_MAP = {
    # Domain 1: Materials
    "bronze_material_master":              "Materials",
    "bronze_material_plant_data":          "Materials",
    "bronze_material_uom":                 "Materials",
    "bronze_material_plant_data_UDS":      "Materials",
    "bronze_material_master_delta":        "Materials",

    # Domain 2: Procurement
    "bronze_vendor_master":                "Procurement",
    "bronze_purchasing_info_record_UDS":   "Procurement",
    "bronze_source_list":                  "Procurement",
    "bronze_vendor_delta":                 "Procurement",

    # Domain 3: Planning/MRP
    "bronze_mrp_parameters":               "Planning_MRP",
    "bronze_mrp_controllers":              "Planning_MRP",
    "bronze_planning_calendar":            "Planning_MRP",

    # Domain 4: Plant Maintenance
    "bronze_equipment_master":             "Plant_Maintenance",
    "bronze_functional_locations":         "Plant_Maintenance",
    "bronze_maintenance_plans":            "Plant_Maintenance",

    # Domain 5: Warehouse
    "bronze_warehouse_master":             "Warehouse",
    "bronze_storage_locations":            "Warehouse",
    "bronze_storage_bins":                 "Warehouse",
    "bronze_material_storage_assignment":  "Warehouse",

    # Domain 6: Quality
    "bronze_quality_info_records":         "Quality",
    "bronze_inspection_plans":             "Quality",
    "bronze_sampling_procedures":          "Quality",

    # Domain 7: Finance
    "bronze_cost_centers":                 "Finance",
    "bronze_profit_centers":               "Finance",
    "bronze_gl_accounts":                  "Finance",
    "bronze_material_costing":             "Finance",

    # Audit tables
    "bronze_load_audit":                   "Audit",
    "bronze_failed_rows":                  "Audit",
}


# =============================================================================
# STEP 2: GET ALL BRONZE TABLES
# =============================================================================

def get_bronze_tables(cursor):
    """
    Returns list of all tables in the bronze database
    excluding the profile table itself and audit tables.
    """
    cursor.execute("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME LIKE 'bronze_%%'
          AND TABLE_NAME != 'bronze_column_profile'
        ORDER BY TABLE_NAME
    """, (DB_CONFIG["database"],))

    tables = [row[0] for row in cursor.fetchall()]
    logger.info(f"Found {len(tables)} bronze tables to profile.")
    return tables


# =============================================================================
# STEP 3: GET ALL COLUMNS FOR A TABLE
# =============================================================================

def get_table_columns(cursor, table_name):
    """
    Returns list of (column_name, position) for all columns in a table.
    Uses INFORMATION_SCHEMA so we get the exact column order.
    """
    cursor.execute("""
        SELECT COLUMN_NAME, ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME   = %s
        ORDER BY ORDINAL_POSITION
    """, (DB_CONFIG["database"], table_name))

    return cursor.fetchall()


# =============================================================================
# STEP 4: PROFILE ONE COLUMN
# Runs a series of SQL queries against one column and returns all metrics.
# =============================================================================

def profile_column(cursor, table_name, column_name, position, total_rows,
                   batch_id, profiled_at):
    """
    Profiles a single column and returns a dict of all metrics.

    Args:
        cursor:      MySQL cursor
        table_name:  Name of the bronze table
        column_name: Name of the column to profile
        position:    Column position in table
        total_rows:  Total rows in the table (pre-computed)
        batch_id:    Profile run batch ID
        profiled_at: Timestamp of this profile run

    Returns:
        dict: All profile metrics for this column
    """
    col = f"`{column_name}`"
    tbl = f"`{table_name}`"

    # ── Null and blank counts ─────────────────────────────────────────────────
    cursor.execute(f"""
        SELECT
            -- Count NULLs
            SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END)              AS null_count,
            -- Count empty strings (blank but not null)
            SUM(CASE WHEN {col} = '' THEN 1 ELSE 0 END)                 AS blank_count,
            -- Count populated (non-null, non-blank)
            SUM(CASE WHEN {col} IS NOT NULL
                      AND TRIM({col}) != '' THEN 1 ELSE 0 END)          AS populated_count,
            -- Count distinct values
            COUNT(DISTINCT {col})                                        AS distinct_count,
            -- String length stats
            MIN(LENGTH({col}))                                           AS min_length,
            MAX(LENGTH({col}))                                           AS max_length,
            AVG(LENGTH({col}))                                           AS avg_length,
            -- Numeric detection: value matches number pattern
            SUM(CASE WHEN {col} REGEXP '^-?[0-9]+\\.?[0-9]*$'
                     THEN 1 ELSE 0 END)                                 AS numeric_count,
            -- Date detection: value matches common date patterns
            SUM(CASE WHEN {col} REGEXP
                '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
                OR {col} REGEXP
                '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
                OR {col} REGEXP
                '^[0-9]{{2}}-[0-9]{{2}}-[0-9]{{4}}$'
                THEN 1 ELSE 0 END)                                      AS date_count
        FROM {tbl}
    """)

    row = cursor.fetchone()
    null_count      = int(row[0] or 0)
    blank_count     = int(row[1] or 0)
    populated_count = int(row[2] or 0)
    distinct_count  = int(row[3] or 0)
    min_length      = int(row[4] or 0)
    max_length      = int(row[5] or 0)
    avg_length      = float(row[6] or 0)
    numeric_count   = int(row[7] or 0)
    date_count      = int(row[8] or 0)

    # ── Calculate percentages ─────────────────────────────────────────────────
    null_pct      = round(null_count      / total_rows * 100, 2) if total_rows > 0 else 0
    blank_pct     = round(blank_count     / total_rows * 100, 2) if total_rows > 0 else 0
    populated_pct = round(populated_count / total_rows * 100, 2) if total_rows > 0 else 0
    distinct_pct  = round(distinct_count  / total_rows * 100, 2) if total_rows > 0 else 0
    numeric_pct   = round(numeric_count   / total_rows * 100, 2) if total_rows > 0 else 0
    date_pct      = round(date_count      / total_rows * 100, 2) if total_rows > 0 else 0

    # ── Top 5 most common values ──────────────────────────────────────────────
    cursor.execute(f"""
        SELECT {col}, COUNT(*) AS cnt
        FROM {tbl}
        WHERE {col} IS NOT NULL AND {col} != ''
        GROUP BY {col}
        ORDER BY cnt DESC
        LIMIT 5
    """)
    top_5 = [
        {"value": str(r[0])[:100], "count": int(r[1])}
        for r in cursor.fetchall()
    ]
    top_5_json = json.dumps(top_5, ensure_ascii=False)

    # ── Sample values (first 5 distinct non-null values) ─────────────────────
    cursor.execute(f"""
        SELECT DISTINCT {col}
        FROM {tbl}
        WHERE {col} IS NOT NULL AND {col} != ''
        LIMIT 5
    """)
    samples     = [str(r[0])[:100] for r in cursor.fetchall()]
    sample_json = json.dumps(samples, ensure_ascii=False)

    # ── Derive DQ flags ───────────────────────────────────────────────────────
    has_nulls        = 1 if null_count > 0 else 0
    high_null_rate   = 1 if null_pct > 20 else 0
    all_null         = 1 if null_count == total_rows else 0
    is_constant      = 1 if distinct_count == 1 else 0
    high_cardinality = 1 if distinct_pct > 90 else 0
    has_blanks       = 1 if blank_count > 0 else 0

    # Mixed types: column has both numeric and non-numeric values
    # (indicates inconsistent data entry)
    mixed_types = 1 if (
        numeric_count > 0 and
        numeric_count < populated_count
    ) else 0

    return {
        "table_name":       table_name,
        "column_name":      column_name,
        "column_position":  position,
        "total_rows":       total_rows,
        "null_count":       null_count,
        "blank_count":      blank_count,
        "populated_count":  populated_count,
        "null_pct":         null_pct,
        "blank_pct":        blank_pct,
        "populated_pct":    populated_pct,
        "distinct_count":   distinct_count,
        "distinct_pct":     distinct_pct,
        "min_length":       min_length,
        "max_length":       max_length,
        "avg_length":       avg_length,
        "numeric_count":    numeric_count,
        "numeric_pct":      numeric_pct,
        "date_count":       date_count,
        "date_pct":         date_pct,
        "top_5_values":     top_5_json,
        "sample_values":    sample_json,
        "has_nulls":        has_nulls,
        "high_null_rate":   high_null_rate,
        "all_null":         all_null,
        "is_constant":      is_constant,
        "high_cardinality": high_cardinality,
        "has_blanks":       has_blanks,
        "mixed_types":      mixed_types,
        "domain":           TABLE_DOMAIN_MAP.get(table_name, "Unknown"),
        "profiled_at":      profiled_at,
        "profile_batch_id": batch_id,
    }


# =============================================================================
# STEP 5: INSERT PROFILE RESULT
# =============================================================================

INSERT_PROFILE_SQL = """
INSERT INTO bronze_column_profile (
    table_name, column_name, column_position,
    total_rows, null_count, blank_count, populated_count,
    null_pct, blank_pct, populated_pct,
    distinct_count, distinct_pct,
    min_length, max_length, avg_length,
    numeric_count, numeric_pct,
    date_count, date_pct,
    top_5_values, sample_values,
    has_nulls, high_null_rate, all_null,
    is_constant, high_cardinality, has_blanks, mixed_types,
    domain, profiled_at, profile_batch_id
) VALUES (
    %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s,
    %s, %s, %s,
    %s, %s,
    %s, %s,
    %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s
)
"""


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":

    batch_id   = datetime.now().strftime("PROFILE_%Y%m%d_%H%M%S")
    profiled_at = datetime.now()

    print("=" * 65)
    print("SAP Migration - Bronze Column Profiler")
    print(f"Batch ID  : {batch_id}")
    print(f"Started   : {profiled_at.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    conn   = get_connection()
    cursor = conn.cursor()

    # ── Create output table ───────────────────────────────────────────────────
    cursor.execute(CREATE_PROFILE_TABLE)

    # ── Clear previous profile results (fresh run each time) ─────────────────
    cursor.execute("TRUNCATE TABLE bronze_column_profile")
    conn.commit()
    logger.info("Profile table ready. Previous results cleared.")

    # ── Get all bronze tables ─────────────────────────────────────────────────
    tables = get_bronze_tables(cursor)

    total_columns_profiled = 0
    table_summary          = []

    # ── Profile each table ────────────────────────────────────────────────────
    for t_idx, table_name in enumerate(tables, start=1):

        logger.info(f"\n[{t_idx}/{len(tables)}] Profiling: {table_name}")

        # Get total row count for this table
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        total_rows = cursor.fetchone()[0]
        logger.info(f"  Total rows: {total_rows:,}")

        # Get all columns
        columns = get_table_columns(cursor, table_name)
        logger.info(f"  Columns:    {len(columns)}")

        columns_done = 0

        for col_name, position in columns:

            # Skip internal audit columns — not useful to profile
            if col_name in ("load_id", "_ingestion_timestamp",
                            "_source_file", "_batch_id",
                            "_source_sheet", "_data_key",
                            "_record_tag"):
                continue

            try:
                # Profile this column
                profile = profile_column(
                    cursor     = cursor,
                    table_name = table_name,
                    column_name = col_name,
                    position   = position,
                    total_rows = total_rows,
                    batch_id   = batch_id,
                    profiled_at = profiled_at,
                )

                # Insert result
                cursor.execute(INSERT_PROFILE_SQL, (
                    profile["table_name"],
                    profile["column_name"],
                    profile["column_position"],
                    profile["total_rows"],
                    profile["null_count"],
                    profile["blank_count"],
                    profile["populated_count"],
                    profile["null_pct"],
                    profile["blank_pct"],
                    profile["populated_pct"],
                    profile["distinct_count"],
                    profile["distinct_pct"],
                    profile["min_length"],
                    profile["max_length"],
                    profile["avg_length"],
                    profile["numeric_count"],
                    profile["numeric_pct"],
                    profile["date_count"],
                    profile["date_pct"],
                    profile["top_5_values"],
                    profile["sample_values"],
                    profile["has_nulls"],
                    profile["high_null_rate"],
                    profile["all_null"],
                    profile["is_constant"],
                    profile["high_cardinality"],
                    profile["has_blanks"],
                    profile["mixed_types"],
                    profile["domain"],
                    profile["profiled_at"],
                    profile["profile_batch_id"],
                ))

                columns_done += 1
                total_columns_profiled += 1

            except Exception as e:
                logger.warning(f"  Column {col_name} failed: {e}")

        conn.commit()
        logger.info(f"  Profiled {columns_done} columns.")

        table_summary.append({
            "table":   table_name,
            "rows":    total_rows,
            "columns": columns_done,
        })

    # ── Final summary ─────────────────────────────────────────────────────────
    completed_at = datetime.now()
    duration     = (completed_at - profiled_at).total_seconds()

    print("\n" + "=" * 65)
    print("PROFILING SUMMARY")
    print("=" * 65)
    print(f"{'Table':<45} {'Rows':>8} {'Cols':>6}")
    print("-" * 65)

    for t in table_summary:
        print(f"{t['table']:<45} {t['rows']:>8,} {t['columns']:>6}")

    print("-" * 65)
    print(f"Total columns profiled: {total_columns_profiled:,}")
    print(f"Duration:               {duration:.1f}s")
    print(f"Batch ID:               {batch_id}")
    print("=" * 65)
    print("\nResults saved to: precision_mfg_bronze.bronze_column_profile")
    print("Open Workbench and run:")
    print("  SELECT * FROM bronze_column_profile ORDER BY table_name, column_position;")
    print("=" * 65)

    cursor.close()
    conn.close()