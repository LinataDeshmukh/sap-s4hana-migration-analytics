# =============================================================================
# ingestion/run_all_bronze.py
#
# PURPOSE:
#   Master script that loads ALL 28 bronze files into MySQL.
#   This is the ONE script you run to reload the entire bronze layer.
#   Calls all 5 format-specific loaders in sequence.
#
# HOW TO RUN:
#   python ingestion/run_all_bronze.py
# =============================================================================

import sys
import os
import logging
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.db_connection import create_database_if_not_exists, create_audit_tables
from ingestion.loaders.csv_loader     import load_all_csvs
from ingestion.loaders.excel_loader   import load_all_excels
from ingestion.loaders.json_loader    import load_all_jsons
from ingestion.loaders.xml_loader     import load_all_xmls
from ingestion.loaders.parquet_loader import load_all_parquets

# ── Force UTF-8 on Windows console ───────────────────────────────────────────
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── Logger ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":

    started_at = datetime.now()
    batch_id   = started_at.strftime("BATCH_%Y%m%d_%H%M%S")

    print("=" * 65)
    print("SAP Migration Project - Full Bronze Layer Load")
    print(f"Batch ID : {batch_id}")
    print(f"Started  : {started_at.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    # ── Step 1: Database and audit table setup ────────────────────────────────
    create_database_if_not_exists()
    create_audit_tables()

    # ── Step 2: Load all formats ──────────────────────────────────────────────
    all_results = []

    print("\n[1/5] Loading CSV files...")
    all_results += load_all_csvs(batch_id)

    print("\n[2/5] Loading Excel files...")
    all_results += load_all_excels(batch_id)

    print("\n[3/5] Loading JSON files...")
    all_results += load_all_jsons(batch_id)

    print("\n[4/5] Loading XML files...")
    all_results += load_all_xmls(batch_id)

    print("\n[5/5] Loading Parquet files...")
    all_results += load_all_parquets(batch_id)

    # ── Step 3: Final summary ─────────────────────────────────────────────────
    completed_at     = datetime.now()
    duration_seconds = (completed_at - started_at).total_seconds()
    duration_mins    = duration_seconds / 60

    print("\n" + "=" * 65)
    print("FINAL SUMMARY - ALL 28 FILES")
    print("=" * 65)
    print(f"{'Table':<45} {'Status':<10} {'Rows':>8} {'Failed':>6}")
    print("-" * 65)

    total_inserted = 0
    total_failed   = 0
    counts         = {"SUCCESS": 0, "PARTIAL": 0, "FAILED": 0}

    for r in all_results:
        status = r.get("status", "UNKNOWN")
        print(
            f"{r['table']:<45} "
            f"{status:<10} "
            f"{r.get('rows_inserted', 0):>8,} "
            f"{r.get('rows_failed', 0):>6,}"
        )
        total_inserted += r.get("rows_inserted", 0)
        total_failed   += r.get("rows_failed", 0)
        counts[status]  = counts.get(status, 0) + 1

    print("-" * 65)
    print(f"{'TOTAL':<45} {'':10} {total_inserted:>8,} {total_failed:>6,}")
    print("=" * 65)
    print(f"SUCCESS : {counts.get('SUCCESS', 0)}")
    print(f"PARTIAL : {counts.get('PARTIAL', 0)}")
    print(f"FAILED  : {counts.get('FAILED', 0)}")
    print(f"Duration: {duration_mins:.1f} minutes")
    print(f"Batch ID: {batch_id}")
    print("Check bronze_load_audit in Workbench for full details.")
    print("=" * 65)