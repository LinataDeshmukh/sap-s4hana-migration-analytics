# =============================================================================
# profiling/export_bronze_silver.py
#
# PURPOSE:
#   Exports bronze and silver tables to Excel for side-by-side comparison.
#   Run this before and after building each silver model to see exactly
#   what data quality improvements were made.
#
#   Output files go to: profiling/comparisons/
#     bronze_material_master.xlsx   ← raw messy data
#     silver_material_master.xlsx   ← cleaned data (after dbt run)
#
# HOW TO RUN:
#   python profiling/export_bronze_silver.py
# =============================================================================

import sys
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DB_CONFIG

# ── Force UTF-8 on Windows ───────────────────────────────────────────────────
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "comparisons")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# TABLES TO EXPORT
# Add more pairs here as we build each silver model
# Format: (bronze_schema, bronze_table, silver_schema, silver_table, label)
# =============================================================================

EXPORT_PAIRS = [
    (
        "precision_mfg_bronze",
        "bronze_material_master",
        "precision_mfg_bronze_silver",
        "silver_material_master",
        "material_master"
    ),
    # Add more as we build silver models:
    # (
    #     "precision_mfg_bronze",
    #     "bronze_vendor_master",
    #     "precision_mfg_bronze_silver",
    #     "silver_vendor_master",
    #     "vendor_master"
    # ),
]

# Columns to highlight in silver that show cleaning happened
HIGHLIGHT_COLS = [
    "material_id_clean",
    "base_uom_clean",
    "material_type_clean",
    "material_group_clean",
    "created_date_clean",
    "changed_date_clean",
    "gross_weight_num",
    "net_weight_num",
    "dq_score",
]

FLAG_COLS = [
    "flag_null_description",
    "flag_null_material_type",
    "flag_invalid_material_type",
    "flag_null_material_group",
    "flag_null_uom",
    "flag_nonstandard_uom",
    "flag_negative_gross_weight",
    "flag_negative_net_weight",
    "flag_weight_invalid",
    "flag_bad_created_date",
    "flag_changed_before_created",
    "flag_generic_user",
    "flag_nonstandard_material_id",
    "flag_duplicate",
]

# =============================================================================
# STYLING
# =============================================================================

def style_header(ws, num_cols):
    for col in range(1, num_cols + 1):
        cell = ws.cell(row=1, column=col)
        cell.fill = PatternFill(
            start_color="1F4E79", end_color="1F4E79", fill_type="solid"
        )
        cell.font      = Font(bold=True, color="FFFFFF", size=10)
        cell.alignment = Alignment(horizontal="center", wrap_text=True)


def style_silver_sheet(ws, df):
    """
    Highlights cleaned columns in green and flag columns in red/green
    so you can instantly see what changed vs bronze.
    """
    col_names = list(df.columns)

    for col_idx, col_name in enumerate(col_names, start=1):
        col_letter = get_column_letter(col_idx)

        # Highlight cleaned value columns in light blue
        if col_name in HIGHLIGHT_COLS:
            for row in range(2, len(df) + 2):
                ws.cell(row=row, column=col_idx).fill = PatternFill(
                    start_color="DDEEFF", end_color="DDEEFF", fill_type="solid"
                )

        # Color flag columns: 0=green (clean), 1=red (issue)
        if col_name in FLAG_COLS:
            for row_idx in range(2, len(df) + 2):
                cell = ws.cell(row=row_idx, column=col_idx)
                if str(cell.value) == "1":
                    cell.fill = PatternFill(
                        start_color="FFC7CE",
                        end_color="FFC7CE",
                        fill_type="solid"
                    )
                elif str(cell.value) == "0":
                    cell.fill = PatternFill(
                        start_color="C6EFCE",
                        end_color="C6EFCE",
                        fill_type="solid"
                    )

        # Color DQ score column: green=high, amber=medium, red=low
        if col_name == "dq_score":
            for row_idx in range(2, len(df) + 2):
                cell = ws.cell(row=row_idx, column=col_idx)
                try:
                    score = float(cell.value)
                    if score >= 95:
                        color = "C6EFCE"   # green
                    elif score >= 80:
                        color = "FFEB9C"   # amber
                    else:
                        color = "FFC7CE"   # red
                    cell.fill = PatternFill(
                        start_color=color, end_color=color, fill_type="solid"
                    )
                except (TypeError, ValueError):
                    pass

    # Auto-fit columns
    for col_idx in range(1, len(col_names) + 1):
        col_letter = get_column_letter(col_idx)
        ws.column_dimensions[col_letter].width = 18

    ws.freeze_panes = "A2"


# =============================================================================
# EXPORT ONE PAIR
# =============================================================================

def export_pair(engine, bronze_schema, bronze_table,
                silver_schema, silver_table, label):
    """
    Exports one bronze/silver table pair to a single Excel file
    with two sheets: Bronze (raw) and Silver (cleaned).
    """
    output_file = os.path.join(OUTPUT_DIR, f"comparison_{label}.xlsx")
    print(f"\nExporting: {label}")

    # ── Read bronze (sample 500 rows for readability) ─────────────────────────
    try:
        df_bronze = pd.read_sql(
            f"SELECT * FROM `{bronze_schema}`.`{bronze_table}` LIMIT 500",
            engine
        )

        # Move _source_file and _batch_id to be the first two columns
        # so they are immediately visible when the file is opened
        priority_cols = [c for c in ["_source_file", "_batch_id"] 
                        if c in df_bronze.columns]
        other_cols    = [c for c in df_bronze.columns 
                        if c not in priority_cols]
        df_bronze     = df_bronze[priority_cols + other_cols]
        print(f"  Bronze: {len(df_bronze):,} rows, {len(df_bronze.columns)} columns")
    except Exception as e:
        print(f"  Bronze FAILED: {e}")
        df_bronze = pd.DataFrame()

    # ── Read silver (sample 500 rows) ─────────────────────────────────────────
    try:
        df_silver = pd.read_sql(
            f"SELECT * FROM `{silver_schema}`.`{silver_table}` LIMIT 500",
            engine
        )
        print(f"  Silver: {len(df_silver):,} rows, {len(df_silver.columns)} columns")
    except Exception as e:
        print(f"  Silver FAILED (model not built yet?): {e}")
        df_silver = pd.DataFrame()

    # ── Write Excel ───────────────────────────────────────────────────────────
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:

        # Sheet 1: Bronze — raw messy data
        if not df_bronze.empty:
            df_bronze.to_excel(writer, sheet_name="Bronze_Raw", index=False)
            ws_bronze = writer.sheets["Bronze_Raw"]
            style_header(ws_bronze, len(df_bronze.columns))
            for col_idx in range(1, len(df_bronze.columns) + 1):
                ws_bronze.column_dimensions[
                    get_column_letter(col_idx)
                ].width = 18
            ws_bronze.freeze_panes = "A2"

        # Sheet 2: Silver — cleaned data with highlights
        if not df_silver.empty:
            df_silver.to_excel(writer, sheet_name="Silver_Cleaned", index=False)
            ws_silver = writer.sheets["Silver_Cleaned"]
            style_header(ws_silver, len(df_silver.columns))
            style_silver_sheet(ws_silver, df_silver)

        # Sheet 3: DQ Summary — counts of each flag
        if not df_silver.empty:
            summary_rows = []
            for flag in FLAG_COLS:
                if flag in df_silver.columns:
                    issue_count = int(df_silver[flag].astype(str).eq("1").sum())
                    clean_count = int(df_silver[flag].astype(str).eq("0").sum())
                    summary_rows.append({
                        "Flag":        flag,
                        "Issue Count": issue_count,
                        "Clean Count": clean_count,
                        "Issue %":     round(
                            issue_count / len(df_silver) * 100, 1
                        ) if len(df_silver) > 0 else 0,
                    })

            if summary_rows:
                df_summary = pd.DataFrame(summary_rows)

                # Add avg DQ score
                if "dq_score" in df_silver.columns:
                    avg_score = round(
                        pd.to_numeric(
                            df_silver["dq_score"], errors="coerce"
                        ).mean(), 1
                    )
                    df_summary.loc[len(df_summary)] = {
                        "Flag": "AVG DQ SCORE",
                        "Issue Count": "",
                        "Clean Count": "",
                        "Issue %": avg_score,
                    }

                df_summary.to_excel(
                    writer, sheet_name="DQ_Summary", index=False
                )
                ws_sum = writer.sheets["DQ_Summary"]
                style_header(ws_sum, len(df_summary.columns))
                for col_idx in range(1, len(df_summary.columns) + 1):
                    ws_sum.column_dimensions[
                        get_column_letter(col_idx)
                    ].width = 20

    print(f"  Saved: {output_file}")
    return output_file


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":

    started_at = datetime.now()

    print("=" * 65)
    print("SAP Migration - Bronze vs Silver Comparison Exporter")
    print(f"Started : {started_at.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Output  : {OUTPUT_DIR}")
    print("=" * 65)

    # Connect via SQLAlchemy
    engine = create_engine(
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        f"?charset=utf8mb4"
    )

    exported = []
    for bronze_schema, bronze_table, silver_schema, silver_table, label \
            in EXPORT_PAIRS:
        f = export_pair(
            engine,
            bronze_schema, bronze_table,
            silver_schema, silver_table,
            label
        )
        exported.append(f)

    duration = (datetime.now() - started_at).total_seconds()

    print("\n" + "=" * 65)
    print("EXPORT COMPLETE")
    print("=" * 65)
    for f in exported:
        print(f"  {f}")
    print(f"\nDuration: {duration:.1f}s")
    print("=" * 65)
    print("\nEach Excel file has 3 sheets:")
    print("  Bronze_Raw     → original messy data from source system")
    print("  Silver_Cleaned → cleaned data with highlights")
    print("                   Blue  = cleaned value columns")
    print("                   Green = flag is 0 (field is clean)")
    print("                   Red   = flag is 1 (field has issue)")
    print("  DQ_Summary     → count of issues per flag type")
    print("=" * 65)