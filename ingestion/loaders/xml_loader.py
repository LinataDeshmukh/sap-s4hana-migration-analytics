# =============================================================================
# ingestion/loaders/xml_loader.py
#
# PURPOSE:
#   Generic loader for any XML file → MySQL bronze table.
#   Scans bronze_data/ folder automatically for all .xml files.
#
#   Handles XML structure automatically:
#     - Finds the repeating record element (the one that appears most often)
#     - Flattens nested child elements into flat columns
#     - Handles both XML attributes and XML text content
#
#   Example XML structure:
#     <FunctionalLocations>           ← root element (ignored)
#       <FunctionalLocation>          ← repeating record element (detected)
#         <FlocId>FL-CHI1-001</FlocId>
#         <Description>Assembly Line</Description>
#         <Attributes>
#           <CostCenter>CC000001</CostCenter>   ← nested → flattened
#         </Attributes>
#       </FunctionalLocation>
#       <FunctionalLocation>...</FunctionalLocation>
#     </FunctionalLocations>
#
#   Result columns: flocid, description, attributes_costcenter
#
# HOW TO RUN:
#   python ingestion/loaders/xml_loader.py
#
# OUR XML FILES:
#   bronze_functional_locations.xml  → root: FunctionalLocations
#   bronze_material_exceptions.xml   → root: MaterialExceptions
#   bronze_inspection_plans.xml      → root: InspectionPlans
#
# FAILURE HANDLING:
#   Scenario 1 — File not found:
#       Logs FAILED to bronze_load_audit. Moves to next file.
#   Scenario 2 — File unreadable / malformed XML:
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
import xml.etree.ElementTree as ET
from collections import Counter
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
# HELPER: FLATTEN ONE XML ELEMENT INTO A FLAT DICT
# =============================================================================

def flatten_element(element, prefix=""):
    """
    Recursively flattens an XML element and all its children
    into a flat dictionary of key-value pairs.

    Handles three things:
      1. XML attributes  → e.g. <Record id="001"> → {"record_id": "001"}
      2. XML text content → e.g. <FlocId>FL-001</FlocId> → {"flocid": "FL-001"}
      3. Nested children  → e.g. <Attributes><CC>CC001</CC></Attributes>
                                → {"attributes_cc": "CC001"}

    Column names are:
      - Lowercased
      - Spaces and hyphens replaced with underscores
      - Nested elements joined with underscore: parent_child

    Args:
        element: xml.etree.ElementTree.Element
        prefix (str): prefix for nested element column names

    Returns:
        dict: flat key-value pairs representing this record
    """
    result = {}

    # ── Handle XML attributes (e.g. <Record id="001" type="A">) ──────────────
    for attr_name, attr_val in element.attrib.items():
        col = f"{prefix}{attr_name}".lower().replace(" ", "_").replace("-", "_")
        result[col] = attr_val.strip() if attr_val else None

    # ── Handle child elements ─────────────────────────────────────────────────
    for child in element:
        # Build column name: parent_child (e.g. attributes_costcenter)
        child_tag = child.tag.lower().replace(" ", "_").replace("-", "_")
        child_prefix = f"{prefix}{child_tag}_" if prefix else f"{child_tag}_"

        if len(child) == 0:
            # Leaf node — has text content, no further children
            col = child_prefix.rstrip("_")
            val = child.text.strip() if child.text and child.text.strip() else None
            result[col] = val
        else:
            # Has nested children — recurse with updated prefix
            nested = flatten_element(child, prefix=child_prefix)
            result.update(nested)

    return result


# =============================================================================
# HELPER: DETECT REPEATING RECORD ELEMENT
# =============================================================================

def find_record_tag(root):
    """
    Finds the XML tag that represents individual records by
    identifying which direct child tag appears most frequently
    under the root element.

    Example:
        <FunctionalLocations>
            <FunctionalLocation>...</FunctionalLocation>  ← appears 2625 times
            <FunctionalLocation>...</FunctionalLocation>
            <Metadata>...</Metadata>                      ← appears 1 time
        </FunctionalLocations>

    Returns: "FunctionalLocation" (the most frequent child tag)

    Args:
        root: xml.etree.ElementTree.Element (root of the document)

    Returns:
        str: the tag name of the repeating record element
    """
    # Count how many times each direct child tag appears
    tag_counts = Counter(child.tag for child in root)

    if not tag_counts:
        raise ValueError("XML root element has no children.")

    # The record element is the one that appears most often
    record_tag = tag_counts.most_common(1)[0][0]
    logger.info(f"  XML child tags: {dict(tag_counts)}")
    logger.info(f"  Record tag:     '{record_tag}' ({tag_counts[record_tag]} records)")

    return record_tag


# =============================================================================
# SINGLE FILE LOADER
# =============================================================================

def load_xml(file_name, table_name, batch_id):
    """
    Loads a single XML file into a MySQL bronze table.

    Args:
        file_name  (str): XML filename inside bronze_data/ folder
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

    # ── Scenario 2: File exists but cannot be parsed ──────────────────────────
    try:
        # Parse XML — encoding is detected automatically from XML declaration
        # ── Detect and handle encoding automatically ──────────────────────
        # Legacy SAP exports come in various encodings depending on the
        # regional settings of the source system:
        #   Windows-1252 → Western European plants (common in SAP)
        #   cp864        → Arabic/Middle Eastern regional settings
        #   utf-8        → Modern systems
        #
        # Strategy:
        #   1. Read file as raw bytes (no encoding assumption)
        #   2. Detect encoding using chardet
        #   3. Decode with detected encoding, replacing unreadable chars
        #   4. Re-encode as UTF-8
        #   5. Parse the clean UTF-8 bytes as XML
        #
        # errors="replace" means if a character still can't be decoded,
        # it becomes a ? rather than crashing — we preserve the row.

        import chardet
        import io

        with open(file_path, "rb") as f:
            raw_bytes = f.read()

        # Detect encoding from raw bytes
        detected    = chardet.detect(raw_bytes)
        encoding    = detected.get("encoding") or "utf-8"
        confidence  = detected.get("confidence", 0)
        logger.info(f"  Detected encoding: {encoding} (confidence: {confidence:.0%})")

        # Decode with detected encoding, replace unreadable characters
        # then re-encode as UTF-8 for clean XML parsing
        decoded     = raw_bytes.decode(encoding, errors="replace")
        utf8_bytes  = decoded.encode("utf-8")

        # Parse XML from the clean UTF-8 bytes
        tree = ET.parse(io.BytesIO(utf8_bytes))
        root = tree.getroot()

        logger.info(f"  Root element:  <{root.tag}>")

        # Detect which child tag represents individual records
        record_tag = find_record_tag(root)

        # Extract all record elements
        record_elements = root.findall(record_tag)
        logger.info(f"  Records found: {len(record_elements):,}")

        # Flatten each record element into a dict
        # Each dict becomes one row in MySQL
        rows = []
        for elem in record_elements:
            flat = flatten_element(elem)
            rows.append(flat)

        # Convert list of dicts to DataFrame
        df = pd.DataFrame(rows)

        # Clean column names — ensure all are valid MySQL identifiers
        df.columns = [
            str(c).strip().lower()
                   .replace(" ", "_")
                   .replace("-", "_")
                   .replace(".", "_")
            for c in df.columns
        ]

        # Convert everything to string to preserve raw values
        df = df.astype(str)

        # Convert "None" and "nan" strings back to actual None → MySQL NULL
        df = df.replace({"None": None, "nan": None, "NaN": None, "": None})

        logger.info(f"  Columns after flatten: {list(df.columns)}")

    except Exception as e:
        error_msg = f"Could not parse XML file: {e}"
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
    df["_record_tag"]          = record_tag  # which XML tag was the record element
    df["_batch_id"]            = batch_id

    # ── Build CREATE TABLE dynamically ────────────────────────────────────────
    col_definitions = ["load_id INT AUTO_INCREMENT PRIMARY KEY"]
    for col in df.columns:
        if col == "_ingestion_timestamp":
            col_definitions.append(f"`{col}` DATETIME")
        elif col in ("_source_file", "_record_tag", "_batch_id"):
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

def load_all_xmls(batch_id):
    """
    Scans bronze_data/ and loads every .xml file found.

    Args:
        batch_id (str): Shared batch ID for this pipeline run

    Returns:
        list of dicts: One result dict per file loaded
    """
    xml_files = sorted([
        f for f in os.listdir(BRONZE_DIR)
        if f.endswith(".xml")
    ])

    if not xml_files:
        logger.warning(f"No XML files found in {BRONZE_DIR}")
        return []

    logger.info(f"Found {len(xml_files)} XML files:")
    for f in xml_files:
        logger.info(f"  {f}")

    results = []
    for i, file_name in enumerate(xml_files, start=1):
        table_name = file_name.replace(".xml", "")
        logger.info(f"\n[{i}/{len(xml_files)}] {file_name} -> {table_name}")
        result = load_xml(file_name, table_name, batch_id)
        results.append(result)

    return results


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":

    batch_id = datetime.now().strftime("BATCH_%Y%m%d_%H%M%S")

    print("=" * 65)
    print("SAP Migration - Bronze XML Loader")
    print(f"Batch ID : {batch_id}")
    print("=" * 65)

    create_database_if_not_exists()
    create_audit_tables()

    results = load_all_xmls(batch_id)

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