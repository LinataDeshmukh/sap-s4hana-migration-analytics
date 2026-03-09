"""
Domain 1: Additional Format Generation
PrecisionManufacturing Inc. - SAP S/4HANA Migration Project

Generates:
  - material_plant_data    → Excel (.xlsx)  UDS-style upload sheet
  - material_uom           → JSON           API-style extract
  - material_exceptions    → XML            SAP legacy error log
  - material_master_delta  → Parquet        Delta load from modern system
"""

import pandas as pd
import numpy as np
import random
import json
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime, timedelta
import os

random.seed(99)
np.random.seed(99)

OUTPUT_DIR = "bronze_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Reuse helpers from your existing script
def random_date(start_year=2018, end_year=2024):
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def messy_date(date):
    fmt = random.choice([
        "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y",
        "%d-%b-%Y", "%Y%m%d",   "%b %d, %Y",
    ])
    return date.strftime(fmt)

def messy_uom(uom):
    variations = {
        "KG":  ["KG","kg","Kg","Kilograms","kgs"],
        "EA":  ["EA","ea","Each","EACH","PCS"],
        "LT":  ["LT","lt","Liter","L","ltr"],
        "PAL": ["PAL","Pal","PALLET","pallet","PLT"],
        "BOX": ["BOX","Box","box","BOXES","BX"],
    }
    if uom in variations and random.random() < 0.30:
        return random.choice(variations[uom][1:])
    return uom

clean_ids = [f"MAT{str(i).zfill(6)}" for i in range(1, 15001)]


# ─────────────────────────────────────────────
# FILE 1: EXCEL - material_plant_data (UDS style)
# ─────────────────────────────────────────────

def generate_excel_plant_data(n=5000):
    """
    Simulates an Upload Data Sheet (UDS) — exactly the format
    used in SAP migrations. Business users fill these in Excel,
    so they're full of formatting issues, merged cells workarounds,
    color-coded rows, free-text comments, and mixed data types.
    """
    print("Generating Excel UDS: material_plant_data...")

    plants      = ["CHI1","HOU2","DET3"]
    mrp_types   = ["PD","VB","ND","MK"]
    bad_mrp     = ["AUTO","MANUAL","Yes","yes","PD "]  # trailing space
    controllers = [f"MC{str(i).zfill(2)}" for i in range(1, 21)]
    bad_ctrl    = ["TBD","N/A","NONE","MC99","?"]
    lot_sizes   = ["EX","FX","HB","WB"]
    stor_locs   = [f"SL{str(i).zfill(2)}" for i in range(1, 31)]
    val_classes = [f"VC{str(i).zfill(4)}" for i in range(3000, 3050)]
    price_ctrls = ["S","V"]
    bad_pc      = ["STD","AVG","Standard","Moving","s"]

    rows = []
    for i in range(n):
        mid   = random.choice(clean_ids)
        plant = random.choice(plants)
        min_l = round(random.uniform(1, 100), 3)
        max_l = round(min_l * random.uniform(5, 50), 3)

        rows.append({
            # UDS sheets often have human-readable headers with spaces
            "Material Number":        mid,
            "Plant":                  plant,
            "MRP Type":               random.choice(bad_mrp) if random.random() < 0.08
                                      else random.choice(mrp_types),
            "MRP Controller":         random.choice(bad_ctrl) if random.random() < 0.10
                                      else random.choice(controllers),
            "Lot Size Key":           None if random.random() < 0.06
                                      else random.choice(lot_sizes),
            "Min Lot Size":           -abs(min_l) if random.random() < 0.04 else min_l,
            "Max Lot Size":           round(min_l * 0.5,3) if random.random() < 0.03
                                      else max_l,
            "Safety Stock":           None if random.random() < 0.08
                                      else round(random.uniform(0,1000),3),
            "Reorder Point":          -abs(round(random.uniform(1,500),3))
                                      if random.random() < 0.04
                                      else round(random.uniform(0,500),3),
            "Planned Delivery (Days)": (0 if random.random() < 0.03 else
                                        -random.randint(1,10) if random.random() < 0.02
                                        else random.randint(1,90)),
            "GR Processing Days":     None if random.random() < 0.05
                                      else random.randint(0,10),
            "Storage Location":       random.choice(["SL99","N/A","SLXX"])
                                      if random.random() < 0.07
                                      else random.choice(stor_locs),
            "Valuation Class":        None if random.random() < 0.09
                                      else random.choice(val_classes),
            "Price Control":          random.choice(bad_pc) if random.random() < 0.06
                                      else random.choice(price_ctrls),
            "Standard Price":         None if random.random() < 0.10
                                      else round(random.uniform(0.5,10000),2),
            "Moving Avg Price":       -abs(round(random.uniform(0.5,10000),2))
                                      if random.random() < 0.05
                                      else round(random.uniform(0.5,10000),2),
            "Valid From":             None if random.random() < 0.06
                                      else messy_date(random_date(2018,2022)),
            "Created Date":           messy_date(random_date(2018,2023)),
            # UDS-specific columns business teams add manually
            "Status":                 random.choice(["Ready","In Review","APPROVED",
                                                     "Pending","approved","TBD",
                                                     "Not Started",None]),
            "Comments":               random.choice([
                                        None, "checked by finance",
                                        "CONFIRM WITH PLANT MANAGER",
                                        "duplicate? check legacy",
                                        "migrated from old system",
                                        "TO DO", "ok", "??"
                                      ]),
            "Responsible":            random.choice([
                                        "John Smith","JSMITH","j.smith",
                                        "Jane Doe","TBD","","Admin",None
                                      ]),
        })

    df = pd.DataFrame(rows)

    # Write to Excel with formatting to simulate real UDS
    path = f"{OUTPUT_DIR}/bronze_material_plant_data_UDS.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        # Sheet 1: Main data (what engineers process)
        df.to_excel(writer, sheet_name="Material Plant Data", index=False)

        # Sheet 2: Instructions tab (like real UDS files have)
        instructions = pd.DataFrame({
            "Field":       ["Material Number","Plant","MRP Type","Safety Stock"],
            "Required":    ["Yes","Yes","Yes","No"],
            "Format":      ["18 char","4 char","2 char","Decimal"],
            "Description": [
                "SAP material number - must match material master",
                "Valid plant code: CHI1, HOU2, DET3",
                "MRP procedure: PD=MRP, VB=Reorder, ND=No planning",
                "Minimum buffer stock quantity"
            ]
        })
        instructions.to_excel(writer, sheet_name="Instructions", index=False)

        # Sheet 3: Valid values reference (like real UDS files have)
        valid_vals = pd.DataFrame({
            "Field":      ["MRP Type","MRP Type","MRP Type","Price Control","Price Control"],
            "Value":      ["PD","VB","ND","S","V"],
            "Meaning":    [
                "MRP - Deterministic planning",
                "Reorder point planning",
                "No planning",
                "Standard price",
                "Moving average price"
            ]
        })
        valid_vals.to_excel(writer, sheet_name="Valid Values", index=False)

    print(f"  Saved: {path} ({len(df):,} rows, 3 sheets)\n")
    return df


# ─────────────────────────────────────────────
# FILE 2: JSON - material_uom (API extract style)
# ─────────────────────────────────────────────

def generate_json_uom(n=5000):
    """
    Simulates a REST API extract from a legacy system.
    Nested structure, inconsistent field names, null handling varies,
    some records have extra unexpected fields.
    """
    print("Generating JSON API extract: material_uom...")

    alt_uoms = ["PAL","BOX","ST","CS","ROL","SET","BAG","CAN"]

    records = []
    used = set()
    record_id = 90001  # offset to not clash with CSV version

    while len(records) < n:
        mid = random.choice(clean_ids)
        alt = random.choice(alt_uoms)
        if (mid, alt) in used:
            continue
        used.add((mid, alt))

        num = round(random.uniform(0.1, 1000), 3)
        den = round(random.uniform(0.1, 100),  3)
        if random.random() < 0.03: num = 0
        if random.random() < 0.02: den = 0

        record = {
            "recordId":     record_id,
            "materialId":   mid,            # camelCase — different from CSV snake_case
            "alternateUOM": messy_uom(alt),
            "conversion": {                 # nested object — common in API responses
                "numerator":   num,
                "denominator": den,
                "factor":      round(num / den, 6) if den != 0 else None,
            },
            "packaging": {
                "length":      None if random.random() < 0.05 else round(random.uniform(1,2000),3),
                "width":       None if random.random() < 0.05 else round(random.uniform(1,2000),3),
                "height":      None if random.random() < 0.05 else round(random.uniform(1,2000),3),
                "volumeLiters":None if random.random() < 0.04 else round(random.uniform(0.001,5000),3),
                "weightKG":    None if random.random() < 0.06 else round(random.uniform(0.1,10000),3),
            },
            # Barcode — 15% missing, 5% duplicate
            "eanUpc": None if random.random() < 0.15 else (
                      "5011234567890" if random.random() < 0.05
                      else f"501{random.randint(1000000000,9999999999)}"),
            "auditInfo": {
                "createdDate": messy_date(random_date(2018,2024)),
                "source":      random.choice(["LEGACY_ERP","MANUAL","API_SYNC","MIGRATE","?"]),
            },
        }

        # 5% of records have unexpected extra fields — real API messiness
        if random.random() < 0.05:
            record["EXTRA_FIELD_DO_NOT_USE"] = "legacy artifact"

        # 3% of records use completely different key naming (old API version)
        if random.random() < 0.03:
            record["material_number"] = record.pop("materialId")  # inconsistent key
            record["uom_code"]        = record.pop("alternateUOM")

        records.append(record)
        record_id += 1

    path = f"{OUTPUT_DIR}/bronze_material_uom.json"
    with open(path, "w") as f:
        json.dump({
            "apiVersion":  "1.2.3",
            "extractedAt": datetime.now().isoformat(),
            "source":      "LEGACY_ERP_SYSTEM",
            "recordCount": len(records),
            "data":        records
        }, f, indent=2)

    print(f"  Saved: {path} ({len(records):,} records)\n")
    return records


# ─────────────────────────────────────────────
# FILE 3: XML - material_exceptions_log
# ─────────────────────────────────────────────

def generate_xml_exceptions(n=2000):
    """
    Simulates SAP legacy system exception/error log export.
    In real SAP migrations, LSMW and BAPI tools generate XML
    error logs when records fail validation during test loads.
    These are gold for showing data quality analysis skills.
    """
    print("Generating XML exception log: material_exceptions...")

    error_types = [
        "DUPLICATE_MATERIAL_ID",
        "INVALID_UOM",
        "MISSING_MATERIAL_GROUP",
        "INVALID_PROCUREMENT_TYPE",
        "NEGATIVE_WEIGHT",
        "NULL_VALUATION_CLASS",
        "ORPHANED_PLANT_ASSIGNMENT",
        "INVALID_MRP_CONTROLLER",
        "DATE_FORMAT_ERROR",
        "NET_WEIGHT_EXCEEDS_GROSS",
    ]

    severity_levels = ["ERROR", "WARNING", "INFO"]
    systems         = ["LSMW","BAPI_MATERIAL_SAVEDATA","Z_MIGRATION_TOOL","MANUAL_CHECK"]
    processors      = ["USR001","USR002","USR003","SYSTEM","ADMIN"]

    root = ET.Element("MaterialExceptionLog")
    root.set("version", "2.1")
    root.set("extractedAt", datetime.now().isoformat())
    root.set("system", "SAP_ECC_600")
    root.set("totalRecords", str(n))

    for i in range(n):
        mid        = random.choice(clean_ids)
        error_type = random.choice(error_types)
        severity   = random.choice(severity_levels)
        created_dt = random_date(2023, 2024)

        exc = ET.SubElement(root, "Exception")
        exc.set("id",       str(100000 + i))
        exc.set("severity", severity)

        ET.SubElement(exc, "MaterialId").text     = mid
        ET.SubElement(exc, "ErrorCode").text      = error_type
        ET.SubElement(exc, "ErrorMessage").text   = {
            "DUPLICATE_MATERIAL_ID":     f"Material {mid} already exists in target system",
            "INVALID_UOM":               f"Unit of measure '{messy_uom('KG')}' not found in T006",
            "MISSING_MATERIAL_GROUP":    f"Material group is required for material type ROH",
            "INVALID_PROCUREMENT_TYPE":  f"Procurement type 'Buy' is not a valid SAP value",
            "NEGATIVE_WEIGHT":           f"Gross weight cannot be negative: {-round(random.uniform(0.1,100),2)}",
            "NULL_VALUATION_CLASS":      f"Valuation class missing — material cannot be valuated",
            "ORPHANED_PLANT_ASSIGNMENT": f"Plant {random.choice(['PLT1','XXXX','CHI2'])} does not exist",
            "INVALID_MRP_CONTROLLER":    f"MRP controller '{random.choice(['TBD','N/A','MC99'])}' not in T024",
            "DATE_FORMAT_ERROR":         f"Cannot parse date '{messy_date(created_dt)}' — expected YYYYMMDD",
            "NET_WEIGHT_EXCEEDS_GROSS":  f"Net weight {round(random.uniform(10,100),2)} > gross weight",
        }.get(error_type, "Unknown error")

        ET.SubElement(exc, "AffectedField").text  = error_type.split("_")[0].lower()
        ET.SubElement(exc, "DetectedBy").text     = random.choice(systems)
        ET.SubElement(exc, "DetectedAt").text     = created_dt.isoformat()
        ET.SubElement(exc, "ProcessedBy").text    = random.choice(processors)
        ET.SubElement(exc, "Status").text         = random.choice([
                                                      "OPEN","IN_PROGRESS",
                                                      "RESOLVED","WAIVED"
                                                    ])
        ET.SubElement(exc, "ResolutionNotes").text = random.choice([
            None,
            "Corrected in UDS v2",
            "Business confirmed - acceptable",
            "Duplicate confirmed - flagged for removal",
            "Awaiting business sign-off",
            "",
        ])

    # Pretty print XML
    xml_str = minidom.parseString(
        ET.tostring(root, encoding="unicode")
    ).toprettyxml(indent="  ")

    path = f"{OUTPUT_DIR}/bronze_material_exceptions.xml"
    with open(path, "w") as f:
        f.write(xml_str)

    print(f"  Saved: {path} ({n:,} exception records)\n")
    return n


# ─────────────────────────────────────────────
# FILE 4: PARQUET - material_master_delta
# ─────────────────────────────────────────────

def generate_parquet_delta(n=3000):
    """
    Simulates a delta load — records that changed between
    the initial mock load and final cutover. In real migrations
    the business keeps operating in the legacy system during
    the migration window, and you need to capture all new and
    changed records before go-live.

    Parquet format = modern data engineering standard.
    Shows you understand big data formats and columnar storage.
    """
    print("Generating Parquet delta load: material_master_delta...")

    mat_types   = ["ROH","HALB","FERT","VERP"]
    mat_groups  = [f"MG{str(i).zfill(4)}" for i in range(1,101)]
    base_uoms   = ["KG","EA","LT","M","ST"]
    proc_types  = ["E","F","X"]
    change_types= ["NEW","MODIFIED","DELETED","REACTIVATED"]
    change_fields=[
        "material_description","base_uom","material_group",
        "procurement_type","gross_weight","shelf_life",
        "deletion_flag","industry_sector"
    ]

    rows = []
    cutover_start = datetime(2024, 6, 1)
    cutover_end   = datetime(2024, 6, 15)

    for i in range(n):
        # Mix of brand new materials and modifications to existing ones
        is_new = random.random() < 0.30
        mid    = (f"MAT{str(random.randint(90000,99999)).zfill(6)}"
                  if is_new
                  else random.choice(clean_ids))

        change_type  = "NEW" if is_new else random.choice(change_types[1:])
        changed_field= None if change_type == "NEW" else random.choice(change_fields)
        delta_ts     = cutover_start + timedelta(
                         seconds=random.randint(0, int((cutover_end-cutover_start).total_seconds()))
                       )

        rows.append({
            "delta_record_id":    800000 + i,
            "material_id":        mid,
            "change_type":        change_type,
            "changed_field":      changed_field,
            "old_value":          None if change_type == "NEW"
                                  else str(random.choice(["ROH","KG","MG0001","E",None])),
            "new_value":          random.choice(["HALB","EA","MG0050","F","X",None]),
            "material_type":      random.choice(mat_types),
            "material_group":     None if random.random() < 0.05
                                  else random.choice(mat_groups),
            "base_uom":           random.choice(base_uoms),
            "procurement_type":   random.choice(proc_types),
            "gross_weight":       round(random.uniform(0.1,5000),3),
            "delta_timestamp":    delta_ts.isoformat(),
            "extracted_by":       random.choice(["ETL_JOB","MANUAL","BAPI_EXTRACT"]),
            "load_status":        random.choice(["PENDING","LOADED","FAILED","SKIPPED"]),
            # Delta-specific audit fields
            "source_system":      "LEGACY_ERP_ECC600",
            "target_system":      "SAP_S4HANA_2023",
            "migration_wave":     random.choice(["WAVE1","WAVE2","CUTOVER"]),
            "validated":          random.choice([True, False, None]),
        })

    df = pd.DataFrame(rows)
    path = f"{OUTPUT_DIR}/bronze_material_master_delta.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")

    print(f"  Saved: {path} ({len(df):,} rows)\n")
    return df


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("PrecisionManufacturing Inc. - Multi-Format Data Generation")
    print("=" * 60)

    df_excel   = generate_excel_plant_data(n=5000)
    records_json = generate_json_uom(n=5000)
    n_xml      = generate_xml_exceptions(n=2000)
    df_parquet = generate_parquet_delta(n=3000)

    print("=" * 60)
    print("MULTI-FORMAT SUMMARY")
    print("=" * 60)
    print(f"  Excel  (UDS plant data):     {len(df_excel):>6,} rows  → .xlsx")
    print(f"  JSON   (API UOM extract):    {len(records_json):>6,} records → .json")
    print(f"  XML    (exception log):      {n_xml:>6,} records → .xml")
    print(f"  Parquet (delta load):        {len(df_parquet):>6,} rows  → .parquet")
    print("=" * 60)
    print(f"\nAll files saved to: ./{OUTPUT_DIR}/")
    print("\nYour bronze_data/ folder now contains:")
    print("  bronze_material_master.csv")
    print("  bronze_material_plant_data.csv")
    print("  bronze_material_uom.csv")
    print("  bronze_material_plant_data_UDS.xlsx")
    print("  bronze_material_uom.json")
    print("  bronze_material_exceptions.xml")
    print("  bronze_material_master_delta.parquet")