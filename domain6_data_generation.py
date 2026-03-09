"""
Domain 6: Quality Management Data Generation Script
PrecisionManufacturing Inc. - SAP S/4HANA Migration Project

Generates:
  - quality_info_records  → Excel    (~21,000 rows)
  - inspection_plans      → XML      (~12,600 rows)
  - sampling_procedures   → JSON     (~207 rows)

Total: ~33,807 rows
"""

import pandas as pd
import numpy as np
import random
import json
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime, timedelta
import os

random.seed(42)
np.random.seed(42)

OUTPUT_DIR = "bronze_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────

def random_date(start_year=2015, end_year=2024):
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
        "M":   ["M","m","Meter","METER","Mtr"],
        "G":   ["G","g","Gram","GRAM","GRM"],
    }
    if uom in variations and random.random() < 0.30:
        return random.choice(variations[uom][1:])
    return uom

# Shared reference data
plants             = ["CHI1","HOU2","DET3"]
invalid_plants     = ["PLT1","XXXX","0000","CHI2"]
generic_users      = ["ADMIN","MIGRATE","SYSTEM","admin","migrate"]
real_users         = [f"USR{str(i).zfill(3)}" for i in range(1, 51)]
clean_material_ids = [f"MAT{str(i).zfill(6)}" for i in range(1, 15001)]
active_vendor_ids  = [f"V{str(i).zfill(6)}" for i in range(1, 7501)]
blocked_vendor_ids = [f"V{str(i).zfill(6)}" for i in range(7501, 8001)]

# Valid sampling procedure IDs for FK references
valid_sampling_ids = [f"SP{str(i).zfill(6)}" for i in range(1, 201)]
invalid_sampling_ids = ["SP999999","SPXXXXXX","N/A","TBD","SP000000"]


# ─────────────────────────────────────────────
# TABLE 1: QUALITY INFO RECORDS → EXCEL
# ─────────────────────────────────────────────

def generate_quality_info_records(n=20000):
    print(f"Generating quality_info_records ({n} rows)...")

    insp_controls     = ["01","02","03","04"]
    bad_insp_controls = ["SKIP","Normal","tight","Reduced","NORMAL","skip","03 "]
    cert_types        = ["COC","COA","COS","ISO"]
    bad_cert_types    = ["Certificate","cert","CERT","Conformance","Analysis","coc"]
    statuses          = ["AC","IN","BL"]
    bad_statuses      = ["Active","BLOCK","Inactive","1","0","active","blocked"]
    yesno_valid       = ["Y","N"]
    bad_yesno         = ["Yes","No","TRUE","FALSE","1","0","true"]
    remarks_pool      = [
        None, None, None,
        "Vendor approved for reduced inspection",
        "TIGHTEN INSPECTION — recent failures",
        "Certificate required for all shipments",
        "migrated from legacy QM system",
        "Annual re-evaluation due",
        "Block stock on receipt — pending approval",
        "ok","TBD","??","TO DO",
        "Waived by quality manager 2023",
        "safety critical material",
    ]

    rows = []
    for i in range(1, n + 1):
        qir_id     = f"QIR{str(i).zfill(7)}"
        mid        = random.choice(clean_material_ids)
        created_dt = random_date(2015, 2023)
        changed_dt = random_date(2023, 2024)
        last_insp  = random_date(2022, 2024)
        next_insp  = last_insp + timedelta(days=random.randint(30, 365))

        # Quality score 0-100 with anomalies
        q_score = round(random.uniform(50, 100), 2)
        if random.random() < 0.05: q_score = round(random.uniform(101, 150), 2)
        if random.random() < 0.04: q_score = -abs(round(random.uniform(1, 50), 2))

        # Certificate logic — required but missing type
        cert_req  = random.choice(yesno_valid)
        if cert_req == "Y" and random.random() < 0.08:
            cert_type = None   # required but no type defined
        elif cert_req == "N":
            cert_type = None
        else:
            cert_type = random.choice(bad_cert_types) \
                        if random.random() < 0.08 \
                        else random.choice(cert_types)

        rows.append({
            "QIR Number":           qir_id,
            "Material Number":      mid,

            # 7% reference blocked vendor
            "Vendor":               random.choice(blocked_vendor_ids)
                                    if random.random() < 0.07
                                    else random.choice(active_vendor_ids),

            "Plant":                random.choice(invalid_plants)
                                    if random.random() < 0.03 else
                                    random.choice(plants),

            "Inspection Control":   random.choice(bad_insp_controls)
                                    if random.random() < 0.06
                                    else random.choice(insp_controls),

            "Quality Score":        q_score,

            "Certificate Required": random.choice(bad_yesno)
                                    if random.random() < 0.05
                                    else cert_req,

            "Certificate Type":     cert_type,

            # 4% negative, 3% zero inspection interval
            "Inspection Interval (Days)": (
                -random.randint(1,30) if random.random() < 0.04 else
                0 if random.random() < 0.03 else
                random.randint(7, 365)
            ),

            # 5% future last inspection date
            "Last Inspection Date": messy_date(
                random_date(2025, 2026)
                if random.random() < 0.05 else last_insp
            ),

            # 6% next before last
            "Next Inspection Date": messy_date(
                last_insp - timedelta(days=random.randint(1, 30))
                if random.random() < 0.06 else next_insp
            ),

            "Block Stock":          random.choice(bad_yesno)
                                    if random.random() < 0.05
                                    else random.choice(yesno_valid),

            "Status":               random.choice(bad_statuses)
                                    if random.random() < 0.05
                                    else random.choice(statuses),

            "Created By":           random.choice(generic_users)
                                    if random.random() < 0.15
                                    else random.choice(real_users),

            "Created Date":         messy_date(created_dt),

            "Changed Date":         messy_date(changed_dt)
                                    if random.random() > 0.05
                                    else messy_date(
                                        created_dt - timedelta(
                                            days=random.randint(1, 30)
                                        )
                                    ),

            "Remarks":              random.choice(remarks_pool),
        })

    df = pd.DataFrame(rows)

    # Add 5% duplicates
    n_dups = int(len(df) * 0.05)
    dups   = df.sample(n=n_dups, replace=True).copy()
    dups["QIR Number"] = [
        f"QIR-{str(random.randint(1,9999999)).zfill(7)}"
        for _ in range(n_dups)
    ]
    df = pd.concat([df, dups], ignore_index=True)

    path = f"{OUTPUT_DIR}/bronze_quality_info_records.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Quality Info Records", index=False)

        instructions = pd.DataFrame({
            "Field":       ["Inspection Control","Quality Score",
                            "Certificate Type","Status"],
            "Required":    ["Yes","Yes","Conditional","Yes"],
            "Format":      ["2 char","Decimal 0-100","3 char","2 char"],
            "Description": [
                "01=Skip, 02=Reduced, 03=Normal, 04=Tightened",
                "Vendor quality rating — must be between 0 and 100",
                "Required when Certificate Required = Y: COC, COA, COS, ISO",
                "AC=Active, IN=Inactive, BL=Blocked"
            ]
        })
        instructions.to_excel(writer, sheet_name="Instructions", index=False)

        valid_vals = pd.DataFrame({
            "Field":   ["Inspection Control","Inspection Control",
                        "Inspection Control","Inspection Control",
                        "Status","Status","Status"],
            "Value":   ["01","02","03","04","AC","IN","BL"],
            "Meaning": ["Skip Lot","Reduced Inspection",
                        "Normal Inspection","Tightened Inspection",
                        "Active","Inactive","Blocked"]
        })
        valid_vals.to_excel(writer, sheet_name="Valid Values", index=False)

    print(f"  Saved: {path} ({len(df):,} rows, 3 sheets)\n")
    return df


# ─────────────────────────────────────────────
# TABLE 2: INSPECTION PLANS → XML
# ─────────────────────────────────────────────

def generate_inspection_plans(n=12000):
    print(f"Generating inspection_plans ({n} rows)...")

    insp_types      = ["01","04","06","08","09","10","11","13","14","15"]
    bad_insp_types  = ["GoodsReceipt","PROD","del","GR","production",
                       "Delivery","gr","01 ","in-process"]
    usages          = ["5","9","1","2"]
    bad_usages      = ["General","Delivery","general","GENERAL","5 "]
    plan_statuses   = ["4","1","2","3"]
    bad_statuses    = ["Active","PREP","A","Approved","prep","active"]
    yesno_valid     = ["Y","N"]
    bad_yesno       = ["Yes","No","TRUE","FALSE","1","0"]
    sample_uoms     = ["KG","EA","G","LT","M"]

    root = ET.Element("InspectionPlans")
    root.set("version", "1.0")
    root.set("extractedAt", datetime.now().isoformat())
    root.set("system", "SAP_QM_ECC600")
    root.set("totalRecords", str(n))

    for i in range(1, n + 1):
        plan_id    = f"QPLAN{str(i).zfill(6)}"
        mid        = random.choice(clean_material_ids)
        plant      = random.choice(plants)
        created_dt = random_date(2015, 2023)

        # Sample size
        sample_sz  = round(random.uniform(1, 500), 3)
        if random.random() < 0.05: sample_sz = -abs(sample_sz)
        if random.random() < 0.04: sample_sz = 0

        # Tolerance logic
        lower_tol  = round(random.uniform(0, 50), 3)
        upper_tol  = round(lower_tol + random.uniform(1, 50), 3)
        target_val = round(random.uniform(lower_tol, upper_tol), 3)

        # 4% lower > upper tolerance
        if random.random() < 0.04:
            lower_tol, upper_tol = upper_tol, lower_tol

        # 5% target outside tolerance
        if random.random() < 0.05:
            target_val = upper_tol + round(random.uniform(1, 20), 3)

        # 6% null lower tolerance for critical characteristics
        lower_display = None if random.random() < 0.06 else str(lower_tol)

        # Characteristic count
        char_count = random.randint(1, 20)
        if random.random() < 0.03: char_count = 0
        if random.random() < 0.04: char_count = -random.randint(1, 5)

        # Acceptance/rejection numbers
        accept_num = random.randint(0, 5)
        reject_num = accept_num + random.randint(1, 5)
        # 3% rejection < acceptance
        if random.random() < 0.03:
            reject_num = max(0, accept_num - random.randint(1, 3))

        plan = ET.SubElement(root, "InspectionPlan")
        plan.set("id", str(500000 + i))

        ET.SubElement(plan, "PlanId").text             = plan_id
        ET.SubElement(plan, "PlanDescription").text    = (
            None if random.random() < 0.08 else
            random.choice([
                f"Inspection Plan {i} - {plant}",
                f"INSPECTION PLAN {i}",
                f"  Inspection Plan {i}  ",
                f"Prüfplan {i}",        # German
                f"Plan de inspección {i}",  # Spanish
            ])
        )

        # 6% orphaned material
        ET.SubElement(plan, "MaterialId").text         = (
            f"GHOST{str(random.randint(1,999)).zfill(5)}"
            if random.random() < 0.06 else mid
        )
        ET.SubElement(plan, "PlantId").text            = (
            random.choice(invalid_plants)
            if random.random() < 0.03 else plant
        )
        ET.SubElement(plan, "InspectionType").text     = (
            random.choice(bad_insp_types)
            if random.random() < 0.06
            else random.choice(insp_types)
        )
        ET.SubElement(plan, "Usage").text              = (
            random.choice(bad_usages)
            if random.random() < 0.05
            else random.choice(usages)
        )
        ET.SubElement(plan, "Status").text             = (
            random.choice(bad_statuses)
            if random.random() < 0.05
            else random.choice(plan_statuses)
        )
        ET.SubElement(plan, "CharacteristicCount").text = str(char_count)

        # 8% orphaned sampling procedure
        ET.SubElement(plan, "SamplingProcedure").text  = (
            random.choice(invalid_sampling_ids)
            if random.random() < 0.08
            else random.choice(valid_sampling_ids)
        )
        ET.SubElement(plan, "SampleSize").text         = str(sample_sz)
        ET.SubElement(plan, "SampleUnit").text         = messy_uom(
            random.choice(sample_uoms)
        )
        ET.SubElement(plan, "AcceptanceNumber").text   = str(accept_num)
        ET.SubElement(plan, "RejectionNumber").text    = str(reject_num)
        ET.SubElement(plan, "LowerTolerance").text     = (
            str(lower_display) if lower_display else ""
        )
        ET.SubElement(plan, "UpperTolerance").text     = str(upper_tol)
        ET.SubElement(plan, "TargetValue").text        = str(target_val)
        ET.SubElement(plan, "DestructiveTesting").text = (
            random.choice(bad_yesno)
            if random.random() < 0.05
            else random.choice(yesno_valid)
        )
        ET.SubElement(plan, "CreatedBy").text          = (
            random.choice(generic_users)
            if random.random() < 0.15
            else random.choice(real_users)
        )
        ET.SubElement(plan, "CreatedDate").text        = messy_date(created_dt)

    # Add 600 orphaned records
    for i in range(600):
        plan = ET.SubElement(root, "InspectionPlan")
        plan.set("id", str(600000 + i))
        ET.SubElement(plan, "PlanId").text      = f"QPLAN-ORPHAN-{str(i).zfill(5)}"
        ET.SubElement(plan, "MaterialId").text  = f"GHOST{str(i).zfill(5)}"
        ET.SubElement(plan, "PlantId").text     = None
        ET.SubElement(plan, "Status").text      = "1"
        ET.SubElement(plan, "CreatedBy").text   = "MIGRATE"
        ET.SubElement(plan, "CreatedDate").text = messy_date(
            random_date(2010, 2015)
        )

    xml_str = minidom.parseString(
        ET.tostring(root, encoding="unicode")
    ).toprettyxml(indent="  ")

    path = f"{OUTPUT_DIR}/bronze_inspection_plans.xml"
    with open(path, "w") as f:
        f.write(xml_str)

    total = n + 600
    print(f"  Saved: {path} ({total:,} records)\n")
    return total


# ─────────────────────────────────────────────
# TABLE 3: SAMPLING PROCEDURES → JSON
# ─────────────────────────────────────────────

def generate_sampling_procedures(n=200):
    print(f"Generating sampling_procedures ({n} rows)...")

    sampling_types     = ["FX","PC","SK"]
    bad_sampling_types = ["Fixed","PCT","skip","FIXED","percent","Skiplog","fx"]
    valuation_modes    = ["01","02"]
    bad_val_modes      = ["Attributive","Variable","ATTR","VAR","attr","1","2"]
    yesno_valid        = ["Y","N"]
    bad_yesno          = ["Yes","No","TRUE","FALSE","1","0"]

    records = []
    used    = set()

    for i in range(1, n + 1):
        proc_id    = f"SP{str(i).zfill(6)}"
        samp_type  = random.choice(sampling_types)
        created_dt = random_date(2015, 2022)

        lot_from   = round(random.uniform(1, 1000), 3)
        lot_to     = round(lot_from * random.uniform(2, 10), 3)
        min_sample = round(random.uniform(1, 50), 3)
        max_sample = round(min_sample * random.uniform(2, 10), 3)

        # Fixed sample size — null 5% when type=FX
        fixed_size = None if (samp_type == "FX" and random.random() < 0.05) \
                     else round(random.uniform(1, 100), 3)
        if fixed_size and random.random() < 0.04:
            fixed_size = -abs(fixed_size)

        # Percentage — null 4% when type=PC, > 100 in 5%
        pct = None if (samp_type == "PC" and random.random() < 0.04) \
              else round(random.uniform(1, 100), 2)
        if pct and random.random() < 0.05:
            pct = round(random.uniform(101, 200), 2)

        record = {
            "procedureId":         proc_id,
            "procedureDescription":None if random.random() < 0.05 else (
                                   random.choice([
                                       f"Sampling Procedure {i}",
                                       f"SAMPLING PROCEDURE {i}",
                                       f"  Sampling Procedure {i}  ",
                                       f"Stichprobenverfahren {i}",  # German
                                   ])),
            "samplingType":        random.choice(bad_sampling_types)
                                   if random.random() < 0.05
                                   else samp_type,
            "sampleSizeFixed":     fixed_size,
            "samplePercentage":    pct,

            # 4% negative min sample
            "minSampleSize":       -abs(min_sample)
                                   if random.random() < 0.04
                                   else min_sample,

            # 3% max < min
            "maxSampleSize":       round(min_sample * 0.5, 3)
                                   if random.random() < 0.03
                                   else max_sample,

            # 4% negative lot size from
            "lotSizeFrom":         -abs(lot_from)
                                   if random.random() < 0.04
                                   else lot_from,

            # 3% lot to < lot from
            "lotSizeTo":           round(lot_from * 0.5, 3)
                                   if random.random() < 0.03
                                   else lot_to,

            "valuationMode":       random.choice(bad_val_modes)
                                   if random.random() < 0.05
                                   else random.choice(valuation_modes),

            "dynamicModification": random.choice(bad_yesno)
                                   if random.random() < 0.05
                                   else random.choice(yesno_valid),

            "createdBy":           random.choice(generic_users)
                                   if random.random() < 0.10
                                   else random.choice(real_users),

            "createdDate":         messy_date(created_dt),

            "auditInfo": {
                "source":      random.choice(["QM_SYSTEM","MANUAL","MIGRATE"]),
                "extractedAt": datetime.now().isoformat(),
            }
        }

        # 3% duplicate with case variation
        records.append(record)
        if random.random() < 0.03:
            dup = record.copy()
            dup["procedureId"] = proc_id.lower()
            records.append(dup)

    path = f"{OUTPUT_DIR}/bronze_sampling_procedures.json"
    with open(path, "w") as f:
        json.dump({
            "apiVersion":  "1.0.0",
            "extractedAt": datetime.now().isoformat(),
            "source":      "SAP_QM_LEGACY",
            "recordCount": len(records),
            "data":        records
        }, f, indent=2)

    print(f"  Saved: {path} ({len(records):,} records)\n")
    return records


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("PrecisionManufacturing Inc. - Domain 6 Data Generation")
    print("=" * 60)

    df_qir   = generate_quality_info_records(n=20000)
    n_plans  = generate_inspection_plans(n=12000)
    sp_recs  = generate_sampling_procedures(n=200)

    print("=" * 60)
    print("DOMAIN 6 GENERATION SUMMARY")
    print("=" * 60)
    print(f"  quality_info_records (Excel): {len(df_qir):>8,} rows")
    print(f"  inspection_plans (XML):       {n_plans:>8,} records")
    print(f"  sampling_procedures (JSON):   {len(sp_recs):>8,} records")
    total = len(df_qir) + n_plans + len(sp_recs)
    print(f"  TOTAL:                        {total:>8,} rows")
    print("=" * 60)
    print(f"\nFiles saved to: ./{OUTPUT_DIR}/")
    print("  bronze_quality_info_records.xlsx")
    print("  bronze_inspection_plans.xml")
    print("  bronze_sampling_procedures.json")
    print("\nCumulative bronze_data/ totals:")
    print("  Domain 1:  ~101,700 rows  (7 files)")
    print("  Domain 2:  ~ 49,870 rows  (4 files)")
    print("  Domain 3:  ~ 38,655 rows  (3 files)")
    print("  Domain 4:  ~ 21,625 rows  (3 files)")
    print("  Domain 5:  ~ 94,543 rows  (4 files)")
    print("  Domain 6:  ~ 33,807 rows  (3 files)")
    print("  RUNNING TOTAL: ~340,200 rows across 24 files")