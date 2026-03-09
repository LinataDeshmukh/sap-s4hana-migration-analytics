"""
Domain 7: Finance / Costing Data Generation Script
PrecisionManufacturing Inc. - SAP S/4HANA Migration Project

Generates:
  - cost_centers      → CSV       (~158 rows)
  - profit_centers    → JSON      (~53 rows)
  - gl_accounts       → Excel     (~525 rows)
  - material_costing  → Parquet   (~37,100 rows)

Total: ~37,836 rows
"""

import pandas as pd
import numpy as np
import random
import json
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
plants              = ["CHI1","HOU2","DET3"]
invalid_plants      = ["PLT1","XXXX","0000","CHI2"]
generic_users       = ["ADMIN","MIGRATE","SYSTEM","admin","migrate"]
real_users          = [f"USR{str(i).zfill(3)}" for i in range(1, 51)]
clean_material_ids  = [f"MAT{str(i).zfill(6)}" for i in range(1, 15001)]
company_codes       = ["1000","2000","3000"]
invalid_cc_codes    = ["XXXX","9999","0000","CC01"]
controlling_areas   = ["A000","A001","A002"]
invalid_ctrl_areas  = ["XXXX","0000","CTRL","CA99"]
currencies          = ["USD","EUR","GBP"]
bad_currencies      = ["Dollars","DOLLAR","$","Euro","EURO"]
valid_profit_centers= [f"PC{str(i).zfill(4)}" for i in range(1, 51)]
invalid_profit_ctrs = ["PC9999","PCXXXX","N/A","TBD","PC0000"]
valid_gl_accounts   = [f"GL{str(i).zfill(8)}" for i in range(1, 501)]
invalid_gl_accounts = ["GL99999999","GLXXXXXXXX","N/A","TBD"]
valid_val_classes   = [f"VC{str(i).zfill(4)}" for i in range(3000, 3050)]
yesno_valid         = ["Y","N"]
bad_yesno           = ["Yes","No","TRUE","FALSE","1","0","true"]


# ─────────────────────────────────────────────
# TABLE 1: COST CENTERS → CSV
# ─────────────────────────────────────────────

def generate_cost_centers(n=150):
    print(f"Generating cost_centers ({n} rows)...")

    cc_types     = ["E","F","H","V","A","P"]
    bad_cc_types = ["Production","Admin","HR","Sales","admin","e","f"]
    departments  = [
        "Production","PRODUCTION","production","Prod",
        "Finance","FINANCE","finance","Fin",
        "Maintenance","MAINTENANCE","maint","Maint",
        "Logistics","LOGISTICS","log","Log",
        "Quality","QUALITY","QM","qm",
        "HR","Human Resources","HR Dept",
        "IT","Information Technology","IT Dept",
    ]
    first_names  = ["James","Sarah","Michael","Emily","Robert",
                    "Jennifer","David","Lisa","John","Mary"]
    last_names   = ["Smith","Johnson","Williams","Brown","Jones",
                    "Garcia","Miller","Davis","Wilson","Taylor"]

    # Cost center structure: 50 per plant
    cc_configs = []
    cc_num = 1
    for plant in plants:
        for dept in ["Production","Finance","Maintenance",
                     "Logistics","Quality","HR","IT",
                     "Procurement","Engineering","Operations"]:
            for sub in range(1, 6):  # 5 cost centers per dept per plant
                cc_configs.append((
                    f"CC{str(cc_num).zfill(6)}",
                    plant,
                    dept,
                ))
                cc_num += 1
                if len(cc_configs) >= n:
                    break
            if len(cc_configs) >= n:
                break
        if len(cc_configs) >= n:
            break

    rows = []
    for cc_id, plant, dept in cc_configs:
        created_dt = random_date(2010, 2022)
        changed_dt = random_date(2022, 2024)
        valid_from = random_date(2010, 2020)
        valid_to   = random_date(2025, 2030)
        fname      = random.choice(first_names)
        lname      = random.choice(last_names)

        rows.append({
            "cost_center_id":          cc_id,
            "cost_center_description": None if random.random() < 0.08 else (
                                       random.choice([
                                           f"{dept} - {plant}",
                                           f"{dept.upper()} - {plant}",
                                           f"  {dept} - {plant}  ",
                                           f"{dept} Abteilung {plant}",  # German
                                       ])),
            "company_code":            random.choice(invalid_cc_codes)
                                       if random.random() < 0.05
                                       else random.choice(company_codes),
            "controlling_area":        random.choice(invalid_ctrl_areas)
                                       if random.random() < 0.06
                                       else random.choice(controlling_areas),
            "cost_center_type":        random.choice(bad_cc_types)
                                       if random.random() < 0.05
                                       else random.choice(cc_types),
            "responsible_person":      None if random.random() < 0.15 else (
                                       random.choice([
                                           f"{fname} {lname}",
                                           f"{fname.upper()} {lname.upper()}",
                                           f"{lname}, {fname}",
                                           f"{fname[0]}. {lname}",
                                           f"{fname}.{lname}",
                                       ])),
            "department":              None if random.random() < 0.08
                                       else random.choice(departments),
            "plant_id":                random.choice(invalid_plants)
                                       if random.random() < 0.03 else plant,

            # 7% null, 4% invalid profit center
            "profit_center":           None if random.random() < 0.07 else (
                                       random.choice(invalid_profit_ctrs)
                                       if random.random() < 0.04
                                       else random.choice(valid_profit_centers)),

            "currency":                None if random.random() < 0.04 else (
                                       random.choice(bad_currencies)
                                       if random.random() < 0.05
                                       else random.choice(currencies)),

            "valid_from":              None if random.random() < 0.06
                                       else messy_date(valid_from),

            # 5% valid_to before valid_from
            "valid_to":                messy_date(
                                           valid_from - timedelta(
                                               days=random.randint(1, 30)
                                           ) if random.random() < 0.05
                                           else valid_to),

            "active_flag":             random.choice(bad_yesno)
                                       if random.random() < 0.05
                                       else random.choice(yesno_valid),
            "created_by":              random.choice(generic_users)
                                       if random.random() < 0.15
                                       else random.choice(real_users),
            "created_date":            messy_date(created_dt),
            "changed_date":            messy_date(changed_dt)
                                       if random.random() > 0.05
                                       else messy_date(
                                           created_dt - timedelta(
                                               days=random.randint(1, 30)
                                           )),
        })

    df = pd.DataFrame(rows)

    # Add 5% duplicates
    n_dups = int(len(df) * 0.05)
    dups   = df.sample(n=n_dups, replace=True).copy()
    dups["cost_center_id"] = dups["cost_center_id"].apply(
        lambda c: c.lower() if random.random() < 0.5
        else c.replace("CC","CC-")
    )
    df = pd.concat([df, dups], ignore_index=True)

    path = f"{OUTPUT_DIR}/bronze_cost_centers.csv"
    df.to_csv(path, index=False)
    print(f"  Saved: {path} ({len(df):,} rows)\n")
    return df


# ─────────────────────────────────────────────
# TABLE 2: PROFIT CENTERS → JSON
# ─────────────────────────────────────────────

def generate_profit_centers(n=50):
    print(f"Generating profit_centers ({n} rows)...")

    pc_types     = ["01","02","03"]
    bad_pc_types = ["Product","SVC","region","PRODUCT","service","01 "]
    segments     = ["SEG001","SEG002","SEG003","SEG004","SEG005"]
    first_names  = ["James","Sarah","Michael","Emily","Robert","Jennifer"]
    last_names   = ["Smith","Johnson","Williams","Brown","Jones","Garcia"]

    pc_configs = [
        ("PC0001","Industrial Equipment Line A","01","SEG001"),
        ("PC0002","Industrial Equipment Line B","01","SEG001"),
        ("PC0003","Industrial Equipment Line C","01","SEG002"),
        ("PC0004","Component Manufacturing","01","SEG002"),
        ("PC0005","Spare Parts Division","01","SEG003"),
        ("PC0006","Maintenance Services","02","SEG003"),
        ("PC0007","Engineering Services","02","SEG004"),
        ("PC0008","Logistics Services","02","SEG004"),
        ("PC0009","Chicago Region","03","SEG005"),
        ("PC0010","Houston Region","03","SEG005"),
        ("PC0011","Detroit Region","03","SEG005"),
    ]

    # Fill remaining with generated records
    for i in range(12, n + 1):
        pc_configs.append((
            f"PC{str(i).zfill(4)}",
            f"Profit Center {i}",
            random.choice(pc_types),
            random.choice(segments),
        ))

    records = []
    now     = datetime.now()

    for pc_id, pc_desc, pc_type, segment in pc_configs[:n]:
        created_dt = random_date(2010, 2022)
        valid_from = random_date(2010, 2020)
        valid_to   = random_date(2023, 2030)
        fname      = random.choice(first_names)
        lname      = random.choice(last_names)

        # 8% expired profit centers still active
        is_expired = random.random() < 0.08
        if is_expired:
            valid_to = random_date(2020, 2023)  # past date
            active   = "Y"                       # but still flagged active
        else:
            active = random.choice(bad_yesno) \
                     if random.random() < 0.05 \
                     else random.choice(yesno_valid)

        record = {
            "profitCenterId":          pc_id,
            "profitCenterDescription": None if random.random() < 0.08 else (
                                       random.choice([
                                           pc_desc,
                                           pc_desc.upper(),
                                           f"  {pc_desc}  ",
                                           f"{pc_desc} GmbH",  # German suffix
                                       ])),
            "companyCode":             random.choice(invalid_cc_codes)
                                       if random.random() < 0.05
                                       else random.choice(company_codes),
            "controllingArea":         random.choice(invalid_ctrl_areas)
                                       if random.random() < 0.05
                                       else random.choice(controlling_areas),
            "profitCenterType":        random.choice(bad_pc_types)
                                       if random.random() < 0.05
                                       else pc_type,
            "responsiblePerson":       None if random.random() < 0.15 else (
                                       random.choice([
                                           f"{fname} {lname}",
                                           f"{fname.upper()} {lname.upper()}",
                                           f"{lname}, {fname}",
                                           f"{fname[0]}. {lname}",
                                       ])),
            "segment":                 None if random.random() < 0.08
                                       else segment,
            "currency":                None if random.random() < 0.04 else (
                                       random.choice(bad_currencies)
                                       if random.random() < 0.05
                                       else random.choice(currencies)),
            "validFrom":               None if random.random() < 0.06
                                       else messy_date(valid_from),
            "validTo":                 messy_date(
                                           valid_from - timedelta(
                                               days=random.randint(1, 30)
                                           ) if random.random() < 0.05
                                           else valid_to),
            "activeFlag":              active,
            "isExpiredButActive":      is_expired,  # flag for validation
            "createdBy":               random.choice(generic_users)
                                       if random.random() < 0.15
                                       else random.choice(real_users),
            "createdDate":             messy_date(created_dt),
            "auditInfo": {
                "source":      random.choice(["FI_SYSTEM","MANUAL","MIGRATE"]),
                "extractedAt": datetime.now().isoformat(),
            }
        }

        # 4% duplicate with case variation
        records.append(record)
        if random.random() < 0.04:
            dup = record.copy()
            dup["profitCenterId"] = pc_id.lower()
            records.append(dup)

    path = f"{OUTPUT_DIR}/bronze_profit_centers.json"
    with open(path, "w") as f:
        json.dump({
            "apiVersion":  "1.0.0",
            "extractedAt": datetime.now().isoformat(),
            "source":      "SAP_FI_LEGACY",
            "recordCount": len(records),
            "data":        records
        }, f, indent=2)

    print(f"  Saved: {path} ({len(records):,} records)\n")
    return records


# ─────────────────────────────────────────────
# TABLE 3: GL ACCOUNTS → EXCEL
# ─────────────────────────────────────────────

def generate_gl_accounts(n=500):
    print(f"Generating gl_accounts ({n} rows)...")

    acct_types       = ["S","P"]
    bad_acct_types   = ["Balance","PNL","profit","sheet","P&L","s","p"]
    acct_groups      = ["RAWA","MATS","PROD","OVHD","LABR",
                        "ADMN","SELL","CASH","RECV","PAYB"]
    bad_acct_groups  = ["RAW","MAT","PROD ","OVH","ADM","XXXX"]
    tax_cats         = ["1","2","3","4","V","A"]
    sort_keys        = ["001","002","003","004","005","010","011","012"]
    fsg_groups       = ["G001","G002","G003","G004","G005"]
    bad_fsg_groups   = ["GXXX","G999","N/A","TBD"]
    remarks_pool     = [
        None, None, None,
        "Primary cost element",
        "DO NOT POST MANUALLY",
        "Reconciliation account — system posts only",
        "migrated from legacy FI",
        "Review with finance team",
        "Tax relevant","ok","TBD","??",
        "Blocked for manual posting",
        "Annual audit required",
    ]

    # GL account structure
    gl_configs = [
        # Balance sheet accounts (S)
        *[(f"GL{str(i).zfill(8)}", f"Raw Material Inventory {i}", "S", "RAWA")
          for i in range(1, 51)],
        *[(f"GL{str(i).zfill(8)}", f"Work in Progress {i}", "S", "MATS")
          for i in range(51, 101)],
        *[(f"GL{str(i).zfill(8)}", f"Finished Goods Inventory {i}", "S", "PROD")
          for i in range(101, 151)],
        # P&L accounts (P)
        *[(f"GL{str(i).zfill(8)}", f"Raw Material Consumption {i}", "P", "RAWA")
          for i in range(151, 201)],
        *[(f"GL{str(i).zfill(8)}", f"Labor Cost {i}", "P", "LABR")
          for i in range(201, 251)],
        *[(f"GL{str(i).zfill(8)}", f"Overhead Cost {i}", "P", "OVHD")
          for i in range(251, 301)],
        *[(f"GL{str(i).zfill(8)}", f"Sales Revenue {i}", "P", "SELL")
          for i in range(301, 351)],
        *[(f"GL{str(i).zfill(8)}", f"Admin Expense {i}", "P", "ADMN")
          for i in range(351, 401)],
        *[(f"GL{str(i).zfill(8)}", f"Accounts Receivable {i}", "S", "RECV")
          for i in range(401, 451)],
        *[(f"GL{str(i).zfill(8)}", f"Accounts Payable {i}", "S", "PAYB")
          for i in range(451, 501)],
    ]

    rows = []
    for gl_id, gl_desc, acct_type, acct_group in gl_configs[:n]:
        created_dt = random_date(2010, 2022)
        changed_dt = random_date(2022, 2024)

        rows.append({
            "GL Account":          gl_id,
            "Description":         None if random.random() < 0.08 else (
                                   random.choice([
                                       gl_desc,
                                       gl_desc.upper(),
                                       f"  {gl_desc}  ",
                                       f"{gl_desc} Konto",  # German
                                   ])),
            "Company Code":        random.choice(invalid_cc_codes)
                                   if random.random() < 0.05
                                   else random.choice(company_codes),
            "Account Type":        random.choice(bad_acct_types)
                                   if random.random() < 0.05
                                   else acct_type,
            "Account Group":       random.choice(bad_acct_groups)
                                   if random.random() < 0.06
                                   else acct_group,
            "FS Item":             None if random.random() < 0.08
                                   else f"FSI{str(random.randint(1,999)).zfill(4)}",
            "Currency":            None if random.random() < 0.04 else (
                                   random.choice(bad_currencies)
                                   if random.random() < 0.05
                                   else random.choice(currencies)),
            "Tax Category":        None if random.random() < 0.08
                                   else random.choice(tax_cats),
            "Reconciliation Acct": random.choice(bad_yesno)
                                   if random.random() < 0.05
                                   else random.choice(yesno_valid),
            "Line Item Display":   random.choice(bad_yesno)
                                   if random.random() < 0.05
                                   else random.choice(yesno_valid),
            "Sort Key":            None if random.random() < 0.07
                                   else random.choice(sort_keys),
            "Field Status Group":  None if random.random() < 0.06 else (
                                   random.choice(bad_fsg_groups)
                                   if random.random() < 0.06
                                   else random.choice(fsg_groups)),
            "Posting Block":       random.choice(bad_yesno)
                                   if random.random() < 0.05
                                   else random.choice(yesno_valid),
            "Deletion Flag":       "X" if random.random() < 0.04 else None,
            "Created By":          random.choice(generic_users)
                                   if random.random() < 0.15
                                   else random.choice(real_users),
            "Created Date":        messy_date(created_dt),
            "Changed Date":        messy_date(changed_dt)
                                   if random.random() > 0.05
                                   else messy_date(
                                       created_dt - timedelta(
                                           days=random.randint(1, 30)
                                       )),
            "Remarks":             random.choice(remarks_pool),
        })

    df = pd.DataFrame(rows)

    # Add 5% duplicates
    n_dups = int(len(df) * 0.05)
    dups   = df.sample(n=n_dups, replace=True).copy()
    dups["GL Account"] = dups["GL Account"].apply(
        lambda g: g.lower() if random.random() < 0.5
        else g.replace("GL","GL-")
    )
    df = pd.concat([df, dups], ignore_index=True)

    path = f"{OUTPUT_DIR}/bronze_gl_accounts.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="GL Accounts", index=False)

        instructions = pd.DataFrame({
            "Field":       ["Account Type","Account Group",
                            "Reconciliation Acct","Posting Block"],
            "Required":    ["Yes","Yes","Yes","Yes"],
            "Format":      ["1 char","4 char","1 char","1 char"],
            "Description": [
                "S=Balance Sheet, P=Profit and Loss",
                "4-char group code: RAWA, MATS, PROD, OVHD, LABR, ADMN, SELL",
                "Y=Reconciliation account (no manual posting), N=normal",
                "Y=Blocked for posting, N=Open for posting"
            ]
        })
        instructions.to_excel(writer, sheet_name="Instructions", index=False)

        valid_vals = pd.DataFrame({
            "Field":   ["Account Type","Account Type",
                        "Account Group","Account Group",
                        "Account Group","Account Group"],
            "Value":   ["S","P","RAWA","MATS","LABR","OVHD"],
            "Meaning": ["Balance Sheet Account","Profit and Loss Account",
                        "Raw Material Accounts","Material/Stock Accounts",
                        "Labor Cost Accounts","Overhead Cost Accounts"]
        })
        valid_vals.to_excel(writer, sheet_name="Valid Values", index=False)

    print(f"  Saved: {path} ({len(df):,} rows, 3 sheets)\n")
    return df


# ─────────────────────────────────────────────
# TABLE 4: MATERIAL COSTING → PARQUET
# ─────────────────────────────────────────────

def generate_material_costing(n=35000):
    print(f"Generating material_costing ({n} rows)...")

    costing_variants  = ["PPC1","PPC2","PPC3","PPC4"]
    bad_cost_variants = ["STD","ACT","ppc1","Standard","Actual","PPC1 "]
    base_uoms         = ["KG","EA","LT","M","G","ST"]
    price_controls    = ["S","V"]
    bad_price_ctrls   = ["STD","AVG","Standard","Moving","s","v"]

    rows        = []
    costing_id  = 1

    for i in range(n):
        mid        = random.choice(clean_material_ids)
        plant      = random.choice(plants)
        created_dt = random_date(2018, 2023)
        changed_dt = random_date(2023, 2024)
        valid_from = random_date(2020, 2023)
        valid_to   = random_date(2023, 2026)

        # Cost components
        mat_cost  = round(random.uniform(10, 5000), 2)
        lab_cost  = round(random.uniform(5,  2000), 2)
        ovh_cost  = round(random.uniform(2,  1000), 2)
        true_total= round(mat_cost + lab_cost + ovh_cost, 2)

        # 5% total doesn't match sum of components
        if random.random() < 0.05:
            std_cost  = round(true_total * random.uniform(0.7, 1.3), 2)
        else:
            std_cost  = true_total

        # 8% null standard cost
        if random.random() < 0.08: std_cost  = None
        # 5% negative standard cost
        if std_cost and random.random() < 0.05:
            std_cost = -abs(std_cost)
        # 3% zero standard cost
        if std_cost and random.random() < 0.03:
            std_cost = 0

        # Component nulls and negatives
        if random.random() < 0.06: mat_cost = None
        elif mat_cost and random.random() < 0.04: mat_cost = -abs(mat_cost)

        if random.random() < 0.06: lab_cost = None
        elif lab_cost and random.random() < 0.04: lab_cost = -abs(lab_cost)

        if random.random() < 0.06: ovh_cost = None
        elif ovh_cost and random.random() < 0.04: ovh_cost = -abs(ovh_cost)

        # Base quantity
        base_qty = round(random.uniform(1, 1000), 3)
        if random.random() < 0.04: base_qty = -abs(base_qty)
        if random.random() < 0.03: base_qty = 0

        rows.append({
            "costing_id":       costing_id,

            # 6% orphaned material
            "material_id":      f"GHOST{str(random.randint(1,999)).zfill(5)}"
                                if random.random() < 0.06 else mid,

            "plant_id":         random.choice(invalid_plants)
                                if random.random() < 0.03 else plant,

            "costing_variant":  random.choice(bad_cost_variants)
                                if random.random() < 0.06
                                else random.choice(costing_variants),

            # 5% zero, 3% negative version
            "costing_version":  0 if random.random() < 0.05 else (
                                -random.randint(1,5)
                                if random.random() < 0.03
                                else random.randint(1, 10)),

            "base_quantity":    base_qty,
            "base_uom":         messy_uom(random.choice(base_uoms)),
            "standard_cost":    std_cost,
            "material_cost":    mat_cost,
            "labor_cost":       lab_cost,
            "overhead_cost":    ovh_cost,

            # Total cost — sometimes doesn't match components
            "total_cost":       std_cost,

            "currency":         None if random.random() < 0.04 else (
                                random.choice(bad_currencies)
                                if random.random() < 0.05
                                else random.choice(currencies)),

            # 7% orphaned GL account
            "cost_element":     random.choice(invalid_gl_accounts)
                                if random.random() < 0.07
                                else random.choice(valid_gl_accounts),

            # 6% invalid profit center
            "profit_center":    random.choice(invalid_profit_ctrs)
                                if random.random() < 0.06
                                else random.choice(valid_profit_centers),

            # 8% null valuation class
            "valuation_class":  None if random.random() < 0.08
                                else random.choice(valid_val_classes),

            # 5% invalid price control
            "price_control":    random.choice(bad_price_ctrls)
                                if random.random() < 0.05
                                else random.choice(price_controls),

            # 6% null valid_from
            "valid_from":       None if random.random() < 0.06
                                else messy_date(valid_from),

            # 5% valid_to before valid_from
            "valid_to":         messy_date(
                                    valid_from - timedelta(
                                        days=random.randint(1, 30)
                                    ) if random.random() < 0.05
                                    else valid_to),

            "created_by":       random.choice(generic_users)
                                if random.random() < 0.15
                                else random.choice(real_users),
            "created_date":     messy_date(created_dt),
            "changed_date":     messy_date(changed_dt)
                                if random.random() > 0.05
                                else messy_date(
                                    created_dt - timedelta(
                                        days=random.randint(1, 30)
                                    )),
        })
        costing_id += 1

    df = pd.DataFrame(rows)

    # Add 6% orphaned records
    n_orph = int(len(df) * 0.06)
    orph   = df.sample(n=n_orph).copy()
    orph["material_id"]  = [
        f"GHOST{str(i).zfill(5)}" for i in range(70000, 70000 + n_orph)
    ]
    orph["costing_id"]   = range(costing_id, costing_id + n_orph)
    df = pd.concat([df, orph], ignore_index=True)

    path = f"{OUTPUT_DIR}/bronze_material_costing.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    print(f"  Saved: {path} ({len(df):,} rows)\n")
    return df


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("PrecisionManufacturing Inc. - Domain 7 Data Generation")
    print("=" * 60)

    df_cc    = generate_cost_centers(n=150)
    pc_recs  = generate_profit_centers(n=50)
    df_gl    = generate_gl_accounts(n=500)
    df_cost  = generate_material_costing(n=35000)

    print("=" * 60)
    print("DOMAIN 7 GENERATION SUMMARY")
    print("=" * 60)
    print(f"  cost_centers (CSV):          {len(df_cc):>8,} rows")
    print(f"  profit_centers (JSON):       {len(pc_recs):>8,} records")
    print(f"  gl_accounts (Excel):         {len(df_gl):>8,} rows")
    print(f"  material_costing (Parquet):  {len(df_cost):>8,} rows")
    total = len(df_cc) + len(pc_recs) + len(df_gl) + len(df_cost)
    print(f"  TOTAL:                       {total:>8,} rows")
    print("=" * 60)
    print(f"\nFiles saved to: ./{OUTPUT_DIR}/")
    print("  bronze_cost_centers.csv")
    print("  bronze_profit_centers.json")
    print("  bronze_gl_accounts.xlsx")
    print("  bronze_material_costing.parquet")
    print("\n" + "=" * 60)
    print("COMPLETE PROJECT SUMMARY — ALL 7 DOMAINS")
    print("=" * 60)
    print("  Domain 1 Materials:      ~101,700 rows   7 files")
    print("  Domain 2 Procurement:    ~ 49,870 rows   4 files")
    print("  Domain 3 Planning/MRP:   ~ 38,655 rows   3 files")
    print("  Domain 4 Plant Maint:    ~ 21,625 rows   3 files")
    print("  Domain 5 Warehouse:      ~ 94,543 rows   4 files")
    print("  Domain 6 Quality:        ~ 33,807 rows   3 files")
    print("  Domain 7 Finance:        ~ 37,836 rows   4 files")
    print("  " + "-" * 44)
    print("  GRAND TOTAL:             ~378,036 rows  28 files")
    print("=" * 60)