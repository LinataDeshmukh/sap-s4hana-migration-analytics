"""
Domain 2: Procurement Data Generation Script
PrecisionManufacturing Inc. - SAP S/4HANA Migration Project

Generates:
  - vendor_master              → CSV         (~8,640 rows)
  - purchasing_info_record     → Excel        (~26,250 rows)
  - source_list                → JSON         (~12,480 rows)
  - vendor_delta               → Parquet      (~2,500 rows)

Total: ~49,870 rows
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

def add_duplicates(df, pct=0.08, id_col="vendor_id"):
    n_dups = int(len(df) * pct)
    dups   = df.sample(n=n_dups, replace=True).copy()
    def vary_id(vid):
        c = random.randint(0, 2)
        if c == 0: return vid.replace("V", "V-")
        if c == 1: return vid.replace("V", "V ")
        return vid.lower()
    dups[id_col] = dups[id_col].apply(vary_id)
    return pd.concat([df, dups], ignore_index=True)

# Reference data shared across tables
clean_material_ids = [f"MAT{str(i).zfill(6)}" for i in range(1, 15001)]
clean_vendor_ids   = [f"V{str(i).zfill(6)}"   for i in range(1,  8001)]
plants             = ["CHI1","HOU2","DET3"]
invalid_plants     = ["PLT1","XXXX","0000","CHI2"]
purchasing_orgs    = ["PO01","PO02","PO03"]
invalid_por        = ["PORG","PO99","N/A","TBD"]
generic_users      = ["ADMIN","MIGRATE","SYSTEM","admin","migrate"]
real_users         = [f"USR{str(i).zfill(3)}" for i in range(1, 51)]


# ─────────────────────────────────────────────
# TABLE 1: VENDOR MASTER → CSV
# ─────────────────────────────────────────────

def generate_vendor_master(n=8000):
    print(f"Generating vendor_master ({n} rows)...")

    vendor_types     = ["01","02","03"]
    bad_vendor_types = ["MFG","DIST","T","Manufacturer","distributor","1","2"]

    # Country code mess
    country_variants = {
        "US":  ["US","USA","United States","U.S.A","u.s.","UNITED STATES","us"],
        "DE":  ["DE","DEU","Germany","GERMANY","Ger","ger","GER"],
        "CN":  ["CN","CHN","China","CHINA","china","PRC"],
        "MX":  ["MX","MEX","Mexico","MEXICO","Mex"],
        "GB":  ["GB","GBR","UK","United Kingdom","UNITED KINGDOM","U.K."],
        "IN":  ["IN","IND","India","INDIA","india"],
        "JP":  ["JP","JPN","Japan","JAPAN","japan"],
        "BR":  ["BR","BRA","Brazil","BRAZIL","brazil"],
    }
    countries     = list(country_variants.keys())

    # Region/state mess
    region_variants = {
        "IL": ["IL","Illinois","ILLINOIS","Ill","ILL"],
        "TX": ["TX","Texas","TEXAS","Tex","TEX"],
        "MI": ["MI","Michigan","MICHIGAN","Mich"],
        "CA": ["CA","California","CALIFORNIA","Cal"],
        "NY": ["NY","New York","NEW YORK","N.Y."],
        "OH": ["OH","Ohio","OHIO"],
        "GA": ["GA","Georgia","GEORGIA","Geo"],
        "FL": ["FL","Florida","FLORIDA","Fla"],
    }
    regions = list(region_variants.keys())

    currency_variants = {
        "USD": ["USD","Dollars","US Dollar","usd","$","DOLLAR"],
        "EUR": ["EUR","Euro","EURO","euro","€"],
        "GBP": ["GBP","Pounds","British Pound","gbp","£"],
        "CNY": ["CNY","Yuan","YUAN","cny","RMB"],
        "MXN": ["MXN","Peso","PESO","mxn"],
    }
    currencies = list(currency_variants.keys())

    payment_terms   = ["NT30","NT60","NT90","NT15","NET30","NET60","IMMD"]
    payment_methods = ["C","T","D"]
    bad_pay_methods = ["Wire","Check","Debit","WIRE","transfer","ACH"]
    incoterms_valid = ["FOB","CIF","EXW","DDP","DAP","CFR"]
    bad_incoterms   = ["Free On Board","Cost Insurance","Ex Works",
                       "fob","cif","FREEDELIVERY"]
    statuses        = ["A","B","X"]
    bad_statuses    = ["Yes","Active","Blocked","1","0","active"]

    company_suffixes = ["Inc.","LLC","Corp","Corporation","Ltd","GmbH",
                        "Co.","Group","Industries","Manufacturing","Supply"]
    company_names    = ["Global","Premier","Advanced","Pacific","Atlantic",
                        "National","Allied","Superior","United","Continental",
                        "Delta","Omega","Alpha","Apex","Summit","Pinnacle",
                        "Heritage","Pioneer","Sterling","Horizon"]

    rows = []
    for i in range(1, n + 1):
        vid        = f"V{str(i).zfill(6)}"
        country    = random.choice(countries)
        currency   = random.choice(currencies)
        region     = random.choice(regions)
        created_dt = random_date(2015, 2023)
        changed_dt = random_date(2023, 2024)

        # Bank details — 20% null (matched pair)
        has_bank   = random.random() > 0.20
        bank_acct  = f"{random.randint(10000000,99999999)}" if has_bank else None
        bank_route = f"{random.randint(100000000,999999999)}" if has_bank else None

        # Vendor name variations
        name_base  = f"{random.choice(company_names)} {random.choice(company_names)}"
        name_suffix= random.choice(company_suffixes)
        name_variants = [
            f"{name_base} {name_suffix}",
            f"{name_base} {name_suffix}".upper(),
            f"  {name_base} {name_suffix}  ",      # leading/trailing spaces
            f"{name_base} {name_suffix}".replace("Corporation","Corp"),
            f"{name_base}",                          # missing suffix
        ]

        rows.append({
            "vendor_id":    vid,
            "vendor_name":  random.choice(name_variants),
            "vendor_type":  random.choice(bad_vendor_types) if random.random() < 0.05
                            else random.choice(vendor_types),
            "street":       None if random.random() < 0.15
                            else f"{random.randint(1,9999)} {random.choice(['Main St','Oak Ave','Industrial Blvd','Commerce Dr','Park Rd','St','Street','AVE','Ave'])}",
            "city":         None if random.random() < 0.05
                            else random.choice(["Chicago","Houston","Detroit","New York",
                                                "Los Angeles","Dallas","Miami","Atlanta",
                                                "Chcago","Housten","Detrot"]),  # misspellings
            "region":       None if random.random() < 0.08 else (
                            random.choice(region_variants[region])
                            if random.random() < 0.30 else region),
            "country":      random.choice(country_variants[country])
                            if random.random() < 0.30 else country,
            "postal_code":  None if random.random() < 0.06 else (
                            str(random.randint(1000,9999))       # missing leading zero
                            if random.random() < 0.05 else
                            f"{random.randint(10000,99999)}-{random.randint(1000,9999)}"
                            if random.random() < 0.10 else
                            str(random.randint(10000,99999))),
            "language":     None if random.random() < 0.05 else (
                            random.choice(["English","German","Chinese","Spanish"])
                            if random.random() < 0.05 else
                            random.choice(["EN","DE","ZH","ES","JA","PT"])),
            "currency":     None if random.random() < 0.04 else (
                            random.choice(currency_variants[currency])
                            if random.random() < 0.15 else currency),
            "payment_terms":None if random.random() < 0.12
                            else random.choice(payment_terms),
            "payment_method":None if random.random() < 0.06 else (
                            random.choice(bad_pay_methods) if random.random() < 0.06
                            else random.choice(payment_methods)),
            "bank_account": bank_acct,
            "bank_routing": bank_route,
            "tax_number":   None if random.random() < 0.15 else (
                            f"{random.randint(10,99)}-{random.randint(1000000,9999999)}"
                            if random.random() > 0.20 else
                            f"{random.randint(100000000,999999999)}"),  # inconsistent format
            "incoterms":    None if random.random() < 0.08 else (
                            random.choice(bad_incoterms) if random.random() < 0.08
                            else random.choice(incoterms_valid)),
            "vendor_status":random.choice(bad_statuses) if random.random() < 0.05
                            else random.choice(statuses),
            "created_by":   random.choice(generic_users) if random.random() < 0.20
                            else random.choice(real_users),
            "created_date": messy_date(created_dt),
            "changed_date": messy_date(changed_dt) if random.random() > 0.05
                            else messy_date(created_dt - timedelta(days=random.randint(1,30))),
            "legacy_vendor_id": None if random.random() < 0.15
                                else f"LV{str(random.randint(1,99999)).zfill(5)}",
            "duplicate_flag": None,  # populated by validation pipeline
        })

    df = pd.DataFrame(rows)
    df = add_duplicates(df, pct=0.08, id_col="vendor_id")

    path = f"{OUTPUT_DIR}/bronze_vendor_master.csv"
    df.to_csv(path, index=False)
    print(f"  Saved: {path} ({len(df):,} rows)\n")
    return df


# ─────────────────────────────────────────────
# TABLE 2: PURCHASING INFO RECORD → EXCEL
# ─────────────────────────────────────────────

def generate_purchasing_info_record(n=25000):
    print(f"Generating purchasing_info_record ({n} rows)...")

    order_uoms      = ["KG","EA","LT","M","PAL","BOX","ST"]
    currencies      = ["USD","EUR","GBP","CNY","MXN"]
    bad_currencies  = ["Dollars","DOLLAR","US$","$","Euro","EURO"]
    incoterms_valid = ["FOB","CIF","EXW","DDP","DAP"]
    bad_incoterms   = ["Free On Board","fob","CIF ","ex works","DDP*"]
    confirm_ctrls   = ["0001","0002","0003","0004"]
    statuses        = ["Ready","Approved","APPROVED","In Review",
                       "Pending","TBD","?","approved","NOT STARTED"]
    comments        = [
        None,"Verified by buyer","CONFIRM PRICE WITH VENDOR",
        "Price from 2019 - needs update","duplicate? check",
        "migrated from legacy","ok","??","TO DO",
        "contract renewal pending","use only for emergency orders"
    ]

    # Use existing valid IDs for FK references
    active_vendor_ids   = [f"V{str(i).zfill(6)}" for i in range(1, 7501)]    # 75% active
    inactive_vendor_ids = [f"V{str(i).zfill(6)}" for i in range(7501, 8001)] # 25% inactive/blocked

    rows = []
    for i in range(1, n + 1):
        pir_id     = f"PIR{str(i).zfill(7)}"
        mid        = random.choice(clean_material_ids)
        created_dt = random_date(2018, 2023)
        valid_from = random_date(2020, 2023)
        valid_to   = random_date(2023, 2026)
        net_price  = round(random.uniform(0.5, 50000), 2)
        price_unit = round(random.uniform(1, 1000), 3)
        min_qty    = round(random.uniform(1, 500), 3)

        # 7% reference inactive vendors
        vendor_id  = random.choice(inactive_vendor_ids) if random.random() < 0.07 \
                     else random.choice(active_vendor_ids)

        rows.append({
            "PIR Number":         pir_id,
            "Material Number":    mid,
            "Vendor":             vendor_id,
            "Plant":              random.choice(invalid_plants) if random.random() < 0.03
                                  else random.choice(plants),
            "Purchasing Org":     None if random.random() < 0.05 else (
                                  random.choice(invalid_por) if random.random() < 0.05
                                  else random.choice(purchasing_orgs)),
            "Net Price":          None if random.random() < 0.08 else (
                                  -abs(net_price) if random.random() < 0.04 else
                                  0 if random.random() < 0.03 else net_price),
            "Price Unit":         0 if random.random() < 0.05 else price_unit,
            "Order Unit":         messy_uom(random.choice(order_uoms)),
            "Currency":           None if random.random() < 0.04 else (
                                  random.choice(bad_currencies) if random.random() < 0.08
                                  else random.choice(currencies)),
            "Valid From":         None if random.random() < 0.08
                                  else messy_date(valid_from),
            "Valid To":           messy_date(valid_from - timedelta(days=random.randint(1,30)))
                                  if random.random() < 0.05
                                  else messy_date(valid_to),
            "Delivery Days":      -random.randint(1,10) if random.random() < 0.03 else (
                                  0 if random.random() < 0.04
                                  else random.randint(1, 120)),
            "Min Order Qty":      -abs(min_qty) if random.random() < 0.05 else (
                                  0 if random.random() < 0.03 else min_qty),
            "Over Tolerance %":   round(random.uniform(101,200),2) if random.random() < 0.06
                                  else round(random.uniform(0,20),2),
            "Under Tolerance %":  round(random.uniform(101,200),2) if random.random() < 0.06
                                  else round(random.uniform(0,20),2),
            "Confirmation Ctrl":  None if random.random() < 0.08
                                  else random.choice(confirm_ctrls),
            "Incoterms":          None if random.random() < 0.06 else (
                                  random.choice(bad_incoterms) if random.random() < 0.06
                                  else random.choice(incoterms_valid)),
            "Created By":         random.choice(generic_users) if random.random() < 0.15
                                  else random.choice(real_users),
            "Created Date":       messy_date(created_dt),
            "Status":             random.choice(statuses),
            "Comments":           random.choice(comments),
        })

    df = pd.DataFrame(rows)

    # Add 5% duplicates
    n_dups = int(len(df) * 0.05)
    dups   = df.sample(n=n_dups, replace=True).copy()
    dups["PIR Number"] = [f"PIR-{str(random.randint(1,99999)).zfill(7)}" for _ in range(n_dups)]
    df = pd.concat([df, dups], ignore_index=True)

    path = f"{OUTPUT_DIR}/bronze_purchasing_info_record_UDS.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Purchasing Info Records", index=False)

        # Instructions sheet
        instructions = pd.DataFrame({
            "Field":       ["Material Number","Vendor","Net Price","Valid From","Valid To"],
            "Required":    ["Yes","Yes","Yes","Yes","Yes"],
            "Format":      ["18 char","10 char","Decimal(12,2)","Date","Date"],
            "Description": [
                "Must exist in material master",
                "Must be active vendor in vendor master",
                "Price per price unit — cannot be negative or zero",
                "Start of price validity — format YYYY-MM-DD",
                "End of price validity — must be after Valid From"
            ]
        })
        instructions.to_excel(writer, sheet_name="Instructions", index=False)

        # Valid values
        valid_vals = pd.DataFrame({
            "Field":   ["Plant","Plant","Plant","Purchasing Org","Purchasing Org"],
            "Value":   ["CHI1","HOU2","DET3","PO01","PO02"],
            "Meaning": ["Chicago Plant","Houston Plant","Detroit Plant",
                        "North America Purchasing","Europe Purchasing"]
        })
        valid_vals.to_excel(writer, sheet_name="Valid Values", index=False)

    print(f"  Saved: {path} ({len(df):,} rows, 3 sheets)\n")
    return df


# ─────────────────────────────────────────────
# TABLE 3: SOURCE LIST → JSON
# ─────────────────────────────────────────────

def generate_source_list(n=12000):
    print(f"Generating source_list ({n} rows)...")

    proc_types     = ["E","U"]
    bad_proc_types = ["External","Stock","ext","INT","e","u"]
    spec_procs     = ["10","20","30","40","50"]
    fixed_variants = ["1","0","Yes","No","yes","no","Y","N",None]

    records = []
    used    = set()
    record_id = 200001

    while len(records) < n:
        mid  = random.choice(clean_material_ids)
        vid  = random.choice(clean_vendor_ids)
        plant= random.choice(plants)
        key  = (mid, vid, plant)
        if key in used:
            continue
        used.add(key)

        valid_from = random_date(2019, 2023)
        valid_to   = random_date(2023, 2026)
        has_agreement = random.random() > 0.15
        agr_num    = f"AGR{str(random.randint(1,9999)).zfill(6)}" if has_agreement else None
        agr_item   = f"{str(random.randint(1,999)).zfill(5)}"     if has_agreement else None

        record = {
            "sourceListId":    f"SL{str(record_id).zfill(8)}",
            "materialId":      mid,
            "plantId":         random.choice(invalid_plants) if random.random() < 0.03
                               else plant,
            "vendorId":        f"GHOST{str(random.randint(1,999)).zfill(5)}"
                               if random.random() < 0.05 else vid,
            "purchasingOrg":   None if random.random() < 0.05 else (
                               random.choice(invalid_por) if random.random() < 0.05
                               else random.choice(purchasing_orgs)),
            "validFrom":       None if random.random() < 0.07
                               else messy_date(valid_from),
            "validTo":         messy_date(valid_from - timedelta(days=random.randint(1,30)))
                               if random.random() < 0.05
                               else messy_date(valid_to),
            "fixedSource":     random.choice(fixed_variants)
                               if random.random() < 0.20 else
                               random.choice(["1","0"]),
            "orderingAddress": None if random.random() < 0.10
                               else f"ADDR{str(random.randint(1,999)).zfill(4)}",
            "agreementNumber": agr_num,
            "agreementItem":   agr_item,
            "procurementType": random.choice(bad_proc_types) if random.random() < 0.06
                               else random.choice(proc_types),
            "specialProcurement": None if random.random() < 0.10
                                  else random.choice(spec_procs),
            "createdBy":       random.choice(generic_users) if random.random() < 0.15
                               else random.choice(real_users),
            "createdDate":     messy_date(random_date(2018, 2023)),
            "auditInfo": {
                "source":       random.choice(["LEGACY_ERP","MANUAL","API_SYNC","MIGRATE"]),
                "extractedAt":  datetime.now().isoformat(),
                "systemVersion": random.choice(["ECC600","S4H2021","S4H2023","UNKNOWN"]),
            }
        }

        # 3% extra unexpected fields
        if random.random() < 0.03:
            record["LEGACY_FIELD_IGNORE"] = "old system artifact"

        records.append(record)
        record_id += 1

    path = f"{OUTPUT_DIR}/bronze_source_list.json"
    with open(path, "w") as f:
        json.dump({
            "apiVersion":  "2.0.1",
            "extractedAt": datetime.now().isoformat(),
            "source":      "LEGACY_ERP_ECC600",
            "recordCount": len(records),
            "data":        records
        }, f, indent=2)

    print(f"  Saved: {path} ({len(records):,} records)\n")
    return records


# ─────────────────────────────────────────────
# TABLE 4: VENDOR DELTA → PARQUET
# ─────────────────────────────────────────────

def generate_vendor_delta(n=2500):
    print(f"Generating vendor_delta ({n} rows)...")

    change_types  = ["NEW","MODIFIED","DELETED","REACTIVATED"]
    change_fields = [
        "vendor_name","country","payment_terms","payment_method",
        "bank_account","currency","vendor_status","incoterms",
        "tax_number","region","street","postal_code"
    ]
    sources       = ["LEGACY_ERP","MANUAL","API_SYNC","FINANCE_SYSTEM"]
    waves         = ["WAVE1","WAVE2","CUTOVER"]
    load_statuses = ["PENDING","LOADED","FAILED","SKIPPED"]

    cutover_start = datetime(2024, 6, 1)
    cutover_end   = datetime(2024, 6, 15)

    rows = []
    for i in range(n):
        is_new      = random.random() < 0.25
        vid         = (f"V{str(random.randint(90000,99999)).zfill(6)}"
                       if is_new else random.choice(clean_vendor_ids))
        change_type = "NEW" if is_new else random.choice(change_types[1:])
        changed_fld = None if change_type == "NEW" else random.choice(change_fields)
        delta_ts    = cutover_start + timedelta(
                        seconds=random.randint(
                            0, int((cutover_end - cutover_start).total_seconds())
                        ))

        rows.append({
            "delta_record_id": 700000 + i,
            "vendor_id":       vid,
            "change_type":     change_type,
            "changed_field":   changed_fld,
            "old_value":       None if change_type == "NEW" else
                               random.choice(["US","NT30","A","FOB","USD",None]),
            "new_value":       None if change_type == "DELETED" else
                               random.choice(["DE","NT60","B","CIF","EUR","USA",None]),
            "delta_timestamp": delta_ts.isoformat(),
            "changed_by":      random.choice(generic_users + real_users),
            "source_system":   random.choice(sources),
            "migration_wave":  random.choice(waves),
            "load_status":     random.choice(load_statuses),
            "validated":       random.choice([True, False, None]),
            "target_system":   "SAP_S4HANA_2023",
            "notes":           random.choice([
                                None,
                                "Confirmed by procurement team",
                                "Pending finance approval",
                                "Duplicate vendor — merge required",
                                "Bank details updated for GDPR compliance",
                                "Reactivated for emergency procurement",
                               ]),
        })

    df   = pd.DataFrame(rows)
    path = f"{OUTPUT_DIR}/bronze_vendor_delta.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")

    print(f"  Saved: {path} ({len(df):,} rows)\n")
    return df


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("PrecisionManufacturing Inc. - Domain 2 Data Generation")
    print("=" * 60)

    df_vendor  = generate_vendor_master(n=8000)
    df_pir     = generate_purchasing_info_record(n=25000)
    sl_records = generate_source_list(n=12000)
    df_delta   = generate_vendor_delta(n=2500)

    print("=" * 60)
    print("DOMAIN 2 GENERATION SUMMARY")
    print("=" * 60)
    print(f"  vendor_master (CSV):           {len(df_vendor):>8,} rows")
    print(f"  purchasing_info_record (XLSX): {len(df_pir):>8,} rows")
    print(f"  source_list (JSON):            {len(sl_records):>8,} records")
    print(f"  vendor_delta (Parquet):        {len(df_delta):>8,} rows")
    total = len(df_vendor) + len(df_pir) + len(sl_records) + len(df_delta)
    print(f"  {'TOTAL':30} {total:>8,} rows")
    print("=" * 60)
    print(f"\nFiles saved to: ./{OUTPUT_DIR}/")
    print("  bronze_vendor_master.csv")
    print("  bronze_purchasing_info_record_UDS.xlsx")
    print("  bronze_source_list.json")
    print("  bronze_vendor_delta.parquet")