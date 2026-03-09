"""
Domain 5: Warehouse / Logistics Data Generation Script
PrecisionManufacturing Inc. - SAP S/4HANA Migration Project

Generates:
  - warehouse_master           → JSON      (~18 rows)
  - storage_locations          → CSV       (~525 rows)
  - storage_bins               → Parquet   (~52,000 rows)
  - material_storage_assignment→ CSV       (~42,000 rows)

Total: ~94,543 rows
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
        "PAL": ["PAL","Pal","PALLET","pallet","PLT"],
        "M3":  ["M3","m3","CBM","cbm","CubicMeter","cubic meter"],
        "SQM": ["SQM","sqm","M2","m2","SQ.M","sq.m"],
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

# Valid warehouse IDs for FK references
valid_warehouse_ids = [f"{str(i).zfill(3)}" for i in range(1, 16)]  # 001-015
invalid_warehouse_ids = ["099","999","WH1","WHX","000","W01"]

# Valid storage location IDs
valid_sl_ids = [f"SL{str(i).zfill(2)}" for i in range(1, 31)]
invalid_sl_ids = ["SL99","SLXX","0000","N/A","SL00"]


# ─────────────────────────────────────────────
# TABLE 1: WAREHOUSE MASTER → JSON
# ─────────────────────────────────────────────

def generate_warehouse_master():
    print("Generating warehouse_master (~18 rows)...")

    warehouse_types     = ["01","02","03","04"]
    bad_warehouse_types = ["Open","RACK","hazmat","Cold","open","rack","03 "]
    capacity_units      = ["PAL","M3","SQM"]
    yesno_valid         = ["Y","N"]
    bad_yesno           = ["Yes","No","TRUE","FALSE","1","0","true","false"]

    # 5 warehouses per plant = 15 total
    warehouse_configs = {
        "CHI1": [
            ("001","Chicago Main Warehouse","01","PAL",5000),
            ("002","Chicago Rack Storage","02","PAL",3000),
            ("003","Chicago Hazmat Store","03","M3",500),
            ("004","Chicago Cold Storage","04","M3",800),
            ("005","Chicago Bulk Area","01","SQM",10000),
        ],
        "HOU2": [
            ("006","Houston Main Warehouse","01","PAL",6000),
            ("007","Houston Rack Storage","02","PAL",4000),
            ("008","Houston Hazmat Store","03","M3",600),
            ("009","Houston Cold Storage","04","M3",1000),
            ("010","Houston Bulk Area","01","SQM",12000),
        ],
        "DET3": [
            ("011","Detroit Main Warehouse","01","PAL",4500),
            ("012","Detroit Rack Storage","02","PAL",2500),
            ("013","Detroit Hazmat Store","03","M3",400),
            ("014","Detroit Cold Storage","04","M3",700),
            ("015","Detroit Bulk Area","01","SQM",8000),
        ],
    }

    records = []
    for plant, warehouses in warehouse_configs.items():
        for wh_id, wh_desc, wh_type, cap_unit, capacity in warehouses:
            created_dt = random_date(2010, 2020)

            # Capacity issues
            cap_value = float(capacity)
            if random.random() < 0.05: cap_value = -abs(cap_value)
            if random.random() < 0.04: cap_value = 0

            record = {
                "warehouseId":          wh_id,
                "warehouseDescription": None if random.random() < 0.08 else (
                                        wh_desc.upper() if random.random() < 0.10 else
                                        f"{wh_desc}  " if random.random() < 0.05 else
                                        wh_desc),
                "plantId":              random.choice(invalid_plants)
                                        if random.random() < 0.03 else plant,
                "warehouseType":        random.choice(bad_warehouse_types)
                                        if random.random() < 0.05
                                        else wh_type,
                "totalCapacity":        cap_value,
                "capacityUnit":         messy_uom(cap_unit),
                "temperatureControlled":random.choice(bad_yesno)
                                        if random.random() < 0.05
                                        else ("Y" if wh_type == "04" else "N"),
                "hazmatApproved":       random.choice(bad_yesno)
                                        if random.random() < 0.05
                                        else ("Y" if wh_type == "03" else "N"),
                "activeFlag":           random.choice(bad_yesno)
                                        if random.random() < 0.05 else "Y",
                "createdBy":            random.choice(generic_users)
                                        if random.random() < 0.10
                                        else random.choice(real_users),
                "createdDate":          messy_date(created_dt),
                "auditInfo": {
                    "source":      random.choice(["WM_SYSTEM","MANUAL","MIGRATE"]),
                    "extractedAt": datetime.now().isoformat(),
                }
            }
            records.append(record)

            # 3% duplicate with case variation
            if random.random() < 0.03:
                dup = record.copy()
                dup["warehouseId"] = wh_id.lower()
                records.append(dup)

    path = f"{OUTPUT_DIR}/bronze_warehouse_master.json"
    with open(path, "w") as f:
        json.dump({
            "apiVersion":  "1.0.0",
            "extractedAt": datetime.now().isoformat(),
            "source":      "WM_LEGACY_SYSTEM",
            "recordCount": len(records),
            "data":        records
        }, f, indent=2)

    print(f"  Saved: {path} ({len(records):,} records)\n")
    return records


# ─────────────────────────────────────────────
# TABLE 2: STORAGE LOCATIONS → CSV
# ─────────────────────────────────────────────

def generate_storage_locations(n=500):
    print(f"Generating storage_locations ({n} rows)...")

    storage_types      = ["001","002","003","004","005"]
    bad_storage_types  = ["Fixed","RANDOM","Bulk","fixed","002 ","HIGH RACK"]
    temp_zones         = ["AM","RF","FZ"]
    bad_temp_zones     = ["Cold","AMBIENT","rf","Frozen","ambient","COLD","fz"]
    yesno_valid        = ["Y","N"]
    bad_yesno          = ["Yes","No","TRUE","FALSE","1","0"]

    rows = []
    record_id = 1

    # ~16-17 storage locations per plant per warehouse
    for plant in plants:
        plant_warehouses = [
            wh for wh in valid_warehouse_ids
            if (plant == "CHI1" and int(wh) <= 5) or
               (plant == "HOU2" and 6 <= int(wh) <= 10) or
               (plant == "DET3" and int(wh) >= 11)
        ]

        for wh_id in plant_warehouses:
            n_locs = random.randint(3, 7)
            for j in range(n_locs):
                sl_id      = f"SL{str(record_id).zfill(2)}" \
                             if record_id <= 30 else \
                             f"SL{str(random.randint(1,30)).zfill(2)}"
                created_dt = random_date(2010, 2022)
                max_wt     = round(random.uniform(100, 50000), 3)
                max_vol    = round(random.uniform(1, 1000), 3)

                rows.append({
                    "storage_location_id":   sl_id,
                    "storage_location_desc": None if random.random() < 0.08 else (
                                             random.choice([
                                                 f"{plant} WH{wh_id} Zone {j+1}",
                                                 f"{plant} WH{wh_id} ZONE {j+1}",
                                                 f"  {plant} WH{wh_id} Zone {j+1}  ",
                                                 f"Lager {plant} {j+1}",  # German
                                             ])),
                    "plant_id":              random.choice(invalid_plants)
                                             if random.random() < 0.03 else plant,

                    # 6% invalid warehouse reference
                    "warehouse_id":          random.choice(invalid_warehouse_ids)
                                             if random.random() < 0.06 else wh_id,

                    "storage_type":          random.choice(bad_storage_types)
                                             if random.random() < 0.05
                                             else random.choice(storage_types),

                    # 5% negative, 4% null max weight
                    "max_weight":            None if random.random() < 0.04 else (
                                             -abs(max_wt) if random.random() < 0.05
                                             else max_wt),

                    # 5% negative, 4% null max volume
                    "max_volume":            None if random.random() < 0.04 else (
                                             -abs(max_vol) if random.random() < 0.05
                                             else max_vol),

                    "temperature_zone":      random.choice(bad_temp_zones)
                                             if random.random() < 0.06
                                             else random.choice(temp_zones),
                    "hazmat_flag":           random.choice(bad_yesno)
                                             if random.random() < 0.05
                                             else random.choice(yesno_valid),
                    "active_flag":           random.choice(bad_yesno)
                                             if random.random() < 0.05
                                             else random.choice(yesno_valid),
                    "created_by":            random.choice(generic_users)
                                             if random.random() < 0.10
                                             else random.choice(real_users),
                    "created_date":          messy_date(created_dt),
                })
                record_id += 1
                if len(rows) >= n:
                    break
            if len(rows) >= n:
                break
        if len(rows) >= n:
            break

    df = pd.DataFrame(rows)

    # Add 5% duplicates
    n_dups = int(len(df) * 0.05)
    dups   = df.sample(n=n_dups, replace=True).copy()
    dups["storage_location_id"] = [
        f"SL-{str(random.randint(1,30)).zfill(2)}"
        for _ in range(n_dups)
    ]
    df = pd.concat([df, dups], ignore_index=True)

    path = f"{OUTPUT_DIR}/bronze_storage_locations.csv"
    df.to_csv(path, index=False)
    print(f"  Saved: {path} ({len(df):,} rows)\n")
    return df


# ─────────────────────────────────────────────
# TABLE 3: STORAGE BINS → PARQUET
# ─────────────────────────────────────────────

def generate_storage_bins(n=50000):
    print(f"Generating storage_bins ({n} rows)...")

    storage_types     = ["001","002","003","004","005"]
    bad_storage_types = ["Fixed","RANDOM","Bulk","fixed","HIGH RACK"]
    storage_sections  = ["A","B","C","D","E"]
    bin_types         = ["ST","OV","SP","BL","FR"]
    bad_bin_types     = ["Standard","OVERSIZE","special","std","OVR"]
    yesno_valid       = ["Y","N"]
    bad_yesno         = ["Yes","No","TRUE","FALSE","1","0"]

    # Row naming inconsistencies across plants
    row_formats = {
        "CHI1": lambda r: f"R{str(r).zfill(2)}",          # R01, R02
        "HOU2": lambda r: f"ROW-{chr(64+r)}",              # ROW-A, ROW-B
        "DET3": lambda r: f"Row_{r}",                       # Row_1, Row_2
    }

    # Level naming inconsistencies
    level_formats = {
        "CHI1": lambda l: str(l),                          # 1, 2, 3
        "HOU2": lambda l: f"L{str(l).zfill(2)}",          # L01, L02
        "DET3": lambda l: f"Level{l}",                     # Level1, Level2
    }

    rows      = []
    record_id = 1
    used_bins = set()

    while len(rows) < n:
        plant   = random.choice(plants)
        wh_id   = random.choice([
            wh for wh in valid_warehouse_ids
            if (plant == "CHI1" and int(wh) <= 5) or
               (plant == "HOU2" and 6 <= int(wh) <= 10) or
               (plant == "DET3" and int(wh) >= 11)
        ])
        sl_id   = random.choice(valid_sl_ids)
        row_num = random.randint(1, 20)
        col_num = random.randint(1, 50)
        lev_num = random.randint(1, 6)

        # Apply plant-specific naming format — shows cross-plant inconsistency
        row_val = row_formats[plant](row_num)
        lev_val = level_formats[plant](lev_num)
        col_val = str(col_num).zfill(3)

        bin_id = f"{wh_id}-{sl_id}-{row_val}-{col_val}-{lev_val}"
        if bin_id in used_bins:
            continue
        used_bins.add(bin_id)

        max_wt  = round(random.uniform(50, 5000), 3)
        max_vol = round(random.uniform(0.1, 50), 3)
        created_dt = random_date(2010, 2022)

        rows.append({
            "bin_id":              bin_id,
            "bin_description":     None if random.random() < 0.10
                                   else f"Bin {bin_id}",

            # 5% invalid warehouse
            "warehouse_id":        random.choice(invalid_warehouse_ids)
                                   if random.random() < 0.05 else wh_id,

            # 6% invalid storage location
            "storage_location_id": random.choice(invalid_sl_ids)
                                   if random.random() < 0.06 else sl_id,

            "storage_type":        random.choice(bad_storage_types)
                                   if random.random() < 0.05
                                   else random.choice(storage_types),

            "storage_section":     None if random.random() < 0.07
                                   else random.choice(storage_sections),

            # Inconsistent row naming (already applied per plant format)
            "bin_row":             None if random.random() < 0.05 else row_val,
            "bin_column":          None if random.random() < 0.05 else col_val,
            "bin_level":           None if random.random() < 0.05 else lev_val,

            # 4% negative weights/volumes
            "max_weight":          -abs(max_wt) if random.random() < 0.04
                                   else max_wt,
            "max_volume":          -abs(max_vol) if random.random() < 0.04
                                   else max_vol,

            "bin_type":            random.choice(bad_bin_types)
                                   if random.random() < 0.05
                                   else random.choice(bin_types),
            "active_flag":         random.choice(bad_yesno)
                                   if random.random() < 0.05
                                   else random.choice(yesno_valid),
            "created_by":          random.choice(generic_users)
                                   if random.random() < 0.10
                                   else random.choice(real_users),
            "created_date":        messy_date(created_dt),
        })
        record_id += 1

    df = pd.DataFrame(rows)

    # Add 4% duplicates
    n_dups = int(len(df) * 0.04)
    dups   = df.sample(n=n_dups, replace=True).copy()
    dups["bin_id"] = dups["bin_id"].apply(
        lambda b: b.lower() if random.random() < 0.5 else b.replace("-","_")
    )
    df = pd.concat([df, dups], ignore_index=True)

    path = f"{OUTPUT_DIR}/bronze_storage_bins.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    print(f"  Saved: {path} ({len(df):,} rows)\n")
    return df


# ─────────────────────────────────────────────
# TABLE 4: MATERIAL STORAGE ASSIGNMENT → CSV
# ─────────────────────────────────────────────

def generate_material_storage_assignment(n=40000):
    print(f"Generating material_storage_assignment ({n} rows)...")

    storage_types      = ["001","002","003","004","005"]
    bad_storage_types  = ["Fixed","RANDOM","Bulk","fixed"]
    picking_areas      = ["PA1","PA2","PA3","PA4","PA5"]
    putaway_strategies = ["P1","P2","P3"]
    bad_putaway        = ["Fixed","OPEN","Bulk","fixed","p1","P1 ","FIXED"]
    picking_strategies = ["F1","F2","F3"]
    bad_picking        = ["FIFO","fifo","LIFO","lifo","first","FEFO","fefo"]
    uoms               = ["KG","EA","PAL","BOX","ST","M3"]
    yesno_valid        = ["Y","N"]
    bad_yesno          = ["Yes","No","TRUE","FALSE","1","0"]

    # Generate some valid bin IDs for FK references
    sample_bins = [
        f"{wh}-SL{str(random.randint(1,30)).zfill(2)}-R{str(random.randint(1,20)).zfill(2)}-{str(random.randint(1,50)).zfill(3)}-{random.randint(1,6)}"
        for wh in valid_warehouse_ids
        for _ in range(100)
    ]

    rows       = []
    assignment_id = 1
    used       = set()

    while len(rows) < n:
        mid   = random.choice(clean_material_ids)
        plant = random.choice(plants)
        wh_id = random.choice([
            wh for wh in valid_warehouse_ids
            if (plant == "CHI1" and int(wh) <= 5) or
               (plant == "HOU2" and 6 <= int(wh) <= 10) or
               (plant == "DET3" and int(wh) >= 11)
        ])
        key = (mid, plant, wh_id)
        if key in used:
            continue
        used.add(key)

        sl_id       = random.choice(valid_sl_ids)
        min_stock   = round(random.uniform(0, 500), 3)
        max_stock   = round(min_stock * random.uniform(2, 10), 3)
        repl_qty    = round(random.uniform(10, 500), 3)
        valid_from  = random_date(2018, 2023)
        valid_to    = random_date(2023, 2026)
        created_dt  = random_date(2018, 2023)

        rows.append({
            "assignment_id":       assignment_id,

            # 6% orphaned material
            "material_id":         f"GHOST{str(random.randint(1,999)).zfill(5)}"
                                   if random.random() < 0.06 else mid,

            "plant_id":            random.choice(invalid_plants)
                                   if random.random() < 0.03 else plant,

            # 5% invalid warehouse
            "warehouse_id":        random.choice(invalid_warehouse_ids)
                                   if random.random() < 0.05 else wh_id,

            # 6% invalid storage location
            "storage_location_id": random.choice(invalid_sl_ids)
                                   if random.random() < 0.06 else sl_id,

            # 7% orphaned bin
            "bin_id":              f"GHOST-BIN-{str(random.randint(1,999)).zfill(5)}"
                                   if random.random() < 0.07
                                   else random.choice(sample_bins),

            "storage_type":        random.choice(bad_storage_types)
                                   if random.random() < 0.05
                                   else random.choice(storage_types),

            "picking_area":        None if random.random() < 0.08
                                   else random.choice(picking_areas),

            # 6% invalid putaway strategy
            "putaway_strategy":    random.choice(bad_putaway)
                                   if random.random() < 0.06
                                   else random.choice(putaway_strategies),

            # 6% invalid picking strategy
            "picking_strategy":    random.choice(bad_picking)
                                   if random.random() < 0.06
                                   else random.choice(picking_strategies),

            # 5% negative min stock
            "min_stock":           -abs(min_stock) if random.random() < 0.05
                                   else min_stock,

            # 4% max < min
            "max_stock":           round(min_stock * 0.5, 3)
                                   if random.random() < 0.04 else max_stock,

            # 5% negative, 4% zero replenishment qty
            "replenishment_qty":   -abs(repl_qty) if random.random() < 0.05 else (
                                   0 if random.random() < 0.04 else repl_qty),

            "uom":                 messy_uom(random.choice(uoms)),

            # 7% null valid_from, mixed formats
            "valid_from":          None if random.random() < 0.07
                                   else messy_date(valid_from),

            # 5% valid_to before valid_from
            "valid_to":            messy_date(
                                       valid_from - timedelta(days=random.randint(1,30))
                                       if random.random() < 0.05
                                       else valid_to),

            "active_flag":         random.choice(bad_yesno)
                                   if random.random() < 0.05
                                   else random.choice(yesno_valid),
            "created_by":          random.choice(generic_users)
                                   if random.random() < 0.10
                                   else random.choice(real_users),
            "created_date":        messy_date(created_dt),
        })
        assignment_id += 1

    df = pd.DataFrame(rows)

    # Add 5% orphaned records
    n_orph = int(len(df) * 0.05)
    orph   = df.sample(n=n_orph).copy()
    orph["material_id"]    = [
        f"GHOST{str(i).zfill(5)}" for i in range(60000, 60000 + n_orph)
    ]
    orph["assignment_id"]  = range(assignment_id, assignment_id + n_orph)
    df = pd.concat([df, orph], ignore_index=True)

    path = f"{OUTPUT_DIR}/bronze_material_storage_assignment.csv"
    df.to_csv(path, index=False)
    print(f"  Saved: {path} ({len(df):,} rows)\n")
    return df


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("PrecisionManufacturing Inc. - Domain 5 Data Generation")
    print("=" * 60)

    wh_records  = generate_warehouse_master()
    df_sl       = generate_storage_locations(n=500)
    df_bins     = generate_storage_bins(n=50000)
    df_assign   = generate_material_storage_assignment(n=40000)

    print("=" * 60)
    print("DOMAIN 5 GENERATION SUMMARY")
    print("=" * 60)
    print(f"  warehouse_master (JSON):              {len(wh_records):>8,} records")
    print(f"  storage_locations (CSV):              {len(df_sl):>8,} rows")
    print(f"  storage_bins (Parquet):               {len(df_bins):>8,} rows")
    print(f"  material_storage_assignment (CSV):    {len(df_assign):>8,} rows")
    total = len(wh_records) + len(df_sl) + len(df_bins) + len(df_assign)
    print(f"  TOTAL:                                {total:>8,} rows")
    print("=" * 60)
    print(f"\nFiles saved to: ./{OUTPUT_DIR}/")
    print("  bronze_warehouse_master.json")
    print("  bronze_storage_locations.csv")
    print("  bronze_storage_bins.parquet")
    print("  bronze_material_storage_assignment.csv")
    print("\nCumulative bronze_data/ totals:")
    print("  Domain 1:  ~101,700 rows  (7 files)")
    print("  Domain 2:  ~ 49,870 rows  (4 files)")
    print("  Domain 3:  ~ 38,655 rows  (3 files)")
    print("  Domain 4:  ~ 21,625 rows  (3 files)")
    print("  Domain 5:  ~ 94,543 rows  (4 files)")
    print("  RUNNING TOTAL: ~306,393 rows across 21 files")