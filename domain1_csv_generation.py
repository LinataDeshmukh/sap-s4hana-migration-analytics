"""
Domain 1: Materials Master Data Generation Script - Part 1
PrecisionManufacturing Inc. - SAP S/4HANA Migration Project
Generates:
  - bronze_material_master.csv        (~16,200 rows)
  - bronze_material_plant_data.csv    (~37,500 rows)
  - bronze_material_uom.csv           (~48,000 rows)
"""

import pandas as pd
import numpy as np
import random
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
        "KG":  ["KG","kg","Kg","Kilograms","kgs","KGS"],
        "EA":  ["EA","ea","Each","EACH","Ea","PCS","pcs"],
        "LT":  ["LT","lt","Liter","LITER","L","ltr"],
        "M":   ["M","m","Meter","METER","Mtr","mtr"],
        "G":   ["G","g","Gram","GRAM","GRM"],
        "ST":  ["ST","st","Piece","PC","pc"],
        "PAL": ["PAL","Pal","PALLET","Pallet","pallet","PLT"],
        "BOX": ["BOX","Box","box","BOXES","BX"],
    }
    if uom in variations and random.random() < 0.30:
        return random.choice(variations[uom][1:])
    return uom

def add_duplicates(df, pct=0.08, id_col="material_id"):
    n_dups = int(len(df) * pct)
    dups   = df.sample(n=n_dups, replace=True).copy()
    def vary_id(mid):
        c = random.randint(0, 2)
        if c == 0: return mid.replace("MAT", "MAT-")
        if c == 1: return mid.replace("MAT", "MAT ")
        return mid.lower()
    dups[id_col] = dups[id_col].apply(vary_id)
    return pd.concat([df, dups], ignore_index=True)


# ─────────────────────────────────────────────
# TABLE 1: MATERIAL MASTER → CSV
# ─────────────────────────────────────────────

def generate_material_master(n=15000):
    print(f"Generating material_master ({n} rows)...")

    mat_types     = ["ROH","HALB","FERT","VERP","NLAG","DIEN"]
    bad_mat_types = ["RAW","SEMI","FIN","PACK","raw","finished"]
    mat_groups    = [f"MG{str(i).zfill(4)}" for i in range(1, 101)]
    base_uoms     = ["KG","EA","LT","M","G","ST"]
    proc_types    = ["E","F","X"]
    bad_proc      = ["Buy","Make","Both","EXTERNAL","ext"]
    ind_sectors   = ["M","C","A","P"]
    users         = [f"USR{str(i).zfill(3)}" for i in range(1, 51)]
    generic_users = ["ADMIN","MIGRATE","SYSTEM","admin","migrate"]

    rows = []
    for i in range(1, n + 1):
        mid        = f"MAT{str(i).zfill(6)}"
        mat_type   = random.choice(mat_types)
        base_uom   = random.choice(base_uoms)
        created_dt = random_date(2018, 2023)
        changed_dt = random_date(2023, 2024)

        gross = round(random.uniform(0.1, 5000), 3)
        net   = round(gross * random.uniform(0.85, 0.99), 3)
        if random.random() < 0.03: net   = round(gross * 1.05, 3)
        if random.random() < 0.07: gross = -abs(gross)
        if random.random() < 0.05: gross = net = None

        rows.append({
            "material_id": mid,
            "material_description": random.choice([
                f"Material {mid} Component",
                f"MATERIAL {mid} COMPONENT",
                f"Material {mid} Componente",
                f"Material {mid} Komponente  ",
                f"  material {mid} component",
            ]),
            "material_type": random.choice(bad_mat_types)
                             if random.random() < 0.05 else mat_type,
            "material_group": None if random.random() < 0.10
                              else random.choice(mat_groups),
            "base_uom":       messy_uom(base_uom),
            "gross_weight":   gross,
            "net_weight":     net,
            "weight_uom":     messy_uom("KG"),
            "length": None if random.random() < 0.08
                      else round(random.uniform(1, 5000), 3),
            "width":  None if random.random() < 0.08
                      else round(random.uniform(1, 3000), 3),
            "height": None if random.random() < 0.08
                      else round(random.uniform(1, 3000), 3),
            "volume": None if random.random() < 0.06
                      else round(random.uniform(0.001, 10000), 3),
            "shelf_life": -random.randint(1, 365) if random.random() < 0.02 else (
                          random.randint(30, 1825)
                          if mat_type in ["ROH","FERT"] else None),
            "procurement_type": None if random.random() < 0.04 else (
                                random.choice(bad_proc) if random.random() < 0.04
                                else random.choice(proc_types)),
            "industry_sector": None if random.random() < 0.03
                               else random.choice(ind_sectors),
            "created_by": random.choice(generic_users) if random.random() < 0.15
                          else random.choice(users),
            "created_date":  messy_date(created_dt),
            "changed_date":  messy_date(changed_dt) if random.random() > 0.05
                             else messy_date(created_dt - timedelta(days=random.randint(1,30))),
            "deletion_flag": "X" if random.random() < 0.06 else None,
            "legacy_material_id": None if random.random() < 0.12
                                  else f"LEG{str(random.randint(1,99999)).zfill(5)}",
        })

    df = pd.DataFrame(rows)
    df = add_duplicates(df, pct=0.08)
    path = f"{OUTPUT_DIR}/bronze_material_master.csv"
    df.to_csv(path, index=False)
    print(f"  Saved: {path} ({len(df):,} rows)\n")
    return df


# ─────────────────────────────────────────────
# TABLE 2: MATERIAL PLANT DATA → CSV
# ─────────────────────────────────────────────

def generate_material_plant_data(material_ids, n_base=35000):
    print(f"Generating material_plant_data (~{n_base} rows)...")

    plants       = ["CHI1","HOU2","DET3"]
    bad_plants   = ["PLT1","XXXX","0000","CHI2"]
    mrp_types    = ["PD","VB","ND","MK","PK"]
    bad_mrp      = ["AUTO","MANUAL","YES","pd"]
    lot_sizes    = ["EX","FX","HB","WB","MB","TB"]
    controllers  = [f"MC{str(i).zfill(2)}" for i in range(1, 21)]
    bad_ctrl     = ["MCX1","NONE","TBD","N/A","MC99"]
    stor_locs    = [f"SL{str(i).zfill(2)}" for i in range(1, 31)]
    bad_sl       = ["SL99","SLXX","0000","N/A"]
    val_classes  = [f"VC{str(i).zfill(4)}" for i in range(3000, 3050)]
    price_ctrls  = ["S","V"]
    bad_pc       = ["STD","AVG","Standard","Moving"]
    statuses     = ["01","02","03","04"]
    bad_status   = ["X1","ZZ","99","NA"]

    rows      = []
    record_id = 1

    for mid in random.sample(material_ids, min(len(material_ids), n_base)):
        for plant in random.sample(plants, k=random.randint(1, 3)):
            lot   = random.choice(lot_sizes)
            min_l = round(random.uniform(1, 100), 3)
            max_l = round(min_l * random.uniform(5, 50), 3)

            rows.append({
                "record_id":   record_id,
                "material_id": mid,
                "plant_id":    random.choice(bad_plants) if random.random() < 0.03
                               else plant,
                "mrp_type":    None if random.random() < 0.08 else (
                               random.choice(bad_mrp) if random.random() < 0.05
                               else random.choice(mrp_types)),
                "mrp_controller": random.choice(bad_ctrl) if random.random() < 0.10
                                  else random.choice(controllers),
                "lot_size":    None if random.random() < 0.06 else lot,
                "minimum_lot_size": -abs(min_l) if random.random() < 0.04 else min_l,
                "maximum_lot_size": round(min_l * 0.5, 3) if random.random() < 0.03
                                    else max_l,
                "fixed_lot_size":   None if (lot == "FX" and random.random() < 0.05)
                                    else round(random.uniform(10, 500), 3),
                "reorder_point":    -abs(round(random.uniform(1,500),3))
                                    if random.random() < 0.04
                                    else round(random.uniform(0, 500), 3),
                "safety_stock":     None if random.random() < 0.08
                                    else round(random.uniform(0, 1000), 3),
                "planned_delivery_days": (0 if random.random() < 0.03 else
                                          -random.randint(1,10) if random.random() < 0.02
                                          else random.randint(1, 90)),
                "goods_receipt_days": None if random.random() < 0.05
                                      else random.randint(0, 10),
                "storage_location":   random.choice(bad_sl) if random.random() < 0.07
                                      else random.choice(stor_locs),
                "valuation_class":    None if random.random() < 0.09
                                      else random.choice(val_classes),
                "price_control":      None if random.random() < 0.06 else (
                                      random.choice(bad_pc) if random.random() < 0.06
                                      else random.choice(price_ctrls)),
                "standard_price":     None if random.random() < 0.10
                                      else round(random.uniform(0.5, 10000), 2),
                "moving_avg_price":   -abs(round(random.uniform(0.5,10000),2))
                                      if random.random() < 0.05
                                      else round(random.uniform(0.5, 10000), 2),
                "plant_specific_status": random.choice(bad_status)
                                         if random.random() < 0.04
                                         else random.choice(statuses),
                "valid_from":   None if random.random() < 0.06
                                else messy_date(random_date(2018, 2022)),
                "created_date": messy_date(random_date(2018, 2023)),
            })
            record_id += 1
            if len(rows) >= n_base:
                break
        if len(rows) >= n_base:
            break

    df = pd.DataFrame(rows)

    # Add 5% orphaned records
    n_orph = int(len(df) * 0.05)
    orph   = df.sample(n=n_orph).copy()
    orph["material_id"] = [f"GHOST{str(i).zfill(5)}" for i in range(n_orph)]
    orph["record_id"]   = range(record_id, record_id + n_orph)
    df = pd.concat([df, orph], ignore_index=True)

    path = f"{OUTPUT_DIR}/bronze_material_plant_data.csv"
    df.to_csv(path, index=False)
    print(f"  Saved: {path} ({len(df):,} rows)\n")
    return df


# ─────────────────────────────────────────────
# TABLE 3: MATERIAL UOM → CSV
# ─────────────────────────────────────────────

def generate_material_uom(material_ids, n_base=45000):
    print(f"Generating material_uom (~{n_base} rows)...")

    alt_uoms  = ["PAL","BOX","ST","CS","ROL","SET","BAG","CAN","DR","TUB"]
    rows      = []
    record_id = 1
    used      = set()

    while len(rows) < n_base:
        mid = random.choice(material_ids)
        alt = random.choice(alt_uoms)
        if (mid, alt) in used:
            continue
        used.add((mid, alt))

        num = round(random.uniform(0.1, 1000), 3)
        den = round(random.uniform(0.1, 100),  3)
        if random.random() < 0.03: num = 0
        if random.random() < 0.02: den = 0

        rows.append({
            "record_id":   record_id,
            "material_id": mid,
            "alt_uom":     messy_uom(alt),
            "numerator":   num,
            "denominator": den,
            "ean_upc":     None if random.random() < 0.15 else (
                           "5011234567890" if random.random() < 0.05
                           else f"501{random.randint(1000000000,9999999999)}"),
            "length": None if random.random() < 0.05
                      else round(random.uniform(1, 2000), 3),
            "width":  None if random.random() < 0.05
                      else round(random.uniform(1, 2000), 3),
            "height": None if random.random() < 0.05
                      else round(random.uniform(1, 2000), 3),
            "volume": None if random.random() < 0.04
                      else round(random.uniform(0.001, 5000), 3),
            "gross_weight": None if random.random() < 0.06
                            else round(random.uniform(0.1, 10000), 3),
            "created_date": messy_date(random_date(2018, 2024)),
        })
        record_id += 1

    df = pd.DataFrame(rows)

    # Add 4% orphaned records
    n_orph = int(len(df) * 0.04)
    orph   = df.sample(n=n_orph).copy()
    orph["material_id"] = [f"GHOST{str(i).zfill(5)}"
                           for i in range(50000, 50000 + n_orph)]
    orph["record_id"]   = range(record_id, record_id + n_orph)
    df = pd.concat([df, orph], ignore_index=True)

    path = f"{OUTPUT_DIR}/bronze_material_uom.csv"
    df.to_csv(path, index=False)
    print(f"  Saved: {path} ({len(df):,} rows)\n")
    return df


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("PrecisionManufacturing Inc. - Domain 1 CSV Generation")
    print("=" * 60)

    df_mm  = generate_material_master(n=15000)

    clean_ids = [f"MAT{str(i).zfill(6)}" for i in range(1, 15001)]

    df_pd  = generate_material_plant_data(clean_ids, n_base=35000)
    df_uom = generate_material_uom(clean_ids, n_base=45000)

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  material_master:     {len(df_mm):>8,} rows → CSV")
    print(f"  material_plant_data: {len(df_pd):>8,} rows → CSV")
    print(f"  material_uom:        {len(df_uom):>8,} rows → CSV")
    total = len(df_mm) + len(df_pd) + len(df_uom)
    print(f"  TOTAL:               {total:>8,} rows")
    print("=" * 60)
    print(f"\nFiles saved to: ./{OUTPUT_DIR}/")
    print("  bronze_material_master.csv")
    print("  bronze_material_plant_data.csv")
    print("  bronze_material_uom.csv")