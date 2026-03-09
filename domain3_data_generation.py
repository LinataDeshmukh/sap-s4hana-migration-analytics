"""
Domain 3: Planning / MRP Data Generation Script
PrecisionManufacturing Inc. - SAP S/4HANA Migration Project

Generates:
  - mrp_parameters      → CSV         (~37,100 rows)
  - mrp_controllers     → JSON        (~55 rows)
  - planning_calendar   → Excel       (~1,500 rows)

Total: ~38,655 rows
"""

from csv import writer

import pandas as pd
import numpy as np
import random
import json
from datetime import datetime, timedelta
import os

from tensorboard import summary

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

# Shared reference data
clean_material_ids = [f"MAT{str(i).zfill(6)}" for i in range(1, 15001)]
plants             = ["CHI1","HOU2","DET3"]
invalid_plants     = ["PLT1","XXXX","0000","CHI2"]
generic_users      = ["ADMIN","MIGRATE","SYSTEM","admin","migrate"]
real_users         = [f"USR{str(i).zfill(3)}" for i in range(1, 51)]
valid_controllers  = [f"MC{str(i).zfill(2)}" for i in range(1, 21)]
invalid_controllers= ["MCX1","NONE","TBD","N/A","MC99","MC00","??"]


# ─────────────────────────────────────────────
# TABLE 1: MRP PARAMETERS → CSV
# ─────────────────────────────────────────────

def generate_mrp_parameters(n=35000):
    print(f"Generating mrp_parameters ({n} rows)...")

    mrp_types        = ["PD","VB","ND","MK","PK","VV"]
    bad_mrp_types    = ["AUTO","MANUAL","YES","MRP","pd","vb","Auto"]
    lot_size_keys    = ["EX","FX","HB","WB","MB","TB","ZB"]
    bad_lot_keys     = ["FIXED","EXACT","Replenish","fixed","HB ","EX "]
    stor_locs        = [f"SL{str(i).zfill(2)}" for i in range(1, 31)]
    bad_stor_locs    = ["SL99","SLXX","0000","N/A","????"]
    spec_procs       = ["10","20","30","40","50","52","60"]
    sched_margins    = ["000","001","002","003","S01","S02"]
    avail_checks     = ["01","02","KP","CH","02","AV"]
    bad_avail        = ["YES","CHECK","1","FULL","avail"]
    mrp_areas        = [f"MRP{str(i).zfill(4)}" for i in range(1, 20)]
    backward_valid   = ["X",""]
    bad_backward     = ["Yes","B","1","Y","backward","FWD"]

    rows      = []
    record_id = 1

    for i in range(n):
        mid      = random.choice(clean_material_ids)
        plant    = random.choice(plants)
        lot_key  = random.choice(lot_size_keys)
        mrp_type = random.choice(mrp_types)
        min_lot  = round(random.uniform(1, 500), 3)
        max_lot  = round(min_lot * random.uniform(5, 50), 3)
        created_dt = random_date(2018, 2023)
        changed_dt = random_date(2023, 2024)

        rows.append({
            "record_id":   record_id,

            # 6% orphaned material
            "material_id": f"GHOST{str(random.randint(1,999)).zfill(5)}"
                           if random.random() < 0.06 else mid,

            # 3% invalid plant
            "plant_id":    random.choice(invalid_plants)
                           if random.random() < 0.03 else plant,

            # 8% null, 5% invalid MRP type
            "mrp_type":    None if random.random() < 0.08 else (
                           random.choice(bad_mrp_types) if random.random() < 0.05
                           else mrp_type),

            # 10% invalid controller
            "mrp_controller": random.choice(invalid_controllers)
                              if random.random() < 0.10
                              else random.choice(valid_controllers),

            # 6% null lot size key, some invalid
            "lot_size_key": None if random.random() < 0.06 else (
                            random.choice(bad_lot_keys) if random.random() < 0.05
                            else lot_key),

            # 5% null when lot_size=FX
            "fixed_lot_size": None if (lot_key == "FX" and random.random() < 0.05)
                              else round(random.uniform(10, 1000), 3),

            # 4% negative min lot
            "minimum_lot_size": -abs(min_lot) if random.random() < 0.04 else min_lot,

            # 3% max < min
            "maximum_lot_size": round(min_lot * 0.5, 3)
                                if random.random() < 0.03 else max_lot,

            # 5% negative rounding value
            "rounding_value": -abs(round(random.uniform(1,100),3))
                              if random.random() < 0.05
                              else round(random.uniform(0, 100), 3),

            # 4% negative reorder point
            "reorder_point": -abs(round(random.uniform(1,500),3))
                             if random.random() < 0.04
                             else round(random.uniform(0, 500), 3),

            # 8% null safety stock for MRP-active materials
            "safety_stock": None if (mrp_type in ["PD","VB"] and random.random() < 0.08)
                            else round(random.uniform(0, 1000), 3),

            # 5% negative safety time
            "safety_time": -random.randint(1,10) if random.random() < 0.05
                           else random.randint(0, 30),

            # 3% zero, 2% negative delivery days
            "planned_delivery_days": (
                0 if random.random() < 0.03 else
                -random.randint(1,10) if random.random() < 0.02
                else random.randint(1, 120)
            ),

            # 5% null GR days
            "goods_receipt_days": None if random.random() < 0.05
                                  else random.randint(0, 10),

            # 6% null in-house production days for HALB/FERT
            "in_house_production_days": None if random.random() < 0.06
                                        else random.randint(0, 60),

            # 8% null scheduling margin
            "scheduling_margin_key": None if random.random() < 0.08
                                     else random.choice(sched_margins),

            # 7% null, some invalid availability check
            "availability_check": None if random.random() < 0.07 else (
                                  random.choice(bad_avail) if random.random() < 0.05
                                  else random.choice(avail_checks)),

            # 5% invalid backward scheduling flag
            "backward_scheduling": random.choice(bad_backward)
                                   if random.random() < 0.05
                                   else random.choice(backward_valid),

            # 7% invalid storage location
            "storage_location": random.choice(bad_stor_locs)
                                if random.random() < 0.07
                                else random.choice(stor_locs),

            # 10% null special procurement
            "special_procurement": None if random.random() < 0.10
                                   else random.choice(spec_procs),

            # 12% null MRP area
            "mrp_area": None if random.random() < 0.12
                        else random.choice(mrp_areas),

            # 5% out of range consumption mode (valid: 1,2,3)
            "consumption_mode": random.choice([0,4,5,9,99])
                                if random.random() < 0.05
                                else random.choice([1, 2, 3]),

            # 6% negative consumption days
            "fwd_consumption_days": -random.randint(1,30)
                                    if random.random() < 0.06
                                    else random.randint(0, 90),

            "bwd_consumption_days": -random.randint(1,30)
                                    if random.random() < 0.06
                                    else random.randint(0, 90),

            "created_by":   random.choice(generic_users) if random.random() < 0.15
                            else random.choice(real_users),
            "created_date": messy_date(created_dt),
            "changed_date": messy_date(changed_dt) if random.random() > 0.05
                            else messy_date(
                                created_dt - timedelta(days=random.randint(1,30))
                            ),
        })
        record_id += 1

    df = pd.DataFrame(rows)

    # Add 6% orphaned records
    n_orph = int(len(df) * 0.06)
    orph   = df.sample(n=n_orph).copy()
    orph["material_id"] = [
        f"GHOST{str(i).zfill(5)}" for i in range(20000, 20000 + n_orph)
    ]
    orph["record_id"] = range(record_id, record_id + n_orph)
    df = pd.concat([df, orph], ignore_index=True)

    path = f"{OUTPUT_DIR}/bronze_mrp_parameters.csv"
    df.to_csv(path, index=False)
    print(f"  Saved: {path} ({len(df):,} rows)\n")
    return df


# ─────────────────────────────────────────────
# TABLE 2: MRP CONTROLLERS → JSON
# ─────────────────────────────────────────────

def generate_mrp_controllers():
    print("Generating mrp_controllers (~55 rows)...")

    departments = [
        "Production Planning","Supply Chain","Procurement",
        "Prod Planning","supply chain","PRODUCTION PLANNING",
        "SC","PP Dept","Operations"
    ]
    active_flags     = ["Y","N"]
    bad_active_flags = ["Yes","No","1","0","Active","Inactive","TRUE","FALSE"]

    first_names = ["James","Sarah","Michael","Emily","Robert","Jennifer",
                   "David","Lisa","John","Mary","Chris","Patricia","Mark","Linda"]
    last_names  = ["Smith","Johnson","Williams","Brown","Jones","Garcia",
                   "Miller","Davis","Wilson","Taylor","Anderson","Thomas"]

    records = []

    for i in range(1, 21):
        ctrl_id    = f"MC{str(i).zfill(2)}"
        fname      = random.choice(first_names)
        lname      = random.choice(last_names)
        plant      = random.choice(plants)
        created_dt = random_date(2015, 2022)

        # Email variations — 10% null, 8% invalid format
        if random.random() < 0.10:
            email = None
        elif random.random() < 0.08:
            email = random.choice([
                f"{fname.lower()}.{lname.lower()}",        # missing @domain
                f"{fname.lower()}@",                        # incomplete
                f"@{lname.lower()}.com",                    # missing local part
                f"{fname.lower()}.{lname.lower()}@company", # missing TLD
            ])
        else:
            email = f"{fname.lower()}.{lname.lower()}@precisionmfg.com"

        # Phone variations — 15% null, inconsistent formats
        if random.random() < 0.15:
            phone = None
        else:
            phone = random.choice([
                f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
                f"({random.randint(200,999)}) {random.randint(100,999)}-{random.randint(1000,9999)}",
                f"{random.randint(2000000000,9999999999)}",  # no formatting
                f"{random.randint(200,999)}.{random.randint(100,999)}.{random.randint(1000,9999)}",
                f"ext {random.randint(1000,9999)}",          # extension only
            ])

        record = {
            "controllerId":   ctrl_id,
            "controllerName": None if random.random() < 0.05 else (
                              f"{fname} {lname}  "   # trailing space 5%
                              if random.random() < 0.05
                              else f"{fname} {lname}"),
            "plantId":        random.choice(invalid_plants) if random.random() < 0.03
                              else plant,
            "email":          email,
            "phone":          phone,
            "department":     None if random.random() < 0.05
                              else random.choice(departments),
            "activeFlag":     random.choice(bad_active_flags) if random.random() < 0.05
                              else random.choice(active_flags),
            "createdDate":    messy_date(created_dt),
            "auditInfo": {
                "source":      random.choice(["HR_SYSTEM","MANUAL","MIGRATE"]),
                "extractedAt": datetime.now().isoformat(),
            }
        }

        # 3% duplicate with case variation
        records.append(record)
        if random.random() < 0.03:
            dup = record.copy()
            dup["controllerId"] = ctrl_id.lower()
            records.append(dup)

    # Add a few invalid controller records that are referenced
    # in mrp_parameters but don't actually exist as valid controllers
    for ghost_ctrl in ["MCX1","MC99","MC00"]:
        records.append({
            "controllerId":   ghost_ctrl,
            "controllerName": "INVALID - DO NOT USE",
            "plantId":        None,
            "email":          None,
            "phone":          None,
            "department":     None,
            "activeFlag":     "N",
            "createdDate":    messy_date(random_date(2015, 2018)),
            "auditInfo": {
                "source":      "LEGACY_MIGRATION",
                "extractedAt": datetime.now().isoformat(),
            }
        })

    path = f"{OUTPUT_DIR}/bronze_mrp_controllers.json"
    with open(path, "w") as f:
        json.dump({
            "apiVersion":  "1.0.0",
            "extractedAt": datetime.now().isoformat(),
            "source":      "HR_PLANNING_SYSTEM",
            "recordCount": len(records),
            "data":        records
        }, f, indent=2)

    print(f"  Saved: {path} ({len(records):,} records)\n")
    return records


# ─────────────────────────────────────────────
# TABLE 3: PLANNING CALENDAR → EXCEL
# ─────────────────────────────────────────────

def generate_planning_calendar():
    print("Generating planning_calendar (~1,500 rows)...")

    calendar_types     = ["WK","SH","PL"]
    bad_calendar_types = ["WEEK","SHIFT","Plant","wk","sh","W","S"]

    # Day name variations
    day_variants = {
        "Monday":    ["Monday","MONDAY","Mon","MON","mon","Mo"],
        "Tuesday":   ["Tuesday","TUESDAY","Tue","TUE","tue","Tu"],
        "Wednesday": ["Wednesday","WEDNESDAY","Wed","WED","wed","We"],
        "Thursday":  ["Thursday","THURSDAY","Thu","THU","thu","Th"],
        "Friday":    ["Friday","FRIDAY","Fri","FRI","fri","Fr"],
        "Saturday":  ["Saturday","SATURDAY","Sat","SAT","sat","Sa"],
        "Sunday":    ["Sunday","SUNDAY","Sun","SUN","sun","Su"],
    }

    # Working day flag variations
    working_day_variants = {
        True:  ["Yes","YES","Y","1","TRUE","True","true","Working"],
        False: ["No","NO","N","0","FALSE","False","false","Holiday","Non-Working"],
    }

    # Shift time variations
    def messy_time(hour, minute=0):
        variants = [
            f"{hour:02d}:{minute:02d}",           # 08:00
            f"{hour}:{minute:02d}",               # 8:00
            f"{hour:02d}{minute:02d}",            # 0800
            f"{hour % 12 or 12}:{minute:02d} {'AM' if hour < 12 else 'PM'}",  # 8:00 AM
            f"{hour:02d}:{minute:02d}:00",        # 08:00:00
        ]
        return random.choice(variants)

    # US holidays
    holidays = {
        "01-01": "New Year's Day",
        "07-04": "Independence Day",
        "11-11": "Veterans Day",
        "12-25": "Christmas Day",
        "12-24": "Christmas Eve",
        "01-15": "Martin Luther King Day",
        "11-25": "Thanksgiving",
        "11-26": "Day after Thanksgiving",
        "05-27": "Memorial Day",
        "09-02": "Labor Day",
    }

    rows       = []
    record_id  = 1
    start_date = datetime(2020, 1, 1)
    end_date   = datetime(2024, 12, 31)

    current = start_date
    while current <= end_date:
        for plant in plants:
            day_name    = current.strftime("%A")
            month_day   = current.strftime("%m-%d")
            is_holiday  = month_day in holidays
            is_weekend  = current.weekday() >= 5
            is_working  = not is_holiday and not is_weekend

            # Messy working day flag
            working_flag = random.choice(working_day_variants[is_working])

            # Available hours
            if is_working:
                avail_hours = round(random.uniform(7.5, 8.5), 2)
                # 3% negative, 4% > 24
                if random.random() < 0.03:
                    avail_hours = -abs(avail_hours)
                elif random.random() < 0.04:
                    avail_hours = round(random.uniform(25, 30), 2)
            else:
                avail_hours = 0

            rows.append({
                "Calendar ID":        f"CAL{plant}{current.strftime('%Y')}",
                "Plant":              random.choice(invalid_plants)
                                      if random.random() < 0.03 else plant,
                "Calendar Date":      messy_date(current),
                "Day of Week":        random.choice(day_variants[day_name])
                                      if random.random() < 0.25 else day_name,
                "Is Working Day":     working_flag,
                "Shift Start":        messy_time(7, 30) if is_working else None,
                "Shift End":          messy_time(16, 0) if is_working else None,
                "Available Hours":    avail_hours,
                "Holiday Description":holidays.get(month_day, None),
                "Calendar Type":      random.choice(bad_calendar_types)
                                      if random.random() < 0.05
                                      else random.choice(calendar_types),
                "Created By":         random.choice(generic_users)
                                      if random.random() < 0.10
                                      else random.choice(real_users),
                "Remarks":            random.choice([
                                        None, None, None,
                                        "confirmed by plant manager",
                                        "NEEDS REVIEW",
                                        "updated for 2024 schedule",
                                        "public holiday - all plants",
                                        "partial day - 4hrs only",
                                        "TBD","?","ok",
                                      ]),
            })
            record_id += 1

        current += timedelta(days=1)

    df = pd.DataFrame(rows)

    path = f"{OUTPUT_DIR}/bronze_planning_calendar.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Planning Calendar", index=False)

        # Instructions sheet
        instructions = pd.DataFrame({
            "Field":       ["Is Working Day","Calendar Type",
                            "Shift Start","Available Hours"],
            "Required":    ["Yes","Yes","No","Yes"],
            "Format":      ["Y/N","2 char","HH:MM","Decimal"],
            "Description": [
                "Y=working day, N=non-working — must be consistent",
                "WK=weekly, SH=shift-based, PL=plant calendar",
                "24-hour format HH:MM — leave blank for non-working days",
                "Total available production hours — cannot exceed 24"
            ]
        })
        instructions.to_excel(writer, sheet_name="Instructions", index=False)

        # Summary by plant and year
        df["year"] = pd.to_datetime(
            df["Calendar Date"], errors="coerce"
        ).dt.year
        summary = df[df["Is Working Day"].isin(["Yes","YES","Y","1","TRUE","True","true","Working"])]
        summary = summary.groupby(["Plant","year"]).size().reset_index()
        summary.columns = ["Plant","Year","Working Days"]
        summary.to_excel(writer, sheet_name="Summary", index=False)

    print(f"  Saved: {path} ({len(df):,} rows, 3 sheets)\n")
    return df


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("PrecisionManufacturing Inc. - Domain 3 Data Generation")
    print("=" * 60)

    df_mrp     = generate_mrp_parameters(n=35000)
    ctrl_recs  = generate_mrp_controllers()
    df_cal     = generate_planning_calendar()

    print("=" * 60)
    print("DOMAIN 3 GENERATION SUMMARY")
    print("=" * 60)
    print(f"  mrp_parameters (CSV):       {len(df_mrp):>8,} rows")
    print(f"  mrp_controllers (JSON):     {len(ctrl_recs):>8,} records")
    print(f"  planning_calendar (Excel):  {len(df_cal):>8,} rows")
    total = len(df_mrp) + len(ctrl_recs) + len(df_cal)
    print(f"  TOTAL:                      {total:>8,} rows")
    print("=" * 60)
    print(f"\nFiles saved to: ./{OUTPUT_DIR}/")
    print("  bronze_mrp_parameters.csv")
    print("  bronze_mrp_controllers.json")
    print("  bronze_planning_calendar.xlsx")