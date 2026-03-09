"""
Domain 4: Plant Maintenance Data Generation Script
PrecisionManufacturing Inc. - SAP S/4HANA Migration Project

Generates:
  - functional_locations  → XML          (~2,625 rows)
  - equipment_master      → CSV          (~10,600 rows)
  - maintenance_plans     → Excel        (~8,400 rows)

Total: ~21,625 rows
"""

import pandas as pd
import numpy as np
import random
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

def random_date(start_year=2000, end_year=2024):
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
        "M":   ["M","m","Meter","METER","Mtr"],
    }
    if uom in variations and random.random() < 0.30:
        return random.choice(variations[uom][1:])
    return uom

# Shared reference data
plants             = ["CHI1","HOU2","DET3"]
invalid_plants     = ["PLT1","XXXX","0000","CHI2"]
generic_users      = ["ADMIN","MIGRATE","SYSTEM","admin","migrate"]
real_users         = [f"USR{str(i).zfill(3)}" for i in range(1, 51)]
cost_centers       = [f"CC{str(i).zfill(6)}" for i in range(1, 151)]
invalid_cc         = ["CC999999","CCXXXXXX","N/A","TBD","0000"]
company_codes      = ["1000","2000","3000"]
invalid_cc_codes   = ["XXXX","9999","0000","CC01"]
work_centers       = [f"WC{str(i).zfill(4)}" for i in range(1, 51)]
planner_groups     = [f"PG{str(i).zfill(2)}" for i in range(1, 11)]
business_areas     = ["BA01","BA02","BA03","BA04"]

# Generate functional location IDs upfront for FK references
def generate_floc_ids(n=2500):
    floc_ids = []
    buildings = ["BLDG-A","BLDG-B","BLDG-C","BLDG-D"]
    lines     = ["LINE-01","LINE-02","LINE-03","LINE-04","LINE-05"]
    stations  = [f"STN-{str(i).zfill(2)}" for i in range(1, 11)]

    for plant in plants:
        for bldg in buildings:
            for line in lines:
                for stn in stations[:random.randint(2,5)]:
                    floc_ids.append(f"{plant}-{bldg}-{line}-{stn}")
                    if len(floc_ids) >= n:
                        return floc_ids
    return floc_ids

valid_floc_ids = generate_floc_ids(2500)


# ─────────────────────────────────────────────
# TABLE 1: FUNCTIONAL LOCATIONS → XML
# ─────────────────────────────────────────────

def generate_functional_locations(n=2500):
    print(f"Generating functional_locations ({n} rows)...")

    floc_categories    = ["M","E","S","I","R"]
    bad_floc_cats      = ["Machine","EQP","SYSTEM","m","e","MACH"]
    structure_inds     = ["A","B","C","D"]
    active_flags       = ["Y","N"]
    bad_active_flags   = ["Yes","No","Active","Inactive","1","0","TRUE"]

    buildings = ["BLDG-A","BLDG-B","BLDG-C","BLDG-D"]
    floors    = ["1","2","3","4","01","02","03","Floor1","Floor2","GND","G"]
    rooms     = [f"R{str(i).zfill(3)}" for i in range(1, 50)]

    root = ET.Element("FunctionalLocations")
    root.set("version", "1.0")
    root.set("extractedAt", datetime.now().isoformat())
    root.set("system", "SAP_ECC_600")
    root.set("totalRecords", str(n))

    for i, floc_id in enumerate(valid_floc_ids[:n]):
        plant      = floc_id.split("-")[0]
        created_dt = random_date(2010, 2022)
        changed_dt = random_date(2022, 2024)

        # 5% invalid FLOC structure
        display_id = floc_id
        if random.random() < 0.05:
            display_id = floc_id.replace("-", random.choice(["/",".","|","_",""]))

        floc = ET.SubElement(root, "FunctionalLocation")
        floc.set("id", str(300000 + i))

        ET.SubElement(floc, "FlocId").text          = display_id
        ET.SubElement(floc, "Description").text     = (
            None if random.random() < 0.08 else
            random.choice([
                f"{plant} {floc_id} Assembly Line",
                f"{plant} {floc_id} ASSEMBLY LINE",
                f"  {plant} {floc_id} Assembly Line  ",
                f"{plant} {floc_id} Montagelinie",      # German
                f"{plant} {floc_id} Línea de ensamble", # Spanish
            ])
        )
        ET.SubElement(floc, "FlocCategory").text    = (
            random.choice(bad_floc_cats) if random.random() < 0.05
            else random.choice(floc_categories)
        )
        ET.SubElement(floc, "StructureIndicator").text = (
            None if random.random() < 0.06
            else random.choice(structure_inds)
        )
        ET.SubElement(floc, "PlantId").text         = (
            random.choice(invalid_plants) if random.random() < 0.03
            else plant
        )
        ET.SubElement(floc, "Location").text        = (
            None if random.random() < 0.07
            else f"LOC{str(random.randint(1,99)).zfill(3)}"
        )
        ET.SubElement(floc, "Building").text        = (
            None if random.random() < 0.08
            else random.choice(buildings)
        )
        ET.SubElement(floc, "Floor").text           = (
            None if random.random() < 0.10
            else random.choice(floors)
        )
        ET.SubElement(floc, "Room").text            = (
            None if random.random() < 0.12
            else random.choice(rooms)
        )
        ET.SubElement(floc, "CostCenter").text      = (
            None if random.random() < 0.08 else
            random.choice(invalid_cc) if random.random() < 0.05
            else random.choice(cost_centers)
        )
        ET.SubElement(floc, "CompanyCode").text     = (
            random.choice(invalid_cc_codes) if random.random() < 0.05
            else random.choice(company_codes)
        )
        ET.SubElement(floc, "BusinessArea").text    = (
            None if random.random() < 0.06
            else random.choice(business_areas)
        )
        ET.SubElement(floc, "WorkCenter").text      = (
            None if random.random() < 0.07
            else random.choice(work_centers)
        )
        ET.SubElement(floc, "PlannerGroup").text    = (
            None if random.random() < 0.08
            else random.choice(planner_groups)
        )
        ET.SubElement(floc, "MainWorkCenter").text  = (
            None if random.random() < 0.07
            else random.choice(work_centers)
        )
        ET.SubElement(floc, "SortField").text       = (
            None if random.random() < 0.15
            else f"SORT-{floc_id}"
        )
        ET.SubElement(floc, "ActiveFlag").text      = (
            random.choice(bad_active_flags) if random.random() < 0.05
            else random.choice(active_flags)
        )
        ET.SubElement(floc, "CreatedBy").text       = (
            random.choice(generic_users) if random.random() < 0.15
            else random.choice(real_users)
        )
        ET.SubElement(floc, "CreatedDate").text     = messy_date(created_dt)
        ET.SubElement(floc, "ChangedDate").text     = (
            messy_date(changed_dt) if random.random() > 0.05
            else messy_date(created_dt - timedelta(days=random.randint(1,30)))
        )

    # Add ~125 invalid/orphaned records
    for i in range(125):
        floc = ET.SubElement(root, "FunctionalLocation")
        floc.set("id", str(400000 + i))
        ET.SubElement(floc, "FlocId").text     = f"INVALID-FLOC-{str(i).zfill(5)}"
        ET.SubElement(floc, "Description").text= "ORPHANED RECORD - NO PLANT"
        ET.SubElement(floc, "PlantId").text    = None
        ET.SubElement(floc, "ActiveFlag").text = "N"
        ET.SubElement(floc, "CreatedBy").text  = "MIGRATE"
        ET.SubElement(floc, "CreatedDate").text= messy_date(random_date(2010,2015))
        ET.SubElement(floc, "ChangedDate").text= None

    xml_str = minidom.parseString(
        ET.tostring(root, encoding="unicode")
    ).toprettyxml(indent="  ")

    path = f"{OUTPUT_DIR}/bronze_functional_locations.xml"
    with open(path, "w") as f:
        f.write(xml_str)

    total = n + 125
    print(f"  Saved: {path} ({total:,} records)\n")
    return total


# ─────────────────────────────────────────────
# TABLE 2: EQUIPMENT MASTER → CSV
# ─────────────────────────────────────────────

def generate_equipment_master(n=10000):
    print(f"Generating equipment_master ({n} rows)...")

    equip_categories   = ["M","V","E","I","R","P"]
    bad_equip_cats     = ["MACH","Veh","elec","machine","VEHICLE","mach"]
    equip_types        = ["PUMP","COMP","CONV","MOTOR","ROBOT","PRESS",
                          "DRILL","LATHE","WELD","CNC","HVAC","CRANE"]
    manufacturers      = ["Siemens","SIEMENS","siemens","ABB","Abb","abb",
                          "Bosch","BOSCH","bosch","Fanuc","FANUC",
                          "Rockwell","ROCKWELL","Mitsubishi","GE","ge","G.E."]
    active_flags       = ["Y","N"]
    bad_active_flags   = ["Yes","No","Active","Decommissioned","1","0"]
    currencies         = ["USD","EUR","GBP"]
    bad_currencies     = ["Dollars","DOLLAR","$","Euro"]
    size_units         = ["MM","CM","M","IN","FT"]
    bad_size_units     = ["Millimeter","mm","in","Inch","feet","ft"]

    rows = []
    for i in range(1, n + 1):
        equip_id   = f"EQ{str(i).zfill(8)}"
        plant      = random.choice(plants)
        created_dt = random_date(2005, 2022)
        acq_dt     = random_date(2000, 2022)
        startup_dt = acq_dt + timedelta(days=random.randint(30, 365))
        warranty_dt= startup_dt + timedelta(days=random.randint(365, 1825))

        # Year constructed anomalies
        year_built = acq_dt.year
        if random.random() < 0.05:
            year_built = random.randint(2025, 2030)  # future year
        elif random.random() < 0.03:
            year_built = random.randint(1850, 1899)  # pre-1900

        # Acquisition value
        acq_value = round(random.uniform(1000, 2000000), 2)
        if random.random() < 0.06: acq_value = -abs(acq_value)
        if random.random() < 0.04: acq_value = 0

        rows.append({
            "equipment_id":          equip_id,
            "equipment_description": None if random.random() < 0.08 else (
                                     random.choice([
                                         f"{random.choice(equip_types)} Unit {i}",
                                         f"{random.choice(equip_types)} UNIT {i}",
                                         f"  {random.choice(equip_types)} Unit {i}  ",
                                         f"{random.choice(equip_types)} Einheit {i}",
                                     ])),
            "equipment_category":    random.choice(bad_equip_cats)
                                     if random.random() < 0.05
                                     else random.choice(equip_categories),
            "equipment_type":        None if random.random() < 0.07
                                     else random.choice(equip_types),

            # 8% orphaned FLOC reference
            "floc_id":               f"INVALID-FLOC-{str(random.randint(1,999)).zfill(5)}"
                                     if random.random() < 0.08
                                     else random.choice(valid_floc_ids),

            "plant_id":              random.choice(invalid_plants)
                                     if random.random() < 0.03 else plant,

            # 4% maintenance plant differs from plant
            "maintenance_plant":     random.choice([p for p in plants if p != plant])
                                     if random.random() < 0.04 else plant,

            "location":              None if random.random() < 0.06
                                     else f"LOC{str(random.randint(1,99)).zfill(3)}",
            "cost_center":           None if random.random() < 0.09 else (
                                     random.choice(invalid_cc) if random.random() < 0.05
                                     else random.choice(cost_centers)),
            "company_code":          random.choice(invalid_cc_codes)
                                     if random.random() < 0.05
                                     else random.choice(company_codes),

            # Manufacturer inconsistencies
            "manufacturer":          None if random.random() < 0.10
                                     else random.choice(manufacturers),
            "model_number":          None if random.random() < 0.12
                                     else f"MDL-{str(random.randint(1000,9999))}",

            # Serial number — 8% null, 5% duplicate
            "serial_number":         None if random.random() < 0.08 else (
                                     "SN-DUPLICATE-001" if random.random() < 0.05
                                     else f"SN-{str(random.randint(100000,999999))}"),

            "year_constructed":      year_built,
            "acquisition_date":      messy_date(acq_dt),
            "acquisition_value":     acq_value,
            "currency":              None if random.random() < 0.04 else (
                                     random.choice(bad_currencies)
                                     if random.random() < 0.05
                                     else random.choice(currencies)),

            # Weight — 7% null, 3% negative
            "weight":                None if random.random() < 0.07 else (
                                     -abs(round(random.uniform(10,50000),3))
                                     if random.random() < 0.03
                                     else round(random.uniform(10, 50000), 3)),

            "size_unit":             random.choice(bad_size_units)
                                     if random.random() < 0.15
                                     else random.choice(size_units),

            # 5% startup before acquisition
            "start_up_date":         messy_date(
                                         acq_dt - timedelta(days=random.randint(1,30))
                                         if random.random() < 0.05
                                         else startup_dt),

            # 6% warranty end before startup
            "warranty_end_date":     messy_date(
                                         startup_dt - timedelta(days=random.randint(1,365))
                                         if random.random() < 0.06
                                         else warranty_dt),

            "active_flag":           random.choice(bad_active_flags)
                                     if random.random() < 0.05
                                     else random.choice(active_flags),
            "created_by":            random.choice(generic_users)
                                     if random.random() < 0.15
                                     else random.choice(real_users),
            "created_date":          messy_date(created_dt),
            "changed_date":          messy_date(random_date(2022,2024))
                                     if random.random() > 0.05
                                     else messy_date(
                                         created_dt - timedelta(days=random.randint(1,30))
                                     ),
            "legacy_equipment_id":   None if random.random() < 0.15
                                     else f"LEQ{str(random.randint(1,99999)).zfill(5)}",
        })

    df = pd.DataFrame(rows)

    # Add 6% duplicates
    n_dups = int(len(df) * 0.06)
    dups   = df.sample(n=n_dups, replace=True).copy()
    def vary_equip_id(eid):
        c = random.randint(0, 2)
        if c == 0: return eid.replace("EQ","EQ-")
        if c == 1: return eid.replace("EQ","EQ ")
        return eid.lower()
    dups["equipment_id"] = dups["equipment_id"].apply(vary_equip_id)
    df = pd.concat([df, dups], ignore_index=True)

    path = f"{OUTPUT_DIR}/bronze_equipment_master.csv"
    df.to_csv(path, index=False)
    print(f"  Saved: {path} ({len(df):,} rows)\n")
    return df


# ─────────────────────────────────────────────
# TABLE 3: MAINTENANCE PLANS → EXCEL
# ─────────────────────────────────────────────

def generate_maintenance_plans(n=8000):
    print(f"Generating maintenance_plans ({n} rows)...")

    plan_types        = ["SP","MP","ST"]
    bad_plan_types    = ["Single","MULTI","Cycle","sp","mp","SINGLE"]
    strategies        = ["PM01","PM02","PM03","PM04","PM05"]
    cycle_units       = ["H","D","MON","YR"]
    bad_cycle_units   = ["Hours","HOURS","Days","DAYS","d","months","Months","MON "]
    priorities        = ["1","2","3","4"]
    bad_priorities    = ["HIGH","Med","Low","VERY HIGH","0","5","high","medium"]
    bad_active_flags  = ["Yes","No","Active","Decommissioned","1","0"]          
    statuses          = ["Ready","Approved","APPROVED","In Review",
                         "Pending","TBD","?","NOT STARTED"]
    comments          = [
        None, None, None,
        "Annual inspection — mandatory",
        "CONFIRM CYCLE WITH ENGINEERING",
        "decommissioned equipment — remove plan",
        "migrated from legacy CMMS",
        "cycle updated per OEM recommendation",
        "cost center TBD","ok","??","TO DO",
        "safety critical — do not skip",
    ]

    # Equipment IDs for FK references
    active_equip_ids  = [f"EQ{str(i).zfill(8)}" for i in range(1, 9001)]
    inactive_equip_ids= [f"EQ{str(i).zfill(8)}" for i in range(9001, 10001)]

    rows = []
    for i in range(1, n + 1):
        plan_id    = f"PLAN{str(i).zfill(7)}"
        plan_type  = random.choice(plan_types)
        cycle_unit = random.choice(cycle_units)
        created_dt = random_date(2015, 2023)
        last_dt    = random_date(2022, 2024)
        next_dt    = last_dt + timedelta(days=random.randint(30, 365))

        # 7% reference decommissioned equipment
        equip_id   = random.choice(inactive_equip_ids) \
                     if random.random() < 0.07 \
                     else random.choice(active_equip_ids)

        cycle_len  = round(random.uniform(1, 8760), 3)

        rows.append({
            "Plan ID":            plan_id,
            "Plan Description":   None if random.random() < 0.08 else (
                                  random.choice([
                                      f"Maintenance Plan {i} - {random.choice(cycle_units)}",
                                      f"MAINTENANCE PLAN {i}",
                                      f"  Maintenance Plan {i}  ",
                                      f"Wartungsplan {i}",  # German
                                  ])),
            "Plan Type":          random.choice(bad_plan_types)
                                  if random.random() < 0.06
                                  else plan_type,
            "Equipment ID":       equip_id,

            # 5% orphaned FLOC
            "Functional Location":f"INVALID-FLOC-{str(random.randint(1,99)).zfill(5)}"
                                  if random.random() < 0.05
                                  else random.choice(valid_floc_ids),

            "Maintenance Strategy": None if random.random() < 0.08
                                    else random.choice(strategies),

            # 5% negative, 4% zero cycle length
            "Cycle Length":       -abs(cycle_len) if random.random() < 0.05 else (
                                  0 if random.random() < 0.04 else cycle_len),

            "Cycle Unit":         random.choice(bad_cycle_units)
                                  if random.random() < 0.15
                                  else cycle_unit,

            # 4% negative, 5% zero duration
            "Estimated Duration (hrs)": -abs(round(random.uniform(0.5,40),2))
                                        if random.random() < 0.04 else (
                                        0 if random.random() < 0.05
                                        else round(random.uniform(0.5, 40), 2)),

            "Work Center":        None if random.random() < 0.07
                                  else random.choice(work_centers),
            "Planner Group":      None if random.random() < 0.08
                                  else random.choice(planner_groups),
            "Cost Center":        None if random.random() < 0.09 else (
                                  random.choice(invalid_cc) if random.random() < 0.05
                                  else random.choice(cost_centers)),

            # 5% invalid priority
            "Priority":           random.choice(bad_priorities)
                                  if random.random() < 0.05
                                  else random.choice(priorities),

            # 6% future last performed date
            "Last Performed Date":messy_date(
                                      random_date(2025, 2026)
                                      if random.random() < 0.06
                                      else last_dt),

            # 5% next due before last performed
            "Next Due Date":      messy_date(
                                      last_dt - timedelta(days=random.randint(1,30))
                                      if random.random() < 0.05
                                      else next_dt),

            # 6% > 100, 4% negative call horizon
            "Call Horizon %":     round(random.uniform(101,200),2)
                                  if random.random() < 0.06 else (
                                  -abs(round(random.uniform(1,50),2))
                                  if random.random() < 0.04
                                  else round(random.uniform(10,90),2)),

            "Active Flag":        random.choice(bad_active_flags)
                                  if random.random() < 0.05
                                  else random.choice(["Y","N"]),
            "Created By":         random.choice(generic_users)
                                  if random.random() < 0.15
                                  else random.choice(real_users),
            "Created Date":       messy_date(created_dt),
            "Status":             random.choice(statuses),
            "Comments":           random.choice(comments),
        })

    df = pd.DataFrame(rows)

    # Add 5% duplicates
    n_dups = int(len(df) * 0.05)
    dups   = df.sample(n=n_dups, replace=True).copy()
    dups["Plan ID"] = [
        f"PLAN-{str(random.randint(1,9999999)).zfill(7)}"
        for _ in range(n_dups)
    ]
    df = pd.concat([df, dups], ignore_index=True)

    path = f"{OUTPUT_DIR}/bronze_maintenance_plans.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Maintenance Plans", index=False)

        instructions = pd.DataFrame({
            "Field":       ["Plan Type","Cycle Unit","Priority",
                            "Call Horizon %","Last Performed Date"],
            "Required":    ["Yes","Yes","Yes","Yes","No"],
            "Format":      ["2 char","1-3 char","1 char","Decimal 0-100","Date YYYY-MM-DD"],
            "Description": [
                "SP=Single Cycle, MP=Multiple Counter, ST=Strategy Plan",
                "H=Hours, D=Days, MON=Months, YR=Years",
                "1=Very High, 2=High, 3=Medium, 4=Low",
                "% of cycle remaining when maintenance order triggered",
                "Date last maintenance was completed"
            ]
        })
        instructions.to_excel(writer, sheet_name="Instructions", index=False)

        valid_vals = pd.DataFrame({
            "Field":   ["Plan Type","Plan Type","Plan Type",
                        "Priority","Priority","Priority","Priority"],
            "Value":   ["SP","MP","ST","1","2","3","4"],
            "Meaning": ["Single Cycle Plan","Multiple Counter Plan",
                        "Strategy Plan","Very High","High","Medium","Low"]
        })
        valid_vals.to_excel(writer, sheet_name="Valid Values", index=False)

    print(f"  Saved: {path} ({len(df):,} rows, 3 sheets)\n")
    return df


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("PrecisionManufacturing Inc. - Domain 4 Data Generation")
    print("=" * 60)

    n_floc   = generate_functional_locations(n=2500)
    df_equip = generate_equipment_master(n=10000)
    df_plans = generate_maintenance_plans(n=8000)

    print("=" * 60)
    print("DOMAIN 4 GENERATION SUMMARY")
    print("=" * 60)
    print(f"  functional_locations (XML):  {n_floc:>8,} records")
    print(f"  equipment_master (CSV):      {len(df_equip):>8,} rows")
    print(f"  maintenance_plans (Excel):   {len(df_plans):>8,} rows")
    total = n_floc + len(df_equip) + len(df_plans)
    print(f"  TOTAL:                       {total:>8,} rows")
    print("=" * 60)
    print(f"\nFiles saved to: ./{OUTPUT_DIR}/")
    print("  bronze_functional_locations.xml")
    print("  bronze_equipment_master.csv")
    print("  bronze_maintenance_plans.xlsx")
    print("\nCumulative bronze_data/ totals:")
    print("  Domain 1:  ~101,700 rows  (7 files)")
    print("  Domain 2:  ~ 49,870 rows  (4 files)")
    print("  Domain 3:  ~ 38,655 rows  (3 files)")
    print("  Domain 4:  ~ 21,625 rows  (3 files)")
    print("  RUNNING TOTAL: ~211,850 rows across 17 files")