# SAP S/4HANA Master Data Migration & Analytics Framework

End-to-end data migration pipeline simulating a real SAP S/4HANA 
master data migration for PrecisionManufacturing Inc. across 3 plants 
(Chicago, Houston, Detroit).

## Project Overview

A manufacturing company migrating from legacy ERP + Excel to SAP S/4HANA.
This project covers the full data migration lifecycle — from raw source 
extraction through bronze ingestion, silver cleansing, gold star schema, 
and Power BI migration readiness dashboards.

## Architecture
```
Source Systems (SAP Legacy, Excel, APIs)
        ↓
Bronze Layer  → Raw data landed as-is (MySQL)
        ↓
Silver Layer  → Cleaned, validated, deduplicated (dbt)
        ↓
Gold Layer    → Star schema, migration readiness metrics (dbt)
        ↓
Power BI      → Migration readiness dashboard
```

## Tech Stack

- Python 3.11
- MySQL 8.0
- dbt (coming)
- Apache Airflow (coming)
- Docker (coming)
- Power BI (coming)

## Bronze Layer — Current Status

| Format  | Files | Rows    | Status  |
|---------|-------|---------|---------|
| CSV     | 9     | 193,075 | Done    |
| Excel   | 6     | 66,656  | Done    |
| JSON    | 6     | 17,295  | Done    |
| XML     | 3     | 14,919  | Done    |
| Parquet | 4     | 94,600  | Done    |
| **Total** | **28** | **386,545** | ✅ |

## Domains Covered

1. Materials Master (PP)
2. Procurement & Vendor Master
3. Planning & MRP Parameters
4. Plant Maintenance
5. Warehouse & Logistics
6. Quality Management
7. Finance & Costing

## Setup

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Copy `config/settings_template.py` to `config/settings.py`
4. Fill in your MySQL credentials in `config/settings.py`
5. Run data generation scripts for each domain
6. Run bronze ingestion: `python ingestion/run_all_bronze.py`

## Project Structure
```
├── bronze_data/          # Raw source files (not in repo — generate locally)
├── config/
│   ├── settings_template.py
│   └── settings.py       # Local only — not committed (has credentials)
├── ingestion/
│   ├── db_connection.py
│   ├── run_all_bronze.py
│   └── loaders/
│       ├── csv_loader.py
│       ├── excel_loader.py
│       ├── json_loader.py
│       ├── xml_loader.py
│       └── parquet_loader.py
├── domain1_csv_generation.py
├── domain1_multiformat_generation.py
├── domain2_data_generation.py
├── domain3_data_generation.py
├── domain4_data_generation.py
├── domain5_data_generation.py
├── domain6_data_generation.py
├── domain7_data_generation.py
├── requirements.txt
└── README.md
```
```

---

**Step 5 — Create `requirements.txt`:**
```
pandas==2.2.0
numpy==1.26.4
mysql-connector-python==9.6.0
openpyxl==3.1.2
pyarrow==15.0.0
chardet==5.2.0
faker==24.0.0