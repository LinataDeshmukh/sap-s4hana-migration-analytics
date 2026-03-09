-- =============================================================================
-- models/silver/silver_material_master.sql
--
-- PURPOSE:
--   Cleans and validates bronze_material_master into a silver layer table.
--
-- CLEANING RULES APPLIED:
--   1. Standardize material_id    → uppercase, remove dashes/spaces
--   2. Standardize base_uom       → map all variants to SAP standard codes
--   3. Standardize material_type  → uppercase, trim, validate against SAP list
--   4. Standardize dates          → parse 6 date formats → YYYY-MM-DD
--   5. Validate weights           → flag net_weight > gross_weight
--   6. Flag duplicates            → same material_id appearing more than once
--   7. Flag null mandatory fields → description, base_uom, material_type,
--                                   material_group
--   8. Flag invalid material_type → not in SAP standard list
--   9. Flag generic created_by    → ADMIN, SYSTEM, MIGRATE users
--  10. Derive overall DQ score    → % of fields passing validation
--
-- MATERIALIZATION: view
-- SOURCE: bronze_material_master
-- =============================================================================

WITH

-- =============================================================================
-- STEP 1: READ RAW BRONZE DATA
-- =============================================================================

raw AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_material_master') }}
),


-- =============================================================================
-- STEP 2: STANDARDIZE MATERIAL ID
-- Bronze has: MAT001, MAT-001, mat001, MAT 001
-- Silver standard: uppercase, no dashes, no spaces
-- =============================================================================

standardize_material_id AS (
    SELECT
        *,
        UPPER(
            REPLACE(
                REPLACE(material_id, '-', ''),
                ' ', ''
            )
        ) AS material_id_clean
    FROM raw
),


-- =============================================================================
-- STEP 3: STANDARDIZE MATERIAL TYPE
-- Valid SAP material types:
--   FERT = Finished Product
--   ROH  = Raw Material
--   HALB = Semi-Finished
--   VERP = Packaging
--   DIEN = Service
--   NLAG = Non-Stock
--   ERSA = Spare Parts
--   HIBE = Operating Supplies
-- Anything else is invalid — flagged in Step 6.
-- =============================================================================

standardize_material_type AS (
    SELECT
        *,
        UPPER(TRIM(material_type)) AS material_type_clean
    FROM standardize_material_id
),


-- =============================================================================
-- STEP 4: STANDARDIZE BASE UOM
-- Maps all known variants to SAP 3-character standard codes.
-- Extended to cover all variants found in actual data:
--   GRAM, Gram, grams → G
--   Piece, PIECE, piece → EA
--   Ea, ea, Each, EACH → EA
--   KGS, kgs, Kilogram → KG
-- =============================================================================

standardize_uom AS (
    SELECT
        *,
        CASE
            -- Kilogram variants
            WHEN UPPER(TRIM(base_uom)) IN (
                'KG','KGS','KGM','KILOGRAM','KILOGRAMS','KG.')
                THEN 'KG'
            -- Each / Piece variants
            WHEN UPPER(TRIM(base_uom)) IN (
                'EA','EACH','PC','PCS','PIECE','PIECES','ST',
                'STCK','NR','UNIT','UNITS')
                THEN 'EA'
            -- Liter variants
            WHEN UPPER(TRIM(base_uom)) IN (
                'LT','LTR','L','LITER','LITERS','LITRE','LITRES','LIT')
                THEN 'LT'
            -- Meter variants
            WHEN UPPER(TRIM(base_uom)) IN (
                'M','MTR','METER','METERS','METRE','METRES','MT.')
                THEN 'M'
            -- Gram variants
            WHEN UPPER(TRIM(base_uom)) IN (
                'G','GRM','GRAM','GRAMS','GR','GM')
                THEN 'G'
            -- Ton variants
            WHEN UPPER(TRIM(base_uom)) IN (
                'TO','TON','TONS','TONNE','TONNES','MT','T')
                THEN 'TO'
            -- Millimeter variants
            WHEN UPPER(TRIM(base_uom)) IN (
                'MM','MILLIMETER','MILLIMETERS','MILLIMETRE')
                THEN 'MM'
            -- Centimeter variants
            WHEN UPPER(TRIM(base_uom)) IN (
                'CM','CENTIMETER','CENTIMETERS','CENTIMETRE')
                THEN 'CM'
            -- Box variants
            WHEN UPPER(TRIM(base_uom)) IN (
                'BX','BOX','BOXES','CS','CASE','CASES')
                THEN 'BX'
            -- Already clean or unknown — keep as-is (will be flagged)
            ELSE UPPER(TRIM(base_uom))
        END AS base_uom_clean
    FROM standardize_material_type
),


-- =============================================================================
-- STEP 5: STANDARDIZE DATES
-- Tries 6 date format patterns using COALESCE + STR_TO_DATE.
-- First match wins. NULL if no pattern matches → flagged in Step 7.
-- =============================================================================

standardize_dates AS (
    SELECT
        *,
        COALESCE(
            STR_TO_DATE(created_date, '%Y-%m-%d'),
            STR_TO_DATE(created_date, '%m/%d/%Y'),
            STR_TO_DATE(created_date, '%d-%m-%Y'),
            STR_TO_DATE(created_date, '%d-%b-%Y'),
            STR_TO_DATE(created_date, '%Y%m%d'),
            STR_TO_DATE(created_date, '%b %d, %Y')
        ) AS created_date_clean,

        COALESCE(
            STR_TO_DATE(changed_date, '%Y-%m-%d'),
            STR_TO_DATE(changed_date, '%m/%d/%Y'),
            STR_TO_DATE(changed_date, '%d-%m-%Y'),
            STR_TO_DATE(changed_date, '%d-%b-%Y'),
            STR_TO_DATE(changed_date, '%Y%m%d'),
            STR_TO_DATE(changed_date, '%b %d, %Y')
        ) AS changed_date_clean
    FROM standardize_uom
),


-- =============================================================================
-- STEP 6: CAST NUMERIC FIELDS
-- Validates string is numeric before casting to DECIMAL.
-- Invalid strings (N/A, TBD, letters) become NULL safely.
-- =============================================================================

cast_numerics AS (
    SELECT
        *,
        CASE
            WHEN gross_weight REGEXP '^-?[0-9]+\\.?[0-9]*$'
                THEN CAST(gross_weight AS DECIMAL(15,3))
            ELSE NULL
        END AS gross_weight_num,

        CASE
            WHEN net_weight REGEXP '^-?[0-9]+\\.?[0-9]*$'
                THEN CAST(net_weight AS DECIMAL(15,3))
            ELSE NULL
        END AS net_weight_num
    FROM standardize_dates
),


-- =============================================================================
-- STEP 7: APPLY DATA QUALITY FLAGS
-- 12 flags total covering all discovered data quality issues.
-- 0 = clean, 1 = issue found.
-- =============================================================================

apply_dq_flags AS (
    SELECT
        *,

        -- FLAG 1: Null or empty material description (mandatory in SAP)
        CASE WHEN material_description IS NULL
              OR TRIM(material_description) = ''
             THEN 1 ELSE 0 END                      AS flag_null_description,

        -- FLAG 2: Null or empty material type (mandatory in SAP)
        CASE WHEN material_type IS NULL
              OR TRIM(material_type) = ''
             THEN 1 ELSE 0 END                      AS flag_null_material_type,

        -- FLAG 3: Invalid material type — not in SAP standard list
        -- Valid types: FERT, ROH, HALB, VERP, DIEN, NLAG, ERSA, HIBE
        -- Catches corrupted values like standalone 'T', 'T DIEN', numbers
        CASE WHEN material_type_clean NOT IN (
                'FERT','ROH','HALB','VERP','DIEN',
                'NLAG','ERSA','HIBE','UNBW','WETT')
             THEN 1 ELSE 0 END                      AS flag_invalid_material_type,

        -- FLAG 4: Null or empty material group
        CASE WHEN material_group IS NULL
              OR TRIM(material_group) = ''
             THEN 1 ELSE 0 END                      AS flag_null_material_group,

        -- FLAG 5: Null or empty base UOM (mandatory in SAP)
        CASE WHEN base_uom IS NULL
              OR TRIM(base_uom) = ''
             THEN 1 ELSE 0 END                      AS flag_null_uom,

        -- FLAG 6: UOM was non-standard (needed mapping to SAP code)
        CASE WHEN base_uom != base_uom_clean
             THEN 1 ELSE 0 END                      AS flag_nonstandard_uom,

        -- FLAG 7: Negative gross weight (physically impossible)
        CASE WHEN gross_weight_num IS NOT NULL
              AND gross_weight_num < 0
             THEN 1 ELSE 0 END                      AS flag_negative_gross_weight,

        -- FLAG 8: Negative net weight (physically impossible)
        CASE WHEN net_weight_num IS NOT NULL
              AND net_weight_num < 0
             THEN 1 ELSE 0 END                      AS flag_negative_net_weight,

        -- FLAG 9: Net weight greater than gross weight
        -- Net weight = weight of contents only
        -- Gross weight = contents + packaging
        -- Net > Gross is physically impossible
        CASE WHEN net_weight_num  IS NOT NULL
              AND gross_weight_num IS NOT NULL
              AND net_weight_num > gross_weight_num
             THEN 1 ELSE 0 END                      AS flag_weight_invalid,

        -- FLAG 10: Created date could not be parsed
        CASE WHEN created_date IS NOT NULL
              AND created_date_clean IS NULL
             THEN 1 ELSE 0 END                      AS flag_bad_created_date,

        -- FLAG 11: Changed date before created date (impossible)
        CASE WHEN created_date_clean IS NOT NULL
              AND changed_date_clean IS NOT NULL
              AND changed_date_clean < created_date_clean
             THEN 1 ELSE 0 END                      AS flag_changed_before_created,

        -- FLAG 12: Generic/system created_by user
        -- Indicates records migrated without proper ownership
        CASE WHEN UPPER(TRIM(created_by)) IN (
                'ADMIN','MIGRATE','SYSTEM','MIGRATION',
                'SYS','ADMINISTRATOR','USR000')
             THEN 1 ELSE 0 END                      AS flag_generic_user,

        -- FLAG 13: Material ID was non-standard (needed cleaning)
        CASE WHEN material_id != UPPER(
                REPLACE(REPLACE(material_id, '-', ''), ' ', ''))
             THEN 1 ELSE 0 END                      AS flag_nonstandard_material_id

    FROM cast_numerics
),


-- =============================================================================
-- STEP 8: FLAG DUPLICATES
-- Window function counts how many rows share the same cleaned material_id.
-- All copies are flagged — business team decides which version to keep.
-- =============================================================================

flag_duplicates AS (
    SELECT
        *,
        COUNT(*) OVER (
            PARTITION BY material_id_clean
        )                                           AS duplicate_count,

        CASE
            WHEN COUNT(*) OVER (
                PARTITION BY material_id_clean) > 1
            THEN 1 ELSE 0
        END                                         AS flag_duplicate

    FROM apply_dq_flags
),


-- =============================================================================
-- STEP 9: CALCULATE DATA QUALITY SCORE
-- 13 flags checked. Score = (clean flags / 13) * 100
-- 100 = perfect record, 0 = every field has an issue
-- =============================================================================

calculate_dq_score AS (
    SELECT
        *,
        ROUND(
            (
                (1 - flag_null_description)         +
                (1 - flag_null_material_type)       +
                (1 - flag_invalid_material_type)    +
                (1 - flag_null_material_group)      +
                (1 - flag_null_uom)                 +
                (1 - flag_nonstandard_uom)          +
                (1 - flag_negative_gross_weight)    +
                (1 - flag_negative_net_weight)      +
                (1 - flag_weight_invalid)           +
                (1 - flag_bad_created_date)         +
                (1 - flag_changed_before_created)   +
                (1 - flag_generic_user)             +
                (1 - flag_duplicate)
            ) / 13.0 * 100
        , 1)                                        AS dq_score

    FROM flag_duplicates
)


-- =============================================================================
-- FINAL SELECT
-- Three categories of columns:
--   1. Raw values     → original bronze values, preserved for audit trail
--   2. Clean values   → standardized values ready for SAP upload
--   3. DQ flags/score → drives Power BI dashboard and exception reports
-- =============================================================================

SELECT

    -- ── Audit columns ─────────────────────────────────────────────────────
    load_id,
    _batch_id,
    _source_file,
    _ingestion_timestamp,

    -- ── Material ID ───────────────────────────────────────────────────────
    material_id,
    material_id_clean,

    -- ── Descriptive fields ────────────────────────────────────────────────
    material_description,
    material_type,
    material_type_clean,
    material_group,
    UPPER(TRIM(material_group))     AS material_group_clean,

    -- ── Unit of measure ───────────────────────────────────────────────────
    base_uom,
    base_uom_clean,

    -- ── Weights (raw strings + cast numerics) ─────────────────────────────
    gross_weight,
    net_weight,
    gross_weight_num,
    net_weight_num,

    -- ── Dates (raw strings + parsed dates) ────────────────────────────────
    created_date,
    changed_date,
    created_date_clean,
    changed_date_clean,

    -- ── Other fields ──────────────────────────────────────────────────────
    created_by,
    deletion_flag,
    legacy_material_id,
    procurement_type,
    industry_sector,

    -- ── Duplicate tracking ────────────────────────────────────────────────
    duplicate_count,

    -- ── Data quality flags (0=clean, 1=issue) ─────────────────────────────
    flag_null_description,
    flag_null_material_type,
    flag_invalid_material_type,
    flag_null_material_group,
    flag_null_uom,
    flag_nonstandard_uom,
    flag_negative_gross_weight,
    flag_negative_net_weight,
    flag_weight_invalid,
    flag_bad_created_date,
    flag_changed_before_created,
    flag_generic_user,
    flag_nonstandard_material_id,
    flag_duplicate,

    -- ── Overall DQ score (0-100) ──────────────────────────────────────────
    dq_score

FROM calculate_dq_score