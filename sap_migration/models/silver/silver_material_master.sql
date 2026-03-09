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
--   4. Standardize procurement_type → E/F/X standard codes
--   5. Standardize dates          → parse 6 date formats → YYYY-MM-DD
--   6. Validate weights           → flag net_weight > gross_weight
--   7. Flag duplicates            → same material_id appearing more than once
--   8. Flag null mandatory fields → description, base_uom, material_type,
--                                   material_group
--   9. Flag invalid material_type → not in SAP standard list
--  10. Flag generic created_by    → ADMIN, SYSTEM, MIGRATE users
--  11. Derive overall DQ score    → % of fields passing validation
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
-- STEP 3: STANDARDIZE MATERIAL TYPE AND PROCUREMENT TYPE
-- Valid SAP material types:
--   FERT = Finished Product
--   ROH  = Raw Material
--   HALB = Semi-Finished
--   VERP = Packaging
--   DIEN = Service
--   NLAG = Non-Stock
--   ERSA = Spare Parts
--   HIBE = Operating Supplies
-- Valid SAP procurement types:
--   E = External procurement
--   F = In-house production
--   X = Both
-- =============================================================================

standardize_material_type AS (
    SELECT
        *,
        UPPER(TRIM(material_type)) AS material_type_clean,

        CASE
            WHEN UPPER(TRIM(procurement_type)) IN ('E','EXTERNAL','EXT','BUY')
                THEN 'E'
            WHEN UPPER(TRIM(procurement_type)) IN ('F','IN-HOUSE','INHOUSE','MAKE')
                THEN 'F'
            WHEN UPPER(TRIM(procurement_type)) IN ('X','BOTH','E+F','E/F')
                THEN 'X'
            ELSE UPPER(TRIM(procurement_type))
        END AS procurement_type_clean

    FROM standardize_material_id
),


-- =============================================================================
-- STEP 4: STANDARDIZE BASE UOM
-- Maps all known variants to SAP 3-character standard codes.
-- =============================================================================

standardize_uom AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(base_uom)) IN (
                'KG','KGS','KGM','KILOGRAM','KILOGRAMS','KG.')
                THEN 'KG'
            WHEN UPPER(TRIM(base_uom)) IN (
                'EA','EACH','PC','PCS','PIECE','PIECES','ST',
                'STCK','NR','UNIT','UNITS')
                THEN 'EA'
            WHEN UPPER(TRIM(base_uom)) IN (
                'LT','LTR','L','LITER','LITERS','LITRE','LITRES','LIT')
                THEN 'LT'
            WHEN UPPER(TRIM(base_uom)) IN (
                'M','MTR','METER','METERS','METRE','METRES','MT.')
                THEN 'M'
            WHEN UPPER(TRIM(base_uom)) IN (
                'G','GRM','GRAM','GRAMS','GR','GM')
                THEN 'G'
            WHEN UPPER(TRIM(base_uom)) IN (
                'TO','TON','TONS','TONNE','TONNES','MT','T')
                THEN 'TO'
            WHEN UPPER(TRIM(base_uom)) IN (
                'MM','MILLIMETER','MILLIMETERS','MILLIMETRE')
                THEN 'MM'
            WHEN UPPER(TRIM(base_uom)) IN (
                'CM','CENTIMETER','CENTIMETERS','CENTIMETRE')
                THEN 'CM'
            WHEN UPPER(TRIM(base_uom)) IN (
                'BX','BOX','BOXES','CS','CASE','CASES')
                THEN 'BX'
            ELSE UPPER(TRIM(base_uom))
        END AS base_uom_clean
    FROM standardize_material_type
),


-- =============================================================================
-- STEP 5: STANDARDIZE DATES
-- Tries 6 date format patterns using COALESCE + STR_TO_DATE.
-- First match wins. NULL if no pattern matches.
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
-- 14 flags total. 0 = clean, 1 = issue found.
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

        -- FLAG 9: Net weight greater than gross weight (impossible)
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
        CASE WHEN UPPER(TRIM(created_by)) IN (
                'ADMIN','MIGRATE','SYSTEM','MIGRATION',
                'SYS','ADMINISTRATOR','USR000')
             THEN 1 ELSE 0 END                      AS flag_generic_user,

        -- FLAG 13: Material ID was non-standard (needed cleaning)
        CASE WHEN material_id != UPPER(
                REPLACE(REPLACE(material_id, '-', ''), ' ', ''))
             THEN 1 ELSE 0 END                      AS flag_nonstandard_material_id,

        -- FLAG 14: Invalid procurement type
        CASE WHEN procurement_type_clean NOT IN ('E','F','X')
             THEN 1 ELSE 0 END                      AS flag_invalid_procurement_type

    FROM cast_numerics
),


-- =============================================================================
-- STEP 8: FLAG DUPLICATES
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
-- 14 flags. Score = (clean flags / 14) * 100
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
                (1 - flag_nonstandard_material_id)  +
                (1 - flag_invalid_procurement_type) +
                (1 - flag_duplicate)
            ) / 15.0 * 100
        , 1)                                        AS dq_score

    FROM flag_duplicates
)


-- =============================================================================
-- FINAL SELECT
-- Column order: raw value → clean value (paired together)
-- =============================================================================

SELECT

    -- ── Audit columns ─────────────────────────────────────────────────────
    load_id,
    _batch_id,
    _source_file,
    _ingestion_timestamp,

    -- ── Material ID (raw → clean) ──────────────────────────────────────────
    material_id,
    material_id_clean,

    -- ── Description ───────────────────────────────────────────────────────
    material_description,

    -- ── Material type (raw → clean) ───────────────────────────────────────
    material_type,
    material_type_clean,

    -- ── Material group (raw → clean) ──────────────────────────────────────
    material_group,
    UPPER(TRIM(material_group))         AS material_group_clean,

    -- ── Unit of measure (raw → clean) ─────────────────────────────────────
    base_uom,
    base_uom_clean,

    -- ── Gross weight (raw → numeric) ──────────────────────────────────────
    gross_weight,
    gross_weight_num,

    -- ── Net weight (raw → numeric) ────────────────────────────────────────
    net_weight,
    net_weight_num,

    -- ── Created date (raw → clean) ────────────────────────────────────────
    created_date,
    created_date_clean,

    -- ── Changed date (raw → clean) ────────────────────────────────────────
    changed_date,
    changed_date_clean,

    -- ── Procurement type (raw → clean) ────────────────────────────────────
    procurement_type,
    procurement_type_clean,

    -- ── Other fields ──────────────────────────────────────────────────────
    created_by,
    deletion_flag,
    legacy_material_id,
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
    flag_invalid_procurement_type,

    -- ── Overall DQ score (0-100) ──────────────────────────────────────────
    dq_score

FROM calculate_dq_score