-- =============================================================================
-- models/silver/silver_equipment_master.sql
--
-- PURPOSE:
--   Cleans and validates bronze_equipment_master into a silver layer table.
--   Equipment master is the foundation of Plant Maintenance in SAP.
--   Bad equipment data = maintenance orders posted to wrong cost centers,
--   decommissioned equipment still generating work orders.
--
-- CLEANING RULES APPLIED:
--   1. Validate plant_id        → must be CHI1, HOU2, DET3
--   2. Validate company_code    → must be 1000, 2000, 3000
--   3. Standardize active_flag  → Y/N only
--   4. Standardize currency     → 3-char ISO codes
--   5. Standardize manufacturer → uppercase, trimmed
--   6. Clean cost_center        → flag invalid placeholders
--   7. Standardize dates        → parse 6 formats → YYYY-MM-DD
--   8. Flag duplicate serials   → same serial_number appearing > once
--   9. Flag warranty issues     → warranty_end before startup_date
--  10. Flag future construction → year_constructed in future
--  11. Derive overall DQ score  → % of fields passing validation
--
-- MATERIALIZATION: view
-- SOURCE: bronze_equipment_master
-- =============================================================================

WITH

raw AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_equipment_master') }}
),

-- =============================================================================
-- STEP 2: STANDARDIZE ACTIVE FLAG
-- Bronze has: Y, N, 0, 1, Yes, No, Active, Decommissioned
-- SAP standard: Y = active, N = inactive/decommissioned
-- =============================================================================

standardize_flags AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(active_flag)) IN (
                'Y','YES','1','TRUE','ACTIVE')
                THEN 'Y'
            WHEN UPPER(TRIM(active_flag)) IN (
                'N','NO','0','FALSE','INACTIVE',
                'DECOMMISSIONED','DECOMM')
                THEN 'N'
            ELSE UPPER(TRIM(active_flag))
        END AS active_flag_clean
    FROM raw
),

-- =============================================================================
-- STEP 3: STANDARDIZE CURRENCY
-- Bronze has: EUR, GBP, USD, Euro, Dollars, $
-- =============================================================================

standardize_currency AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(currency)) IN (
                'USD','DOLLAR','DOLLARS','$','US$')
                THEN 'USD'
            WHEN UPPER(TRIM(currency)) IN (
                'EUR','EURO','EUROS','€')
                THEN 'EUR'
            WHEN UPPER(TRIM(currency)) IN (
                'GBP','POUND','POUNDS','£')
                THEN 'GBP'
            ELSE UPPER(TRIM(currency))
        END AS currency_clean
    FROM standardize_flags
),

-- =============================================================================
-- STEP 4: STANDARDIZE MANUFACTURER
-- Bronze has: Siemens, siemens, SIEMENS, bosch, BOSCH, abb, ABB
-- Standardize to proper case for consistency
-- =============================================================================

standardize_manufacturer AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(manufacturer)) = 'SIEMENS' THEN 'Siemens'
            WHEN UPPER(TRIM(manufacturer)) = 'BOSCH'   THEN 'Bosch'
            WHEN UPPER(TRIM(manufacturer)) = 'ABB'     THEN 'ABB'
            WHEN UPPER(TRIM(manufacturer)) = 'FANUC'   THEN 'FANUC'
            WHEN UPPER(TRIM(manufacturer)) = 'ROCKWELL'THEN 'Rockwell'
            WHEN UPPER(TRIM(manufacturer)) = 'GE'      THEN 'GE'
            ELSE TRIM(manufacturer)
        END AS manufacturer_clean
    FROM standardize_currency
),

-- =============================================================================
-- STEP 5: STANDARDIZE DATES
-- Equipment has 6 date fields — all need parsing
-- =============================================================================

standardize_dates AS (
    SELECT
        *,
        -- Acquisition date
        COALESCE(
            STR_TO_DATE(acquisition_date, '%Y-%m-%d'),
            STR_TO_DATE(acquisition_date, '%Y%m%d'),
            STR_TO_DATE(acquisition_date, '%d-%b-%Y'),
            STR_TO_DATE(acquisition_date, '%b %d, %Y'),
            STR_TO_DATE(acquisition_date, '%m/%d/%Y'),
            STR_TO_DATE(acquisition_date, '%d-%m-%Y')
        ) AS acquisition_date_clean,

        -- Start up date
        COALESCE(
            STR_TO_DATE(start_up_date, '%Y-%m-%d'),
            STR_TO_DATE(start_up_date, '%Y%m%d'),
            STR_TO_DATE(start_up_date, '%d-%b-%Y'),
            STR_TO_DATE(start_up_date, '%b %d, %Y'),
            STR_TO_DATE(start_up_date, '%m/%d/%Y'),
            STR_TO_DATE(start_up_date, '%d-%m-%Y')
        ) AS start_up_date_clean,

        -- Warranty end date
        COALESCE(
            STR_TO_DATE(warranty_end_date, '%Y-%m-%d'),
            STR_TO_DATE(warranty_end_date, '%Y%m%d'),
            STR_TO_DATE(warranty_end_date, '%d-%b-%Y'),
            STR_TO_DATE(warranty_end_date, '%b %d, %Y'),
            STR_TO_DATE(warranty_end_date, '%m/%d/%Y'),
            STR_TO_DATE(warranty_end_date, '%d-%m-%Y')
        ) AS warranty_end_date_clean,

        -- Created date
        COALESCE(
            STR_TO_DATE(created_date, '%Y-%m-%d'),
            STR_TO_DATE(created_date, '%Y%m%d'),
            STR_TO_DATE(created_date, '%d-%b-%Y'),
            STR_TO_DATE(created_date, '%b %d, %Y'),
            STR_TO_DATE(created_date, '%m/%d/%Y'),
            STR_TO_DATE(created_date, '%d-%m-%Y')
        ) AS created_date_clean,

        -- Changed date
        COALESCE(
            STR_TO_DATE(changed_date, '%Y-%m-%d'),
            STR_TO_DATE(changed_date, '%Y%m%d'),
            STR_TO_DATE(changed_date, '%d-%b-%Y'),
            STR_TO_DATE(changed_date, '%b %d, %Y'),
            STR_TO_DATE(changed_date, '%m/%d/%Y'),
            STR_TO_DATE(changed_date, '%d-%m-%Y')
        ) AS changed_date_clean

    FROM standardize_manufacturer
),

-- =============================================================================
-- STEP 6: CAST NUMERIC FIELDS
-- =============================================================================

cast_numerics AS (
    SELECT
        *,
        CASE
            WHEN acquisition_value REGEXP '^-?[0-9]+\\.?[0-9]*$'
                THEN CAST(acquisition_value AS DECIMAL(15,2))
            ELSE NULL
        END AS acquisition_value_num,

        CASE
            WHEN year_constructed REGEXP '^[0-9]{4}$'
                THEN CAST(year_constructed AS UNSIGNED)
            ELSE NULL
        END AS year_constructed_num

    FROM standardize_dates
),

-- =============================================================================
-- STEP 7: APPLY DATA QUALITY FLAGS
-- =============================================================================

apply_dq_flags AS (
    SELECT
        *,

        -- FLAG 1: Invalid plant ID
        CASE WHEN plant_id NOT IN ('CHI1','HOU2','DET3')
             THEN 1 ELSE 0 END                  AS flag_invalid_plant,

        -- FLAG 2: Invalid company code
        CASE WHEN company_code NOT IN ('1000','2000','3000')
             THEN 1 ELSE 0 END                  AS flag_invalid_company_code,

        -- FLAG 3: Invalid active flag
        CASE WHEN active_flag_clean NOT IN ('Y','N')
             THEN 1 ELSE 0 END                  AS flag_invalid_active_flag,

        -- FLAG 4: Invalid cost center placeholder
        -- CC999999, CCXXXXXX, TBD, N/A are not real cost centers
        CASE WHEN UPPER(TRIM(cost_center)) IN (
                'CC999999','CCXXXXXX','TBD','N/A',
                '0000','NA','UNKNOWN','?')
             THEN 1 ELSE 0 END                  AS flag_invalid_cost_center,

        -- FLAG 5: Null cost center
        CASE WHEN cost_center IS NULL
              OR TRIM(cost_center) = ''
             THEN 1 ELSE 0 END                  AS flag_null_cost_center,

        -- FLAG 6: Null equipment description
        CASE WHEN equipment_description IS NULL
              OR TRIM(equipment_description) = ''
             THEN 1 ELSE 0 END                  AS flag_null_description,

        -- FLAG 7: Future construction year
        -- Equipment cannot be constructed in the future
        CASE WHEN year_constructed_num IS NOT NULL
              AND year_constructed_num > YEAR(CURDATE())
             THEN 1 ELSE 0 END                  AS flag_future_construction,

        -- FLAG 8: Unrealistic construction year (before 1900)
        CASE WHEN year_constructed_num IS NOT NULL
              AND year_constructed_num < 1900
             THEN 1 ELSE 0 END                  AS flag_old_construction_year,

        -- FLAG 9: Warranty end before startup date (impossible)
        CASE WHEN warranty_end_date_clean IS NOT NULL
              AND start_up_date_clean IS NOT NULL
              AND warranty_end_date_clean < start_up_date_clean
             THEN 1 ELSE 0 END                  AS flag_warranty_before_startup,

        -- FLAG 10: Changed date before created date
        CASE WHEN created_date_clean IS NOT NULL
              AND changed_date_clean IS NOT NULL
              AND changed_date_clean < created_date_clean
             THEN 1 ELSE 0 END                  AS flag_changed_before_created,

        -- FLAG 11: Generic created_by user
        CASE WHEN UPPER(TRIM(created_by)) IN (
                'ADMIN','MIGRATE','SYSTEM','MIGRATION','SYS')
             THEN 1 ELSE 0 END                  AS flag_generic_user,

        -- FLAG 12: Non-standard currency
        CASE WHEN currency != currency_clean
             THEN 1 ELSE 0 END                  AS flag_nonstandard_currency

    FROM cast_numerics
),

-- =============================================================================
-- STEP 8: FLAG DUPLICATE SERIAL NUMBERS
-- SN-DUPLICATE-001 appears 493 times — clearly a placeholder
-- Any serial number appearing more than once is flagged
-- =============================================================================

flag_duplicates AS (
    SELECT
        *,
        COUNT(*) OVER (
            PARTITION BY serial_number
        )                                       AS serial_duplicate_count,

        CASE
            WHEN serial_number IS NULL
              OR UPPER(TRIM(serial_number)) LIKE '%DUPLICATE%'
              OR COUNT(*) OVER (
                    PARTITION BY serial_number) > 1
            THEN 1 ELSE 0
        END                                     AS flag_duplicate_serial

    FROM apply_dq_flags
),

-- =============================================================================
-- STEP 9: CALCULATE DQ SCORE
-- 13 flags. Score = (clean flags / 13) * 100
-- =============================================================================

calculate_dq_score AS (
    SELECT
        *,
        ROUND(
            (
                (1 - flag_invalid_plant)            +
                (1 - flag_invalid_company_code)     +
                (1 - flag_invalid_active_flag)      +
                (1 - flag_invalid_cost_center)      +
                (1 - flag_null_cost_center)         +
                (1 - flag_null_description)         +
                (1 - flag_future_construction)      +
                (1 - flag_old_construction_year)    +
                (1 - flag_warranty_before_startup)  +
                (1 - flag_changed_before_created)   +
                (1 - flag_generic_user)             +
                (1 - flag_nonstandard_currency)     +
                (1 - flag_duplicate_serial)
            ) / 13.0 * 100
        , 1)                                    AS dq_score
    FROM flag_duplicates
)

SELECT

    -- ── Audit columns ─────────────────────────────────────────────────────
    load_id,
    _batch_id,
    _source_file,
    _ingestion_timestamp,

    -- ── Equipment identifiers ─────────────────────────────────────────────
    equipment_id,
    equipment_description,
    equipment_category,
    equipment_type,
    floc_id,

    -- ── Plant and company ─────────────────────────────────────────────────
    plant_id,
    maintenance_plant,
    company_code,

    -- ── Location ──────────────────────────────────────────────────────────
    location,
    cost_center,

    -- ── Manufacturer details ──────────────────────────────────────────────
    manufacturer,
    manufacturer_clean,
    model_number,
    serial_number,
    serial_duplicate_count,

    -- ── Construction and dates ────────────────────────────────────────────
    year_constructed,
    year_constructed_num,
    acquisition_date,
    acquisition_date_clean,
    acquisition_value,
    acquisition_value_num,

    -- ── Currency (raw → clean) ────────────────────────────────────────────
    currency,
    currency_clean,

    -- ── Physical details ──────────────────────────────────────────────────
    weight,
    size_unit,

    -- ── Key dates (raw → clean) ───────────────────────────────────────────
    start_up_date,
    start_up_date_clean,
    warranty_end_date,
    warranty_end_date_clean,
    created_date,
    created_date_clean,
    changed_date,
    changed_date_clean,

    -- ── Status (raw → clean) ──────────────────────────────────────────────
    active_flag,
    active_flag_clean,

    -- ── Reference ─────────────────────────────────────────────────────────
    created_by,
    legacy_equipment_id,

    -- ── DQ flags ──────────────────────────────────────────────────────────
    flag_invalid_plant,
    flag_invalid_company_code,
    flag_invalid_active_flag,
    flag_invalid_cost_center,
    flag_null_cost_center,
    flag_null_description,
    flag_future_construction,
    flag_old_construction_year,
    flag_warranty_before_startup,
    flag_changed_before_created,
    flag_generic_user,
    flag_nonstandard_currency,
    flag_duplicate_serial,

    -- ── DQ score ──────────────────────────────────────────────────────────
    dq_score

FROM calculate_dq_score