-- =============================================================================
-- models/silver/silver_cost_centers.sql
--
-- PURPOSE:
--   Cleans and validates bronze_cost_centers into a silver layer table.
--   Cost centers are mandatory for financial posting in SAP.
--   Bad cost center data = postings to wrong organizational units,
--   incorrect management reporting, failed budget allocations.
--
-- CLEANING RULES APPLIED:
--   1. Validate company_code      → must be 1000, 2000, 3000
--   2. Validate controlling_area  → must be A000, A001, A002
--   3. Standardize active_flag    → Y/N only
--   4. Standardize currency       → 3-char ISO codes
--   5. Standardize cost_center_type → valid SAP type codes
--   6. Standardize department     → consistent naming
--   7. Standardize dates          → parse 6 formats → YYYY-MM-DD
--   8. Flag invalid placeholders  → profit_center TBD, 0000 plant
--   9. Derive overall DQ score    → % of fields passing validation
--
-- MATERIALIZATION: view
-- SOURCE: bronze_cost_centers
-- =============================================================================

WITH

raw AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_cost_centers') }}
),

-- =============================================================================
-- STEP 2: STANDARDIZE ACTIVE FLAG
-- =============================================================================

standardize_active_flag AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(active_flag)) IN ('Y','YES','1','TRUE','ACTIVE')
                THEN 'Y'
            WHEN UPPER(TRIM(active_flag)) IN ('N','NO','0','FALSE','INACTIVE')
                THEN 'N'
            ELSE UPPER(TRIM(active_flag))
        END AS active_flag_clean
    FROM raw
),

-- =============================================================================
-- STEP 3: STANDARDIZE CURRENCY
-- =============================================================================

standardize_currency AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(currency)) IN ('USD','DOLLAR','DOLLARS','$','US$')
                THEN 'USD'
            WHEN UPPER(TRIM(currency)) IN ('EUR','EURO','EUROS','€')
                THEN 'EUR'
            WHEN UPPER(TRIM(currency)) IN ('GBP','POUND','POUNDS','£')
                THEN 'GBP'
            ELSE UPPER(TRIM(currency))
        END AS currency_clean
    FROM standardize_active_flag
),

-- =============================================================================
-- STEP 4: STANDARDIZE COST CENTER TYPE
-- Valid SAP cost center types:
--   E = Administration
--   P = Production
--   F = Finance
--   A = Asset
--   H = Overhead
--   V = Sales
-- =============================================================================

standardize_cc_type AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(cost_center_type)) IN (
                'E','ADMIN','ADMINISTRATION','ADM')
                THEN 'E'
            WHEN UPPER(TRIM(cost_center_type)) IN (
                'P','PROD','PRODUCTION','MANUFACTURING','MFG')
                THEN 'P'
            WHEN UPPER(TRIM(cost_center_type)) IN (
                'F','FIN','FINANCE','FINANCIAL')
                THEN 'F'
            WHEN UPPER(TRIM(cost_center_type)) IN (
                'A','ASSET','ASSETS')
                THEN 'A'
            WHEN UPPER(TRIM(cost_center_type)) IN (
                'H','OH','OVERHEAD','OVHD')
                THEN 'H'
            WHEN UPPER(TRIM(cost_center_type)) IN (
                'V','SALES','SELL','SELLING')
                THEN 'V'
            ELSE UPPER(TRIM(cost_center_type))
        END AS cost_center_type_clean
    FROM standardize_currency
),

-- =============================================================================
-- STEP 5: STANDARDIZE DEPARTMENT
-- Bronze has: production, QUALITY, Fin, QM, Prod, SC, supply chain
-- Standardize to consistent naming
-- =============================================================================

standardize_department AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(department)) IN (
                'PRODUCTION','PROD','MANUFACTURING','MFG')
                THEN 'Production'
            WHEN UPPER(TRIM(department)) IN (
                'QUALITY','QM','QUALITY MANAGEMENT','QA')
                THEN 'Quality'
            WHEN UPPER(TRIM(department)) IN (
                'FINANCE','FIN','FINANCIAL','FINANZ')
                THEN 'Finance'
            WHEN UPPER(TRIM(department)) IN (
                'MAINTENANCE','MAINT','PM')
                THEN 'Maintenance'
            WHEN UPPER(TRIM(department)) IN (
                'PROCUREMENT','PURCH','PURCHASING')
                THEN 'Procurement'
            WHEN UPPER(TRIM(department)) IN (
                'SUPPLY CHAIN','SC','SCM','LOGISTICS')
                THEN 'Supply Chain'
            WHEN UPPER(TRIM(department)) IN (
                'HR','HUMAN RESOURCES','PERSONNEL')
                THEN 'HR'
            ELSE TRIM(department)
        END AS department_clean
    FROM standardize_cc_type
),

-- =============================================================================
-- STEP 6: STANDARDIZE DATES
-- Cost centers have 4 date fields
-- =============================================================================

standardize_dates AS (
    SELECT
        *,
        COALESCE(
            STR_TO_DATE(valid_from, '%Y-%m-%d'),
            STR_TO_DATE(valid_from, '%Y%m%d'),
            STR_TO_DATE(valid_from, '%d-%b-%Y'),
            STR_TO_DATE(valid_from, '%b %d, %Y'),
            STR_TO_DATE(valid_from, '%m/%d/%Y'),
            STR_TO_DATE(valid_from, '%d-%m-%Y')
        ) AS valid_from_clean,

        COALESCE(
            STR_TO_DATE(valid_to, '%Y-%m-%d'),
            STR_TO_DATE(valid_to, '%Y%m%d'),
            STR_TO_DATE(valid_to, '%d-%b-%Y'),
            STR_TO_DATE(valid_to, '%b %d, %Y'),
            STR_TO_DATE(valid_to, '%m/%d/%Y'),
            STR_TO_DATE(valid_to, '%d-%m-%Y')
        ) AS valid_to_clean,

        COALESCE(
            STR_TO_DATE(created_date, '%Y-%m-%d'),
            STR_TO_DATE(created_date, '%Y%m%d'),
            STR_TO_DATE(created_date, '%d-%b-%Y'),
            STR_TO_DATE(created_date, '%b %d, %Y'),
            STR_TO_DATE(created_date, '%m/%d/%Y'),
            STR_TO_DATE(created_date, '%d-%m-%Y')
        ) AS created_date_clean,

        COALESCE(
            STR_TO_DATE(changed_date, '%Y-%m-%d'),
            STR_TO_DATE(changed_date, '%Y%m%d'),
            STR_TO_DATE(changed_date, '%d-%b-%Y'),
            STR_TO_DATE(changed_date, '%b %d, %Y'),
            STR_TO_DATE(changed_date, '%m/%d/%Y'),
            STR_TO_DATE(changed_date, '%d-%m-%Y')
        ) AS changed_date_clean

    FROM standardize_department
),

-- =============================================================================
-- STEP 7: APPLY DATA QUALITY FLAGS
-- =============================================================================

apply_dq_flags AS (
    SELECT
        *,

        -- FLAG 1: Invalid company code
        CASE WHEN company_code NOT IN ('1000','2000','3000')
             THEN 1 ELSE 0 END                  AS flag_invalid_company_code,

        -- FLAG 2: Invalid controlling area
        CASE WHEN controlling_area NOT IN ('A000','A001','A002')
             THEN 1 ELSE 0 END                  AS flag_invalid_controlling_area,

        -- FLAG 3: Invalid plant ID
        CASE WHEN plant_id NOT IN ('CHI1','HOU2','DET3')
             THEN 1 ELSE 0 END                  AS flag_invalid_plant,

        -- FLAG 4: Invalid active flag
        CASE WHEN active_flag_clean NOT IN ('Y','N')
             THEN 1 ELSE 0 END                  AS flag_invalid_active_flag,

        -- FLAG 5: Invalid cost center type
        CASE WHEN cost_center_type_clean NOT IN (
                'E','P','F','A','H','V')
             THEN 1 ELSE 0 END                  AS flag_invalid_cc_type,

        -- FLAG 6: Null responsible person
        CASE WHEN responsible_person IS NULL
              OR TRIM(responsible_person) = ''
             THEN 1 ELSE 0 END                  AS flag_null_responsible_person,

        -- FLAG 7: Null profit center
        CASE WHEN profit_center IS NULL
              OR TRIM(profit_center) = ''
             THEN 1 ELSE 0 END                  AS flag_null_profit_center,

        -- FLAG 8: Valid_to before valid_from (impossible)
        CASE WHEN valid_from_clean IS NOT NULL
              AND valid_to_clean IS NOT NULL
              AND valid_to_clean < valid_from_clean
             THEN 1 ELSE 0 END                  AS flag_invalid_validity_dates,

        -- FLAG 9: Expired cost center still active
        -- Cost center valid_to is in the past but active_flag = Y
        CASE WHEN valid_to_clean IS NOT NULL
              AND valid_to_clean < CURDATE()
              AND active_flag_clean = 'Y'
             THEN 1 ELSE 0 END                  AS flag_expired_but_active,

        -- FLAG 10: Bad created date
        CASE WHEN created_date IS NOT NULL
              AND created_date_clean IS NULL
             THEN 1 ELSE 0 END                  AS flag_bad_created_date,

        -- FLAG 11: Changed before created
        CASE WHEN created_date_clean IS NOT NULL
              AND changed_date_clean IS NOT NULL
              AND changed_date_clean < created_date_clean
             THEN 1 ELSE 0 END                  AS flag_changed_before_created,

        -- FLAG 12: Generic created_by user
        CASE WHEN UPPER(TRIM(created_by)) IN (
                'ADMIN','MIGRATE','SYSTEM','MIGRATION','SYS')
             THEN 1 ELSE 0 END                  AS flag_generic_user

    FROM standardize_dates
),

-- =============================================================================
-- STEP 8: CALCULATE DQ SCORE
-- 12 flags. Score = (clean flags / 12) * 100
-- =============================================================================

calculate_dq_score AS (
    SELECT
        *,
        ROUND(
            (
                (1 - flag_invalid_company_code)     +
                (1 - flag_invalid_controlling_area) +
                (1 - flag_invalid_plant)            +
                (1 - flag_invalid_active_flag)      +
                (1 - flag_invalid_cc_type)          +
                (1 - flag_null_responsible_person)  +
                (1 - flag_null_profit_center)       +
                (1 - flag_invalid_validity_dates)   +
                (1 - flag_expired_but_active)       +
                (1 - flag_bad_created_date)         +
                (1 - flag_changed_before_created)   +
                (1 - flag_generic_user)
            ) / 12.0 * 100
        , 1)                                    AS dq_score
    FROM apply_dq_flags
)

SELECT

    -- ── Audit columns ─────────────────────────────────────────────────────
    load_id,
    _batch_id,
    _source_file,
    _ingestion_timestamp,

    -- ── Cost center identifier ────────────────────────────────────────────
    cost_center_id,
    cost_center_description,

    -- ── Organizational assignment ─────────────────────────────────────────
    company_code,
    controlling_area,
    plant_id,

    -- ── Cost center type (raw → clean) ────────────────────────────────────
    cost_center_type,
    cost_center_type_clean,

    -- ── Department (raw → clean) ──────────────────────────────────────────
    department,
    department_clean,

    -- ── Ownership ─────────────────────────────────────────────────────────
    responsible_person,
    profit_center,

    -- ── Currency (raw → clean) ────────────────────────────────────────────
    currency,
    currency_clean,

    -- ── Validity dates (raw → clean) ──────────────────────────────────────
    valid_from,
    valid_from_clean,
    valid_to,
    valid_to_clean,

    -- ── Status (raw → clean) ──────────────────────────────────────────────
    active_flag,
    active_flag_clean,

    -- ── Audit dates (raw → clean) ─────────────────────────────────────────
    created_by,
    created_date,
    created_date_clean,
    changed_date,
    changed_date_clean,

    -- ── DQ flags ──────────────────────────────────────────────────────────
    flag_invalid_company_code,
    flag_invalid_controlling_area,
    flag_invalid_plant,
    flag_invalid_active_flag,
    flag_invalid_cc_type,
    flag_null_responsible_person,
    flag_null_profit_center,
    flag_invalid_validity_dates,
    flag_expired_but_active,
    flag_bad_created_date,
    flag_changed_before_created,
    flag_generic_user,

    -- ── DQ score ──────────────────────────────────────────────────────────
    dq_score

FROM calculate_dq_score