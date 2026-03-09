-- =============================================================================
-- models/silver/silver_gl_accounts.sql
--
-- PURPOSE:
--   Cleans and validates bronze_gl_accounts into a silver layer table.
--   GL accounts are the backbone of SAP financial posting.
--   Bad GL data = postings rejected, financial statements incorrect,
--   month-end close failures.
--
-- CLEANING RULES APPLIED:
--   1. Validate company_code      → must be 1000, 2000, 3000
--   2. Standardize account_type   → S (balance sheet) or P (P&L)
--   3. Standardize currency       → 3-char ISO codes
--   4. Standardize boolean fields → Y/N for all flag columns
--   5. Standardize dates          → parse 6 formats → YYYY-MM-DD
--   6. Flag deletion_flag         → X means marked for deletion
--   7. Flag missing mandatory     → description, account_group
--   8. Derive overall DQ score    → % of fields passing validation
--
-- MATERIALIZATION: view
-- SOURCE: bronze_gl_accounts
-- =============================================================================

WITH

raw AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_gl_accounts') }}
),

-- =============================================================================
-- STEP 2: STANDARDIZE COMPANY CODE LOOKUP
-- Valid: 1000=Chicago, 2000=Houston, 3000=Detroit
-- =============================================================================

standardize_company AS (
    SELECT
        *,
        CASE
            WHEN TRIM(company_code) IN ('1000','2000','3000')
                THEN TRIM(company_code)
            ELSE TRIM(company_code)
        END AS company_code_clean
    FROM raw
),

-- =============================================================================
-- STEP 3: STANDARDIZE ACCOUNT TYPE
-- Bronze has: S, P, sheet, Balance, profit, balance sheet, P&L
-- SAP standard:
--   S = Balance sheet account
--   P = Profit and loss account
-- =============================================================================

standardize_account_type AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(account_type)) IN (
                'S','BALANCE','BALANCE SHEET','BS','SHEET')
                THEN 'S'
            WHEN UPPER(TRIM(account_type)) IN (
                'P','PROFIT','PROFIT AND LOSS','P&L',
                'INCOME','EXPENSE','PROFIT/LOSS')
                THEN 'P'
            ELSE UPPER(TRIM(account_type))
        END AS account_type_clean
    FROM standardize_company
),

-- =============================================================================
-- STEP 4: STANDARDIZE CURRENCY
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
    FROM standardize_account_type
),

-- =============================================================================
-- STEP 5: STANDARDIZE BOOLEAN FLAG COLUMNS
-- Multiple columns have Y/N/True/False/1/0/Yes/No mixed
-- Standardize all to Y/N
-- =============================================================================

standardize_booleans AS (
    SELECT
        *,
        -- Reconciliation account flag
        CASE
            WHEN UPPER(TRIM(reconciliation_acct)) IN (
                'Y','YES','1','TRUE','X')
                THEN 'Y'
            WHEN UPPER(TRIM(reconciliation_acct)) IN (
                'N','NO','0','FALSE','')
                THEN 'N'
            ELSE UPPER(TRIM(reconciliation_acct))
        END AS reconciliation_acct_clean,

        -- Line item display flag
        CASE
            WHEN UPPER(TRIM(line_item_display)) IN (
                'Y','YES','1','TRUE','X')
                THEN 'Y'
            WHEN UPPER(TRIM(line_item_display)) IN (
                'N','NO','0','FALSE','')
                THEN 'N'
            ELSE UPPER(TRIM(line_item_display))
        END AS line_item_display_clean,

        -- Posting block flag
        CASE
            WHEN UPPER(TRIM(posting_block)) IN (
                'Y','YES','1','TRUE','X')
                THEN 'Y'
            WHEN UPPER(TRIM(posting_block)) IN (
                'N','NO','0','FALSE','')
                THEN 'N'
            ELSE UPPER(TRIM(posting_block))
        END AS posting_block_clean

    FROM standardize_currency
),

-- =============================================================================
-- STEP 6: STANDARDIZE DATES
-- =============================================================================

standardize_dates AS (
    SELECT
        *,
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

    FROM standardize_booleans
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

        -- FLAG 2: Null or empty GL description
        CASE WHEN description IS NULL
              OR TRIM(description) = ''
             THEN 1 ELSE 0 END                  AS flag_null_description,

        -- FLAG 3: Invalid account type
        CASE WHEN account_type_clean NOT IN ('S','P')
             THEN 1 ELSE 0 END                  AS flag_invalid_account_type,

        -- FLAG 4: Null account group
        -- Required for GL master data creation in SAP
        CASE WHEN account_group IS NULL
              OR TRIM(account_group) = ''
             THEN 1 ELSE 0 END                  AS flag_null_account_group,

        -- FLAG 5: Missing currency
        CASE WHEN currency IS NULL
              OR TRIM(currency) = ''
             THEN 1 ELSE 0 END                  AS flag_missing_currency,

        -- FLAG 6: Non-standard currency
        CASE WHEN currency != currency_clean
             THEN 1 ELSE 0 END                  AS flag_nonstandard_currency,

        -- FLAG 7: Invalid reconciliation account flag
        CASE WHEN reconciliation_acct_clean NOT IN ('Y','N')
             THEN 1 ELSE 0 END                  AS flag_invalid_recon_acct,

        -- FLAG 8: Invalid line item display flag
        CASE WHEN line_item_display_clean NOT IN ('Y','N')
             THEN 1 ELSE 0 END                  AS flag_invalid_line_item,

        -- FLAG 9: Invalid posting block flag
        CASE WHEN posting_block_clean NOT IN ('Y','N')
             THEN 1 ELSE 0 END                  AS flag_invalid_posting_block,

        -- FLAG 10: Marked for deletion
        -- X means this GL account should not be migrated
        CASE WHEN UPPER(TRIM(deletion_flag)) = 'X'
             THEN 1 ELSE 0 END                  AS flag_marked_for_deletion,

        -- FLAG 11: Bad created date
        CASE WHEN created_date IS NOT NULL
              AND created_date_clean IS NULL
             THEN 1 ELSE 0 END                  AS flag_bad_created_date,

        -- FLAG 12: Changed before created
        CASE WHEN created_date_clean IS NOT NULL
              AND changed_date_clean IS NOT NULL
              AND changed_date_clean < created_date_clean
             THEN 1 ELSE 0 END                  AS flag_changed_before_created,

        -- FLAG 13: Generic created_by user
        CASE WHEN UPPER(TRIM(created_by)) IN (
                'ADMIN','MIGRATE','SYSTEM','MIGRATION','SYS')
             THEN 1 ELSE 0 END                  AS flag_generic_user

    FROM standardize_dates
),

-- =============================================================================
-- STEP 8: CALCULATE DQ SCORE
-- 13 flags. Score = (clean flags / 13) * 100
-- =============================================================================

calculate_dq_score AS (
    SELECT
        *,
        ROUND(
            (
                (1 - flag_invalid_company_code)     +
                (1 - flag_null_description)         +
                (1 - flag_invalid_account_type)     +
                (1 - flag_null_account_group)       +
                (1 - flag_missing_currency)         +
                (1 - flag_nonstandard_currency)     +
                (1 - flag_invalid_recon_acct)       +
                (1 - flag_invalid_line_item)        +
                (1 - flag_invalid_posting_block)    +
                (1 - flag_marked_for_deletion)      +
                (1 - flag_bad_created_date)         +
                (1 - flag_changed_before_created)   +
                (1 - flag_generic_user)
            ) / 13.0 * 100
        , 1)                                    AS dq_score
    FROM apply_dq_flags
)

SELECT

    -- ── Audit columns ─────────────────────────────────────────────────────
    load_id,
    _batch_id,
    _source_file,
    _ingestion_timestamp,

    -- ── GL account identifier ─────────────────────────────────────────────
    gl_account,
    description,

    -- ── Company ───────────────────────────────────────────────────────────
    company_code,
    company_code_clean,

    -- ── Account classification (raw → clean) ──────────────────────────────
    account_type,
    account_type_clean,
    account_group,

    -- ── Financial statement item ───────────────────────────────────────────
    fs_item,

    -- ── Currency (raw → clean) ────────────────────────────────────────────
    currency,
    currency_clean,

    -- ── Tax and posting settings ──────────────────────────────────────────
    tax_category,
    sort_key,
    field_status_group,

    -- ── Boolean flags (raw → clean) ───────────────────────────────────────
    reconciliation_acct,
    reconciliation_acct_clean,
    line_item_display,
    line_item_display_clean,
    posting_block,
    posting_block_clean,

    -- ── Deletion flag ─────────────────────────────────────────────────────
    deletion_flag,

    -- ── Dates (raw → clean) ───────────────────────────────────────────────
    created_date,
    created_date_clean,
    changed_date,
    changed_date_clean,

    -- ── Reference ─────────────────────────────────────────────────────────
    created_by,
    remarks,

    -- ── DQ flags ──────────────────────────────────────────────────────────
    flag_invalid_company_code,
    flag_null_description,
    flag_invalid_account_type,
    flag_null_account_group,
    flag_missing_currency,
    flag_nonstandard_currency,
    flag_invalid_recon_acct,
    flag_invalid_line_item,
    flag_invalid_posting_block,
    flag_marked_for_deletion,
    flag_bad_created_date,
    flag_changed_before_created,
    flag_generic_user,

    -- ── DQ score ──────────────────────────────────────────────────────────
    dq_score

FROM calculate_dq_score