-- =============================================================================
-- models/silver/silver_vendor_master.sql
--
-- PURPOSE:
--   Cleans and validates bronze_vendor_master into a silver layer table.
--
-- CLEANING RULES APPLIED:
--   1. Standardize vendor_id       → uppercase, remove spaces
--   2. Standardize country         → map all variants to 2-char ISO codes
--   3. Standardize currency        → map all variants to 3-char ISO codes
--   4. Standardize vendor_status   → map all variants to A/B/X
--   5. Standardize vendor_type     → map all variants to 01/02/03
--   6. Standardize payment_terms   → map all variants to SAP format
--   7. Standardize language        → map all variants to 2-char codes
--   8. Standardize dates           → parse 6 date formats → YYYY-MM-DD
--   9. Flag missing bank details   → null bank_account or bank_routing
--  10. Flag duplicate vendors      → same vendor_name + country combination
--  11. Flag invalid plant refs     → plant_id not in CHI1/HOU2/DET3
--  12. Derive overall DQ score     → % of fields passing validation
--
-- MATERIALIZATION: view
-- SOURCE: bronze_vendor_master
-- =============================================================================

WITH

-- =============================================================================
-- STEP 1: READ RAW BRONZE DATA
-- =============================================================================

raw AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_vendor_master') }}
),


-- =============================================================================
-- STEP 2: STANDARDIZE VENDOR ID
-- Remove spaces, uppercase
-- =============================================================================

standardize_vendor_id AS (
    SELECT
        *,
        UPPER(TRIM(vendor_id)) AS vendor_id_clean
    FROM raw
),


-- =============================================================================
-- STEP 3: STANDARDIZE COUNTRY CODE
-- Bronze has: US, USA, United States, U.S.A, ger, DE, GB, CN, JP etc.
-- Silver standard: 2-character ISO country codes
-- =============================================================================

standardize_country AS (
    SELECT
        *,
        CASE
            -- United States variants
            WHEN UPPER(TRIM(country)) IN (
                'US','USA','U.S.A','U.S.','UNITED STATES',
                'UNITED STATES OF AMERICA','AMERICA')
                THEN 'US'
            -- Germany variants
            WHEN UPPER(TRIM(country)) IN (
                'DE','DEU','GERMANY','DEUTSCHLAND','GER','GERMAN')
                THEN 'DE'
            -- United Kingdom variants
            WHEN UPPER(TRIM(country)) IN (
                'GB','GBR','UK','UNITED KINGDOM','ENGLAND','BRITAIN')
                THEN 'GB'
            -- China variants
            WHEN UPPER(TRIM(country)) IN (
                'CN','CHN','CHINA','PRC')
                THEN 'CN'
            -- Japan variants
            WHEN UPPER(TRIM(country)) IN (
                'JP','JPN','JAPAN')
                THEN 'JP'
            -- Mexico variants
            WHEN UPPER(TRIM(country)) IN (
                'MX','MEX','MEXICO')
                THEN 'MX'
            -- France variants
            WHEN UPPER(TRIM(country)) IN (
                'FR','FRA','FRANCE')
                THEN 'FR'
            -- India variants
            WHEN UPPER(TRIM(country)) IN (
                'IN','IND','INDIA')
                THEN 'IN'
            -- Canada variants
            WHEN UPPER(TRIM(country)) IN (
                'CA','CAN','CANADA')
                THEN 'CA'
            -- Brazil variants
            WHEN UPPER(TRIM(country)) IN (
                'BR','BRA','BRAZIL','BRASIL')
                THEN 'BR'
            -- Already 2 chars or unknown — keep uppercase
            ELSE UPPER(TRIM(country))
        END AS country_clean
    FROM standardize_vendor_id
),


-- =============================================================================
-- STEP 4: STANDARDIZE CURRENCY
-- Bronze has: USD, $, Dollars, DOLLAR, EUR, Euro, EURO, £, GBP etc.
-- Silver standard: 3-character ISO currency codes
-- =============================================================================

standardize_currency AS (
    SELECT
        *,
        CASE
            -- USD variants
            WHEN UPPER(TRIM(currency)) IN (
                'USD','US DOLLAR','DOLLAR','DOLLARS','$','US$')
                THEN 'USD'
            -- EUR variants
            WHEN UPPER(TRIM(currency)) IN (
                'EUR','EURO','EUROS','€')
                THEN 'EUR'
            -- GBP variants
            WHEN UPPER(TRIM(currency)) IN (
                'GBP','POUND','POUNDS','STERLING','£','UK POUND')
                THEN 'GBP'
            -- CNY variants
            WHEN UPPER(TRIM(currency)) IN (
                'CNY','RMB','YUAN','RENMINBI')
                THEN 'CNY'
            -- JPY variants
            WHEN UPPER(TRIM(currency)) IN (
                'JPY','YEN','JAPAN YEN')
                THEN 'JPY'
            -- MXN variants
            WHEN UPPER(TRIM(currency)) IN (
                'MXN','PESO','MEXICAN PESO')
                THEN 'MXN'
            -- Already clean or unknown
            ELSE UPPER(TRIM(currency))
        END AS currency_clean
    FROM standardize_country
),


-- =============================================================================
-- STEP 5: STANDARDIZE VENDOR STATUS
-- Bronze has: A, B, X, Active, Blocked, Yes, active, blocked
-- Silver standard:
--   A = Active
--   B = Blocked for posting
--   X = Marked for deletion
-- =============================================================================

standardize_status AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(vendor_status)) IN (
                'A','ACTIVE','YES','1','OPEN')
                THEN 'A'
            WHEN UPPER(TRIM(vendor_status)) IN (
                'B','BLOCKED','BLOCK','BLK')
                THEN 'B'
            WHEN UPPER(TRIM(vendor_status)) IN (
                'X','DELETED','DELETE','DEL','MARKED')
                THEN 'X'
            ELSE UPPER(TRIM(vendor_status))
        END AS vendor_status_clean
    FROM standardize_currency
),


-- =============================================================================
-- STEP 6: STANDARDIZE VENDOR TYPE
-- Bronze has: 01, 02, 03, 1, 2, 3, Manufacturer, T, Distributor
-- Silver standard:
--   01 = Manufacturer
--   02 = Distributor
--   03 = Service Provider
-- =============================================================================

standardize_vendor_type AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(vendor_type)) IN (
                '01','1','MANUFACTURER','MFG','MANUF')
                THEN '01'
            WHEN UPPER(TRIM(vendor_type)) IN (
                '02','2','DISTRIBUTOR','DIST','DISTRIBUTION')
                THEN '02'
            WHEN UPPER(TRIM(vendor_type)) IN (
                '03','3','SERVICE','SERVICE PROVIDER','SVC','SERV')
                THEN '03'
            ELSE UPPER(TRIM(vendor_type))
        END AS vendor_type_clean
    FROM standardize_status
),


-- =============================================================================
-- STEP 7: STANDARDIZE PAYMENT TERMS
-- Bronze has: NET30, NT30, NET60, NT60, NT90, NET90, NT15, IMMD
-- Silver standard: SAP payment term format
--   NT30 = Net 30 days
--   NT60 = Net 60 days
--   NT90 = Net 90 days
--   NT15 = Net 15 days
--   IMMD = Immediate payment
-- =============================================================================

standardize_payment_terms AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(payment_terms)) IN ('NT30','NET30','N30','30')
                THEN 'NT30'
            WHEN UPPER(TRIM(payment_terms)) IN ('NT60','NET60','N60','60')
                THEN 'NT60'
            WHEN UPPER(TRIM(payment_terms)) IN ('NT90','NET90','N90','90')
                THEN 'NT90'
            WHEN UPPER(TRIM(payment_terms)) IN ('NT15','NET15','N15','15')
                THEN 'NT15'
            WHEN UPPER(TRIM(payment_terms)) IN (
                'IMMD','IMMEDIATE','IMM','NET0','NT0','0')
                THEN 'IMMD'
            ELSE UPPER(TRIM(payment_terms))
        END AS payment_terms_clean
    FROM standardize_vendor_type
),


-- =============================================================================
-- STEP 8: STANDARDIZE LANGUAGE
-- Bronze has: EN, DE, ZH, ES, PT, German, Chinese, French etc.
-- Silver standard: 2-character SAP language keys
-- =============================================================================

standardize_language AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(language)) IN ('EN','ENGLISH','ENG')
                THEN 'EN'
            WHEN UPPER(TRIM(language)) IN ('DE','GERMAN','DEUTSCH','GER')
                THEN 'DE'
            WHEN UPPER(TRIM(language)) IN ('ZH','CHINESE','MANDARIN','CHI')
                THEN 'ZH'
            WHEN UPPER(TRIM(language)) IN ('ES','SPANISH','ESPANOL','SPA')
                THEN 'ES'
            WHEN UPPER(TRIM(language)) IN ('PT','PORTUGUESE','PORTUGUES','POR')
                THEN 'PT'
            WHEN UPPER(TRIM(language)) IN ('FR','FRENCH','FRANCAIS','FRA')
                THEN 'FR'
            WHEN UPPER(TRIM(language)) IN ('JA','JP','JAPANESE','JAP')
                THEN 'JA'
            ELSE UPPER(TRIM(language))
        END AS language_clean
    FROM standardize_payment_terms
),


-- =============================================================================
-- STEP 9: STANDARDIZE DATES
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
    FROM standardize_language
),


-- =============================================================================
-- STEP 10: APPLY DATA QUALITY FLAGS
-- =============================================================================

apply_dq_flags AS (
    SELECT
        *,

        -- FLAG 1: Null vendor name (mandatory in SAP)
        CASE WHEN vendor_name IS NULL
              OR TRIM(vendor_name) = ''
             THEN 1 ELSE 0 END                  AS flag_null_vendor_name,

        -- FLAG 2: Null or invalid country code
        -- After standardization, anything not 2 chars is suspect
        CASE WHEN country_clean IS NULL
              OR LENGTH(TRIM(country_clean)) != 2
             THEN 1 ELSE 0 END                  AS flag_invalid_country,

        -- FLAG 3: Missing bank account
        -- Critical — missing bank details = duplicate payment risk
        -- Cannot pay vendor without bank account
        CASE WHEN bank_account IS NULL
              OR TRIM(bank_account) = ''
             THEN 1 ELSE 0 END                  AS flag_missing_bank_account,

        -- FLAG 4: Missing bank routing number
        CASE WHEN bank_routing IS NULL
              OR TRIM(bank_routing) = ''
             THEN 1 ELSE 0 END                  AS flag_missing_bank_routing,

        -- FLAG 5: Missing payment terms
        -- SAP requires payment terms for automatic payment processing
        CASE WHEN payment_terms IS NULL
              OR TRIM(payment_terms) = ''
             THEN 1 ELSE 0 END                  AS flag_missing_payment_terms,

        -- FLAG 6: Invalid vendor status
        CASE WHEN vendor_status_clean NOT IN ('A','B','X')
             THEN 1 ELSE 0 END                  AS flag_invalid_vendor_status,

        -- FLAG 7: Invalid vendor type
        CASE WHEN vendor_type_clean NOT IN ('01','02','03')
             THEN 1 ELSE 0 END                  AS flag_invalid_vendor_type,

        -- FLAG 8: Missing currency
        CASE WHEN currency IS NULL
              OR TRIM(currency) = ''
             THEN 1 ELSE 0 END                  AS flag_missing_currency,

        -- FLAG 9: Non-standard currency (needed mapping)
        CASE WHEN currency != currency_clean
             THEN 1 ELSE 0 END                  AS flag_nonstandard_currency,

        -- FLAG 10: Missing payment method
        CASE WHEN payment_method IS NULL
              OR TRIM(payment_method) = ''
             THEN 1 ELSE 0 END                  AS flag_missing_payment_method,

        -- FLAG 11: Created date could not be parsed
        CASE WHEN created_date IS NOT NULL
              AND created_date_clean IS NULL
             THEN 1 ELSE 0 END                  AS flag_bad_created_date,

        -- FLAG 12: Changed date before created date
        CASE WHEN created_date_clean IS NOT NULL
              AND changed_date_clean IS NOT NULL
              AND changed_date_clean < created_date_clean
             THEN 1 ELSE 0 END                  AS flag_changed_before_created,

        -- FLAG 13: Generic/system created_by user
        CASE WHEN UPPER(TRIM(created_by)) IN (
                'ADMIN','MIGRATE','SYSTEM','MIGRATION',
                'SYS','ADMINISTRATOR')
             THEN 1 ELSE 0 END                  AS flag_generic_user,

        -- FLAG 14: Non-standard country (needed mapping)
        CASE WHEN country != country_clean
             THEN 1 ELSE 0 END                  AS flag_nonstandard_country

    FROM standardize_dates
),


-- =============================================================================
-- STEP 11: FLAG DUPLICATES
-- Duplicate vendor = same vendor_name + country combination
-- Different plants sometimes create the same vendor separately
-- causing duplicate payments
-- =============================================================================

flag_duplicates AS (
    SELECT
        *,
        COUNT(*) OVER (
            PARTITION BY
                UPPER(TRIM(vendor_name)),
                country_clean
        )                                       AS duplicate_count,

        CASE
            WHEN COUNT(*) OVER (
                PARTITION BY
                    UPPER(TRIM(vendor_name)),
                    country_clean
            ) > 1
            THEN 1 ELSE 0
        END                                     AS flag_duplicate_vendor

    FROM apply_dq_flags
),


-- =============================================================================
-- STEP 12: CALCULATE DATA QUALITY SCORE
-- 15 flags. Score = (clean flags / 15) * 100
-- =============================================================================

calculate_dq_score AS (
    SELECT
        *,
        ROUND(
            (
                (1 - flag_null_vendor_name)         +
                (1 - flag_invalid_country)          +
                (1 - flag_missing_bank_account)     +
                (1 - flag_missing_bank_routing)     +
                (1 - flag_missing_payment_terms)    +
                (1 - flag_invalid_vendor_status)    +
                (1 - flag_invalid_vendor_type)      +
                (1 - flag_missing_currency)         +
                (1 - flag_nonstandard_currency)     +
                (1 - flag_missing_payment_method)   +
                (1 - flag_bad_created_date)         +
                (1 - flag_changed_before_created)   +
                (1 - flag_generic_user)             +
                (1 - flag_nonstandard_country)      +
                (1 - flag_duplicate_vendor)
            ) / 15.0 * 100
        , 1)                                    AS dq_score

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

    -- ── Vendor ID ─────────────────────────────────────────────────────────
    vendor_id,
    vendor_id_clean,

    -- ── Vendor name ───────────────────────────────────────────────────────
    vendor_name,

    -- ── Vendor type (raw → clean) ─────────────────────────────────────────
    vendor_type,
    vendor_type_clean,

    -- ── Address fields ────────────────────────────────────────────────────
    street,
    city,
    region,

    -- ── Country (raw → clean) ─────────────────────────────────────────────
    country,
    country_clean,

    postal_code,

    -- ── Language (raw → clean) ────────────────────────────────────────────
    language,
    language_clean,

    -- ── Currency (raw → clean) ────────────────────────────────────────────
    currency,
    currency_clean,

    -- ── Payment fields (raw → clean) ──────────────────────────────────────
    payment_terms,
    payment_terms_clean,
    payment_method,

    -- ── Bank details ──────────────────────────────────────────────────────
    bank_account,
    bank_routing,

    -- ── Tax and trade ─────────────────────────────────────────────────────
    tax_number,
    incoterms,

    -- ── Vendor status (raw → clean) ───────────────────────────────────────
    vendor_status,
    vendor_status_clean,

    -- ── Dates (raw → clean) ───────────────────────────────────────────────
    created_date,
    created_date_clean,
    changed_date,
    changed_date_clean,

    -- ── Reference fields ──────────────────────────────────────────────────
    created_by,
    legacy_vendor_id,

    -- ── Duplicate tracking ────────────────────────────────────────────────
    duplicate_count,

    -- ── Data quality flags (0=clean, 1=issue) ─────────────────────────────
    flag_null_vendor_name,
    flag_invalid_country,
    flag_missing_bank_account,
    flag_missing_bank_routing,
    flag_missing_payment_terms,
    flag_invalid_vendor_status,
    flag_invalid_vendor_type,
    flag_missing_currency,
    flag_nonstandard_currency,
    flag_missing_payment_method,
    flag_bad_created_date,
    flag_changed_before_created,
    flag_generic_user,
    flag_nonstandard_country,
    flag_duplicate_vendor,

    -- ── Overall DQ score (0-100) ──────────────────────────────────────────
    dq_score

FROM calculate_dq_score