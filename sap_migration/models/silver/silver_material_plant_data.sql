-- =============================================================================
-- models/silver/silver_material_plant_data.sql
--
-- PURPOSE:
--   Cleans and validates bronze_material_plant_data.
--   Plant data controls how each material is planned, valued and stored
--   at each plant. Bad plant data = wrong automatic orders and valuations.
--
-- CLEANING RULES APPLIED:
--   1. Validate plant_id          → must be CHI1, HOU2, DET3
--   2. Standardize mrp_type       → valid SAP MRP type codes
--   3. Standardize price_control  → S (standard) or V (moving average)
--   4. Standardize lot_size       → valid SAP lot size codes
--   5. Cast numeric fields        → safety_stock, prices, lead times
--   6. Standardize dates          → parse 6 formats → YYYY-MM-DD
--   7. Flag null mandatory fields → mrp_type, valuation_class, price_control
--   8. Flag invalid values        → plant_id, price_control, mrp_type
--   9. Derive overall DQ score    → % of fields passing validation
--
-- MATERIALIZATION: view
-- SOURCE: bronze_material_plant_data
-- =============================================================================

WITH

raw AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_material_plant_data') }}
),

-- =============================================================================
-- STEP 2: STANDARDIZE MRP TYPE
-- Same valid codes as mrp_parameters:
-- PD, VB, VV, ND, MK, PK, X0
-- =============================================================================

standardize_mrp_type AS (
    SELECT
        *,
        UPPER(TRIM(mrp_type)) AS mrp_type_clean
    FROM raw
),

-- =============================================================================
-- STEP 3: STANDARDIZE PRICE CONTROL
-- Bronze has: S, V, Moving, STD, AVG, Standard, moving avg
-- SAP standard:
--   S = Standard price (fixed, used for finished goods)
--   V = Moving average price (used for raw materials)
-- =============================================================================

standardize_price_control AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(price_control)) IN (
                'S','STD','STANDARD','STANDARD PRICE','SP')
                THEN 'S'
            WHEN UPPER(TRIM(price_control)) IN (
                'V','AVG','MOVING','MOVING AVG',
                'MOVING AVERAGE','MAP','MA')
                THEN 'V'
            ELSE UPPER(TRIM(price_control))
        END AS price_control_clean
    FROM standardize_mrp_type
),

-- =============================================================================
-- STEP 4: STANDARDIZE LOT SIZE
-- Same valid codes as mrp_parameters:
-- EX, FX, HB, MB, TB, WB, ZB
-- =============================================================================

standardize_lot_size AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(lot_size)) IN ('EX','EXACT','LOT FOR LOT','LFL')
                THEN 'EX'
            WHEN UPPER(TRIM(lot_size)) IN ('FX','FIXED','FIXED LOT')
                THEN 'FX'
            WHEN UPPER(TRIM(lot_size)) IN ('HB','REPLENISH','MAX STOCK')
                THEN 'HB'
            WHEN UPPER(TRIM(lot_size)) IN ('MB','MONTHLY','MONTH')
                THEN 'MB'
            WHEN UPPER(TRIM(lot_size)) IN ('TB','DAILY','DAY')
                THEN 'TB'
            WHEN UPPER(TRIM(lot_size)) IN ('WB','WEEKLY','WEEK')
                THEN 'WB'
            ELSE UPPER(TRIM(lot_size))
        END AS lot_size_clean
    FROM standardize_price_control
),

-- =============================================================================
-- STEP 5: CAST NUMERIC FIELDS
-- =============================================================================

cast_numerics AS (
    SELECT
        *,
        CASE
            WHEN safety_stock REGEXP '^-?[0-9]+\\.?[0-9]*$'
                THEN CAST(safety_stock AS DECIMAL(15,3))
            ELSE NULL
        END AS safety_stock_num,

        CASE
            WHEN standard_price REGEXP '^-?[0-9]+\\.?[0-9]*$'
                THEN CAST(standard_price AS DECIMAL(15,2))
            ELSE NULL
        END AS standard_price_num,

        CASE
            WHEN moving_avg_price REGEXP '^-?[0-9]+\\.?[0-9]*$'
                THEN CAST(moving_avg_price AS DECIMAL(15,2))
            ELSE NULL
        END AS moving_avg_price_num,

        CASE
            WHEN planned_delivery_days REGEXP '^-?[0-9]+\\.?[0-9]*$'
                THEN CAST(planned_delivery_days AS DECIMAL(10,2))
            ELSE NULL
        END AS planned_delivery_days_num,

        CASE
            WHEN goods_receipt_days REGEXP '^-?[0-9]+\\.?[0-9]*$'
                THEN CAST(goods_receipt_days AS DECIMAL(10,2))
            ELSE NULL
        END AS goods_receipt_days_num

    FROM standardize_lot_size
),

-- =============================================================================
-- STEP 6: STANDARDIZE DATES
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
            STR_TO_DATE(created_date, '%Y-%m-%d'),
            STR_TO_DATE(created_date, '%Y%m%d'),
            STR_TO_DATE(created_date, '%d-%b-%Y'),
            STR_TO_DATE(created_date, '%b %d, %Y'),
            STR_TO_DATE(created_date, '%m/%d/%Y'),
            STR_TO_DATE(created_date, '%d-%m-%Y')
        ) AS created_date_clean

    FROM cast_numerics
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

        -- FLAG 2: Null MRP type
        CASE WHEN mrp_type IS NULL
              OR TRIM(mrp_type) = ''
             THEN 1 ELSE 0 END                  AS flag_null_mrp_type,

        -- FLAG 3: Invalid MRP type
        CASE WHEN mrp_type_clean NOT IN (
                'PD','VB','VV','ND','MK','PK','X0','VM','R1','S1')
             THEN 1 ELSE 0 END                  AS flag_invalid_mrp_type,

        -- FLAG 4: Null or invalid price control
        -- S and V are the only valid values in SAP
        CASE WHEN price_control_clean NOT IN ('S','V')
             THEN 1 ELSE 0 END                  AS flag_invalid_price_control,

        -- FLAG 5: Null valuation class
        -- Required for SAP to determine which GL account to post to
        CASE WHEN valuation_class IS NULL
              OR TRIM(valuation_class) = ''
             THEN 1 ELSE 0 END                  AS flag_null_valuation_class,

        -- FLAG 6: Null safety stock
        CASE WHEN safety_stock IS NULL
              OR TRIM(safety_stock) = ''
             THEN 1 ELSE 0 END                  AS flag_null_safety_stock,

        -- FLAG 7: Negative standard price (impossible)
        CASE WHEN standard_price_num IS NOT NULL
              AND standard_price_num < 0
             THEN 1 ELSE 0 END                  AS flag_negative_standard_price,

        -- FLAG 8: Negative moving average price (impossible)
        CASE WHEN moving_avg_price_num IS NOT NULL
              AND moving_avg_price_num < 0
             THEN 1 ELSE 0 END                  AS flag_negative_moving_avg_price,

        -- FLAG 9: Invalid lot size
        CASE WHEN lot_size_clean NOT IN (
                'EX','FX','HB','MB','TB','WB','ZB')
             THEN 1 ELSE 0 END                  AS flag_invalid_lot_size,

        -- FLAG 10: Invalid lead time
        CASE WHEN planned_delivery_days_num IS NOT NULL
              AND planned_delivery_days_num < 0
             THEN 1 ELSE 0 END                  AS flag_invalid_lead_time,

        -- FLAG 11: Bad valid_from date
        CASE WHEN valid_from IS NOT NULL
              AND valid_from_clean IS NULL
             THEN 1 ELSE 0 END                  AS flag_bad_valid_from,

        -- FLAG 12: Bad created date
        CASE WHEN created_date IS NOT NULL
              AND created_date_clean IS NULL
             THEN 1 ELSE 0 END                  AS flag_bad_created_date

    FROM standardize_dates
),

-- =============================================================================
-- STEP 8: CALCULATE DQ SCORE
-- =============================================================================

calculate_dq_score AS (
    SELECT
        *,
        ROUND(
            (
                (1 - flag_invalid_plant)            +
                (1 - flag_null_mrp_type)            +
                (1 - flag_invalid_mrp_type)         +
                (1 - flag_invalid_price_control)    +
                (1 - flag_null_valuation_class)     +
                (1 - flag_null_safety_stock)        +
                (1 - flag_negative_standard_price)  +
                (1 - flag_negative_moving_avg_price)+
                (1 - flag_invalid_lot_size)         +
                (1 - flag_invalid_lead_time)        +
                (1 - flag_bad_valid_from)           +
                (1 - flag_bad_created_date)
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

    -- ── Record identifier ─────────────────────────────────────────────────
    record_id,
    material_id,
    plant_id,

    -- ── MRP settings (raw → clean) ────────────────────────────────────────
    mrp_type,
    mrp_type_clean,
    mrp_controller,

    -- ── Lot size (raw → clean) ────────────────────────────────────────────
    lot_size,
    lot_size_clean,
    minimum_lot_size,
    maximum_lot_size,
    fixed_lot_size,

    -- ── Stock levels (raw → numeric) ──────────────────────────────────────
    reorder_point,
    safety_stock,
    safety_stock_num,

    -- ── Lead times (raw → numeric) ────────────────────────────────────────
    planned_delivery_days,
    planned_delivery_days_num,
    goods_receipt_days,
    goods_receipt_days_num,

    -- ── Valuation (raw → clean) ───────────────────────────────────────────
    valuation_class,
    price_control,
    price_control_clean,
    standard_price,
    standard_price_num,
    moving_avg_price,
    moving_avg_price_num,

    -- ── Other fields ──────────────────────────────────────────────────────
    storage_location,
    plant_specific_status,

    -- ── Dates (raw → clean) ───────────────────────────────────────────────
    valid_from,
    valid_from_clean,
    created_date,
    created_date_clean,

    -- ── DQ flags ──────────────────────────────────────────────────────────
    flag_invalid_plant,
    flag_null_mrp_type,
    flag_invalid_mrp_type,
    flag_invalid_price_control,
    flag_null_valuation_class,
    flag_null_safety_stock,
    flag_negative_standard_price,
    flag_negative_moving_avg_price,
    flag_invalid_lot_size,
    flag_invalid_lead_time,
    flag_bad_valid_from,
    flag_bad_created_date,

    -- ── DQ score ──────────────────────────────────────────────────────────
    dq_score

FROM calculate_dq_score