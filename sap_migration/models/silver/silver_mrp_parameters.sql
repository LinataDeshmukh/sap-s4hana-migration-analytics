-- =============================================================================
-- models/silver/silver_mrp_parameters.sql
--
-- PURPOSE:
--   Cleans and validates bronze_mrp_parameters into a silver layer table.
--   MRP parameters control automatic purchase order generation.
--   Bad MRP data = wrong quantities ordered = stockouts or excess inventory.
--
-- CLEANING RULES APPLIED:
--   1. Standardize mrp_type          → valid SAP MRP type codes
--   2. Standardize lot_size_key      → valid SAP lot size codes
--   3. Standardize backward_scheduling → X (active) or blank (inactive)
--   4. Standardize availability_check → valid SAP check codes
--   5. Validate plant_id             → must be CHI1, HOU2, or DET3
--   6. Standardize dates             → parse 6 formats → YYYY-MM-DD
--   7. Flag null mandatory fields    → mrp_type, safety_stock
--   8. Flag invalid values           → plant, mrp_type, lot_size
--   9. Flag negative/zero lead times → planned_delivery_days
--  10. Derive overall DQ score       → % of fields passing validation
--
-- MATERIALIZATION: view
-- SOURCE: bronze_mrp_parameters
-- =============================================================================

WITH

-- =============================================================================
-- STEP 1: READ RAW BRONZE DATA
-- =============================================================================

raw AS (
    SELECT *
    FROM {{ source('bronze', 'bronze_mrp_parameters') }}
),


-- =============================================================================
-- STEP 2: STANDARDIZE MRP TYPE
-- Valid SAP MRP types:
--   PD = MRP (standard deterministic planning)
--   VB = Reorder point planning
--   VV = Forecast-based planning
--   ND = No planning
--   MK = Master production scheduling
--   PK = Predictive MRP
--   X0 = External requirements
-- Invalid values like AUTO, MANUAL, auto get flagged
-- =============================================================================

standardize_mrp_type AS (
    SELECT
        *,
        UPPER(TRIM(mrp_type)) AS mrp_type_clean
    FROM raw
),


-- =============================================================================
-- STEP 3: STANDARDIZE LOT SIZE KEY
-- Valid SAP lot size keys:
--   EX = Exact lot size (lot for lot)
--   FX = Fixed lot size
--   HB = Replenish to maximum stock level
--   MB = Monthly lot size
--   TB = Daily lot size
--   WB = Weekly lot size
--   ZB = Periodic lot size (custom)
-- =============================================================================

standardize_lot_size AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(lot_size_key)) IN ('EX','EXACT','LOT FOR LOT','LFL')
                THEN 'EX'
            WHEN UPPER(TRIM(lot_size_key)) IN ('FX','FIXED','FIXED LOT')
                THEN 'FX'
            WHEN UPPER(TRIM(lot_size_key)) IN ('HB','REPLENISH','MAX STOCK')
                THEN 'HB'
            WHEN UPPER(TRIM(lot_size_key)) IN ('MB','MONTHLY','MONTH')
                THEN 'MB'
            WHEN UPPER(TRIM(lot_size_key)) IN ('TB','DAILY','DAY')
                THEN 'TB'
            WHEN UPPER(TRIM(lot_size_key)) IN ('WB','WEEKLY','WEEK')
                THEN 'WB'
            WHEN UPPER(TRIM(lot_size_key)) IN ('ZB','PERIODIC','PERIOD')
                THEN 'ZB'
            ELSE UPPER(TRIM(lot_size_key))
        END AS lot_size_key_clean
    FROM standardize_mrp_type
),


-- =============================================================================
-- STEP 4: STANDARDIZE BACKWARD SCHEDULING
-- Bronze has: X, 1, Y, Yes, backward, B, FWD, NULL
-- SAP standard: X = backward scheduling active, NULL/blank = not active
-- FWD means forward scheduling — opposite of backward
-- =============================================================================

standardize_scheduling AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(backward_scheduling)) IN (
                'X','1','Y','YES','TRUE','BACKWARD','B','BACK')
                THEN 'X'
            WHEN UPPER(TRIM(backward_scheduling)) IN (
                'FWD','FORWARD','0','N','NO','FALSE')
                THEN ''   -- blank = forward scheduling in SAP
            WHEN backward_scheduling IS NULL
                THEN NULL
            ELSE UPPER(TRIM(backward_scheduling))
        END AS backward_scheduling_clean
    FROM standardize_lot_size
),


-- =============================================================================
-- STEP 5: STANDARDIZE AVAILABILITY CHECK
-- Valid SAP availability check codes:
--   01 = Daily requirements
--   02 = Individual requirements
--   KP = No check
--   AV = Availability check active
--   CH = Check at plant level
-- =============================================================================

standardize_availability AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(availability_check)) IN ('01','1')
                THEN '01'
            WHEN UPPER(TRIM(availability_check)) IN ('02','2')
                THEN '02'
            WHEN UPPER(TRIM(availability_check)) IN ('KP','NO CHECK','NONE')
                THEN 'KP'
            WHEN UPPER(TRIM(availability_check)) IN ('AV','AVAILABLE','ACTIVE')
                THEN 'AV'
            WHEN UPPER(TRIM(availability_check)) IN ('CH','CHECK','PLANT')
                THEN 'CH'
            ELSE UPPER(TRIM(availability_check))
        END AS availability_check_clean
    FROM standardize_scheduling
),


-- =============================================================================
-- STEP 6: CAST NUMERIC FIELDS
-- Lead times and lot sizes stored as strings in bronze
-- Cast to DECIMAL for business rule validation
-- =============================================================================

cast_numerics AS (
    SELECT
        *,
        -- Planned delivery days
        CASE
            WHEN planned_delivery_days REGEXP '^-?[0-9]+\\.?[0-9]*$'
                THEN CAST(planned_delivery_days AS DECIMAL(10,2))
            ELSE NULL
        END AS planned_delivery_days_num,

        -- Safety stock
        CASE
            WHEN safety_stock REGEXP '^-?[0-9]+\\.?[0-9]*$'
                THEN CAST(safety_stock AS DECIMAL(15,3))
            ELSE NULL
        END AS safety_stock_num,

        -- Reorder point
        CASE
            WHEN reorder_point REGEXP '^-?[0-9]+\\.?[0-9]*$'
                THEN CAST(reorder_point AS DECIMAL(15,3))
            ELSE NULL
        END AS reorder_point_num,

        -- Goods receipt days
        CASE
            WHEN goods_receipt_days REGEXP '^-?[0-9]+\\.?[0-9]*$'
                THEN CAST(goods_receipt_days AS DECIMAL(10,2))
            ELSE NULL
        END AS goods_receipt_days_num

    FROM standardize_availability
),


-- =============================================================================
-- STEP 7: STANDARDIZE DATES
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

    FROM cast_numerics
),


-- =============================================================================
-- STEP 8: APPLY DATA QUALITY FLAGS
-- =============================================================================

apply_dq_flags AS (
    SELECT
        *,

        -- FLAG 1: Invalid plant ID
        -- Only CHI1, HOU2, DET3 are valid plants for this migration
        CASE WHEN plant_id NOT IN ('CHI1','HOU2','DET3')
             THEN 1 ELSE 0 END                  AS flag_invalid_plant,

        -- FLAG 2: Null MRP type (mandatory — determines how SAP plans)
        CASE WHEN mrp_type IS NULL
              OR TRIM(mrp_type) = ''
             THEN 1 ELSE 0 END                  AS flag_null_mrp_type,

        -- FLAG 3: Invalid MRP type — not in SAP standard list
        CASE WHEN mrp_type_clean NOT IN (
                'PD','VB','VV','ND','MK','PK','X0','VM','R1','S1')
             THEN 1 ELSE 0 END                  AS flag_invalid_mrp_type,

        -- FLAG 4: Null lot size key (mandatory for MRP calculation)
        CASE WHEN lot_size_key IS NULL
              OR TRIM(lot_size_key) = ''
             THEN 1 ELSE 0 END                  AS flag_null_lot_size_key,

        -- FLAG 5: Null safety stock on MRP-active materials
        -- If MRP type is active (PD, VB, VV, MK, PK) and safety stock
        -- is null, SAP cannot calculate correct order quantities
        CASE WHEN mrp_type_clean IN ('PD','VB','VV','MK','PK')
              AND safety_stock_num IS NULL
             THEN 1 ELSE 0 END                  AS flag_null_safety_stock,

        -- FLAG 6: Negative or zero planned delivery days
        -- Zero lead time means SAP assumes instant delivery
        -- Negative lead time is impossible
        CASE WHEN planned_delivery_days_num IS NOT NULL
              AND planned_delivery_days_num <= 0
             THEN 1 ELSE 0 END                  AS flag_invalid_lead_time,

        -- FLAG 7: Negative safety stock (impossible)
        CASE WHEN safety_stock_num IS NOT NULL
              AND safety_stock_num < 0
             THEN 1 ELSE 0 END                  AS flag_negative_safety_stock,

        -- FLAG 8: Negative reorder point (impossible)
        CASE WHEN reorder_point_num IS NOT NULL
              AND reorder_point_num < 0
             THEN 1 ELSE 0 END                  AS flag_negative_reorder_point,

        -- FLAG 9: Invalid consumption mode
        -- Valid SAP values: 1=backward, 2=forward, 3=both, 4=other
        CASE WHEN consumption_mode NOT IN ('1','2','3','4')
             THEN 1 ELSE 0 END                  AS flag_invalid_consumption_mode,

        -- FLAG 10: Created date could not be parsed
        CASE WHEN created_date IS NOT NULL
              AND created_date_clean IS NULL
             THEN 1 ELSE 0 END                  AS flag_bad_created_date,

        -- FLAG 11: Changed date before created date
        CASE WHEN created_date_clean IS NOT NULL
              AND changed_date_clean IS NOT NULL
              AND changed_date_clean < created_date_clean
             THEN 1 ELSE 0 END                  AS flag_changed_before_created,

        -- FLAG 12: Generic/system created_by user
        CASE WHEN UPPER(TRIM(created_by)) IN (
                'ADMIN','MIGRATE','SYSTEM','MIGRATION','SYS')
             THEN 1 ELSE 0 END                  AS flag_generic_user

    FROM standardize_dates
),


-- =============================================================================
-- STEP 9: CALCULATE DATA QUALITY SCORE
-- 12 flags. Score = (clean flags / 12) * 100
-- =============================================================================

calculate_dq_score AS (
    SELECT
        *,
        ROUND(
            (
                (1 - flag_invalid_plant)            +
                (1 - flag_null_mrp_type)            +
                (1 - flag_invalid_mrp_type)         +
                (1 - flag_null_lot_size_key)        +
                (1 - flag_null_safety_stock)        +
                (1 - flag_invalid_lead_time)        +
                (1 - flag_negative_safety_stock)    +
                (1 - flag_negative_reorder_point)   +
                (1 - flag_invalid_consumption_mode) +
                (1 - flag_bad_created_date)         +
                (1 - flag_changed_before_created)   +
                (1 - flag_generic_user)
            ) / 12.0 * 100
        , 1)                                    AS dq_score

    FROM apply_dq_flags
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

    -- ── Record identifier ─────────────────────────────────────────────────
    record_id,
    material_id,
    plant_id,

    -- ── MRP type (raw → clean) ────────────────────────────────────────────
    mrp_type,
    mrp_type_clean,

    -- ── MRP controller ────────────────────────────────────────────────────
    mrp_controller,

    -- ── Lot size (raw → clean) ────────────────────────────────────────────
    lot_size_key,
    lot_size_key_clean,

    -- ── Lot size parameters ───────────────────────────────────────────────
    fixed_lot_size,
    minimum_lot_size,
    maximum_lot_size,
    rounding_value,

    -- ── Stock levels (raw → numeric) ──────────────────────────────────────
    reorder_point,
    reorder_point_num,
    safety_stock,
    safety_stock_num,
    safety_time,

    -- ── Lead times (raw → numeric) ────────────────────────────────────────
    planned_delivery_days,
    planned_delivery_days_num,
    goods_receipt_days,
    goods_receipt_days_num,
    in_house_production_days,

    -- ── Scheduling (raw → clean) ──────────────────────────────────────────
    backward_scheduling,
    backward_scheduling_clean,
    scheduling_margin_key,

    -- ── Availability check (raw → clean) ──────────────────────────────────
    availability_check,
    availability_check_clean,

    -- ── MRP area and procurement ──────────────────────────────────────────
    mrp_area,
    special_procurement,
    storage_location,

    -- ── Consumption parameters ────────────────────────────────────────────
    consumption_mode,
    fwd_consumption_days,
    bwd_consumption_days,

    -- ── Dates (raw → clean) ───────────────────────────────────────────────
    created_date,
    created_date_clean,
    changed_date,
    changed_date_clean,

    -- ── Other ─────────────────────────────────────────────────────────────
    created_by,

    -- ── Data quality flags (0=clean, 1=issue) ─────────────────────────────
    flag_invalid_plant,
    flag_null_mrp_type,
    flag_invalid_mrp_type,
    flag_null_lot_size_key,
    flag_null_safety_stock,
    flag_invalid_lead_time,
    flag_negative_safety_stock,
    flag_negative_reorder_point,
    flag_invalid_consumption_mode,
    flag_bad_created_date,
    flag_changed_before_created,
    flag_generic_user,

    -- ── Overall DQ score (0-100) ──────────────────────────────────────────
    dq_score

FROM calculate_dq_score