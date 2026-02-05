{{
    config(
        materialized='table',
        schema='callum',
        tags=['loan_repayment_type', 'data_lineage'],
        description='Derives Loan_Repayment_Type from ultimate source systems using documented business rules'
    )
}}

{#
/*******************************************************************************
* DBT MODEL: stg_loan_repayment_type
* 
* TARGET ATTRIBUTE: Loan_Repayment_Type
* VALID VALUES: 'Amortising', 'Interest Only', 'Revolving', ''
*
* SOURCE SYSTEMS:
*   - LIS, DDA, CHA/LNS, CHS (ZSGD_Acct_Loan / ZSGD_Acct)
*   - RAMS/RMS (ZRMS_Loan_Account)
*   - MB/MU-001 (ZWGD_Mrtg_Loan_M_Hist - Product-based inference)
*   - Nexus/ML-005 (ZNEX_LOAN)
*   - TBK (ZTBK_Account)
*
* BUSINESS RULES (Priority Order):
*   Rule 1: CHS OD_Type mapping via Refs_Repay_Type_Map
*   Rule 2: MU-001 EAL Products (11752, 11753, 11758) -> 'Interest Only'
*   Rule 3: MU-001 Product 11206 -> 'Interest Only'
*   Rule 4: COVID-19 Repayment Pause Carryover
*   Rule 5: Reference table mapping via Refs_Repay_Type_Map
*   Rule 99: Default to 'Amortising'
*******************************************************************************/
#}

{% set edw_process_date = var('edw_process_date', '20260101') %}
{% set ref_effective_date = var('ref_effective_date', '20260101') %}

WITH 
-- ============================================================================
-- STEP 1: ZSGD Sources (LIS, DDA, CHA, LNS, CHS)
-- ============================================================================
zsgd_base AS (
    SELECT
        zsgd.armt_key,
        zsgd.src_sys_code,
        CASE
            WHEN zsgd.src_sys_code = 'CHS' THEN COALESCE(zsgd_a.od_type, '')
            ELSE COALESCE(zsgd_al.repay_type_code, '')
        END AS src_repay_type_code,
        CASE
            WHEN zsgd.src_sys_code = 'CHS' THEN 'OD_Type'
            ELSE 'Repay_Type_Code'
        END AS src_column,
        CAST(NULL AS VARCHAR(10)) AS mu001_product_code,
        CAST(NULL AS VARCHAR(10)) AS mu001_system_product_type,
        CAST('{{ edw_process_date }}' AS DATE FORMAT 'YYYYMMDD') AS process_date
    FROM {{ source('edw_views', 'zsgd_acct') }} zsgd
    LEFT JOIN {{ source('edw_views', 'zsgd_acct_loan') }} zsgd_al
        ON zsgd.acct_key = zsgd_al.acct_key
        AND CAST('{{ edw_process_date }}' AS DATE FORMAT 'YYYYMMDD') 
            BETWEEN zsgd_al.from_date AND zsgd_al.to_date
    LEFT JOIN {{ source('edw_views', 'zsgd_acct') }} zsgd_a
        ON zsgd.acct_key = zsgd_a.acct_key
        AND zsgd.src_sys_code = 'CHS'
        AND CAST('{{ edw_process_date }}' AS DATE FORMAT 'YYYYMMDD') 
            BETWEEN zsgd_a.from_date AND zsgd_a.to_date
    WHERE CAST('{{ edw_process_date }}' AS DATE FORMAT 'YYYYMMDD') 
        BETWEEN zsgd.from_date AND zsgd.to_date
        AND zsgd.src_sys_code IN ('LIS', 'DDA', 'CHA', 'LNS', 'CHS')
),

-- ============================================================================
-- STEP 2: RAMS/RMS Source
-- ============================================================================
rms_base AS (
    SELECT
        zrms.armt_key,
        'RMS' AS src_sys_code,
        COALESCE(zrms_la.repayment_type, '') AS src_repay_type_code,
        'Repayment_Type' AS src_column,
        CAST(NULL AS VARCHAR(10)) AS mu001_product_code,
        CAST(NULL AS VARCHAR(10)) AS mu001_system_product_type,
        CAST('{{ edw_process_date }}' AS DATE FORMAT 'YYYYMMDD') AS process_date
    FROM {{ source('edw_views', 'zrms_account') }} zrms
    LEFT JOIN {{ source('edw_views', 'zrms_loan_account') }} zrms_la
        ON zrms.account_id = zrms_la.account_id
        AND CAST('{{ edw_process_date }}' AS DATE FORMAT 'YYYYMMDD') 
            BETWEEN zrms_la.from_date AND zrms_la.to_date
    WHERE CAST('{{ edw_process_date }}' AS DATE FORMAT 'YYYYMMDD') 
        BETWEEN zrms.from_date AND zrms.to_date
),

-- ============================================================================
-- STEP 3: MB/MU-001 Source (Product-based inference)
-- ============================================================================
mu001_base AS (
    SELECT
        zaam.armt_key,
        'MU-001' AS src_sys_code,
        '' AS src_repay_type_code,
        'Product_Code' AS src_column,
        zmlm.product_code AS mu001_product_code,
        zmlm.system_product_type AS mu001_system_product_type,
        CAST('{{ edw_process_date }}' AS DATE FORMAT 'YYYYMMDD') AS process_date
    FROM {{ source('edw_views', 'zwgd_all_acct_m_hist') }} zaam
    LEFT JOIN {{ source('edw_views', 'zwgd_mrtg_loan_m_hist') }} zmlm
        ON zaam.account_id = zmlm.account_id
        AND CAST('{{ edw_process_date }}' AS DATE FORMAT 'YYYYMMDD') 
            BETWEEN zmlm.from_date AND zmlm.to_date
    WHERE zaam.source_system_code = 'MB'
        AND CAST('{{ edw_process_date }}' AS DATE FORMAT 'YYYYMMDD') 
            BETWEEN zaam.from_date AND zaam.to_date
),

-- ============================================================================
-- STEP 4: Nexus/ML-005 Source
-- ============================================================================
nexus_base AS (
    SELECT
        znex.armt_key,
        'ML-005' AS src_sys_code,
        COALESCE(znex_l.repay_type_code, '') AS src_repay_type_code,
        'Repay_Type_Code' AS src_column,
        CAST(NULL AS VARCHAR(10)) AS mu001_product_code,
        CAST(NULL AS VARCHAR(10)) AS mu001_system_product_type,
        CAST('{{ edw_process_date }}' AS DATE FORMAT 'YYYYMMDD') AS process_date
    FROM {{ source('sgdw_views', 'znex_account') }} znex
    LEFT JOIN {{ source('sgdw_views', 'znex_loan') }} znex_l
        ON znex.account_id = znex_l.account_id
        AND CAST('{{ edw_process_date }}' AS DATE FORMAT 'YYYYMMDD') 
            BETWEEN znex_l.from_date AND znex_l.to_date
    WHERE CAST('{{ edw_process_date }}' AS DATE FORMAT 'YYYYMMDD') 
        BETWEEN znex.from_date AND znex.to_date
),

-- ============================================================================
-- STEP 5: TBK Source
-- ============================================================================
tbk_base AS (
    SELECT
        ztbk.armt_key,
        'TBK' AS src_sys_code,
        COALESCE(ztbk.repay_type_code, '') AS src_repay_type_code,
        'Repay_Type_Code' AS src_column,
        CAST(NULL AS VARCHAR(10)) AS mu001_product_code,
        CAST(NULL AS VARCHAR(10)) AS mu001_system_product_type,
        CAST('{{ edw_process_date }}' AS DATE FORMAT 'YYYYMMDD') AS process_date
    FROM {{ source('edw_views', 'ztbk_account') }} ztbk
    WHERE CAST('{{ edw_process_date }}' AS DATE FORMAT 'YYYYMMDD') 
        BETWEEN ztbk.from_date AND ztbk.to_date
),

-- ============================================================================
-- STEP 6: Union all source systems
-- ============================================================================
all_sources AS (
    SELECT * FROM zsgd_base
    UNION ALL
    SELECT * FROM rms_base
    UNION ALL
    SELECT * FROM mu001_base
    UNION ALL
    SELECT * FROM nexus_base
    UNION ALL
    SELECT * FROM tbk_base
),

-- ============================================================================
-- STEP 7: Apply business rules and derive Loan_Repayment_Type
-- ============================================================================
derived AS (
    SELECT
        b.armt_key,
        b.src_sys_code,
        b.src_repay_type_code,
        b.src_column,
        b.mu001_product_code,
        b.mu001_system_product_type,
        b.process_date,
        
        -- Reference mapping lookups
        rrtm.repay_type_code AS mapped_repay_type,
        rctm.repay_type_code AS chs_mapped_repay_type,
        COALESCE(covid.covid_efs_repay_type_code, '') AS covid_carryover_value,
        
        -- ====================================================================
        -- BUSINESS RULES APPLICATION
        -- ====================================================================
        CASE
            -- Rule 1: CHS OD_Type mapping
            WHEN b.src_sys_code = 'CHS' 
                 AND COALESCE(rctm.repay_type_code, '') <> ''
            THEN rctm.repay_type_code
            
            -- Rule 2: MU-001 EAL Products -> Interest Only
            WHEN b.src_sys_code = 'MU-001'
                 AND b.mu001_product_code IN ('11752', '11753', '11758')
                 AND b.mu001_system_product_type = 'EAL'
                 AND COALESCE(rrtm.repay_type_code, '') = ''
            THEN 'Interest Only'
            
            -- Rule 3: MU-001 Product 11206 -> Interest Only
            WHEN b.src_sys_code = 'MU-001'
                 AND b.mu001_product_code = '11206'
                 AND COALESCE(rrtm.repay_type_code, '') = ''
            THEN 'Interest Only'
            
            -- Rule 4: COVID-19 Carryover (LIS)
            WHEN b.src_sys_code = 'LIS'
                 AND COALESCE(covid.covid_efs_repay_type_code, '') <> ''
            THEN covid.covid_efs_repay_type_code
            
            -- Rule 5: Standard Reference Mapping
            WHEN b.src_sys_code <> 'CHS'
                 AND COALESCE(rrtm.repay_type_code, '') <> ''
            THEN rrtm.repay_type_code
            
            -- Rule 99: Default to Amortising
            ELSE 'Amortising'
        END AS loan_repayment_type,
        
        -- Applied rule indicator
        CASE
            WHEN b.src_sys_code = 'CHS' 
                 AND COALESCE(rctm.repay_type_code, '') <> ''
            THEN 'Rule 1 - CHS OD_Type Mapping'
            
            WHEN b.src_sys_code = 'MU-001'
                 AND b.mu001_product_code IN ('11752', '11753', '11758')
                 AND b.mu001_system_product_type = 'EAL'
                 AND COALESCE(rrtm.repay_type_code, '') = ''
            THEN 'Rule 2 - MU-001 EAL Product Default'
            
            WHEN b.src_sys_code = 'MU-001'
                 AND b.mu001_product_code = '11206'
                 AND COALESCE(rrtm.repay_type_code, '') = ''
            THEN 'Rule 3 - MU-001 Product 11206 Default'
            
            WHEN b.src_sys_code = 'LIS'
                 AND COALESCE(covid.covid_efs_repay_type_code, '') <> ''
            THEN 'Rule 4 - COVID Carryover'
            
            WHEN b.src_sys_code <> 'CHS'
                 AND COALESCE(rrtm.repay_type_code, '') <> ''
            THEN 'Rule 5 - Reference Mapping'
            
            ELSE 'Rule 99 - Default Amortising'
        END AS applied_rule
        
    FROM all_sources b
    
    -- Standard repay type mapping
    LEFT JOIN {{ source('efs', 'refs_repay_type_map') }} rrtm
        ON b.src_sys_code = rrtm.src_sys_code
        AND b.src_repay_type_code = rrtm.src_repay_type_code
        AND rrtm.src_column = 'Repay_Type_Code'
        AND CAST('{{ ref_effective_date }}' AS DATE FORMAT 'YYYYMMDD') 
            BETWEEN rrtm.from_date AND rrtm.to_date
    
    -- CHS OD_Type mapping
    LEFT JOIN {{ source('efs', 'refs_repay_type_map') }} rctm
        ON b.src_sys_code = rctm.src_sys_code
        AND b.src_repay_type_code = rctm.src_repay_type_code
        AND rctm.src_column = 'OD_Type'
        AND b.src_sys_code = 'CHS'
        AND CAST('{{ ref_effective_date }}' AS DATE FORMAT 'YYYYMMDD') 
            BETWEEN rctm.from_date AND rctm.to_date
    
    -- COVID carryover
    LEFT JOIN {{ source('efs', 'refs_covid_repay_type') }} covid
        ON b.armt_key = covid.armt_key
        AND b.src_sys_code = 'LIS'
)

SELECT
    armt_key,
    src_sys_code AS source_system_code,
    src_repay_type_code AS source_value,
    src_column,
    mu001_product_code,
    mu001_system_product_type,
    mapped_repay_type AS reference_mapped_value,
    covid_carryover_value,
    loan_repayment_type,
    applied_rule,
    process_date,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM derived
