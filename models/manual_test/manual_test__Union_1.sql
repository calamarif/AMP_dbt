{{
  config({    
    "materialized": "ephemeral",
    "database": "main",
    "schema": "default"
  })
}}

WITH zwgd_mrtg_loan_m_hist AS (

  SELECT * 
  
  FROM {{ source('westpac_edw_views', 'zwgd_mrtg_loan_m_hist') }}

),

ztbk_account AS (

  SELECT * 
  
  FROM {{ source('westpac_edw_views', 'ztbk_account') }}

),

zwgd_all_acct_m_hist AS (

  SELECT * 
  
  FROM {{ source('westpac_edw_views', 'zwgd_all_acct_m_hist') }}

),

account_loan_product_join AS (

  SELECT 
    zwgd_all_acct_m_hist.Armt_Key,
    zwgd_all_acct_m_hist.Source_System_Code,
    zwgd_all_acct_m_hist.From_Date,
    zwgd_all_acct_m_hist.To_Date,
    zwgd_mrtg_loan_m_hist.Product_Code,
    zwgd_mrtg_loan_m_hist.System_Product_Type
  
  FROM zwgd_all_acct_m_hist
  LEFT JOIN zwgd_mrtg_loan_m_hist
     ON zwgd_all_acct_m_hist.Account_Id = zwgd_mrtg_loan_m_hist.Account_Id
    AND TO_DATE('{{ edw_process_date }}', 'yyyyMMdd') BETWEEN zwgd_mrtg_loan_m_hist.From_Date AND zwgd_mrtg_loan_m_hist.To_Date

),

mb_accounts_filtered AS (

  SELECT * 
  
  FROM account_loan_product_join
  
  WHERE Source_System_Code = 'MB'
        AND TO_DATE('{{ edw_process_date }}', 'yyyyMMdd') BETWEEN From_Date AND To_Date

),

znex_loan AS (

  SELECT * 
  
  FROM {{ source('westpac_sgdw_views', 'znex_loan') }}

),

zsgd_acct_loan AS (

  SELECT * 
  
  FROM {{ source('westpac_edw_views', 'zsgd_acct_loan') }}

),

zsgd_acct AS (

  SELECT * 
  
  FROM {{ source('westpac_edw_views', 'zsgd_acct') }}

),

source_repay_type_data AS (

  SELECT 
    zsgd.From_Date AS zsgd_From_Date,
    zsgd.To_Date AS zsgd_To_Date,
    zsgd.Acct_Key AS zsgd_Acct_Key,
    zsgd_al.Acct_Key AS zsgd_al_Acct_Key,
    zsgd.Src_Sys_Code AS zsgd_Src_Sys_Code,
    zsgd_al.From_Date AS zsgd_al_From_Date,
    zsgd_a.From_Date AS zsgd_a_From_Date,
    zsgd_al.To_Date AS zsgd_al_To_Date,
    zsgd_a.To_Date AS zsgd_a_To_Date,
    zsgd_al.Repay_Type_Code AS zsgd_al_Repay_Type_Code,
    zsgd_a.OD_Type AS zsgd_a_OD_Type,
    zsgd.Armt_Key AS zsgd_Armt_Key,
    zsgd_a.Acct_Key AS zsgd_a_Acct_Key
  
  FROM zsgd_acct AS zsgd
  LEFT JOIN zsgd_acct_loan AS zsgd_al
     ON zsgd.Acct_Key = zsgd_al.Acct_Key
    AND TO_DATE('{{ edw_process_date }}', 'yyyyMMdd') BETWEEN zsgd_al.From_Date AND zsgd_al.To_Date
  LEFT JOIN zsgd_acct AS zsgd_a
     ON zsgd.Acct_Key = zsgd_a.Acct_Key
    AND zsgd.Src_Sys_Code = 'CHS'
    AND TO_DATE('{{ edw_process_date }}', 'yyyyMMdd') BETWEEN zsgd_a.From_Date AND zsgd_a.To_Date

),

filtered_source_data AS (

  SELECT * 
  
  FROM source_repay_type_data
  
  WHERE TO_DATE('{{ edw_process_date }}', 'yyyyMMdd') BETWEEN zsgd_From_Date AND zsgd_To_Date
        AND zsgd_Src_Sys_Code IN ('LIS', 'DDA', 'CHA', 'LNS', 'CHS')

),

repay_type_source_data AS (

  SELECT 
    zsgd_Armt_Key AS armt_key,
    zsgd_Src_Sys_Code AS src_sys_code,
    CASE
      WHEN zsgd_Src_Sys_Code = 'CHS'
        THEN COALESCE(zsgd_a_OD_Type, '')
      ELSE COALESCE(zsgd_al_Repay_Type_Code, '')
    END AS src_repay_type_code,
    CASE
      WHEN zsgd_Src_Sys_Code = 'CHS'
        THEN 'OD_Type'
      ELSE 'Repay_Type_Code'
    END AS src_column,
    CAST(NULL AS STRING) AS mu001_product_code,
    CAST(NULL AS STRING) AS mu001_system_product_type,
    TO_DATE('{{ edw_process_date }}', 'yyyyMMdd') AS process_date
  
  FROM filtered_source_data

),

znex_account AS (

  SELECT * 
  
  FROM {{ source('westpac_sgdw_views', 'znex_account') }}

),

account_loan_joined_data AS (

  SELECT 
    znex_l.Account_Id AS znex_l_Account_Id,
    znex.Account_Id AS znex_Account_Id,
    znex_l.From_Date AS znex_l_From_Date,
    znex.Armt_Key AS znex_Armt_Key,
    znex_l.Repay_Type_Code AS znex_l_Repay_Type_Code,
    znex.From_Date AS znex_From_Date,
    znex.To_Date AS znex_To_Date,
    znex_l.To_Date AS znex_l_To_Date
  
  FROM znex_account AS znex
  LEFT JOIN znex_loan AS znex_l
     ON znex.Account_Id = znex_l.Account_Id
    AND TO_DATE('{{ edw_process_date }}', 'yyyyMMdd') BETWEEN znex_l.From_Date AND znex_l.To_Date

),

mu001_product_code_mapping AS (

  SELECT 
    Armt_Key AS ARMT_KEY,
    'MU-001' AS SRC_SYS_CODE,
    '' AS SRC_REPAY_TYPE_CODE,
    'Product_Code' AS SRC_COLUMN,
    Product_Code AS MU001_PRODUCT_CODE,
    System_Product_Type AS MU001_SYSTEM_PRODUCT_TYPE,
    TO_DATE('{{ edw_process_date }}', 'yyyyMMdd') AS PROCESS_DATE
  
  FROM mb_accounts_filtered

),

zrms_loan_account AS (

  SELECT * 
  
  FROM {{ source('westpac_edw_views', 'zrms_loan_account') }}

),

zrms_account AS (

  SELECT * 
  
  FROM {{ source('westpac_edw_views', 'zrms_account') }}

),

joined_account_repayment_data AS (

  SELECT 
    zrms_la.To_Date AS zrms_la_To_Date,
    zrms_la.From_Date AS zrms_la_From_Date,
    zrms.To_Date AS zrms_To_Date,
    zrms_la.Repayment_Type AS zrms_la_Repayment_Type,
    zrms_la.Account_Id AS zrms_la_Account_Id,
    zrms.Account_Id AS zrms_Account_Id,
    zrms.From_Date AS zrms_From_Date
  
  FROM zrms_account AS zrms
  LEFT JOIN zrms_loan_account AS zrms_la
     ON zrms.Account_Id = zrms_la.Account_Id
    AND TO_DATE('{{ edw_process_date }}', 'yyyyMMdd') BETWEEN zrms_la.From_Date AND zrms_la.To_Date

),

filtered_active_accounts AS (

  SELECT * 
  
  FROM joined_account_repayment_data
  
  WHERE TO_DATE('{{ edw_process_date }}', 'yyyyMMdd') BETWEEN zrms_From_Date AND zrms_To_Date

),

repayment_type_extraction AS (

  SELECT 
    zrms_Account_Id AS armt_key,
    'RMS' AS src_sys_code,
    COALESCE(zrms_la_Repayment_Type, '') AS src_repay_type_code,
    'Repayment_Type' AS src_column,
    CAST(NULL AS STRING) AS mu001_product_code,
    CAST(NULL AS STRING) AS mu001_system_product_type,
    TO_DATE('{{ edw_process_date }}', 'yyyyMMdd') AS process_date
  
  FROM filtered_active_accounts

),

account_date_filter AS (

  SELECT * 
  
  FROM account_loan_joined_data
  
  WHERE TO_DATE('{{ edw_process_date }}', 'yyyyMMdd') BETWEEN znex_From_Date AND znex_To_Date

),

repay_type_code_extract AS (

  SELECT 
    znex_Armt_Key AS armt_key,
    'ML-005' AS src_sys_code,
    COALESCE(znex_l_Repay_Type_Code, '') AS src_repay_type_code,
    'Repay_Type_Code' AS src_column,
    CAST(NULL AS STRING) AS mu001_product_code,
    CAST(NULL AS STRING) AS mu001_system_product_type,
    TO_DATE('{{ edw_process_date }}', 'yyyyMMdd') AS process_date
  
  FROM account_date_filter

),

filtered_tbk_accounts AS (

  SELECT * 
  
  FROM ztbk_account AS ztbk
  
  WHERE TO_DATE('{{ edw_process_date }}', 'yyyyMMdd') BETWEEN ztbk.From_Date AND ztbk.To_Date

),

tbk_base AS (

  SELECT 
    ztbk.Armt_Key AS armt_key,
    'TBK' AS src_sys_code,
    COALESCE(ztbk.Repay_Type_Code, '') AS src_repay_type_code,
    'Repay_Type_Code' AS src_column,
    CAST(NULL AS STRING) AS mu001_product_code,
    CAST(NULL AS STRING) AS mu001_system_product_type,
    TO_DATE('{{ edw_process_date }}', 'yyyyMMdd') AS process_date
  
  FROM filtered_tbk_accounts AS ztbk

),

Union_1 AS (

  SELECT * 
  
  FROM repay_type_source_data AS in0
  
  UNION
  
  SELECT * 
  
  FROM tbk_base AS in1
  
  UNION
  
  SELECT * 
  
  FROM repayment_type_extraction AS in2
  
  UNION
  
  SELECT * 
  
  FROM repay_type_code_extract AS in3
  
  UNION
  
  SELECT * 
  
  FROM mu001_product_code_mapping AS in4

)

SELECT *

FROM Union_1
