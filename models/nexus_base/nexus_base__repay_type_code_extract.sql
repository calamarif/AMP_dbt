{{
  config({    
    "materialized": "ephemeral",
    "database": "main",
    "schema": "default"
  })
}}

WITH znex_account AS (

  SELECT * 
  
  FROM {{ source('westpac_sgdw_views', 'znex_account') }}

),

znex_loan AS (

  SELECT * 
  
  FROM {{ source('westpac_sgdw_views', 'znex_loan') }}

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

)

SELECT *

FROM repay_type_code_extract
