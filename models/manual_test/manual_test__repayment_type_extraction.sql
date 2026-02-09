{{
  config({    
    "materialized": "ephemeral",
    "database": "main",
    "schema": "default"
  })
}}

WITH zrms_loan_account AS (

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

)

SELECT *

FROM repayment_type_extraction
