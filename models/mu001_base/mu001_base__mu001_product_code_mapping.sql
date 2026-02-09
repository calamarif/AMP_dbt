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

)

SELECT *

FROM mu001_product_code_mapping
