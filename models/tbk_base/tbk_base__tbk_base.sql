{{
  config({    
    "materialized": "ephemeral",
    "database": "main",
    "schema": "default"
  })
}}

WITH ztbk_account AS (

  SELECT * 
  
  FROM {{ source('westpac_edw_views', 'ztbk_account') }}

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

)

SELECT *

FROM tbk_base
