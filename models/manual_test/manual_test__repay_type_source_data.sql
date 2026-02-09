{{
  config({    
    "materialized": "ephemeral",
    "database": "main",
    "schema": "default"
  })
}}

WITH zsgd_acct_loan AS (

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

)

SELECT *

FROM repay_type_source_data
