{{ config(
  materialized='table',
  schema='core'
) }}

SELECT 
    date,
    rank,
    value,
    hl
  FROM 
    {{ref('stg_trends')}} 
  WHERE 
    rank <= 20 
  ORDER by 
    date DESC, 
    rank ASC
