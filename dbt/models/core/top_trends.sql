{{ config(
  materialized='table',
  schema='core'
) }}

SELECT 
    date, 
    geoCode,
    rank,
    value,
    hl
  FROM 
    {{ref('stg_trends')}} 
  WHERE 
    rank <= 20 
  ORDER by 
    date DESC,
    hl,
    rank ASC
