{{ config(
  schema='core', 
  partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "month"
    }
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
