{{ config(
  schema='core'
) }}

SELECT 
    date,
    geo,
    query,
    value,
    rank
  FROM 
    {{ref('stg_queries')}} 
  WHERE 
    rank <= 10 and
    results_type = 'top'
  ORDER by 
    date DESC, 
    geo,
    rank ASC
