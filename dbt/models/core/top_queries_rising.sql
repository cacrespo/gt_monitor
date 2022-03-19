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
    rank <= 5 and
    results_type = 'rising'
  ORDER by 
    date DESC, 
    geo,
    rank ASC
