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
    CASE geo -- with this fix key value to join tables
        WHEN 'AR' THEN 'es-AR'
        WHEN 'ES' THEN 'es-ES'
        WHEN 'MX' THEN 'es-MX'
        WHEN 'US' THEN 'en-US'
        WHEN 'DE' THEN 'de-DE'                            
        END as hl,
    query,
    value,
    rank
  FROM 
    {{ref('stg_queries')}} 
  WHERE 
    rank <= 5 and
    results_type = 'rising'
