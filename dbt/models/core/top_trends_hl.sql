-- TODO: Refactor for loop
{{ config(
  schema='core', 
  partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "month"
    }
) }}

(
  SELECT 
    date, 
    geoCode, 
    value, 
    rank 
  FROM 
    {{ref('stg_trends') }} 
  WHERE 
    geoCode = 'AR' 
    AND hl = 'es-AR' 
  ORDER by 
    date DESC, 
    geoCode ASC
) 
UNION ALL 
  (
    SELECT 
      date, 
      geoCode, 
      value, 
      rank 
    FROM 
      {{ref('stg_trends') }} 
    WHERE 
      geoCode = 'ES' 
      AND hl = 'es-ES' 
    ORDER by 
      date DESC, 
      geoCode ASC
  ) 
UNION ALL 
  (
    SELECT 
      date, 
      geoCode, 
      value, 
      rank 
    FROM 
      {{ref('stg_trends') }} 
    WHERE 
      geoCode = 'MX' 
      AND hl = 'es-MX' 
    ORDER by 
      date DESC, 
      geoCode ASC
  ) 
UNION ALL 
  (
    SELECT 
      date, 
      geoCode, 
      value, 
      rank 
    FROM 
      {{ref('stg_trends') }} 
    WHERE 
      geoCode = 'US' 
      AND hl = 'en-US' 
    ORDER by 
      date DESC, 
      geoCode ASC
  ) 
UNION ALL 
  (
    SELECT 
      date, 
      geoCode, 
      value, 
      rank 
    FROM 
      {{ref('stg_trends') }} 
    WHERE 
      geoCode = 'DE' 
      AND hl = 'de-DE' 
    ORDER by 
      date DESC, 
      geoCode ASC
  )
