{{ config(materialized='table') }}

WITH ranking as (
  SELECT 
    distinct date, 
    rank 
  FROM 
    {{ref('stg_trends')}} 
  WHERE 
    rank <= 20 
  ORDER by 
    date DESC, 
    rank ASC
) 
SELECT 
  a.*, 
  b.geoCode AS geo_MX, 
  b.value AS value_MX, 
  c.geoCode AS geo_US, 
  c.value AS value_US, 
  d.geoCode AS geo_ES, 
  d.value AS value_ES, 
  e.geoCode AS geo_AR, 
  e.value AS value_AR, 
  f.geoCode AS geo_DE, 
  f.value AS value_DE 
from 
  ranking a 
  LEFT JOIN staging_trends b on a.date = b.date 
  AND a.rank = b.rank 
  LEFT JOIN staging_trends c on a.date = c.date 
  AND a.rank = c.rank 
  LEFT JOIN staging_trends d on a.date = d.date 
  AND a.rank = d.rank 
  LEFT JOIN staging_trends e on a.date = e.date 
  AND a.rank = e.rank 
  LEFT JOIN staging_trends f on a.date = f.date 
  AND a.rank = f.rank 
WHERE 
  b.hl = 'es-MX' 
  AND c.hl = 'en-US' 
  AND d.hl = 'es-ES' 
  AND e.hl = 'es-AR' 
  AND f.hl = 'de-DE' 
ORDER BY 
  date DESC, 
  rank ASC

