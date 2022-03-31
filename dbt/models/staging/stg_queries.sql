{{ config(
  schema='staging'
) }}

SELECT
DATE(
CAST(SUBSTR(cast(date as string), 0, 4) AS INT),
CAST(SUBSTR(cast(date as string), 5, 2) AS INT), 
CAST(SUBSTR(cast(date as string), 7, 2) AS INT)) as date,
date as date_original,
geo,
query, 
value,
results_type,
row_number() over (partition by geo, date, results_type order by value desc) as rank
FROM {{source('staging','queries_external_table')}}
