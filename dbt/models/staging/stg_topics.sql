{{ config(
  schema='staging'
) }}

SELECT
DATE(
CAST(SUBSTR(cast(date as string), 0, 4) AS INT),
CAST(SUBSTR(cast(date as string), 5, 2) AS INT), 
CAST(SUBSTR(cast(date as string), 7, 2) AS INT)) as date,
geo,
topic_title, 
value,
results_type,
row_number() over (partition by geo, date, results_type order by value desc) as rank
FROM {{source('staging','topics_external_table')}}
