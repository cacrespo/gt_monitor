{{ config(materialized='view') }}

SELECT
geoCode,
value, 
DATE(
CAST(SUBSTR(cast(date as string), 0, 4) AS INT),
CAST(SUBSTR(cast(date as string), 5, 2) AS INT), 
CAST(SUBSTR(cast(date as string), 7, 2) AS INT)) as date,
hl,
row_number()  over (partition by hl, date order by value desc) as rank
FROM {{source('staging','trends_external_table')}}
