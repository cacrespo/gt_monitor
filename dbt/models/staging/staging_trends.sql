{{ config(materialized='table') }}

SELECT
geoCode,
value,
CAST(SUBSTR(cast(date as string), 0, 4) AS INT) as year,
CAST(SUBSTR(cast(date as string), 5, 2) AS INT) as month, 
CAST(SUBSTR(cast(date as string), 7, 2) AS INT) as day,
hl,
row_number()  over (partition by hl, date order by value desc) as rank
FROM {{source('staging','trends_external_table')}}
