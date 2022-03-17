{{ config(materialized='table') }}

DECLARE codes ARRAY<STRUCT<country STRING, code_hl STRING>>;
DECLARE dsql string;


set codes = (
[('AR','es-AR'),
('MX','es-MX'),
('ES','es-ES'),
('US','en-US'),
('DE','de-DE')]);

SET dsql = """
(SELECT 
date,
geoCode,
value,
rank
  FROM 
    {{ref('stg_trends')}} 
  WHERE 
    geoCode = '%s'
    AND hl = '%s'
  ORDER by 
    date DESC, hl ASC)
""";

EXECUTE IMMEDIATE(
select 
format(dsql, codes[OFFSET(0)].country, codes[OFFSET(0)].code_hl)
|| 'UNION ALL' ||
format(dsql, codes[OFFSET(1)].country, codes[OFFSET(1)].code_hl)
|| 'UNION ALL' ||
format(dsql, codes[OFFSET(2)].country, codes[OFFSET(2)].code_hl)
|| 'UNION ALL' ||
format(dsql, codes[OFFSET(3)].country, codes[OFFSET(3)].code_hl)
|| 'UNION ALL' ||
format(dsql, codes[OFFSET(4)].country, codes[OFFSET(4)].code_hl)
)
