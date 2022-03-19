{{ config(
  schema='core'
) }}

SELECT 
    date,
    geo,
    topic_title,
    value
    rank
  FROM 
    {{ref('stg_topics')}} 
  WHERE 
    rank <= 5 and
    results_type = 'rising'
  ORDER by 
    date DESC, 
    geo,
    rank ASC
