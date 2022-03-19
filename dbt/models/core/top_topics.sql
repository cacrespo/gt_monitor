{{ config(
  schema='core'
) }}

SELECT 
    date,
    geo,
    topic_title,
    value,
    rank
  FROM 
    {{ref('stg_topics')}} 
  WHERE 
    rank <= 10 and
    results_type = 'top'
  ORDER by 
    date DESC, 
    geo,
    rank ASC
