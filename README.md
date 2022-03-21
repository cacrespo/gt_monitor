# Google Trends Monitor

End-to-end data pipeline to process data from [Google Trends](https://trends.google.com).

## Technologies
- Google Cloud Platform (GCP): Google Cloud Storage and BigQuery
- Docker
- Airflow
- dbt
- Python: pandas, pytrends
- Google Data Studio

> This work is the final project of [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp). 
[Ankush](https://linkedin.com/in/ankushkhanna2), [Sejal](https://linkedin.com/in/vaidyasejal), [Victoria](https://www.linkedin.com/in/victoriaperezmola/) and [Alexey](https://linkedin.com/in/agrigorev): Thank you very much! 😀

## Problem Description
- We are interested in explore the relevance and related queries for *'audiobook'* word in searchs from **Google Explore Bar**. Actually the official page support several of our needs, but the team requires:
- Storage the historical data.
- Easy way to access to results from specific locations (Argentine, Germany, Mexico, Spain and United States).
- Link with the previous: 'audiobook' word needs to adapt to distinct languages. "audiolibro" in Spanish, 'Hörbuch' in German, etc.
- Dashboard in-house with automatic ingestion of information.

## Data Pipeline
First setting up Airflow with Docker-Compose and [pytrends](https://github.com/GeneralMills/pytrends) library. That is a unnoficial API for Google Trends.

In Airflow we have two dags:
- The first "data_ingestion_gcs_trends":
    - Download the interest by region for "audiobook" for each (five) locations.
    - Download related queries
    - Download related topics
    - Add some context information and convert into .parquet format
    - Finally upload to Google Cloud Storage
- "gcs_to_bq_dag":
    - Move the files to better order and create external tables in Bigquery.

Next transforming the data loaded in DWH to Analytical Views developing a dbt project.

In dbt we configured two models:
- Staging: 
    - Separate views from queries, topics and trends. 
    - For every table, cast string date to correct format and include a column with rank value. 
- Core: 
    - Transform HL column to join all tables (this could be do it in the first but we want to work with a hipotetical needs case).
    - Divide tables in tops and rising (see Google Trends page to visualizate).
    - Mantain only up to 20 rows in trends, 10 queries/topics and 5 rising.
    - Create a specific table with the data from our selected countrys.

> All this transformations has the objetive to make more easy future visualization of results.

Finally we connect Bigquery Producion Schema with Google Studio and make this dashboard.

Some aditionals notes:
- We put comments in all files for more details.
- Dags scheduled daily.
- In dbt we have two enviroments accord branches for the repository (main and development). "run all" production job run daily from main.


## Instructions to Run
1. Clone the repository.
2. Complete .env_example file with correct information (you need gcp-service-accounts-credentials)
3. Run

`sudo docker compose up`

4. Go to localhost:8080 in browser (user: airflow, pass: airflow) and run all dags.

5. Then, for analytcs engineering, you need setup dbt cloud with bigquery. Please follow this [steps](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_4_analytics_engineering/dbt_cloud_setup.md).

Check values from these files: 
- schemas.yml in staging model
- dbt_project.yml

In dbt console run:
`dbt deps`
`dbt run`

If all goes OK you can see "staging" and "core" datasets in Bigquery.

6. Finally, connect BigQuery and Data Studio with just a few clicks.
