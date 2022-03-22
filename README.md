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
We are interested in exploring the relevance and queries related to the word *'audiobook'* in **Google Explore Bar** searches. Actually the [official page](https://trends.google.com) supports several of our needs, but the (hypothetical) team requires:
- **Storage** of historical data.
- Ease of access to **results from specific locations** (Argentina, Germany, Mexico, Spain and United States).
- Linked to the above: The word 'audiobook' needs to be **adapted to different languages**. "audiolibro" in Spanish, 'Hörbuch' in German, etc.
- Internal **Dashboard with automatic information ingestion**.

To solve all this we are going to implement the following data pipeline. 

## Data Pipeline

> The current development obtains the results for the word "audiobook" exclusively. However, you only need to edit the dictionary in the [following file](dags/data_ingestion_gcs_trends.py#L26-L31) to get the results for any other word.

The pipeline works on Airflow with Docker-Compose and the [pytrends](https://github.com/GeneralMills/pytrends) library. The latter is an unofficial API for Google Trends.

### Airflow
In Airflow we have two dags:
- The first one `data_ingestion_gcs_trends.py`:
    - Download the interest by region for "audiobook" for each (five) locations.
    - Download related queries
    - Download related topics
    - Add some context information and convert into `.parquet` format (the files aren't big size and there is probably no substantial improvement by implementing this but we do it anyway for rather pedagogical purposes).
    - Finally upload to Google Cloud Storage
- `gcs_to_bq_dag.py`:
    - Moves and refine the order of files and folders.
    - Create external tables in Bigquery.

### dbt
We then transform the loaded data by developing a **dbt project**.

In dbt we set up two models:
- Staging: 
    - Separate queries, topics and trends. 
    - For each table, cast string date to the correct format and include a column with rank value. 
- Core: 
    - Transform *geo* column to join all tables (this could be done at the beginning but we chose to work with a hypothetical needs case).
    - Split the tables into tops and rising (see Google Trends page to visualize this).
    - Keep only up to 20 rows in trends, 10 queries/topics and 5 rising.
    - Create a specific table with data from our selected countries.

> All these transformations are intended to facilitate the future visualization of the results.

### Google Studio
Finally we connect Bigquery (core dataset) with Google Studio and make [this dashboard](https://datastudio.google.com/reporting/b750ff84-5922-411d-8609-53e7b300fa93).

Some aditionals notes:
- We put comments in all files for more details.
- Dags and dbt scheduled daily.

## Instructions to Run
1. Clone the repository.
2. Complete `.env_example` file with correct information (you need gcp-service-accounts-credentials)
3. Run

`sudo docker compose up`

4. Go to [localhost:8080](localhost:8080) in browser (user: airflow, pass: airflow) and run all dags.

5. Then, for analytcs engineering, you need setup dbt cloud with Bigquery. Please follow these [steps](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_4_analytics_engineering/dbt_cloud_setup.md).

Check values from these files: 
- `schemas.yml` in staging model
- `dbt_project.yml`

In dbt console run:
`dbt deps`
`dbt run`

If all goes OK you can see "staging" and "core" datasets in Bigquery.

6. Finally, connect BigQuery and Data Studio with just a [few clicks](https://support.google.com/datastudio/answer/6370296#zippy=%2Cin-this-article).
