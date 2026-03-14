"""One-time DAG: load historical SFO weather from NOAA BigQuery public data into GCS.

This DAG was run once to bootstrap the historical dataset and is NOT included
in Cloud Composer.  The daily pipeline (dags/daily_weather_pipeline.py) handles
all ongoing ingestion.
"""

import json
import requests
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

BUCKET_NAME = "msds697_group_project_bucket1"
LAT_LON = "37.6188,-122.3754 "
GCS_CONN_ID = "google_cloud_default"
STAGING_TABLE = "msds697-group-project.sfo_weather.historical_weather"
# Pull data for SFO station (724940)
SFO_WEATHER_QUERY = f"""
    SELECT
      year,
      mo AS month,
      da AS day,
      temp AS mean_temp_f,
      min AS min_temp_f,
      max AS max_temp_f,
      prcp AS precipitation_inches,
      wdsp AS wind_speed_knots
    FROM
      `bigquery-public-data.noaa_gsod.gsod*`
    WHERE
      stn = '724940' AND wban = '23234'
    ORDER BY
      year DESC, month DESC, day DESC
"""

def scrape_weather_to_gcs(ds_nodash, **kwargs):
    """
    Scrapes NWS API and uploads the JSON directly to GCS.
    """
    headers = {
        'User-Agent': '(myweatherapp.com, contact@example.com)',
        'Accept': 'application/geo+json'
    }

    points_url = f"https://api.weather.gov/points/{LAT_LON}"
    response = requests.get(points_url, headers=headers)
    response.raise_for_status()
    
    forecast_url = response.json()['properties']['forecast']
    forecast_response = requests.get(forecast_url, headers=headers)
    forecast_response.raise_for_status()
    weather_data = forecast_response.json()

    hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
    
    object_name = f"daily_weather_data/{ds_nodash}.json"
    
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        data=json.dumps(weather_data),
        mime_type='application/json'
    )
    print(f"Successfully uploaded weather data to gs://{BUCKET_NAME}/{object_name}")


with DAG(
    dag_id="sfo_weather_to_gcs",
    start_date=datetime(2026, 2, 20),
    schedule_interval="@once",
    catchup=False,
) as dag:

    # Run query and save to table
    query_sfo_weather = BigQueryInsertJobOperator(
        task_id="query_sfo_weather",
        gcp_conn_id=GCS_CONN_ID,
        project_id="msds697-group-project",
        location="US",
        configuration={
            "query": {
                "query": SFO_WEATHER_QUERY,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": STAGING_TABLE.split('.')[0],
                    "datasetId": STAGING_TABLE.split('.')[1],
                    "tableId": STAGING_TABLE.split('.')[2],
                },
                "writeDisposition": "WRITE_TRUNCATE", 
            }
        }
    )
    # Export table to GCS as CSV
    export_sfo_to_gcs = BigQueryToGCSOperator(
        task_id="export_sfo_to_gcs",
        source_project_dataset_table=STAGING_TABLE,
        destination_cloud_storage_uris=[f"gs://{BUCKET_NAME}/weather_data/sfo_historical_weather.csv"],
        export_format="CSV",
        print_header=True,
        gcp_conn_id=GCS_CONN_ID,
    )
    
    # Scrape and upload API weather data
    scrape_task = PythonOperator(
        task_id="scrape_nws_api",
        python_callable=scrape_weather_to_gcs,
    )

    query_sfo_weather >> export_sfo_to_gcs