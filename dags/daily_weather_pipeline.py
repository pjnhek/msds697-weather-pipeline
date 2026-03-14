"""Unified daily weather pipeline DAG.

Task chain:
  ingest_yesterday_to_mongo
    >> load_yesterday_doc_to_bq_staging
    >> merge_staging_to_raw
    >> clear_staging
    >> refresh_train_table
    >> train_model
    >> ensure_predictions_table
    >> upsert_prediction
"""

from __future__ import annotations

from datetime import date, timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery

from nws_ingest import Config, run_ingest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOCAL_TZ = pendulum.timezone("America/Los_Angeles")
PROJECT_ID = "msds697-group-project"
DATASET_ID = "dds_weather_ml"
RAW_TABLE = f"{PROJECT_ID}.{DATASET_ID}.raw_weather_v5"
STAGING_TABLE = f"{PROJECT_ID}.{DATASET_ID}.raw_weather_v5_staging"
TRAIN_TABLE = f"{PROJECT_ID}.{DATASET_ID}.weather_train"
MODEL_NAME = f"{PROJECT_ID}.{DATASET_ID}.tomorrow_max_model_lr"
PREDICTIONS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.weather_predictions"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _target_date_from_context(context: dict) -> date:
    """Derive the target local date from the Airflow execution context.

    For a run triggered at 00:01 local time, this resolves to local yesterday.
    """
    run_end_local = context["data_interval_end"].in_timezone(LOCAL_TZ)
    return (run_end_local.date() - timedelta(days=1))


def _build_config_from_variables() -> Config:
    """Build a ``Config`` from Airflow Variables (local) / Secret Manager (prod)."""
    return Config(
        mongo_uri=Variable.get("MONGO_URI"),
        db_name=Variable.get("MONGO_DB_NAME", default_var="dds-group-project"),
        collection_name=Variable.get("MONGO_COLLECTION_NAME", default_var="big_query_SFO_weather_v5"),
        user_agent=Variable.get("NOAA_USER_AGENT"),
        station_id=Variable.get("NWS_STATION_ID", default_var="KSFO"),
        stn=int(Variable.get("WEATHER_STN", default_var="724940")),
        wban=int(Variable.get("WEATHER_WBAN", default_var="23234")),
        tz_name="America/Los_Angeles",
        min_hourly_successes=18,
        request_timeout_seconds=30,
        per_call_sleep_seconds=0.05,
    )


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def ingest_yesterday_to_mongo(**context) -> dict:
    """Fetch NWS data, upsert to MongoDB, return the doc for XCom."""
    target = _target_date_from_context(context)
    config = _build_config_from_variables()
    return run_ingest(config=config, target_date=target)


def load_yesterday_doc_to_bq_staging(**context) -> None:
    """Load the ingested doc into BQ staging.  Prefers XCom; falls back to MongoDB."""
    ti = context["ti"]
    doc = ti.xcom_pull(task_ids="ingest_yesterday_to_mongo")

    if not doc:
        # Fallback: re-query MongoDB directly.
        from pymongo import MongoClient

        target = _target_date_from_context(context)
        config = _build_config_from_variables()
        client = MongoClient(config.mongo_uri)
        collection = client[config.db_name][config.collection_name]
        doc = collection.find_one(
            {
                "stn": config.stn,
                "wban": config.wban,
                "year": target.year,
                "mo": target.month,
                "da": target.day,
            },
            {"_id": 0},
        )
        client.close()
        if not doc:
            raise ValueError(
                f"No Mongo document found for {target.isoformat()} "
                f"(stn={config.stn}, wban={config.wban})."
            )

    # BigQuery DATE expects YYYY-MM-DD.
    doc["date"] = f"{doc['year']}-{doc['mo']:02d}-{doc['da']:02d}"

    bq_client = bigquery.Client(project=PROJECT_ID)
    load_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = bq_client.load_table_from_json([doc], STAGING_TABLE, job_config=load_config)
    job.result()


# ---------------------------------------------------------------------------
# SQL statements
# ---------------------------------------------------------------------------

MERGE_STAGING_SQL = f"""
MERGE `{RAW_TABLE}` T
USING (SELECT * FROM `{STAGING_TABLE}`) S
ON T.stn = S.stn
AND T.wban = S.wban
AND T.year = S.year
AND T.mo = S.mo
AND T.da = S.da
WHEN MATCHED THEN UPDATE SET
  date = S.date,
  temp = S.temp,
  max = S.max,
  min = S.min,
  prcp = S.prcp,
  fog = S.fog,
  rain_drizzle = S.rain_drizzle,
  snow_ice_pellets = S.snow_ice_pellets,
  hail = S.hail,
  thunder = S.thunder,
  tornado_funnel_cloud = S.tornado_funnel_cloud,
  day_of_the_year = S.day_of_the_year,
  month_of_the_year = S.month_of_the_year,
  weekly_avg_max_temp = S.weekly_avg_max_temp,
  weekly_avg_min_temp = S.weekly_avg_min_temp,
  weekly_avg_temp = S.weekly_avg_temp,
  weekly_max_max_temp = S.weekly_max_max_temp,
  weekly_min_min_temp = S.weekly_min_min_temp,
  weekly_sum_precip = S.weekly_sum_precip,
  monthly_avg_max_temp = S.monthly_avg_max_temp,
  monthly_avg_min_temp = S.monthly_avg_min_temp,
  monthly_avg_temp = S.monthly_avg_temp,
  monthly_max_max_temp = S.monthly_max_max_temp,
  monthly_min_min_temp = S.monthly_min_min_temp,
  monthly_sum_precip = S.monthly_sum_precip
WHEN NOT MATCHED THEN INSERT ROW
"""


REFRESH_TRAIN_TABLE_SQL = f"""
CREATE OR REPLACE TABLE `{TRAIN_TABLE}` AS
WITH base AS (
  SELECT
    DATE(year, mo, da) AS d,
    max,
    min,
    temp,
    prcp,
    fog,
    rain_drizzle,
    snow_ice_pellets,
    hail,
    thunder,
    tornado_funnel_cloud,
    day_of_the_year,
    month_of_the_year,
    weekly_avg_max_temp,
    weekly_avg_min_temp,
    weekly_avg_temp,
    weekly_max_max_temp,
    weekly_min_min_temp,
    weekly_sum_precip,
    monthly_avg_max_temp,
    monthly_avg_min_temp,
    monthly_avg_temp,
    monthly_max_max_temp,
    monthly_min_min_temp,
    monthly_sum_precip
  FROM `{RAW_TABLE}`
),
labeled AS (
  SELECT
    *,
    LEAD(max) OVER (ORDER BY d) AS target_tomorrow_max
  FROM base
)
SELECT *
FROM labeled
WHERE target_tomorrow_max IS NOT NULL
"""


TRAIN_MODEL_SQL = f"""
CREATE OR REPLACE MODEL `{MODEL_NAME}`
OPTIONS(
  model_type = 'LINEAR_REG',
  input_label_cols = ['target_tomorrow_max']
) AS
SELECT
  target_tomorrow_max,
  min,
  temp,
  prcp,
  fog,
  rain_drizzle,
  snow_ice_pellets,
  hail,
  thunder,
  tornado_funnel_cloud,
  day_of_the_year,
  month_of_the_year,
  weekly_avg_max_temp,
  weekly_avg_min_temp,
  weekly_avg_temp,
  weekly_max_max_temp,
  weekly_min_min_temp,
  weekly_sum_precip,
  monthly_avg_max_temp,
  monthly_avg_min_temp,
  monthly_avg_temp,
  monthly_max_max_temp,
  monthly_min_min_temp,
  monthly_sum_precip
FROM `{TRAIN_TABLE}`
"""


CREATE_PREDICTIONS_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{PREDICTIONS_TABLE}` (
  prediction_date DATE NOT NULL,
  predicted_max_temp_f FLOAT64,
  model_name STRING,
  source_row_date DATE,
  created_at TIMESTAMP
)
"""


UPSERT_PREDICTION_SQL = f"""
MERGE `{PREDICTIONS_TABLE}` T
USING (
  WITH latest AS (
    SELECT
      DATE(year, mo, da) AS d,
      min, temp, prcp, fog, rain_drizzle, snow_ice_pellets, hail, thunder, tornado_funnel_cloud,
      day_of_the_year, month_of_the_year,
      weekly_avg_max_temp, weekly_avg_min_temp, weekly_avg_temp,
      weekly_max_max_temp, weekly_min_min_temp, weekly_sum_precip,
      monthly_avg_max_temp, monthly_avg_min_temp, monthly_avg_temp,
      monthly_max_max_temp, monthly_min_min_temp, monthly_sum_precip
    FROM `{RAW_TABLE}`
    ORDER BY d DESC
    LIMIT 1
  ),
  scored AS (
    SELECT
      DATE_ADD(d, INTERVAL 1 DAY) AS prediction_date,
      d AS source_row_date,
      predicted_target_tomorrow_max AS predicted_max_temp_f,
      'tomorrow_max_model_lr' AS model_name,
      CURRENT_TIMESTAMP() AS created_at
    FROM ML.PREDICT(MODEL `{MODEL_NAME}`, TABLE latest)
  )
  SELECT * FROM scored
) S
ON T.prediction_date = S.prediction_date
WHEN MATCHED THEN UPDATE SET
  predicted_max_temp_f = S.predicted_max_temp_f,
  model_name = S.model_name,
  source_row_date = S.source_row_date,
  created_at = S.created_at
WHEN NOT MATCHED THEN
  INSERT (prediction_date, predicted_max_temp_f, model_name, source_row_date, created_at)
  VALUES (S.prediction_date, S.predicted_max_temp_f, S.model_name, S.source_row_date, S.created_at)
"""


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="daily_weather_pipeline",
    start_date=pendulum.datetime(2026, 3, 8, tz=LOCAL_TZ),
    schedule="1 0 * * *",  # 12:01 AM local, every day
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["weather", "mongodb", "bigquery", "ml"],
) as dag:
    ingest_mongo = PythonOperator(
        task_id="ingest_yesterday_to_mongo",
        python_callable=ingest_yesterday_to_mongo,
    )

    load_staging = PythonOperator(
        task_id="load_yesterday_doc_to_bq_staging",
        python_callable=load_yesterday_doc_to_bq_staging,
    )

    merge_staging_to_raw = BigQueryInsertJobOperator(
        task_id="merge_staging_to_raw",
        location="US",
        project_id=PROJECT_ID,
        configuration={"query": {"query": MERGE_STAGING_SQL, "useLegacySql": False}},
    )

    clear_staging = BigQueryInsertJobOperator(
        task_id="clear_staging",
        location="US",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": f"TRUNCATE TABLE `{STAGING_TABLE}`",
                "useLegacySql": False,
            }
        },
    )

    refresh_train_table = BigQueryInsertJobOperator(
        task_id="refresh_train_table",
        location="US",
        project_id=PROJECT_ID,
        configuration={"query": {"query": REFRESH_TRAIN_TABLE_SQL, "useLegacySql": False}},
    )

    train_model = BigQueryInsertJobOperator(
        task_id="train_model",
        location="US",
        project_id=PROJECT_ID,
        configuration={"query": {"query": TRAIN_MODEL_SQL, "useLegacySql": False}},
    )

    ensure_predictions_table = BigQueryInsertJobOperator(
        task_id="ensure_predictions_table",
        location="US",
        project_id=PROJECT_ID,
        configuration={"query": {"query": CREATE_PREDICTIONS_TABLE_SQL, "useLegacySql": False}},
    )

    upsert_prediction = BigQueryInsertJobOperator(
        task_id="upsert_prediction",
        location="US",
        project_id=PROJECT_ID,
        configuration={"query": {"query": UPSERT_PREDICTION_SQL, "useLegacySql": False}},
    )

    (
        ingest_mongo
        >> load_staging
        >> merge_staging_to_raw
        >> clear_staging
        >> refresh_train_table
        >> train_model
        >> ensure_predictions_table
        >> upsert_prediction
    )
