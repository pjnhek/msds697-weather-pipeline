# MSDS 697 — Daily Weather Pipeline

Unified Airflow DAG that ingests daily NWS weather observations into MongoDB, loads them into BigQuery, retrains a linear regression model, and generates a next-day max temperature prediction.

## Pipeline Overview

```
ingest_yesterday_to_mongo       # Fetch NWS hourly data → upsert daily doc to MongoDB
  >> load_yesterday_doc_to_bq_staging  # Load doc into BQ staging table
  >> merge_staging_to_raw              # MERGE staging into raw_weather_v5
  >> clear_staging                     # TRUNCATE staging table
  >> refresh_train_table               # Rebuild weather_train with target label
  >> train_model                       # Retrain LINEAR_REG model (BigQuery ML)
  >> ensure_predictions_table          # CREATE TABLE IF NOT EXISTS
  >> upsert_prediction                 # Score latest row → upsert prediction
```

## Project Structure

```
dags/
  nws_ingest.py                # NWS ingest module (importable + standalone CLI)
  daily_weather_pipeline.py    # Unified 8-task Airflow DAG
weather_to_gcs.py              # One-time historical data load (already ran, not in Composer)
requirements.txt               # Python dependencies for Composer
.github/workflows/
  deploy-dags.yml              # Auto-deploy DAGs to Composer on push to main
```

## Local Testing Guide

### 1. Prerequisites

- macOS with [Homebrew](https://brew.sh/) installed
- Python 3.10+
- A GCP account with access to the `msds697-group-project` project
- Your own MongoDB credentials for the shared cluster

### 2. Install Google Cloud SDK

```bash
brew install --cask google-cloud-sdk
```

If you get a Python path error during install, create the missing symlink first:

```bash
ln -s python /opt/homebrew/opt/python@3.13/libexec/bin/python3
```

### 3. Install Python Dependencies

```bash
pip install apache-airflow \
  apache-airflow-providers-google \
  pymongo \
  requests \
  google-cloud-bigquery \
  pendulum
```

### 4. Authenticate with GCP

```bash
# Login for ADC (used by Airflow tasks)
gcloud auth application-default login

# Login for gcloud/bq CLI tools
gcloud auth login

# Set default project
gcloud config set project msds697-group-project
gcloud auth application-default set-quota-project msds697-group-project

# Set project env var (required for BigQuery operators)
export GOOGLE_CLOUD_PROJECT=msds697-group-project
```

Add this to your `~/.zshrc` (or `~/.bashrc`) so it persists across terminals:

```bash
echo 'export GOOGLE_CLOUD_PROJECT=msds697-group-project' >> ~/.zshrc
```

### 5. Start Airflow

```bash
airflow standalone
```

This starts the webserver, scheduler, and creates an admin user. Note the password printed in the terminal — you'll need it to log in at http://localhost:8080.

### 6. Set Airflow Variables

Open a **new terminal** (leave airflow running) and set the required variables:

```bash
# Required
airflow variables set MONGO_URI "mongodb+srv://<your_username>:<your_password>@msds697-group-project.lcvihtb.mongodb.net/?retryWrites=true&w=majority"
airflow variables set NOAA_USER_AGENT "(weather-data-script, contact@example.com)"

# Optional (these have defaults in the code, but you can override)
airflow variables set MONGO_DB_NAME "dds-group-project"
airflow variables set MONGO_COLLECTION_NAME "big_query_SFO_weather_v5"
airflow variables set NWS_STATION_ID "KSFO"
airflow variables set WEATHER_STN "724940"
airflow variables set WEATHER_WBAN "23234"
```

### 7. Add GCP Connection

```bash
airflow connections add google_cloud_default --conn-type google_cloud_platform --conn-extra '{"project_id": "msds697-group-project"}'
```

### 8. Copy DAG Files

```bash
cp dags/*.py ~/airflow/dags/
```

### 9. Trigger the DAG

Either use the Airflow UI at http://localhost:8080 or run:

```bash
airflow dags trigger daily_weather_pipeline
```

### 10. Verify Results

Check each task in the Airflow UI. You can also query BigQuery directly:

```bash
# Check latest raw data
bq query --use_legacy_sql=false \
  "SELECT * FROM \`msds697-group-project.dds_weather_ml.raw_weather_v5\` ORDER BY year DESC, mo DESC, da DESC LIMIT 5"

# Check predictions
bq query --use_legacy_sql=false \
  "SELECT * FROM \`msds697-group-project.dds_weather_ml.weather_predictions\` ORDER BY prediction_date DESC LIMIT 5"
```

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `ModuleNotFoundError: airflow.providers.google` | `pip install apache-airflow-providers-google` |
| `DefaultCredentialsError` | Run `gcloud auth application-default login` |
| `conn_id google_cloud_default isn't defined` | Run the connection add command from step 6 |
| `Project ID could not be determined` | Set `export GOOGLE_CLOUD_PROJECT=msds697-group-project` and restart airflow |
| `MERGE must match at most one source row` | Staging table has duplicate rows — run `bq query --use_legacy_sql=false "TRUNCATE TABLE \`msds697-group-project.dds_weather_ml.raw_weather_v5_staging\`"` then re-run from `load_yesterday_doc_to_bq_staging` |
| `ReadTimeout` on NWS API | NWS servers are slow — clear the task and retry |
| DAG not showing in UI | Check **Admin → Import Errors** in the UI for Python errors |

## BigQuery Resources

| Resource | Full Name |
|----------|-----------|
| Project | `msds697-group-project` |
| Dataset | `dds_weather_ml` |
| Raw table | `raw_weather_v5` |
| Staging table | `raw_weather_v5_staging` |
| Training table | `weather_train` |
| ML model | `tomorrow_max_model_lr` |
| Predictions table | `weather_predictions` |
