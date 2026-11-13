# REST API JSON Ingestion, S3 Partitioning & Snowflake Star Schema CI Module (`api_s3_ingestion_ci_mart`)

Extension module adding semi-structured **REST API GeoJSON ingestion**, **AWS S3 Hive-partitioned Bronze key mapping (`dt=YYYY-MM-DD`)**, **Snowflake `VARIANT` + `LATERAL FLATTEN` star-schema modeling** (`DIM_SEISMIC_NETWORK`, `DIM_MAGNITUDE_TIER`, `DIM_REGION`, `FCT_SEISMIC_EVENTS`), **Apache Airflow DAG orchestration**, and **10 automated data-quality & CI gates** (`.github/workflows/ci.yml`).

## Primary & Backup Public Datasets

- **Primary REST API Source:** [USGS Earthquake Hazards GeoJSON REST API](https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2026-10-01&endtime=2026-10-31&minmagnitude=2.5) (`2,411` October 2026 GeoJSON events `M >= 2.5`; raw uncompressed SHA-256 `d20cd5fa21aa7bfbc824a5699ecab1e3e47eb1dd611e735832ca6cb351a72698`).
- **Backup Public REST API Source:** [NOAA National Weather Service Active Alerts GeoJSON API](https://api.weather.gov/alerts/active).

## Run & Test

```bash
python3 -m api_s3_ingestion_ci_mart.src.api_s3_snowflake_etl
python3 -m unittest discover -s api_s3_ingestion_ci_mart/tests -p "test_*.py" -v
```
