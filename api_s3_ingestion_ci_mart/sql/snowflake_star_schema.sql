-- Snowflake Star Schema DDL, Semi-Structured VARIANT JSON Ingestion & Quality Audit CTEs
-- Source: USGS Earthquake Hazards GeoJSON REST API (s3://<bucket>/bronze/usgs_geojson/dt=YYYY-MM-DD/)

CREATE SCHEMA IF NOT EXISTS TELEMETRY_ANALYTICS;

CREATE OR REPLACE STAGE TELEMETRY_ANALYTICS.STG_S3_REST_BRONZE
  URL = 's3://telemetry-lakehouse-bronze/usgs_geojson/'
  FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE);

CREATE OR REPLACE TABLE TELEMETRY_ANALYTICS.RAW_REST_API_EVENTS (
  ingest_batch_id   STRING        NOT NULL,
  partition_dt      DATE          NOT NULL,
  s3_object_key     STRING        NOT NULL,
  source_sha256     STRING        NOT NULL,
  raw_payload       VARIANT       NOT NULL,
  ingested_at_utc   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE TELEMETRY_ANALYTICS.DIM_SEISMIC_NETWORK (
  network_key       INTEGER       AUTOINCREMENT PRIMARY KEY,
  network_code      STRING        NOT NULL UNIQUE,
  event_type        STRING        NOT NULL
);

CREATE OR REPLACE TABLE TELEMETRY_ANALYTICS.DIM_MAGNITUDE_TIER (
  mag_tier_key      INTEGER       PRIMARY KEY,
  tier_code         STRING        NOT NULL UNIQUE,
  min_mag_incl      NUMBER(4,2)   NOT NULL,
  max_mag_excl      NUMBER(4,2)   NOT NULL
);

CREATE OR REPLACE TABLE TELEMETRY_ANALYTICS.DIM_REGION (
  region_key        INTEGER       AUTOINCREMENT PRIMARY KEY,
  region_name       STRING        NOT NULL UNIQUE,
  tsunami_monitored BOOLEAN       NOT NULL
);

CREATE OR REPLACE TABLE TELEMETRY_ANALYTICS.FCT_SEISMIC_EVENTS (
  event_id          STRING        PRIMARY KEY,
  network_key       INTEGER       NOT NULL REFERENCES TELEMETRY_ANALYTICS.DIM_SEISMIC_NETWORK(network_key),
  mag_tier_key      INTEGER       NOT NULL REFERENCES TELEMETRY_ANALYTICS.DIM_MAGNITUDE_TIER(mag_tier_key),
  region_key        INTEGER       NOT NULL REFERENCES TELEMETRY_ANALYTICS.DIM_REGION(region_key),
  event_dt          DATE          NOT NULL,
  event_time_utc    TIMESTAMP_NTZ NOT NULL,
  updated_time_utc  TIMESTAMP_NTZ NOT NULL,
  magnitude         NUMBER(5,2)   NOT NULL,
  sig_score         INTEGER       NOT NULL,
  longitude         NUMBER(9,5)   NOT NULL,
  latitude          NUMBER(9,5)   NOT NULL,
  depth_km          NUMBER(8,3)   NOT NULL,
  alert_level       STRING,
  tsunami_flag      INTEGER       NOT NULL,
  review_status     STRING        NOT NULL
);

-- Incremental deduplicating MERGE from semi-structured VARIANT staging into Gold Fact Table
MERGE INTO TELEMETRY_ANALYTICS.FCT_SEISMIC_EVENTS AS tgt
USING (
  WITH parsed AS (
    SELECT
      f.value:id::STRING AS event_id,
      COALESCE(LOWER(f.value:properties:net::STRING), 'unknown') AS network_code,
      COALESCE(LOWER(f.value:properties:type::STRING), 'earthquake') AS event_type,
      TRIM(REGEXP_SUBSTR(f.value:properties:place::STRING, '[^,]+$')) AS region_name,
      TO_TIMESTAMP_NTZ(f.value:properties:time::NUMBER / 1000) AS event_time_utc,
      TO_TIMESTAMP_NTZ(f.value:properties:updated::NUMBER / 1000) AS updated_time_utc,
      f.value:properties:mag::NUMBER(5,2) AS magnitude,
      COALESCE(f.value:properties:sig::INTEGER, 0) AS sig_score,
      f.value:geometry:coordinates[0]::NUMBER(9,5) AS longitude,
      f.value:geometry:coordinates[1]::NUMBER(9,5) AS latitude,
      f.value:geometry:coordinates[2]::NUMBER(8,3) AS depth_km,
      COALESCE(f.value:properties:alert::STRING, 'none') AS alert_level,
      COALESCE(f.value:properties:tsunami::INTEGER, 0) AS tsunami_flag,
      COALESCE(f.value:properties:status::STRING, 'automatic') AS review_status,
      ROW_NUMBER() OVER (
        PARTITION BY f.value:id::STRING
        ORDER BY f.value:properties:updated::NUMBER DESC
      ) AS rn
    FROM TELEMETRY_ANALYTICS.RAW_REST_API_EVENTS r,
         LATERAL FLATTEN(input => r.raw_payload:features) f
    WHERE f.value:id IS NOT NULL
      AND f.value:properties:mag IS NOT NULL
  )
  SELECT * FROM parsed WHERE rn = 1
) AS src
ON tgt.event_id = src.event_id
WHEN MATCHED AND src.updated_time_utc > tgt.updated_time_utc THEN
  UPDATE SET
    tgt.magnitude = src.magnitude,
    tgt.sig_score = src.sig_score,
    tgt.depth_km = src.depth_km,
    tgt.review_status = src.review_status,
    tgt.updated_time_utc = src.updated_time_utc
WHEN NOT MATCHED THEN
  INSERT (event_id, network_key, mag_tier_key, region_key, event_dt, event_time_utc,
          updated_time_utc, magnitude, sig_score, longitude, latitude, depth_km,
          alert_level, tsunami_flag, review_status)
  VALUES (src.event_id, 1, 1, 1, DATE(src.event_time_utc), src.event_time_utc,
          src.updated_time_utc, src.magnitude, src.sig_score, src.longitude, src.latitude,
          src.depth_km, src.alert_level, src.tsunami_flag, src.review_status);

-- Reporting & Governance Gold View via SQL Joins & CTEs
CREATE OR REPLACE VIEW TELEMETRY_ANALYTICS.VW_DAILY_REGION_SUMMARY AS
WITH base_events AS (
  SELECT
    f.event_dt,
    r.region_name,
    t.tier_code,
    n.network_code,
    f.magnitude,
    f.depth_km,
    f.sig_score,
    f.tsunami_flag
  FROM TELEMETRY_ANALYTICS.FCT_SEISMIC_EVENTS f
  JOIN TELEMETRY_ANALYTICS.DIM_REGION r ON f.region_key = r.region_key
  JOIN TELEMETRY_ANALYTICS.DIM_MAGNITUDE_TIER t ON f.mag_tier_key = t.mag_tier_key
  JOIN TELEMETRY_ANALYTICS.DIM_SEISMIC_NETWORK n ON f.network_key = n.network_key
)
SELECT
  event_dt,
  region_name,
  tier_code,
  COUNT(*) AS event_count,
  ROUND(AVG(magnitude), 3) AS avg_magnitude,
  MAX(magnitude) AS max_magnitude,
  ROUND(AVG(depth_km), 2) AS avg_depth_km,
  SUM(tsunami_flag) AS tsunami_alerts
FROM base_events
GROUP BY 1, 2, 3;
