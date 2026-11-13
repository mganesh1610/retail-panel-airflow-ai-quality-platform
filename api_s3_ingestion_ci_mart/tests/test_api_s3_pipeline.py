"""Unit and integration tests for REST API JSON -> S3 Partitioning -> Snowflake Star Schema pipeline."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from api_s3_ingestion_ci_mart.dags.api_s3_snowflake_dag import (
    build_api_s3_snowflake_dag,
    enforce_snowflake_dq_gates,
)
from api_s3_ingestion_ci_mart.src.api_s3_snowflake_etl import (
    classify_magnitude_tier,
    extract_region_name,
    flatten_and_deduplicate_features,
    run_pipeline,
    s3_bronze_partition_key,
)


class TestApiS3SnowflakePipeline(unittest.TestCase):
    def setUp(self) -> None:
        self.module_dir = Path(__file__).resolve().parent.parent

    def test_region_and_s3_partition_helpers(self) -> None:
        self.assertEqual(extract_region_name("12 km NW of The Geysers, CA"), "CA")
        self.assertEqual(extract_region_name("South Sandwich Islands region"), "South Sandwich Islands region")
        self.assertEqual(extract_region_name(None), "Unspecified Offshore")
        self.assertEqual(
            s3_bronze_partition_key("2026-10-15"),
            "s3://telemetry-lakehouse-bronze/usgs_geojson/dt=2026-10-15/events.jsonl",
        )
        self.assertEqual(classify_magnitude_tier(2.7), (1, "MINOR_2.5_TO_3.9"))
        self.assertEqual(classify_magnitude_tier(6.4), (4, "STRONG_6.0_PLUS"))

    def test_deduplication_keeps_latest_update_and_quarantines_invalid(self) -> None:
        sample = [
            {
                "id": "ev100",
                "properties": {"mag": 3.1, "place": "10 km W of Reno, Nevada", "time": 1791000000000, "updated": 100},
                "geometry": {"coordinates": [-119.8, 39.5, 5.2]},
            },
            {
                "id": "ev100",
                "properties": {"mag": 3.4, "place": "10 km W of Reno, Nevada", "time": 1791000000000, "updated": 250},
                "geometry": {"coordinates": [-119.8, 39.5, 5.2]},
            },
            {
                "id": "bad_coords",
                "properties": {"mag": 4.0, "place": "Nowhere", "time": 1791000000000, "updated": 200},
                "geometry": {"coordinates": [999.0, 20.0, 10.0]},
            },
        ]
        valid, quarantine, dup_updates = flatten_and_deduplicate_features(sample)
        self.assertEqual(len(valid), 1)
        self.assertEqual(valid[0]["magnitude"], 3.4)
        self.assertEqual(dup_updates, 1)
        self.assertEqual(len(quarantine), 1)
        self.assertEqual(quarantine[0]["reason"], "coordinate_or_mag_out_of_bounds")

    def test_dag_structure_and_dq_guardrail(self) -> None:
        dag = build_api_s3_snowflake_dag()
        self.assertEqual(dag.dag_id, "rest_api_s3_snowflake_star_schema_pipeline")
        self.assertIn("enforce_snowflake_star_schema_dq_gates", dag.tasks)
        with self.assertRaises(RuntimeError):
            enforce_snowflake_dq_gates({"star_schema_and_dq": {"all_dq_gates_passed": False}})

    def test_full_usgs_geojson_pipeline_and_10_dq_gates(self) -> None:
        report = run_pipeline(self.module_dir, fetch_live=False)
        self.assertTrue(report["expected_raw_sha256_match"])
        self.assertGreaterEqual(report["promoted_fact_count"], 2400)
        dq = report["star_schema_and_dq"]
        self.assertEqual(dq["dq_gates_passed"], 10)
        self.assertEqual(dq["dq_gates_total"], 10)
        self.assertTrue(dq["all_dq_gates_passed"])
        report_file = self.module_dir / "outputs" / "api_s3_snowflake_dq_report.json"
        self.assertTrue(report_file.exists())
        parsed = json.loads(report_file.read_text(encoding="utf-8"))
        self.assertEqual(parsed["promoted_fact_count"], report["promoted_fact_count"])


if __name__ == "__main__":
    unittest.main()
