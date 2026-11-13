"""REST API GeoJSON Ingestion, S3 Partition Staging, Snowflake Star Schema & Data Quality Pipeline."""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

USGS_API_URL = (
    "https://earthquake.usgs.gov/fdsnws/event/1/query"
    "?format=geojson&starttime=2026-10-01&endtime=2026-10-31&minmagnitude=2.5"
)
EXPECTED_RAW_SHA256 = "d20cd5fa21aa7bfbc824a5699ecab1e3e47eb1dd611e735832ca6cb351a72698"


def extract_region_name(place: str | None) -> str:
    """Normalize trailing comma-delimited geographic region from USGS place labels."""
    if not place or not place.strip():
        return "Unspecified Offshore"
    parts = [segment.strip() for segment in place.split(",") if segment.strip()]
    return parts[-1] if len(parts) > 1 else parts[0]


def classify_magnitude_tier(mag: float) -> tuple[int, str]:
    """Map continuous Richter/moment magnitude to governed Snowflake dimension tier."""
    if mag < 4.0:
        return (1, "MINOR_2.5_TO_3.9")
    if mag < 5.0:
        return (2, "LIGHT_4.0_TO_4.9")
    if mag < 6.0:
        return (3, "MODERATE_5.0_TO_5.9")
    return (4, "STRONG_6.0_PLUS")


def s3_bronze_partition_key(event_dt: str, prefix: str = "s3://telemetry-lakehouse-bronze/usgs_geojson") -> str:
    """Generate Hive-style S3 bronze partition URI for semi-structured REST API JSON payloads."""
    return f"{prefix.rstrip('/')}/dt={event_dt}/events.jsonl"


def load_geojson_payload(
    local_gz_path: Path,
    fetch_live: bool = False,
    api_url: str = USGS_API_URL,
) -> tuple[dict[str, Any], str]:
    """Load verified GeoJSON bytes from compressed local snapshot or live REST API."""
    if fetch_live:
        req = urllib.request.Request(api_url, headers={"User-Agent": "telemetry-data-pipeline/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw_bytes = resp.read()
    else:
        with gzip.open(local_gz_path, "rb") as fh:
            raw_bytes = fh.read()
    sha256_hex = hashlib.sha256(raw_bytes).hexdigest()
    payload = json.loads(raw_bytes.decode("utf-8"))
    return payload, sha256_hex


def flatten_and_deduplicate_features(
    features: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """Flatten nested GeoJSON objects, enforce schema/range checks, and deduplicate by updated_ms."""
    by_event_id: dict[str, dict[str, Any]] = {}
    quarantine: list[dict[str, Any]] = []
    duplicate_updates = 0

    for idx, feat in enumerate(features):
        event_id = feat.get("id")
        props = feat.get("properties") or {}
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates") or []

        mag = props.get("mag")
        time_ms = props.get("time")
        updated_ms = props.get("updated") or time_ms

        if not event_id or mag is None or time_ms is None or len(coords) < 3:
            quarantine.append({"source_index": idx, "event_id": event_id, "reason": "missing_required_field"})
            continue

        lon, lat, depth_km = float(coords[0]), float(coords[1]), float(coords[2])
        mag_f = float(mag)
        if not (-180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0 and -5.0 <= mag_f <= 10.5):
            quarantine.append({"source_index": idx, "event_id": event_id, "reason": "coordinate_or_mag_out_of_bounds"})
            continue

        dt_utc = datetime.fromtimestamp(time_ms / 1000.0, tz=timezone.utc)
        upd_utc = datetime.fromtimestamp(updated_ms / 1000.0, tz=timezone.utc)
        tier_key, tier_code = classify_magnitude_tier(mag_f)

        record = {
            "event_id": str(event_id),
            "network_code": str(props.get("net") or "unknown").lower(),
            "event_type": str(props.get("type") or "earthquake").lower(),
            "region_name": extract_region_name(props.get("place")),
            "mag_tier_key": tier_key,
            "tier_code": tier_code,
            "event_dt": dt_utc.strftime("%Y-%m-%d"),
            "event_time_utc": dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "updated_time_utc": upd_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "updated_ms": int(updated_ms),
            "magnitude": round(mag_f, 2),
            "sig_score": int(props.get("sig") or 0),
            "longitude": round(lon, 5),
            "latitude": round(lat, 5),
            "depth_km": round(depth_km, 3),
            "alert_level": str(props.get("alert") or "none").lower(),
            "tsunami_flag": int(props.get("tsunami") or 0),
            "review_status": str(props.get("status") or "automatic").lower(),
            "s3_bronze_uri": s3_bronze_partition_key(dt_utc.strftime("%Y-%m-%d")),
        }

        existing = by_event_id.get(record["event_id"])
        if existing is not None:
            duplicate_updates += 1
            if record["updated_ms"] > existing["updated_ms"]:
                by_event_id[record["event_id"]] = record
        else:
            by_event_id[record["event_id"]] = record

    valid_records = sorted(by_event_id.values(), key=lambda r: (r["event_dt"], r["event_id"]))
    return valid_records, quarantine, duplicate_updates


def build_sqlite_star_schema(records: list[dict[str, Any]], db_path: Path) -> dict[str, Any]:
    """Load flattened records into a normalized star schema mirroring Snowflake DDL and run 10 DQ gates."""
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")

    cur.executescript(
        """
        CREATE TABLE dim_seismic_network (
            network_key INTEGER PRIMARY KEY AUTOINCREMENT,
            network_code TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL
        );
        CREATE TABLE dim_magnitude_tier (
            mag_tier_key INTEGER PRIMARY KEY,
            tier_code TEXT NOT NULL UNIQUE,
            min_mag_incl REAL NOT NULL,
            max_mag_excl REAL NOT NULL
        );
        CREATE TABLE dim_region (
            region_key INTEGER PRIMARY KEY AUTOINCREMENT,
            region_name TEXT NOT NULL UNIQUE,
            tsunami_monitored INTEGER NOT NULL
        );
        CREATE TABLE fct_seismic_events (
            event_id TEXT PRIMARY KEY,
            network_key INTEGER NOT NULL REFERENCES dim_seismic_network(network_key),
            mag_tier_key INTEGER NOT NULL REFERENCES dim_magnitude_tier(mag_tier_key),
            region_key INTEGER NOT NULL REFERENCES dim_region(region_key),
            event_dt TEXT NOT NULL,
            event_time_utc TEXT NOT NULL,
            updated_time_utc TEXT NOT NULL,
            magnitude REAL NOT NULL,
            sig_score INTEGER NOT NULL,
            longitude REAL NOT NULL,
            latitude REAL NOT NULL,
            depth_km REAL NOT NULL,
            alert_level TEXT NOT NULL,
            tsunami_flag INTEGER NOT NULL,
            review_status TEXT NOT NULL,
            s3_bronze_uri TEXT NOT NULL
        );
        """
    )

    tiers = [
        (1, "MINOR_2.5_TO_3.9", 2.5, 4.0),
        (2, "LIGHT_4.0_TO_4.9", 4.0, 5.0),
        (3, "MODERATE_5.0_TO_5.9", 5.0, 6.0),
        (4, "STRONG_6.0_PLUS", 6.0, 10.5),
    ]
    cur.executemany("INSERT INTO dim_magnitude_tier VALUES (?, ?, ?, ?);", tiers)

    network_map: dict[str, int] = {}
    for net, etype in sorted({(r["network_code"], r["event_type"]) for r in records}):
        if net not in network_map:
            cur.execute("INSERT INTO dim_seismic_network (network_code, event_type) VALUES (?, ?);", (net, etype))
            network_map[net] = int(cur.lastrowid)

    region_tsunami: dict[str, int] = {}
    for r in records:
        region_tsunami[r["region_name"]] = max(region_tsunami.get(r["region_name"], 0), r["tsunami_flag"])

    region_map: dict[str, int] = {}
    for reg_name in sorted(region_tsunami):
        cur.execute(
            "INSERT INTO dim_region (region_name, tsunami_monitored) VALUES (?, ?);",
            (reg_name, region_tsunami[reg_name]),
        )
        region_map[reg_name] = int(cur.lastrowid)

    fact_rows = [
        (
            r["event_id"],
            network_map[r["network_code"]],
            r["mag_tier_key"],
            region_map[r["region_name"]],
            r["event_dt"],
            r["event_time_utc"],
            r["updated_time_utc"],
            r["magnitude"],
            r["sig_score"],
            r["longitude"],
            r["latitude"],
            r["depth_km"],
            r["alert_level"],
            r["tsunami_flag"],
            r["review_status"],
            r["s3_bronze_uri"],
        )
        for r in records
    ]
    cur.executemany(
        "INSERT INTO fct_seismic_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
        fact_rows,
    )
    conn.commit()

    # Execute 10 SQL Data Quality & Governance Checks
    fact_count = cur.execute("SELECT COUNT(*) FROM fct_seismic_events;").fetchone()[0]
    distinct_pk = cur.execute("SELECT COUNT(DISTINCT event_id) FROM fct_seismic_events;").fetchone()[0]
    null_mags = cur.execute("SELECT COUNT(*) FROM fct_seismic_events WHERE magnitude IS NULL;").fetchone()[0]
    orphan_regions = cur.execute(
        "SELECT COUNT(*) FROM fct_seismic_events f LEFT JOIN dim_region r ON f.region_key = r.region_key WHERE r.region_key IS NULL;"
    ).fetchone()[0]
    orphan_nets = cur.execute(
        "SELECT COUNT(*) FROM fct_seismic_events f LEFT JOIN dim_seismic_network n ON f.network_key = n.network_key WHERE n.network_key IS NULL;"
    ).fetchone()[0]
    orphan_tiers = cur.execute(
        "SELECT COUNT(*) FROM fct_seismic_events f LEFT JOIN dim_magnitude_tier t ON f.mag_tier_key = t.mag_tier_key WHERE t.mag_tier_key IS NULL;"
    ).fetchone()[0]
    bad_coords = cur.execute(
        "SELECT COUNT(*) FROM fct_seismic_events WHERE longitude < -180 OR longitude > 180 OR latitude < -90 OR latitude > 90;"
    ).fetchone()[0]
    s3_partitions = cur.execute("SELECT COUNT(DISTINCT event_dt) FROM fct_seismic_events;").fetchone()[0]
    integrity = cur.execute("PRAGMA integrity_check;").fetchone()[0]

    tier_breakdown = [
        {"tier_code": row[0], "event_count": row[1], "avg_magnitude": round(row[2], 3), "max_magnitude": row[3]}
        for row in cur.execute(
            """
            SELECT t.tier_code, COUNT(*), AVG(f.magnitude), MAX(f.magnitude)
            FROM fct_seismic_events f
            JOIN dim_magnitude_tier t ON f.mag_tier_key = t.mag_tier_key
            GROUP BY t.mag_tier_key, t.tier_code
            ORDER BY t.mag_tier_key;
            """
        ).fetchall()
    ]

    top_regions = [
        {"region_name": row[0], "event_count": row[1], "max_magnitude": row[2]}
        for row in cur.execute(
            """
            SELECT r.region_name, COUNT(*) AS cnt, MAX(f.magnitude)
            FROM fct_seismic_events f
            JOIN dim_region r ON f.region_key = r.region_key
            GROUP BY r.region_name
            ORDER BY cnt DESC, r.region_name ASC
            LIMIT 10;
            """
        ).fetchall()
    ]

    conn.close()

    gates = {
        "gate_01_non_empty_fact_load": fact_count > 0,
        "gate_02_pk_uniqueness": fact_count == distinct_pk,
        "gate_03_zero_null_magnitude": null_mags == 0,
        "gate_04_fk_region_integrity": orphan_regions == 0,
        "gate_05_fk_network_integrity": orphan_nets == 0,
        "gate_06_fk_tier_integrity": orphan_tiers == 0,
        "gate_07_geospatial_bounds": bad_coords == 0,
        "gate_08_daily_s3_partitions_present": s3_partitions >= 25,
        "gate_09_multi_network_dim_populated": len(network_map) >= 5,
        "gate_10_sqlite_integrity_ok": integrity == "ok",
    }

    return {
        "fact_row_count": fact_count,
        "dim_network_count": len(network_map),
        "dim_region_count": len(region_map),
        "dim_magnitude_tier_count": len(tiers),
        "s3_daily_partition_count": s3_partitions,
        "dq_gates": gates,
        "dq_gates_passed": sum(1 for v in gates.values() if v),
        "dq_gates_total": len(gates),
        "all_dq_gates_passed": all(gates.values()),
        "tier_breakdown": tier_breakdown,
        "top_regions": top_regions,
    }


def run_pipeline(module_dir: Path, fetch_live: bool = False) -> dict[str, Any]:
    """Execute end-to-end REST API JSON -> S3 Partition -> Snowflake Star Schema pipeline."""
    data_gz = module_dir / "data" / "usgs_events_oct2026.geojson.gz"
    outputs_dir = module_dir / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    db_path = outputs_dir / "seismic_star_schema.sqlite"

    payload, sha256_hex = load_geojson_payload(data_gz, fetch_live=fetch_live)
    raw_features = payload.get("features", [])
    records, quarantine, dup_updates = flatten_and_deduplicate_features(raw_features)
    star_summary = build_sqlite_star_schema(records, db_path)

    # Remove transient SQLite file before git commit; keep deterministic JSON summary report
    if db_path.exists():
        db_path.unlink()

    report = {
        "pipeline_name": "rest_api_s3_snowflake_star_schema_mart",
        "source_api_url": USGS_API_URL,
        "source_raw_sha256": sha256_hex,
        "expected_raw_sha256_match": sha256_hex == EXPECTED_RAW_SHA256,
        "source_feature_count": len(raw_features),
        "promoted_fact_count": len(records),
        "quarantined_feature_count": len(quarantine),
        "duplicate_updates_resolved": dup_updates,
        "star_schema_and_dq": star_summary,
    }

    report_path = outputs_dir / "api_s3_snowflake_dq_report.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


if __name__ == "__main__":
    base = Path(__file__).resolve().parent.parent
    result = run_pipeline(base, fetch_live=False)
    print(
        f"Promoted {result['promoted_fact_count']} events across "
        f"{result['star_schema_and_dq']['s3_daily_partition_count']} S3 daily partitions; "
        f"DQ gates {result['star_schema_and_dq']['dq_gates_passed']}/"
        f"{result['star_schema_and_dq']['dq_gates_total']} passed."
    )
