"""
Metrics Exporter — DynamoDB (f1_live_state) → Prometheus /metrics

Reads the current live state from DynamoDB every SCRAPE_INTERVAL_SECONDS
and updates Prometheus Gauge metrics. Prometheus scrapes /metrics on demand.

Exposed metrics (all labeled with session_key, driver_number, acronym, team):
  f1_driver_position              — current race position (1 = leader)
  f1_driver_last_lap_seconds      — last lap duration in seconds
  f1_driver_sector1_seconds       — sector 1 time
  f1_driver_sector2_seconds       — sector 2 time
  f1_driver_sector3_seconds       — sector 3 time
  f1_driver_pit_stops_total       — cumulative pit stops
  f1_driver_laps_completed        — cumulative laps completed
  f1_driver_gap_to_leader_seconds — gap to race leader (by cumulative time)
  f1_simulation_active            — 1 if simulation is processing, 0 otherwise

Track layout metrics (loaded once from S3 when a session becomes active):
  f1_track_x{session_key, idx}   — X coordinate of track outline point N
  f1_track_y{session_key, idx}   — Y coordinate of track outline point N
"""

import json
import os
import sys
import time
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from prometheus_client import Gauge, start_http_server


# ── Config ─────────────────────────────────────────────────────────────────

ENDPOINT = os.getenv("AWS_ENDPOINT_URL")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
LIVE_TABLE = os.getenv("LIVE_STATE_TABLE", "f1_live_state")
SIM_TABLE = os.getenv("SIMULATOR_STATE_TABLE", "f1_simulator_state")
RAW_BUCKET = os.getenv("RAW_BUCKET", "f1-raw-data")
SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL_SECONDS", "5"))
PORT = int(os.getenv("METRICS_PORT", "8000"))

_DRIVER_LABELS = ["session_key", "driver_number", "acronym", "team"]

# ── Prometheus metric definitions ───────────────────────────────────────────

DRIVER_POSITION = Gauge(
    "f1_driver_position", "Current race position (1 = leader)", _DRIVER_LABELS
)
DRIVER_LAP_TIME = Gauge(
    "f1_driver_last_lap_seconds", "Last lap duration in seconds", _DRIVER_LABELS
)
DRIVER_SECTOR1 = Gauge(
    "f1_driver_sector1_seconds", "Sector 1 duration in seconds", _DRIVER_LABELS
)
DRIVER_SECTOR2 = Gauge(
    "f1_driver_sector2_seconds", "Sector 2 duration in seconds", _DRIVER_LABELS
)
DRIVER_SECTOR3 = Gauge(
    "f1_driver_sector3_seconds", "Sector 3 duration in seconds", _DRIVER_LABELS
)
DRIVER_PIT_STOPS = Gauge(
    "f1_driver_pit_stops_total", "Cumulative pit stops", _DRIVER_LABELS
)
DRIVER_LAPS = Gauge(
    "f1_driver_laps_completed", "Cumulative laps completed", _DRIVER_LABELS
)
DRIVER_GAP = Gauge(
    "f1_driver_gap_to_leader_seconds", "Gap to race leader in seconds", _DRIVER_LABELS
)
SIMULATION_ACTIVE = Gauge(
    "f1_simulation_active",
    "1 if simulation is currently processing, 0 otherwise",
    ["session_key"],
)
TRACK_X = Gauge("f1_track_x", "Track layout X coordinate", ["session_key", "idx"])
TRACK_Y = Gauge("f1_track_y", "Track layout Y coordinate", ["session_key", "idx"])

# Track layout state: loaded once from S3 per session, re-emitted every cycle.
_track_session: int | None = None
_track_points: list = []


# ── Helpers ─────────────────────────────────────────────────────────────────

def _boto_kwargs() -> dict:
    kwargs = {"region_name": REGION}
    if ENDPOINT:
        kwargs["endpoint_url"] = ENDPOINT
    return kwargs


def _f(value) -> float | None:
    """Convert Decimal or numeric value to float; return None if absent."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ── Track layout ────────────────────────────────────────────────────────────

def _load_track_layout(s3_client, session_key: int) -> None:
    global _track_session, _track_points
    if _track_session == session_key:
        return
    try:
        resp = s3_client.get_object(
            Bucket=RAW_BUCKET,
            Key=f"sessions/{session_key}/track_layout.json",
        )
        data = json.loads(resp["Body"].read().decode("utf-8"))
        _track_points = data.get("points", [])
        _track_session = session_key
        print(f"[exporter] Track layout loaded: {len(_track_points)} points", flush=True)
    except Exception as exc:
        print(f"[exporter] Track layout unavailable: {exc}", file=sys.stderr, flush=True)
        _track_points = []


def _emit_track_metrics(session_key: int) -> None:
    sk = str(session_key)
    for i, pt in enumerate(_track_points):
        TRACK_X.labels(session_key=sk, idx=str(i)).set(float(pt.get("x", 0)))
        TRACK_Y.labels(session_key=sk, idx=str(i)).set(float(pt.get("y", 0)))


# ── Core update ─────────────────────────────────────────────────────────────

def update_metrics(live_table, sim_table, s3_client) -> None:
    # 1 — Find all sessions and their status
    try:
        sim_response = sim_table.scan()
    except ClientError as exc:
        print(f"[exporter] DynamoDB scan error (sim_state): {exc}", file=sys.stderr, flush=True)
        return

    sim_items = sim_response.get("Items", [])
    active_sessions: set[int] = set()

    for item in sim_items:
        sk_int = int(item["session_key"])
        sk_str = str(sk_int)
        is_active = item.get("status") == "processing"
        SIMULATION_ACTIVE.labels(session_key=sk_str).set(1 if is_active else 0)
        if is_active:
            active_sessions.add(sk_int)

    # 2 — Update driver metrics and track layout for each active session
    for session_key in active_sessions:
        _load_track_layout(s3_client, session_key)
        _emit_track_metrics(session_key)
        sk_str = str(session_key)
        try:
            response = live_table.query(
                KeyConditionExpression=Key("session_key").eq(session_key)
            )
        except ClientError as exc:
            print(f"[exporter] DynamoDB query error (live_state): {exc}", file=sys.stderr, flush=True)
            continue

        drivers = response.get("Items", [])
        if not drivers:
            continue

        # Determine leader's cumulative time for gap calculation
        leader_cumulative: float | None = None
        for d in drivers:
            if d.get("position") in (1, Decimal("1")):
                leader_cumulative = _f(d.get("cumulative_time", 0))
                break
        if leader_cumulative is None:
            # Fallback: use the minimum cumulative time across all drivers
            times = [_f(d.get("cumulative_time")) for d in drivers]
            valid = [t for t in times if t is not None]
            leader_cumulative = min(valid) if valid else 0.0

        for d in drivers:
            dn = str(int(d.get("driver_number", 0)))
            acronym = d.get("acronym", "")
            team = d.get("team_name", "")
            lbl = dict(session_key=sk_str, driver_number=dn, acronym=acronym, team=team)

            pos = _f(d.get("position"))
            if pos is not None:
                DRIVER_POSITION.labels(**lbl).set(pos)

            lap = _f(d.get("lap_duration"))
            if lap is not None:
                DRIVER_LAP_TIME.labels(**lbl).set(lap)

            for field, metric in (
                ("sector_1", DRIVER_SECTOR1),
                ("sector_2", DRIVER_SECTOR2),
                ("sector_3", DRIVER_SECTOR3),
            ):
                val = _f(d.get(field))
                if val is not None:
                    metric.labels(**lbl).set(val)

            DRIVER_PIT_STOPS.labels(**lbl).set(int(d.get("pit_stops_total", 0)))
            DRIVER_LAPS.labels(**lbl).set(int(d.get("laps_completed", 0)))

            cumulative = _f(d.get("cumulative_time", 0)) or 0.0
            gap = max(0.0, cumulative - (leader_cumulative or 0.0))
            DRIVER_GAP.labels(**lbl).set(gap)


# ── Entry point ─────────────────────────────────────────────────────────────

def main() -> None:
    print(f"[exporter] Starting metrics exporter on port {PORT}...", flush=True)

    ddb = boto3.resource("dynamodb", **_boto_kwargs())
    live_table = ddb.Table(LIVE_TABLE)
    sim_table = ddb.Table(SIM_TABLE)
    s3_client = boto3.client("s3", **_boto_kwargs())

    start_http_server(PORT)
    print(f"[exporter] /metrics available at http://0.0.0.0:{PORT}/metrics", flush=True)

    while True:
        try:
            update_metrics(live_table, sim_table, s3_client)
        except Exception as exc:
            print(f"[exporter] Unexpected error: {exc}", file=sys.stderr, flush=True)
        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    main()
