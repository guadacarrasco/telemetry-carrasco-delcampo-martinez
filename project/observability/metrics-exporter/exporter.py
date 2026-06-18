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
SESSIONS_TABLE = os.getenv("SESSIONS_TABLE", "f1_sessions")
LAPS_TABLE = os.getenv("LAPS_TABLE", "f1_laps")
DRIVER_STATS_TABLE = os.getenv("DRIVER_STATS_TABLE", "f1_driver_stats")
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
SESSION_INFO = Gauge(
    "f1_session_info", "Session metadata",
    ["session_key", "circuit", "country", "session_type", "date_start", "year"],
)

SESSION_LAPS_TOTAL = Gauge(
    "f1_session_laps_total", "Total scheduled laps for the session", ["session_key"]
)
_session_cache: dict = {}  # session_key → {circuit, country, session_type, date_start, year}
_total_laps_cache: dict = {}  # session_key → int total laps (cached after first query)
DRIVER_TYRE_LIFE = Gauge(
    "f1_driver_tyre_life_laps",
    "Laps on current tire set",
    ["session_key", "driver_number", "acronym", "team", "compound"],
)
_tyre_compound_cache: dict = {}  # (sk_str, dn) → last known compound


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


def _emit_session_laps_total(driver_stats_table, session_key: int) -> None:
    sk_str = str(session_key)
    if sk_str not in _total_laps_cache:
        try:
            resp = driver_stats_table.query(
                KeyConditionExpression=Key("session_key").eq(session_key)
            )
            total = max(
                (int(s.get("total_laps", 0) or 0) for s in resp.get("Items", [])),
                default=0,
            )
            if total > 0:
                _total_laps_cache[sk_str] = total
        except Exception as exc:
            print(f"[exporter] Could not fetch total_laps: {exc}", file=sys.stderr, flush=True)
    total = _total_laps_cache.get(sk_str, 0)
    if total > 0:
        SESSION_LAPS_TOTAL.labels(session_key=sk_str).set(total)


def _emit_session_info(sessions_table, session_key: int) -> None:
    global _session_cache
    sk_str = str(session_key)
    if sk_str not in _session_cache:
        try:
            resp = sessions_table.get_item(Key={"session_key": session_key})
            item = resp.get("Item", {})
            date_start = str(item.get("date_start", ""))[:10]
            raw_year = item.get("year")
            year = str(int(raw_year)) if raw_year is not None else date_start[:4]
            _session_cache[sk_str] = {
                "circuit": item.get("circuit", "Unknown"),
                "country": item.get("country", ""),
                "session_type": item.get("session_type", ""),
                "date_start": date_start,
                "year": year,
            }
        except Exception as exc:
            print(f"[exporter] Session info unavailable: {exc}", file=sys.stderr, flush=True)
            return
    info = _session_cache[sk_str]
    SESSION_INFO.labels(session_key=sk_str, **info).set(1)


# ── Core update ─────────────────────────────────────────────────────────────

def update_metrics(live_table, sim_table, sessions_table, laps_table, driver_stats_table) -> None:
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
        _emit_session_info(sessions_table, sk_int)
        _emit_session_laps_total(driver_stats_table, sk_int)
        if is_active:
            active_sessions.add(sk_int)

    # 2 — Update driver metrics for each active session
    for session_key in active_sessions:
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

        # Rank by (laps_completed DESC, cumulative_time ASC): leader is index 0.
        sorted_drivers = sorted(
            drivers,
            key=lambda d: (-int(d.get("laps_completed", 0)), float(_f(d.get("cumulative_time", 0)) or 0.0)),
        )
        computed_positions = {
            str(int(d.get("driver_number", 0))): rank + 1
            for rank, d in enumerate(sorted_drivers)
        }

        leader_cumulative = _f(sorted_drivers[0].get("cumulative_time", 0)) if sorted_drivers else 0.0

        for d in drivers:
            dn = str(int(d.get("driver_number", 0)))
            acronym = d.get("acronym", "")
            team = d.get("team_name", "")
            lbl = dict(session_key=sk_str, driver_number=dn, acronym=acronym, team=team)

            computed_pos = computed_positions.get(dn)
            if computed_pos is not None:
                DRIVER_POSITION.labels(**lbl).set(computed_pos)

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

            laps_done = int(d.get("laps_completed", 0))
            if laps_done > 0:
                try:
                    lap_resp = laps_table.get_item(
                        Key={"session_driver": f"{session_key}#{int(dn)}", "lap_number": laps_done}
                    )
                    lap_item = lap_resp.get("Item", {})
                    compound = str(lap_item.get("compound") or "")
                    tyre_life = int(lap_item.get("tyre_life_laps") or 0)
                    if compound:
                        cache_key = (sk_str, dn)
                        prev = _tyre_compound_cache.get(cache_key)
                        if prev and prev != compound:
                            try:
                                DRIVER_TYRE_LIFE.remove(sk_str, dn, acronym, team, prev)
                            except Exception:
                                pass
                        _tyre_compound_cache[cache_key] = compound
                        DRIVER_TYRE_LIFE.labels(
                            session_key=sk_str, driver_number=dn, acronym=acronym, team=team, compound=compound
                        ).set(tyre_life)
                except Exception:
                    pass


# ── Entry point ─────────────────────────────────────────────────────────────

def main() -> None:
    print(f"[exporter] Starting metrics exporter on port {PORT}...", flush=True)

    ddb = boto3.resource("dynamodb", **_boto_kwargs())
    live_table = ddb.Table(LIVE_TABLE)
    sim_table = ddb.Table(SIM_TABLE)
    sessions_table = ddb.Table(SESSIONS_TABLE)
    laps_table = ddb.Table(LAPS_TABLE)
    driver_stats_table = ddb.Table(DRIVER_STATS_TABLE)

    start_http_server(PORT)
    print(f"[exporter] /metrics available at http://0.0.0.0:{PORT}/metrics", flush=True)

    while True:
        try:
            update_metrics(live_table, sim_table, sessions_table, laps_table, driver_stats_table)
        except Exception as exc:
            print(f"[exporter] Unexpected error: {exc}", file=sys.stderr, flush=True)
        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    main()
