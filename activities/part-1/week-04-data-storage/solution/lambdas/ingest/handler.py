import json
import os
import urllib.error
import urllib.request
from decimal import Decimal


def _http_get_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "f1-telemetry-ingest/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _ddb_resource():
    import boto3

    kwargs = {"region_name": os.environ.get("AWS_REGION", "us-east-1")}
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.resource("dynamodb", **kwargs)


def _s3_client():
    import boto3

    kwargs = {"region_name": os.environ.get("AWS_REGION", "us-east-1")}
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("s3", **kwargs)


def handler(event, context):
    base = os.environ.get("OPENF1_BASE_URL", "https://api.openf1.org").rstrip("/")
    session_key = int(os.environ["SESSION_KEY"])
    sessions_table_name = os.environ["SESSIONS_TABLE"]
    drivers_table_name = os.environ["DRIVER_STATS_TABLE"]
    bucket = os.environ["RAW_BUCKET"]

    session_url = f"{base}/v1/sessions?session_key={session_key}"
    try:
        sessions = _http_get_json(session_url)
    except urllib.error.URLError as e:
        return {"ok": False, "error": f"openf1_sessions: {e}"}

    if not sessions:
        return {"ok": False, "error": "session_not_found"}

    session = sessions[0]

    s3c = _s3_client()
    s3c.put_object(
        Bucket=bucket,
        Key=f"sessions/{session_key}/session_raw.json",
        Body=json.dumps(sessions, default=str).encode("utf-8"),
        ContentType="application/json",
    )

    ddb = _ddb_resource()
    sessions_tbl = ddb.Table(sessions_table_name)
    item = {
        "session_key": Decimal(str(session.get("session_key", session_key))),
        "session_name": session.get("session_name") or "",
        "session_type": session.get("session_type") or "",
        "circuit_short_name": session.get("circuit_short_name") or "",
        "date_start": session.get("date_start") or "",
        "date_end": session.get("date_end") or "",
    }
    sessions_tbl.put_item(Item=item)

    drivers_url = f"{base}/v1/drivers?session_key={session_key}"
    try:
        drivers = _http_get_json(drivers_url)
    except urllib.error.URLError as e:
        return {"ok": False, "error": f"openf1_drivers: {e}"}

    s3c.put_object(
        Bucket=bucket,
        Key=f"sessions/{session_key}/drivers_raw.json",
        Body=json.dumps(drivers, default=str).encode("utf-8"),
        ContentType="application/json",
    )

    drivers_tbl = ddb.Table(drivers_table_name)
    for d in drivers:
        num = d.get("driver_number")
        if num is None:
            continue
        row = {
            "session_key": Decimal(str(session_key)),
            "driver_number": Decimal(str(num)),
            "name_acronym": d.get("name_acronym") or "",
            "full_name": d.get("full_name") or "",
            "team_name": d.get("team_name") or "",
        }
        drivers_tbl.put_item(Item=row)

    return {
        "ok": True,
        "session_key": session_key,
        "drivers_indexed": len(drivers),
    }
