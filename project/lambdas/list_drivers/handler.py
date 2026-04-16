import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "layer", "python"))

from repositories import DriverStatsRepository
from utils import error, ok


def handler(event, context):
    path_params = event.get("pathParameters") or {}
    session_key_str = path_params.get("session_key")

    if not session_key_str:
        return error(400, "session_key is required")

    try:
        session_key = int(session_key_str)
    except ValueError:
        return error(400, "session_key must be an integer")

    try:
        drivers = DriverStatsRepository().list_for_session(session_key)
    except Exception as e:
        return error(500, str(e))

    if not drivers:
        return error(404, f"No drivers found for session {session_key}. Has it been ingested?")

    result = [
        {
            "driver_number": d.get("driver_number"),
            "full_name": d.get("full_name"),
            "acronym": d.get("acronym"),
            "team_name": d.get("team_name"),
        }
        for d in drivers
    ]
    return ok(result)
