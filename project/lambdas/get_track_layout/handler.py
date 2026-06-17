import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "layer", "python"))

from repositories import RawDataRepository
from utils import error


def handler(event, context):
    path_params = event.get("pathParameters") or {}
    session_key_str = path_params.get("session_key")

    if not session_key_str:
        return error(400, "session_key is required")

    try:
        session_key = int(session_key_str)
    except ValueError:
        return error(400, "session_key must be an integer")

    raw_repo = RawDataRepository()
    try:
        data = raw_repo.get_json(f"sessions/{session_key}/track_layout.json")
    except Exception:
        return error(404, f"Track layout not found for session {session_key}. Run ingest first.")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(data),
    }
