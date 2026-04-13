import json
import logging
import os

import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

OPENF1_BASE_URL = os.getenv("OPENF1_BASE_URL", "https://api.openf1.org")
HEADERS = {"Content-Type": "application/json"}


def list_sessions(event, context):
    """GET /sessions — Return list of 2024 race sessions from OpenF1."""
    try:
        response = requests.get(
            f"{OPENF1_BASE_URL}/v1/sessions",
            params={"session_type": "Race", "year": 2024},
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException as e:
        return {
            "statusCode": 500,
            "headers": HEADERS,
            "body": json.dumps({"error": str(e)}),
        }

    sessions = [
        {
            "session_key": s.get("session_key"),
            "session_name": s.get("session_name"),
            "session_type": s.get("session_type"),
            "circuit": s.get("circuit_short_name"),
            "date_start": s.get("date_start"),
        }
        for s in response.json()
    ]

    return {
        "statusCode": 200,
        "headers": HEADERS,
        "body": json.dumps({"sessions": sessions}),
    }


def get_session(event, context):
    """GET /sessions/{session_key} — Get session details from OpenF1."""
    session_key = event["pathParameters"]["session_key"]

    try:
        response = requests.get(
            f"{OPENF1_BASE_URL}/v1/sessions",
            params={"session_key": session_key},
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException as e:
        return {
            "statusCode": 500,
            "headers": HEADERS,
            "body": json.dumps({"error": str(e)}),
        }

    sessions = response.json()

    if not sessions:
        return {
            "statusCode": 404,
            "headers": HEADERS,
            "body": json.dumps({"error": "Session not found"}),
        }

    return {
        "statusCode": 200,
        "headers": HEADERS,
        "body": json.dumps(sessions[0]),
    }


def ingest_session(event, context):
    """POST /sessions/{session_key}/ingest — Fetch session and driver data."""
    session_key = event["pathParameters"]["session_key"]

    try:
        session_response = requests.get(
            f"{OPENF1_BASE_URL}/v1/sessions",
            params={"session_key": session_key},
            timeout=10,
        )
        session_response.raise_for_status()

        sessions = session_response.json()

        if not sessions:
            return {
                "statusCode": 404,
                "headers": HEADERS,
                "body": json.dumps({"error": "Session not found"}),
            }

        session = sessions[0]

        drivers_response = requests.get(
            f"{OPENF1_BASE_URL}/v1/drivers",
            params={"session_key": session_key},
            timeout=30,
        )
        drivers_response.raise_for_status()

    except requests.RequestException as e:
        return {
            "statusCode": 500,
            "headers": HEADERS,
            "body": json.dumps({"error": str(e)}),
        }

    drivers = [
        {
            "driver_number": d.get("driver_number"),
            "name_acronym": d.get("name_acronym"),
        }
        for d in drivers_response.json()
    ]

    return {
        "statusCode": 200,
        "headers": HEADERS,
        "body": json.dumps({
            "session_key": session.get("session_key"),
            "session_name": session.get("session_name"),
            "drivers_found": len(drivers),
            "drivers": drivers,
        }),
    }
