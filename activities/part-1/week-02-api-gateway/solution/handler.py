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
        )
        response.raise_for_status()
        data = response.json()

        sessions = [
            {
                "session_key": s["session_key"],
                "session_name": s["session_name"],
                "session_type": s["session_type"],
                "circuit_short_name": s["circuit_short_name"],
                "date_start": s["date_start"],
            }
            for s in data
        ]

        return {
            "statusCode": 200,
            "headers": HEADERS,
            "body": json.dumps({"sessions": sessions}),
        }

    except requests.RequestException as e:
        logger.error("Failed to fetch sessions: %s", e)
        return {
            "statusCode": 500,
            "headers": HEADERS,
            "body": json.dumps({"error": str(e)}),
        }


def get_session(event, context):
    """GET /sessions/{session_key} — Get session details from OpenF1."""
    session_key = event["pathParameters"]["session_key"]

    try:
        response = requests.get(
            f"{OPENF1_BASE_URL}/v1/sessions",
            params={"session_key": session_key},
        )
        response.raise_for_status()
        data = response.json()

        if not data:
            return {
                "statusCode": 404,
                "headers": HEADERS,
                "body": json.dumps({"error": "Session not found"}),
            }

        session = data[0]
        return {
            "statusCode": 200,
            "headers": HEADERS,
            "body": json.dumps({
                "session_key": session["session_key"],
                "session_name": session["session_name"],
                "session_type": session["session_type"],
                "circuit_short_name": session["circuit_short_name"],
                "date_start": session["date_start"],
            }),
        }

    except requests.RequestException as e:
        logger.error("Failed to fetch session %s: %s", session_key, e)
        return {
            "statusCode": 500,
            "headers": HEADERS,
            "body": json.dumps({"error": str(e)}),
        }


def ingest_session(event, context):
    """POST /sessions/{session_key}/ingest — Fetch session and driver data."""
    session_key = event["pathParameters"]["session_key"]

    try:
        # Fetch session details
        session_response = requests.get(
            f"{OPENF1_BASE_URL}/v1/sessions",
            params={"session_key": session_key},
        )
        session_response.raise_for_status()
        session_data = session_response.json()

        if not session_data:
            return {
                "statusCode": 404,
                "headers": HEADERS,
                "body": json.dumps({"error": "Session not found"}),
            }

        session = session_data[0]

        # Fetch drivers for the session
        drivers_response = requests.get(
            f"{OPENF1_BASE_URL}/v1/drivers",
            params={"session_key": session_key},
        )
        drivers_response.raise_for_status()
        drivers_data = drivers_response.json()

        drivers = [
            {
                "driver_number": d["driver_number"],
                "name_acronym": d["name_acronym"],
            }
            for d in drivers_data
        ]

        return {
            "statusCode": 200,
            "headers": HEADERS,
            "body": json.dumps({
                "session_key": session["session_key"],
                "session_name": session["session_name"],
                "drivers_found": len(drivers),
                "drivers": drivers,
            }),
        }

    except requests.RequestException as e:
        logger.error("Failed to ingest session %s: %s", session_key, e)
        return {
            "statusCode": 500,
            "headers": HEADERS,
            "body": json.dumps({"error": str(e)}),
        }
