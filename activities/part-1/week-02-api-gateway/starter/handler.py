"""
Week 2: API Gateway — Session Handlers

TODO: Implement three API endpoints:
1. GET /sessions — List 2024 race sessions from OpenF1
2. GET /sessions/{session_key} — Get a specific session from OpenF1
3. POST /sessions/{session_key}/ingest — Fetch session + driver data from OpenF1

Hints:
- Path parameters: event["pathParameters"]["session_key"]
- Query parameters: event.get("queryStringParameters") or {}
- Request body: json.loads(event["body"])
- Always include Content-Type header in response
- Catch requests.RequestException for API errors
"""
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
    # TODO: Call the OpenF1 API: GET /v1/sessions?session_type=Race&year=2024
    # TODO: Extract key fields from each session (session_key, session_name,
    #       session_type, circuit_short_name, date_start)
    # TODO: Return {"statusCode": 200, "body": json.dumps({"sessions": [...]}), "headers": HEADERS}
    # TODO: Catch requests.RequestException and return 500
    return {
        "statusCode": 501,
        "body": json.dumps({"error": "Not implemented"}),
        "headers": HEADERS,
    }


def get_session(event, context):
    """GET /sessions/{session_key} — Get session details from OpenF1."""
    # TODO: Extract session_key from event["pathParameters"]["session_key"]
    # TODO: Call the OpenF1 API: GET /v1/sessions?session_key={session_key}
    # TODO: Return 404 if no session found
    # TODO: Return the session details with status 200
    # TODO: Catch requests.RequestException and return 500
    return {
        "statusCode": 501,
        "body": json.dumps({"error": "Not implemented"}),
        "headers": HEADERS,
    }


def ingest_session(event, context):
    """POST /sessions/{session_key}/ingest — Fetch session and driver data."""
    # TODO: Extract session_key from event["pathParameters"]["session_key"]
    # TODO: Fetch session from OpenF1: GET /v1/sessions?session_key={session_key}
    # TODO: Return 404 if no session found
    # TODO: Fetch drivers from OpenF1: GET /v1/drivers?session_key={session_key}
    # TODO: Return a summary: session_key, session_name, drivers_found count,
    #       and list of drivers (driver_number, name_acronym)
    # TODO: Catch requests.RequestException and return 500
    return {
        "statusCode": 501,
        "body": json.dumps({"error": "Not implemented"}),
        "headers": HEADERS,
    }
