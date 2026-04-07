import json
import os

import requests


def handler(event, context):
    session_key = os.getenv("SESSION_KEY")

    if not session_key:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "SESSION_KEY environment variable is not set"}),
        }

    url = f"https://api.openf1.org/v1/sessions?session_key={session_key}"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        return {
            "statusCode": 504,
            "body": json.dumps({"error": "OpenF1 API request timed out"}),
        }
    except requests.exceptions.HTTPError as e:
        return {
            "statusCode": 502,
            "body": json.dumps({"error": f"OpenF1 API error: {e.response.status_code}"}),
        }
    except requests.exceptions.RequestException as e:
        return {
            "statusCode": 502,
            "body": json.dumps({"error": f"Failed to reach OpenF1 API: {str(e)}"}),
        }

    sessions = response.json()

    if not sessions:
        return {
            "statusCode": 404,
            "body": json.dumps({"error": f"No session found for session_key={session_key}"}),
        }

    session = sessions[0]
    data = {
        "session_key": session.get("session_key"),
        "session_name": session.get("session_name"),
        "circuit": session.get("circuit_short_name"),
        "date_start": session.get("date_start"),
        "date_end": session.get("date_end"),
    }

    return {
        "statusCode": 200,
        "body": json.dumps(data),
    }
