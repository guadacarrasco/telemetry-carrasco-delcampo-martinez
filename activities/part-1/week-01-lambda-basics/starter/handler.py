"""
Week 1: Lambda Basics — Start Simulation Handler

TODO: Implement a Lambda function that fetches session data from the OpenF1 API.

Steps:
1. Read SESSION_KEY from environment variables
2. Call the OpenF1 API: https://api.openf1.org/v1/sessions?session_key={SESSION_KEY}
3. Return the session information as a JSON response

Hints:
- Use os.getenv() to read environment variables
- Use the requests library for HTTP calls
- Return a dict with statusCode and body keys
- Handle errors gracefully (missing env var, API failure)
"""
import json
import os


def handler(event, context):
    # TODO: Read SESSION_KEY from environment variables
    session_key = None

    # TODO: Validate that SESSION_KEY is set
    if not session_key:
        pass  # Return 400 error

    # TODO: Call the OpenF1 API to fetch session data
    # URL: https://api.openf1.org/v1/sessions?session_key={session_key}

    # TODO: Parse the API response

    # TODO: Return the session data as a Lambda response
    # Format: {"statusCode": 200, "body": json.dumps({...})}
    return {
        "statusCode": 501,
        "body": json.dumps({"error": "Not implemented"}),
    }
