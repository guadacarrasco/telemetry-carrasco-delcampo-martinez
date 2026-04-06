# Week 2: API Gateway and Event Sources

## Objectives
- Understand API Gateway REST API concepts
- Expose Lambda functions via HTTP endpoints
- Handle request parameters, path variables, and request bodies
- Implement proper HTTP status codes and error handling

## Tools
- SAM CLI, API Gateway, Lambda, Python 3.12

## Activity
Expose the simulation as a REST API with the following endpoints:
- `GET /sessions` — List all race sessions from OpenF1 (2024 season)
- `GET /sessions/{session_key}` — Get session details
- `POST /sessions/{session_key}/ingest` — Trigger data ingestion from OpenF1

## Steps

### 1. Review the starter code
The starter includes:
- `handler.py` — three handler skeletons with TODO comments
- `template.yaml` — SAM template with one function defined and two more for you to add
- `requirements.txt` — Python dependencies

### 2. Understand the API Gateway event format

When API Gateway invokes your Lambda, the `event` object looks like this:

```json
{
  "httpMethod": "GET",
  "path": "/sessions/9158",
  "pathParameters": {
    "session_key": "9158"
  },
  "queryStringParameters": {
    "year": "2024"
  },
  "body": null,
  "headers": {
    "Content-Type": "application/json"
  }
}
```

Key fields:
- **Path parameters**: `event["pathParameters"]["session_key"]` — values from `{placeholders}` in the URL
- **Query parameters**: `event.get("queryStringParameters") or {}` — values after `?` in the URL (can be `None`)
- **Request body**: `json.loads(event["body"])` — for POST/PUT requests (string that needs parsing)

### 3. Implement the handlers

**`list_sessions`** — Call the OpenF1 API to fetch race sessions. Use query parameters `session_type=Race` and `year=2024` to filter results. Return a list with each session's key, name, type, circuit, and start date.

**`get_session`** — Extract `session_key` from path parameters. Call the OpenF1 API with that key. Return 404 if the session is not found.

**`ingest_session`** — Extract `session_key` from path parameters. Fetch both the session and its drivers from OpenF1. Return a summary with the session name and the list of drivers found. This endpoint does more work — it will need a longer timeout (see step 4).

All three handlers should:
- Include `{"Content-Type": "application/json"}` in response headers
- Return appropriate status codes (200, 404, 500)
- Catch `requests.RequestException` and return 500 with the error message

### 4. Configure API Gateway in SAM

The starter template has `ListSessionsFunction` fully defined. You need to add two more functions following the same pattern:

- **GetSessionFunction**: handler `handler.get_session`, path `/sessions/{session_key}`, method `GET`
- **IngestSessionFunction**: handler `handler.ingest_session`, path `/sessions/{session_key}/ingest`, method `POST`

Note the `{session_key}` syntax in the path — this is how SAM defines path parameters. API Gateway will extract the value and pass it in `event["pathParameters"]`.

> **Tip**: The ingest endpoint fetches more data and may take longer. Consider setting a higher `Timeout` (e.g., 120 seconds) and `MemorySize` (e.g., 512 MB) for that function.

### 5. Build and test locally

SAM uses Docker to emulate API Gateway and Lambda locally. You must build before running:

```bash
sam build
sam local start-api
```

Then in another terminal:
```bash
# List all 2024 race sessions
curl http://localhost:3000/sessions

# Get a specific session
curl http://localhost:3000/sessions/9158

# Trigger ingestion
curl -X POST http://localhost:3000/sessions/9158/ingest
```

> **Note**: The first invocation may be slow as Docker pulls the Python 3.12 Lambda image. Subsequent calls are faster.

### 6. Deploy (optional)
```bash
sam build
sam deploy --guided
```

## Expected Results
- `GET /sessions` returns a JSON array of race sessions
- `GET /sessions/9158` returns a single session's details
- `GET /sessions/0000` returns `{"error": "Session not found"}` with status 404
- `POST /sessions/9158/ingest` returns the session name and a list of drivers
- All error responses include appropriate status codes and messages

## Key Concepts
- **REST API**: Resource-based URLs with HTTP methods (GET, POST, PUT, DELETE)
- **API Gateway Event**: The event object Lambda receives from API Gateway — includes path parameters, query strings, headers, and body
- **Path Parameters**: `/sessions/{session_key}` → `event["pathParameters"]["session_key"]`
- **Lambda Proxy Integration**: API Gateway passes the full HTTP request to Lambda and expects a response with `statusCode`, `headers`, and `body`
- **CORS**: Cross-Origin Resource Sharing headers for browser access

## OpenF1 API Reference
These are the endpoints you will call from your handlers:

| Endpoint | Description |
|----------|-------------|
| `GET /v1/sessions?session_type=Race&year=2024` | List race sessions |
| `GET /v1/sessions?session_key=9158` | Get a specific session |
| `GET /v1/drivers?session_key=9158` | Get drivers for a session |

Base URL: `https://api.openf1.org`

## Resources
- [AWS Lambda Developer Guide](https://docs.aws.amazon.com/lambda/latest/dg/)
- [API Gateway REST API](https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-rest-api.html)
- [OpenF1 API Documentation](https://openf1.org/)
- [SAM CLI Reference](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/)
