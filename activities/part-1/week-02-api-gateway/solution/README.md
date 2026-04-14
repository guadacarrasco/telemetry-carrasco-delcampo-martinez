# Week 02 - API Gateway: Testing Guide

This document explains how to test the deployed F1 API.

## Live API

The API is deployed on AWS API Gateway:

```
https://e7f45mcg68.execute-api.us-east-2.amazonaws.com/dev
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/sessions` | List all 2024 F1 race sessions |
| GET | `/sessions/{session_key}` | Get details for a specific session |
| POST | `/sessions/{session_key}/ingest` | Fetch session and driver data |

---

## Option 1: Run the test script 

Requires `bash` and `curl` (available by default on Mac/Linux).

```bash
bash test_api.sh https://e7f45mcg68.execute-api.us-east-2.amazonaws.com/dev
```

This will call all three endpoints and pretty-print the JSON responses.

---

## Option 2: Use the Makefile

Requires `make`, `bash`, and `curl`.

```bash
make test URL=https://e7f45mcg68.execute-api.us-east-2.amazonaws.com/dev
```

---

## Option 3: Manual curl

```bash
BASE=https://e7f45mcg68.execute-api.us-east-2.amazonaws.com/dev

# List all 2024 race sessions
curl $BASE/sessions

# Get a specific session
curl $BASE/sessions/9158

# Ingest session and driver data
curl -X POST $BASE/sessions/9158/ingest
```

---

## Expected responses

**GET /sessions** — returns 30 race sessions from the 2024 F1 season:
```json
{
  "sessions": [
    {
      "session_key": 9472,
      "session_name": "Race",
      "session_type": "Race",
      "circuit_short_name": "Sakhir",
      "date_start": "2024-03-02T15:00:00+00:00"
    },
    ...
  ]
}
```

**GET /sessions/9158** — returns details for the requested session:
```json
{
  "session_key": 9158,
  "session_name": "Practice 1",
  "session_type": "Practice",
  "circuit_short_name": "Singapore",
  "date_start": "2023-09-15T09:30:00+00:00"
}
```

**POST /sessions/9158/ingest** — returns the session and all 20 drivers:
```json
{
  "session_key": 9158,
  "session_name": "Practice 1",
  "drivers_found": 20,
  "drivers": [
    { "driver_number": 1, "name_acronym": "VER" },
    { "driver_number": 44, "name_acronym": "HAM" },
    ...
  ]
}
```
