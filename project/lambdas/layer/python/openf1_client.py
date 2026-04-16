import json
import os
import urllib.error
import urllib.parse
import urllib.request


BASE_URL = os.getenv("OPENF1_BASE_URL", "https://api.openf1.org").rstrip("/")


def _get(path: str, params: dict = None) -> list:
    url = f"{BASE_URL}{path}"
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "f1-telemetry/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_session(session_key: int):
    results = _get("/v1/sessions", {"session_key": session_key})
    return results[0] if results else None


def get_drivers(session_key: int) -> list:
    return _get("/v1/drivers", {"session_key": session_key})


def get_laps(session_key: int, driver_number: int) -> list:
    return _get("/v1/laps", {"session_key": session_key, "driver_number": driver_number})


def get_position(session_key: int, driver_number: int) -> list:
    return _get("/v1/position", {"session_key": session_key, "driver_number": driver_number})


def get_car_data(session_key: int, driver_number: int) -> list:
    return _get("/v1/car_data", {"session_key": session_key, "driver_number": driver_number})
