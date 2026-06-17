"""
F1 Consumer — SQS → DynamoDB (f1_live_state)

Polls f1_simulation_events queue. For each lap_completed message:
  - Upserts driver state in f1_live_state (accumulates pit stops, laps, cumulative time).
  - Updates f1_simulator_state to track simulation lifecycle.

After IDLE_TIMEOUT_SECONDS with no messages, marks simulation as completed
and stays alive waiting for the next one.
"""

import json
import os
import sys
import time
from decimal import Decimal, InvalidOperation


import boto3
from botocore.exceptions import ClientError


# ── Config ─────────────────────────────────────────────────────────────────

ENDPOINT = os.getenv("AWS_ENDPOINT_URL")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "f1_simulation_events")
LIVE_TABLE = os.getenv("LIVE_STATE_TABLE", "f1_live_state")
SIM_TABLE = os.getenv("SIMULATOR_STATE_TABLE", "f1_simulator_state")
IDLE_TIMEOUT = int(os.getenv("IDLE_TIMEOUT_SECONDS", "60"))


def _boto_kwargs() -> dict:
    kwargs = {"region_name": REGION}
    if ENDPOINT:
        kwargs["endpoint_url"] = ENDPOINT
    return kwargs


def _queue_url(sqs_client) -> str:
    """Build a path-style SQS URL compatible with LocalStack.

    LocalStack's get_queue_url() returns a subdomain URL
    (e.g. http://sqs.us-east-1.localhost.localstack.cloud:4566/...) that
    doesn't resolve from inside Docker. We construct the equivalent path-style
    URL using the configured endpoint directly, which LocalStack also accepts.
    """
    ep = (ENDPOINT or "http://host.docker.internal:4566").rstrip("/")
    # Verify the queue exists (raises ClientError if not yet deployed)
    sqs_client.get_queue_url(QueueName=QUEUE_NAME)
    return f"{ep}/000000000000/{QUEUE_NAME}"


# ── Message processing ──────────────────────────────────────────────────────

def _to_decimal(value) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def process_message(body: dict, live_table, sim_table, active_session: int | None) -> int:
    """Upsert driver state in f1_live_state; return the session_key."""
    session_key = int(body["session_key"])
    driver_number = int(body["driver_number"])

    # Read existing record to accumulate counters
    existing = live_table.get_item(
        Key={"session_key": session_key, "driver_number": driver_number}
    ).get("Item", {})

    lap_duration = _to_decimal(body.get("lap_duration"))

    # laps_completed = highest lap_number seen (idempotent: re-running same sim stays at 58)
    incoming_lap = int(body["lap_number"]) if body.get("lap_number") is not None else 0
    laps_completed = max(int(existing.get("laps_completed", 0)), incoming_lap)

    # pit_stops = size of the SET of distinct pit-out lap numbers (idempotent)
    # DynamoDB stores this as a string set to avoid Decimal set issues.
    prev_pit_set: set = set(existing.get("pit_lap_set", set()))
    if body.get("is_pit_out") and incoming_lap > 0:
        prev_pit_set.add(str(incoming_lap))
    pit_stops = len(prev_pit_set)

    # cumulative_time = sum of lap_times map (lap_number → duration), idempotent.
    # Stored as a DynamoDB map with string keys to avoid Decimal set issues.
    prev_lap_times: dict = dict(existing.get("lap_times_map", {}))
    if incoming_lap > 0 and lap_duration is not None:
        prev_lap_times[str(incoming_lap)] = lap_duration
    cumulative_time = sum(prev_lap_times.values(), Decimal("0"))

    item = {
        "session_key": session_key,
        "driver_number": driver_number,
        "driver_name": body.get("driver_name", ""),
        "acronym": body.get("driver_acronym", ""),
        "team_name": body.get("team_name", ""),
        "lap_number": int(body["lap_number"]) if body.get("lap_number") is not None else 0,
        "lap_duration": lap_duration,
        "position": int(body["position"]) if body.get("position") is not None else None,
        "sector_1": _to_decimal(body.get("sector_1")),
        "sector_2": _to_decimal(body.get("sector_2")),
        "sector_3": _to_decimal(body.get("sector_3")),
        "is_pit_out": bool(body.get("is_pit_out", False)),
        "laps_completed": laps_completed,
        "pit_stops_total": pit_stops,
        "pit_lap_set": prev_pit_set if prev_pit_set else None,
        "lap_times_map": prev_lap_times if prev_lap_times else None,
        "cumulative_time": cumulative_time,
        "processed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    # Remove None values — DynamoDB rejects them
    item = {k: v for k, v in item.items() if v is not None}

    live_table.put_item(Item=item)

    # Mark simulation as processing on the first message of a new session
    if active_session != session_key:
        sim_table.put_item(Item={
            "session_key": session_key,
            "status": "processing",
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
        print(f"[consumer] Simulation started — session {session_key}", flush=True)

    return session_key


# ── Main loop ───────────────────────────────────────────────────────────────

def run() -> None:
    print("[consumer] Starting F1 consumer...", flush=True)

    sqs = boto3.client("sqs", **_boto_kwargs())
    ddb = boto3.resource("dynamodb", **_boto_kwargs())
    live_table = ddb.Table(LIVE_TABLE)
    sim_table = ddb.Table(SIM_TABLE)

    # Retry queue URL resolution (LocalStack may not be ready immediately)
    queue_url = None
    for attempt in range(10):
        try:
            queue_url = _queue_url(sqs)
            print(f"[consumer] Queue URL: {queue_url}", flush=True)
            break
        except Exception as exc:
            print(f"[consumer] Queue not ready ({exc}), retrying in 5s...", flush=True)
            time.sleep(5)

    if queue_url is None:
        print("[consumer] Could not resolve queue URL after retries. Exiting.", file=sys.stderr)
        sys.exit(1)

    active_session: int | None = None
    last_message_time: float = time.time()

    print(f"[consumer] Polling queue every 20s (long poll). Idle timeout: {IDLE_TIMEOUT}s.", flush=True)

    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
            )
        except Exception as exc:
            print(f"[consumer] SQS error: {exc}", file=sys.stderr, flush=True)
            time.sleep(5)
            continue

        messages = response.get("Messages", [])

        if messages:
            last_message_time = time.time()

            for msg in messages:
                try:
                    body = json.loads(msg["Body"])
                    active_session = process_message(body, live_table, sim_table, active_session)
                    acronym = body.get("driver_acronym", body.get("driver_number", "?"))
                    lap = body.get("lap_number", "?")
                    print(f"[consumer] Lap {lap:>2} — {acronym}", flush=True)
                except Exception as exc:
                    print(f"[consumer] Failed to process message: {exc}", file=sys.stderr, flush=True)
                finally:
                    # Always delete to avoid reprocessing
                    try:
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=msg["ReceiptHandle"],
                        )
                    except Exception:
                        pass

        else:
            idle = time.time() - last_message_time

            if active_session is not None and idle >= IDLE_TIMEOUT:
                print(
                    f"[consumer] No messages for {idle:.0f}s — simulation complete (session {active_session}).",
                    flush=True,
                )
                try:
                    sim_table.put_item(Item={
                        "session_key": active_session,
                        "status": "completed",
                        "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    })
                except Exception as exc:
                    print(f"[consumer] Could not update sim state: {exc}", file=sys.stderr, flush=True)

                active_session = None
                last_message_time = time.time()
                print("[consumer] Ready for next simulation.", flush=True)


if __name__ == "__main__":
    run()
