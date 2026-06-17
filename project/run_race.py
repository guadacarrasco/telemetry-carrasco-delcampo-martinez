#!/usr/bin/env python3
"""Start an F1 race simulation at a chosen speed.

Usage:
    python run_race.py --session 9662 --minutes 5
    python run_race.py --session 9662 --minutes 10

The simulation replays all laps of the chosen session compressed into
the requested number of minutes. A typical F1 race lasts ~90 min, so:
  --minutes 1   → ~90x speed
  --minutes 5   → ~18x speed
  --minutes 10  → ~9x speed
  --minutes 90  → real time (not recommended for tests)

Make sure docker-compose services are up and 'make deploy' has been run first.
"""

import argparse
import json
import os
import ssl
import sys
import urllib.error
import urllib.request

# LocalStack uses a self-signed certificate that may be expired locally.
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def load_api_url() -> str:
    api_url_file = os.path.join(os.path.dirname(__file__), ".api_url")
    if not os.path.exists(api_url_file):
        print("Error: .api_url not found. Run 'make deploy' first.", file=sys.stderr)
        sys.exit(1)
    with open(api_url_file) as f:
        return f.read().strip().rstrip("/")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Start an F1 race simulation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--session", type=int, required=True,
                        help="OpenF1 session key (e.g. 9662 for Bahrain 2024 FP1)")
    parser.add_argument("--minutes", type=float, required=True,
                        help="Total playback duration in minutes")
    args = parser.parse_args()

    if args.minutes <= 0:
        print("Error: --minutes must be positive.", file=sys.stderr)
        sys.exit(1)

    api_url = load_api_url()
    playback_seconds = max(1, int(args.minutes * 60))

    payload = json.dumps({
        "session_key": args.session,
        "playback_seconds": playback_seconds,
    }).encode()

    url = f"{api_url}/start-simulation"
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    print(f"Starting simulation for session {args.session} over {args.minutes:.1f} min ({playback_seconds}s)...")

    try:
        with urllib.request.urlopen(req, timeout=15, context=_SSL_CTX) as resp:
            body = json.loads(resp.read().decode())
            print(f"  Simulation queued successfully.")
            print(f"  Session key:    {body.get('session_key')}")
            print(f"  Playback time:  {args.minutes:.1f} min ({playback_seconds}s)")
            hint = body.get("hint", "")
            if hint:
                print(f"  {hint}")
            print()
            print("  Monitor at: http://localhost:3000  (Grafana)")
            print("  Metrics at: http://localhost:8000/metrics")
            print("  Prometheus: http://localhost:9090")
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode())
            msg = body.get("message", str(body))
        except Exception:
            msg = str(e)
        print(f"Error {e.code}: {msg}", file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"Connection error: {e.reason}", file=sys.stderr)
        print("Is LocalStack running? Try: docker-compose up -d localstack", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
