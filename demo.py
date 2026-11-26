"""Serve a small shopping demo and generate telemetry for its pipeline."""
import argparse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import random
from threading import Lock
import time
from uuid import uuid4

LOG_PATH = Path("/logs/shop.jsonl")
LOG_LOCK = Lock()


def event(rng, anomaly=False, **extra):
    return {
        "event_id": str(uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "path": rng.choice(("/products", "/cart", "/checkout")),
        "latency_ms": round(rng.uniform(1800, 5000) if anomaly else rng.uniform(25, 180), 2),
        "status": rng.choice((500, 503)) if anomaly else rng.choice((200, 200, 201)),
        "bytes": rng.randint(18_000, 50_000) if anomaly else rng.randint(30, 4500),
        **extra,
    }


def write_events(path, events):
    path.parent.mkdir(parents=True, exist_ok=True)
    with LOG_LOCK, path.open("a", encoding="utf-8") as stream:
        for record in events:
            stream.write(json.dumps(record) + "\n")


class Shop(BaseHTTPRequestHandler):
    def do_GET(self):
        start = time.perf_counter()
        route = self.path.split("?", 1)[0]
        results = {
            "/": {"routes": ["/products", "/cart", "/checkout"]},
            "/products": {"products": [{"sku": "notebook", "price": 5}]},
            "/cart": {"items": [{"sku": "notebook", "quantity": 1}], "total": 5},
            "/checkout": {"quote": 5, "currency": "USD"},
        }
        status = 200 if route in results else 404
        body = json.dumps(results.get(route, {"error": "not found"})).encode()
        time.sleep(random.uniform(0.025, 0.18))
        write_events(LOG_PATH, [event(random, path=route, status=status, bytes=len(body),
                                      latency_ms=round((time.perf_counter() - start) * 1000, 2))])
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("serve")
    emit = sub.add_parser("emit")
    emit.add_argument("--normal", type=int, default=100)
    emit.add_argument("--anomalies", type=int, default=10)
    emit.add_argument("--seed", type=int, default=2026)
    args = parser.parse_args()
    if args.command == "serve":
        ThreadingHTTPServer(("0.0.0.0", 8000), Shop).serve_forever()
        return
    if min(args.normal, args.anomalies) < 0:
        parser.error("Event counts must be nonnegative")
    rng = random.Random(args.seed)
    run_id = str(uuid4())
    flags = [False] * args.normal + [True] * args.anomalies
    rng.shuffle(flags)
    write_events(LOG_PATH, (event(rng, flag, run_id=run_id, demo_anomaly=flag) for flag in flags))
    print(run_id)


if __name__ == "__main__":
    main()
