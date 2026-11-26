"""Initialize indices, train an Isolation Forest, and score ingested events."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import os
import random
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from demo import event, write_events

RAW = "cloud-logs"
SCORED = "cloud-anomalies"
FEATURES = ("latency_ms", "status", "bytes")
BASE_URL = os.getenv("ELASTICSEARCH_URL", "http://127.0.0.1:9200")

FIELDS = {
    "@timestamp": {"type": "date"},
    "event_id": {"type": "keyword"},
    "run_id": {"type": "keyword"},
    "path": {"type": "keyword"},
    "status": {"type": "integer"},
    "latency_ms": {"type": "double"},
    "bytes": {"type": "long"},
    "is_error": {"type": "boolean"},
    "demo_anomaly": {"type": "boolean"},
}


def es(method, path, payload=None, ignore=()):
    data = None if payload is None else json.dumps(payload).encode()
    request = Request(BASE_URL + path, data=data, method=method,
                      headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=30) as response:
            return json.load(response)
    except HTTPError as error:
        body = error.read().decode()
        if error.code in ignore:
            return None
        if error.code == 400 and "resource_already_exists_exception" in body and method == "PUT":
            return None
        raise RuntimeError(f"{method} {path}: HTTP {error.code}: {body}") from error


def init():
    scoring = {"score": {"type": "double"}, "is_anomaly": {"type": "boolean"},
               "alert_state": {"type": "keyword"}, "scored_at": {"type": "date"}}
    for index, fields in ((RAW, FIELDS), (SCORED, FIELDS | scoring)):
        es("PUT", f"/{index}", {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {"dynamic": "strict", "properties": fields},
        })


def vector(document):
    return [float(document[name]) for name in FEATURES]


def train(history, model_path):
    import joblib
    from sklearn.ensemble import IsolationForest

    if history is None:
        # Rebuild the fixed-seed baseline in a temporary file outside source control.
        from tempfile import TemporaryDirectory
        with TemporaryDirectory() as directory:
            baseline = Path(directory) / "historical-normal.jsonl"
            rng = random.Random(19)
            write_events(baseline, (event(rng) for _ in range(2000)))
            return train(baseline, model_path)
    with history.open(encoding="utf-8") as stream:
        documents = [json.loads(line) for line in stream if line.strip()]
    if len(documents) < 50 or any(record.get("demo_anomaly") for record in documents):
        raise ValueError("Provide at least 50 historical normal records")
    model = IsolationForest(n_estimators=150, contamination=0.02, random_state=19)
    model.fit([vector(record) for record in documents])
    model_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = model_path.with_suffix(".tmp")
    joblib.dump({"model": model, "features": FEATURES}, temporary)
    temporary.replace(model_path)
    print(f"Trained on {len(documents)} historical records. Saved {model_path}.")


def pages(index, query):
    scroll_id = None
    try:
        result = es("POST", f"/{index}/_search?scroll=2m",
                    {"size": 250, "sort": ["_doc"], "query": query})
        while True:
            scroll_id = result["_scroll_id"]
            hits = result["hits"]["hits"]
            if not hits:
                break
            yield hits
            result = es("POST", "/_search/scroll", {"scroll": "2m", "scroll_id": scroll_id})
    finally:
        if scroll_id:
            es("DELETE", "/_search/scroll", {"scroll_id": [scroll_id]}, ignore=(404,))


def score(model, sns_enabled):
    count = 0
    for hits in pages(RAW, {"match_all": {}}):
        existing = es("POST", f"/{SCORED}/_mget", {"ids": [hit["_id"] for hit in hits]})
        unseen = [hit for hit, result in zip(hits, existing["docs"], strict=True)
                  if not result["found"]]
        if not unseen:
            continue
        margins = model.decision_function([vector(hit["_source"]) for hit in unseen])
        for hit, margin in zip(unseen, margins, strict=True):
            anomaly = bool(margin < 0)
            document = hit["_source"] | {
                "score": -float(margin),
                "is_anomaly": anomaly,
                "scored_at": datetime.now(timezone.utc).isoformat(),
                "alert_state": "pending" if anomaly and sns_enabled else "not_requested",
            }
            created = es("PUT", f"/{SCORED}/_create/{quote(hit['_id'], safe='')}",
                         document, ignore=(409,))
            count += created is not None
    return count


def publish_pending(client, arn):
    for hits in pages(SCORED, {"term": {"alert_state": "pending"}}):
        for hit in hits:
            document = hit["_source"]
            try:
                client.publish(TopicArn=arn, Subject="Shopping log anomaly",
                               Message=json.dumps(document, indent=2))
            except Exception as error:
                print(f"SNS publish will be retried: {error}", flush=True)
                return
            es("POST", f"/{SCORED}/_update/{quote(hit['_id'], safe='')}?refresh=wait_for",
               {"doc": {"alert_state": "sent"}})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init")
    trainer = commands.add_parser("train")
    trainer.add_argument("--history", type=Path)
    trainer.add_argument("--model", type=Path, default=Path("/model/model.joblib"))
    scorer = commands.add_parser("score")
    scorer.add_argument("--model", type=Path, default=Path("/model/model.joblib"))
    scorer.add_argument("--watch", action="store_true")
    scorer.add_argument("--interval", type=float, default=10)
    args = parser.parse_args()
    if args.command == "init":
        init()
        return
    if args.command == "train":
        train(args.history, args.model)
        return
    if args.interval <= 0:
        parser.error("--interval must be positive")
    import joblib

    bundle = joblib.load(args.model)
    if bundle["features"] != FEATURES:
        raise ValueError("Model feature order differs from the scorer")
    arn = os.getenv("SNS_TOPIC_ARN")
    if arn:
        import boto3
        client = boto3.client("sns", region_name=os.getenv("AWS_DEFAULT_REGION") or arn.split(":")[3])
    while True:
        try:
            count = score(bundle["model"], bool(arn))
            if count or not args.watch:
                print(f"Newly scored events: {count}", flush=True)
            if arn:
                publish_pending(client, arn)
        except (URLError, RuntimeError) as error:
            if not args.watch:
                raise
            print(f"Scoring will be retried: {error}", flush=True)
        if not args.watch:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
