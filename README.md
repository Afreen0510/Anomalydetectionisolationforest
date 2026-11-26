# EC2 shopping-log anomaly detection

A single-host demonstration of Filebeat → Apache Kafka → Logstash → Elasticsearch. A separately trained Isolation Forest reads from `cloud-logs` and writes one document per event to `cloud-anomalies`. Kibana shows requests, mean latency, HTTP errors, and flagged anomalies. Optional Amazon SNS sends an email notification for each new anomaly.

| Stage | Input | Output |
| --- | --- | --- |
| Shopping demo | HTTP requests or generated demo traffic | `/logs/shop.jsonl` |
| Filebeat | JSON lines | Kafka topic `application-logs` |
| Logstash | Kafka events | Typed records in `cloud-logs` |
| Trainer | Historical normal records | `/model/model.joblib` |
| Scorer | Model and `cloud-logs` records | Scores and flags in `cloud-anomalies` |
| Kibana / SNS | Indexed events / new anomalies | Dashboard / optional email |

## Start locally or on EC2

Use a Linux x86-64 host with Docker Engine and the Docker Compose v2 plugin. On EC2, an Ubuntu instance with at least 8 GiB RAM and 30 GiB storage is a suitable starting point. From the extracted repository root, run:

```bash
sudo sysctl -w vm.max_map_count=1048576
docker compose run --build --rm trainer
docker compose up -d
./scripts/import-dashboard.sh
python3 scripts/smoke.py
```

The demo uses an unauthenticated Elastic stack bound to localhost. Keep the EC2 security group closed on ports 5601, 9200, 8000, and 9092. For EC2, forward the UI and shopping demo over SSH from your computer:

```bash
ssh -i /path/to/key.pem -L 5601:127.0.0.1:5601 -L 8000:127.0.0.1:8000 ubuntu@YOUR_EC2_HOST
```

Open [Shopping Log Monitoring](http://127.0.0.1:5601/app/dashboards#/view/cloud-log-monitoring). Select a recent time range and refresh after sending traffic. Inspect individual records in Discover with the imported `cloud-logs` and `cloud-anomalies` data views.

```bash
curl -fsS http://127.0.0.1:8000/products
docker compose exec -T app python demo.py emit --normal 100 --anomalies 10
docker compose logs -f scorer
```

The shopping demo responds to GET requests for `/products`, `/cart`, and `/checkout`. It logs request latency, HTTP status, and response size. The batch generator adds a run ID and a synthetic ground-truth flag. Training and scoring use only `latency_ms`, `status`, and `bytes`. The anomaly score is the negative of the Isolation Forest decision function, so positive values are flagged. Both flagged and normal events are stored in `cloud-anomalies`.

The trainer generates 2,000 synthetic normal records by default. For your own normal-only history, place a JSON-lines file containing the three numeric features at `data/normal.jsonl`, then run:

```bash
docker compose run --rm trainer python pipeline.py train --history /historical/normal.jsonl
docker compose restart scorer
```

The new model applies to events as they are first scored. Existing results retain their original scores. The scorer checks the raw index by pages and skips scored IDs. This scan is suitable for a small demonstration.

## Optional SNS email

1. In AWS, create a **Standard** SNS topic and an **Email** subscription. Confirm the subscription from the recipient inbox.
2. Attach an EC2 instance role granting `sns:Publish` only on that topic. If the scorer uses the EC2 instance profile from the Docker bridge, keep IMDSv2 required and set the metadata response hop limit to 2.
3. Copy `.env.example` to `.env`, set `SNS_TOPIC_ARN` and `AWS_DEFAULT_REGION`, then run `docker compose up -d scorer`. Emit a new demo batch.
4. Inspect `alert_state` in the scored index. The scorer writes pending alerts before publishing and retries failures. A crash after publishing but before saving `sent` can cause a duplicate email. Run one scorer replica.

## Push to GitHub

Create an empty GitHub repository and run these commands inside the extracted project folder.

```bash
git init -b main
git add .
git commit -m "Add EC2 log anomaly detection demo"
git remote add origin https://github.com/YOUR_ACCOUNT/YOUR_REPOSITORY.git
git push -u origin main
```

GitHub Actions starts Compose, imports the dashboard, and checks a fresh batch through Elasticsearch and the scorer. It runs without SNS credentials.

Use `docker compose down` to stop the stack while retaining data. `docker compose down --volumes` deletes all demo indices, Kafka data, local logs, Kibana state, and the trained model.
