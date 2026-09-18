## <span id="inspecting"></span><span style="color:red">🕵️ 7. Inspecting Message Queues</span> <span style="font-size: 14px; font-weight: normal;">[⬆️ Back to TOC](README.md#toc)</span>

If you want to look "under the hood" and see the raw JSON messages flowing through your streams, you can use these commands to peek at the events:

### 🖥️ 7.1 For Local Kafka
You can use the built-in Kafka console consumer inside your Docker container to read messages directly off the topics:
```bash
# View raw transactions
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning

# View ML fraud alerts
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fraud_alerts --from-beginning

# View bank analyst resolutions
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fraud_resolutions --from-beginning
```

### ☁️ 7.2 For Cloud Pub/Sub
You can use the Google Cloud CLI (`gcloud`) to pull messages from your subscriptions. By omitting the `--auto-ack` flag, the CLI will let you view the message without deleting it, allowing the Python dashboard to still process it a few seconds later once the lock expires!
```bash
# View ML fraud alerts
gcloud pubsub subscriptions pull fraud-alerts-sub --limit=5

# View bank analyst resolutions
gcloud pubsub subscriptions pull fraud-resolutions-sub --limit=5
```

### 🔍 7.3 Inspecting Cloud Function Logs
If you want to watch your deployed ML model grade every transaction in real-time, you can view the live serverless logs:

**Option A: Using the Terminal (CLI)**
```bash
gcloud functions logs read fraud_inference_function --region=us-central1 --limit=50
```

**Option B: Using the GCP Console (Web)**
1. Navigate to [console.cloud.google.com](https://console.cloud.google.com).
2. Search for **Cloud Run Functions** in the top search bar.
3. Click on your `fraud_inference_function`.
4. Click on the **Logs** tab to see a live streaming tail of every `✅ Approved` or `🚨 FRAUD ALERT!` event.


---

