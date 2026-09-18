## <span id="appendix-a"></span><span style="color:orange">🧑‍🤝‍🧑 Appendix B: Organization Simulation Setup</span> <span style="font-size: 14px; font-weight: normal;">[⬆️ Back to TOC](README.md#toc)</span>

📝 Note: This roleplay is specifically designed for the **Stage 1 (Local Kafka)** architecture, as it provides the most hands-on terminal execution for every participant. If you wish to run the simulation using the **Stage 2 (Cloud)** architecture, see the adaptation guide at the bottom of this document.

Because of the decoupled event-driven architecture, every collaborator can join a massive live simulation. Collaborators configure their `.env` file to point to the Data Engineer's IP address (`BROKER_HOST_IP="[DATA_ENGINEER_IP]"` and `KAFKA_BROKER="[DATA_ENGINEER_IP]:9092"`).

Here is the role breakdown for the simulation:

## ⚙️ Role A: The Data Engineer
* **Configuration:** Find your local network IP address and broadcast it to the team.
  * **Mac/WSL:** Run `ipconfig getifaddr en0` (or `en1`) in Terminal.
  * **Windows:** Run `ipconfig` in Command Prompt and look for "IPv4 Address".
  * **Critical Step:** Open `docker-compose.yml` and change `localhost` in `KAFKA_CFG_ADVERTISED_LISTENERS` to your actual IP address (e.g., `PLAINTEXT://192.168.1.15:9092`).
* **Execution:** Run `docker-compose up -d`
* **Observation:** The headless Kafka broker runs in the background, routing thousands of messages across the network.

## 🛍️ Role B: The Retail Merchant POS Terminals (Any Number of Collaborators)
* **Configuration:** Set `.env` to `BROKER_HOST_IP="192.168.1.15"` and `KAFKA_BROKER="192.168.1.15:9092"`
* **Execution:** Run `streamlit run scripts/pos_terminal_local.py`
* **Observation:** Navigate a browser to `http://localhost:8501` to view the POS Terminal UI. Collaborators click "Start Transactions" to generate simulated retail point-of-sale swipes.

## 🧠 Role C: The ML Engineer (Strictly 1 Collaborator)
* **Configuration:** Set `.env` to `BROKER_HOST_IP="192.168.1.15"` and `KAFKA_BROKER="192.168.1.15:9092"`
* **Execution:** Run `python -u scripts/consumer_local.py`
* **Observation:** A scrolling terminal logs the incoming transactions evaluated by the XGBoost model. (Note: using `-u` prevents Windows from buffering the terminal output).

## 🕵️ Role D: The Fraud Analysts (Any Number of Collaborators)
* **Configuration:** Set `.env` to `BROKER_HOST_IP="192.168.1.15"` and `KAFKA_BROKER="192.168.1.15:9092"`
* **Execution:** Run `uvicorn scripts.api_local:app --host 0.0.0.0 --reload`
* **Observation:** Navigate a browser to `http://localhost:8000` to view the interactive dashboard, monitor global alerts in real-time, and execute "Freeze" commands on suspicious accounts.

---

# 🌊 System Architecture Chart

![Simulation Architecture Diagram](images/architecture_simulation.jpeg)

---

# ☁️ Adapting for Stage 2 (Enterprise Cloud Deployment)

If you have already deployed the project to GCP using Terraform, you can still run the organizational simulation! However, because the Cloud architecture uses fully managed serverless components, the roles change significantly:

### ⚙️ Role A (Data Engineer)
No longer needs to run Docker. Instead, they must ensure the GCP resources are deployed via Terraform. The Data Engineer must also ensure that all collaborators' Google accounts are granted the necessary IAM permissions in the GCP project by running the following commands (replace `collaborator@gmail.com` and `YOUR_GCP_PROJECT_ID`):

```bash
# Grant them permission to publish transactions and resolutions
gcloud projects add-iam-policy-binding YOUR_GCP_PROJECT_ID \
    --member="user:collaborator@gmail.com" \
    --role="roles/pubsub.publisher"

# Grant them permission to subscribe to fraud alerts
gcloud projects add-iam-policy-binding YOUR_GCP_PROJECT_ID \
    --member="user:collaborator@gmail.com" \
    --role="roles/pubsub.subscriber"
```

### 🛍️ Role B (Retail Merchants)
Ensure you have authenticated by running `gcloud auth application-default login`, then run `streamlit run scripts/pos_terminal_cloud.py`. Your local script will automatically authenticate and push data directly into the Data Engineer's Google Cloud Pub/Sub!

### 🧠 Role C (ML Engineers)
**Automated by Cloud Functions!** The ML Engineers no longer run a local consumer script. The serverless GCP Cloud Run Function automatically scales to handle all inference. The Data Engineer (Role A) can screen-share the live Cloud Logs from their GCP Console so the ML Engineers can watch the cloud inference in real-time.

### 🕵️ Role D (Fraud Analysts)
Ensure you have authenticated by running `gcloud auth application-default login`, then run `uvicorn scripts.api_cloud:app --host 0.0.0.0 --reload`. Once running, navigate a browser to `http://localhost:8000` to view the dashboard!
