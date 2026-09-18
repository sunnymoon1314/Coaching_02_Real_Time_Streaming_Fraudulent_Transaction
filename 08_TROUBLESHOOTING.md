## <span id="troubleshooting"></span><span style="color:red">🚑 8. Troubleshooting Guide</span> <span style="font-size: 14px; font-weight: normal;">[⬆️ Back to TOC](README.md#toc)</span>

If you run into issues during deployment, check these common solutions:

### 🐳 8.1 Docker & Apache Kafka
- **`Cannot connect to the Docker daemon`**: Docker Desktop is not running. 
  - **MacOS**: Launch the Docker Desktop application from your Applications folder.
  - **Windows (WSL)**: Launch Docker Desktop, and ensure **Settings > Resources > WSL Integration** is enabled for your Linux distribution. Wait for the green icon, then try `docker-compose up -d` again.
- **`Port 9092 is already in use`**: Another service is running on the Kafka port. Find and kill it:
  - **MacOS & WSL Terminal**: 
    ```bash
    lsof -t -i:9092 | xargs kill -9
    ```
- **`NoBrokersAvailable` error in Python scripts**: Your Kafka container crashed or isn't fully booted. Run `docker-compose ps` to verify it's running. Sometimes it takes 15 seconds to fully initialize.

### 📊 8.2 Streamlit & FastAPI Dashboards
- **`[Errno 48] Address already in use` (uvicorn)**: A previous run of the dashboard is still clinging to port 8000. Kill it:
  - **MacOS & WSL Terminal**:
    ```bash
    lsof -t -i:8000 | xargs kill -9
    ```
- **Streamlit page is completely blank**: Ensure you are running the command exactly as `streamlit run scripts/pos_terminal_local.py` from the root directory of the project, not from inside the `scripts/` folder.

### ☁️ 8.3 GCP Pub/Sub & Environment
- **`DefaultCredentialsError` or `PermissionDenied`**: You are not authenticated with GCP on your laptop. Run this in your terminal and log in via the browser:
  ```bash
  gcloud auth application-default login
  ```
- **Transactions failing to publish to Pub/Sub**: Double check that you updated `GCP_PROJECT_ID` inside the `.env` file to match your actual Google Cloud Project ID.

### 🏗️ 8.4 Terraform & Infrastructure
- **`Error 403: Cloud Functions API has not been used`**: New GCP projects have their APIs disabled by default. You can enable them by clicking the link in the terminal error message, or by running:
  ```bash
  gcloud services enable cloudfunctions.googleapis.com cloudbuild.googleapis.com run.googleapis.com artifactregistry.googleapis.com pubsub.googleapis.com eventarc.googleapis.com
  ```
- **`terraform init` fails**: Ensure you are in the correct directory (`cd terraform`).
- **`Required variable not set`**: You forgot to copy the example variables file. Run:
  ```bash
  cp terraform.tfvars.example terraform.tfvars
  ```
  Then, edit the new `terraform.tfvars` file and insert your GCP project details.

### 🔐 8.5 IAM & Permissions
- **Cloud Run function returning 403 Forbidden**: By default, GCP Cloud Run Functions require authentication. If your Pub/Sub push subscription or external test requests cannot trigger the cloud function:
  - **Option A (Make Public for Testing)**: Go to the GCP Console -> Cloud Run -> select your function -> Permissions tab -> Click "Add Principal" -> Type `allUsers` -> Assign the role **Cloud Run Invoker** -> Click Save.
  - **Option B (Keep Secure)**: You must pass an identity token in your HTTP headers. You can generate a temporary token locally by running: `gcloud auth print-identity-token`.
- **Pub/Sub "Permission Denied"**: If running `scripts/pos_terminal_cloud.py` throws a Pub/Sub permissions error locally, your authenticated GCP user account lacks the correct roles. Go to the GCP Console -> IAM & Admin -> find your email address -> Edit -> Ensure you have both the **Pub/Sub Publisher** and **Pub/Sub Subscriber** roles assigned.


### <span id="8-6-networking"></span>🛜 8.6 Collaboration & Networking
- **Learners cannot access my IP address (`http://192.168.x.x:8000`)**: Corporate and classroom Wi-Fi networks often have **"Client Isolation" (AP Isolation)** enabled for security. This prevents devices on the same Wi-Fi from talking to each other directly.
  - **Solution A (Ngrok)**: The host can install [ngrok](https://ngrok.com/) and run `ngrok http 8000` in their terminal. This creates a public internet URL (e.g., `https://xyz.ngrok-free.app`) that routes safely to your local machine, completely bypassing local Wi-Fi restrictions!
  - **Solution B (Mobile Hotspot)**: Have the host create a mobile Wi-Fi hotspot from their phone, and have the learners connect to that hotspot. Mobile hotspots generally do not have Client Isolation enabled.
  - **Solution C (Cloud First)**: Skip Stage 1 Collaboration and proceed directly to **Stage 2 (Enterprise Cloud Deployment)**. Once deployed to GCP, learners can hit the Cloud Run and Pub/Sub endpoints over the public internet, avoiding local network restrictions entirely.

---
