# ⚡ Real-Time Streaming Pipelines (Fraud Transaction Detection)

This guide immerses you in the high-stakes world of **Real-Time Data Streaming**. You will build a machine learning pipeline that ingests credit card transactions as they happen and flags fraudulent activity instantly. 

The project is split into two powerful stages:
1. **Local Development:** Build the foundation using **Apache Kafka** to process streams natively on your machine.
2. **Enterprise Cloud Deployment:** Translate your architecture into a fully managed, serverless cloud environment using **Google Cloud Pub/Sub** and **Cloud Run Functions** via Terraform.


### 🚨 The Problem
Credit card fraud must be detected before the transaction settles. A scheduled batch job at midnight is useless if the thief has already walked out of the store with the TV.

### 💡 The Solution
An event-driven architecture using a Message Broker.

---

## <span id="toc"></span>📑 Table Of Contents (TOC)

- [0. Pre-requisite Software](#prerequisites)
- [1. Architecture Overview](#arch-overview)
- [2. Directory Structure](#folder-structure)
- [3. Stage 1: Local Development (Apache Kafka)](#stage-1)
- [4. Stage 2: Enterprise Cloud Deployment (GCP Serverless)](#stage-2)
- [5. Collaboration Mode (Host the Bank HQ)](#collaboration)
- [6. Clean-Up](#clean-up)
- [7. Inspecting Message Queues](07_INSPECTING.md)
- [8. Troubleshooting Guide](08_TROUBLESHOOTING.md)
- [9. Frequently Asked Questions (FAQ)](09_FAQ.md)
- [Appendix A: High-Performance Streaming with Redpanda](A_APPENDIX_A.md)
- [Appendix B: Organization Simulation Setup](B_SIMULATION.md)

---

## <span id="prerequisites"></span><span style="color:red">📦 0. Pre-requisite Software</span> <span style="font-size: 14px; font-weight: normal;">[⬆️ Back to TOC](#toc)</span>

### 🛠️ 0.1 Pre-requisite Software

**0.1.1 For Stage 1 (Local Apache Kafka):**
*   **Docker Desktop:** Required to run the Kafka broker locally.
    *   Mac/Windows: Download and install from [Docker's official website](https://www.docker.com/products/docker-desktop).

**0.1.2 For Stage 2 (GCP Cloud Deployment):**
*   **Google Cloud Account:** An active Google Cloud Platform (GCP) account with a new Project created and Billing enabled.
*   **Google Cloud CLI (`gcloud`):** Required to authenticate your local machine with GCP.
    *   Install Guide: [Google Cloud CLI Docs](https://cloud.google.com/sdk/docs/install)
*   **Terraform:** Required to deploy the cloud infrastructure as code.
    *   Download: [Terraform Official Download](https://developer.hashicorp.com/terraform/install)
    *   MacOS (via Homebrew):
        ```bash
        brew tap hashicorp/tap
        
        brew install hashicorp/tap/terraform
        ```
    *   **🪟 Windows Users (Easiest Method):** If you download the raw `.zip` from the website, extract `terraform.exe` and place it directly inside the `terraform/` folder of this project. When executing Terraform commands, use `./terraform.exe` instead of `terraform`.

**0.1.3 Verification:**
To verify that all tools are successfully installed, run the following commands in your terminal:
```bash
docker --version
gcloud --version
# Mac/WSL
terraform --version

# Windows
./terraform.exe --version
conda --version
```
If the software is installed correctly, these commands will output the currently installed version numbers.

![alt text](images/check_versions_terminal.png)

### 🐍 0.2 Python Environment

Before starting the pipeline, set up your Python environment using Conda. This ensures all necessary libraries (Pandas, XGBoost, Kafka-Python, and Google Cloud Pub/Sub) are installed correctly.

**Instruction:**
- Run these commands in your terminal:

```bash
cd <your_stream_fraud_detection_project_path>

conda env create -f stream-fraud-detection.yml

conda activate stream-fraud-detection
```

![alt text](images/conda_env_create.png)

![alt text](images/conda_activate.png)

---

## <span id="arch-overview"></span><span style="color:red">🕒 1. Architecture Overview</span> <span style="font-size: 14px; font-weight: normal;">[⬆️ Back to TOC](#toc)</span>

![Architecture Diagram Local](images/architecture_local.jpeg)
Architecture flow for local deployment. Apache Kafka is deployed via Docker, and the XGBoost model inference is executed continuously by a local Python consumer script (consumer_local.py).

![Architecture Diagram Cloud](images/architecture_cloud.png)
Architecture flow for cloud deployment. Kafka is replaced by Google Cloud Pub/Sub, and the XGBoost model inference is deployed as a serverless Google Cloud Run Function.

### 📝 Implementation Steps
To successfully implement this project, you will perform the following steps:
1. Start the Apache Kafka Message Broker locally via Docker.
2. Train the XGBoost fraud detection model on historical transaction data.
3. Launch the Consumer, API Backend, and Point-of-Sale (POS) Terminal to simulate real-time transactions.
4. Provision the production cloud infrastructure using Terraform (Google Cloud Pub/Sub, Cloud Run Functions, Cloud Storage).
5. Seamlessly migrate the local streaming architecture to the highly available, serverless GCP environment.

---

## <span id="folder-structure"></span><span style="color:red">📂 2. Directory Structure</span> <span style="font-size: 14px; font-weight: normal;">[⬆️ Back to TOC](#toc)</span>

Here is a quick map of the project files to help you navigate:

```text
Coaching_02_Real_Time_Streaming_Fraudulent_Transaction/
├── cloud_function/
│   ├── fraud_model.joblib
│   ├── main.py
│   └── requirements.txt
├── dashboard/
│   ├── app.js
│   ├── index.html
│   └── style.css
├── data/
│   ├── fraudTest.csv
│   ├── fraudTrain.csv
│   └── README.txt
├── images/
│   ├── architecture_cloud.png
│   ├── architecture_local.jpeg
│   └── architecture_simulation.jpeg
├── scripts/
│   ├── api_cloud.py
│   ├── api_local.py
│   ├── consumer_local.py
│   ├── pos_terminal_cloud.py
│   ├── pos_terminal_local.py
│   ├── producer_cloud.py
│   ├── producer_local.py
│   └── train_model.py
├── terraform/
│   ├── cloud_function.zip
│   ├── function.tf
│   ├── iam.tf
│   ├── provider.tf
│   ├── pubsub.tf
│   ├── storage.tf
│   ├── terraform.tfvars
│   ├── terraform.tfvars.example
│   └── variables.tf
├── .env.example
├── .gitignore
├── 07_INSPECTING.md
├── 08_TROUBLESHOOTING.md
├── 09_FAQ.md
├── A_APPENDIX_A.md
├── B_SIMULATION.md
├── docker-compose-redpanda.yml
├── docker-compose.yml
├── Dockerfile
├── README.md
├── requirements.txt
└── stream-fraud-detection.yml
```

---

## <span id="stage-1"></span><span style="color:red">💻 3. Stage 1: Local Development (Apache Kafka)</span> <span style="font-size: 14px; font-weight: normal;">[⬆️ Back to TOC](#toc)</span>

### 🛠️ 3.1 Start Apache Kafka

**Objective:** Launch the enterprise-standard message broker.

We are using Kafka in **KRaft mode**, which runs as a single self-contained broker without needing external dependencies like Zookeeper.

**Instruction:** Start the Kafka broker using Docker Compose:
```bash
docker-compose up -d
```
💡 **Tip:**
Ensure you have Docker installed and running on your system before proceeding. You can download it from [Docker's official website](https://www.docker.com/products/docker-desktop).

![alt text](images/docker_running_mac_wsl.png)

Ensure Docker Desktop is running in Mac environment.

![alt text](images/docker_running_windows.png)

Ensure Docker Desktop is running in Windows/WSL environment.

![alt text](images/windows_desktop_settings_resources_wsl_integration.png)

Additional check for WSL users: Make sure the **Enabled integration with my default WSL distro** setting in Settings/Resources/WSL Integration page is checked and the Ubuntu distribution is enabled.

![alt text](images/docker-compose-up-kafka.png)
Run `docker-compose` to start the Kafka broker.

---

### 🧠 3.2 Train the ML Model

**Objective:** Train an XGBoost model on the historical data.

Please refer to the FAQ section 8.4 Fast Data & Fraud Machine Learning for the reason we chose XGBoost for this project.

📝 **Note:**
You will need to download `fraudTrain.csv` and `fraudTest.csv` from Kaggle and place them in the `data/` folder before training!
Link: [Kaggle Fraud Detection Dataset](https://www.kaggle.com/datasets/kartik2112/fraud-detection)

Open and review `scripts/train_model.py`. 

**Instruction:** Run the training script:
```bash
python scripts/train_model.py
```
Please wait for 1 to 2 minutes for this command to complete. This will generate `fraud_model.joblib`. This is the "brain" your streaming consumer will use.

📝 **Note:**
The very first time you run this script, it might take a moment to download the `fraud_model.joblib` artifact from Kafka.

![alt text](images/train_model.png)
After training, you will find the model file (fraud_model.joblib) in the current project folder.

---

### 📤 3.3 Start the Real-Time Pipeline

**Objective:** Simulate the real-time payment gateway and the internal fraud dashboard.

You will need **three separate terminal windows** for this step to see the full architecture in action.

#### 🖥️ 3.3.1 Terminal 1 (The Consumer - Local):
This script listens to the Kafka **transactions** topic. It will sit idle until transactions arrive, at which point it runs them through the XGBoost model instantly.
```bash
python scripts/consumer_local.py
```

![alt text](images/consumer_local.png)
This terminal will monitor any new messages in the Kafka **transactions** topic. It will also print any messages being consumed and predicted label of the transaction.

#### 🏦 3.3.2 Terminal 2 (The Bank's Modern Fraud Detection Dashboard - Local):
This is the FastAPI backend and web dashboard for the Fraud Analysts. It listens for alerts and serves a beautiful User Interface.
```bash
uvicorn scripts.api_local:app --host 0.0.0.0 --reload
```

![alt text](images/uvicorn_local_localhost_8000.png)

Once running, open your browser to **http://localhost:8000** for the dashboard, and keep it visible on your screen. It will say "Listening for Fraud Alerts".

#### 🛒 3.3.3 Terminal 3 (The Point of Sale Terminal - Local):
This acts as the merchant's credit card machine. It will pump transactions into Kafka.
```bash
streamlit run scripts/pos_terminal_local.py
```

![alt text](images/pos_terminal_local_terminal_8501.png)

![alt text](images/pos_terminal_local_streamlit_8501.png)
The Streamlit application is now accessible in the browser via (http://localhost:8501). Arrange it side-by-side with your Dashboard. Click **"Start Transactions"** on the POS Terminal, and watch the fraud alerts flow into the web dashboard in real-time!

💡 **Tip:**
Multi-Store Simulation: Streamlit's default port is 8501. You can simulate multiple different stores simultaneously by opening new terminal windows and running the POS Terminal on different ports! When running multiple Streamlit terminals, it is best practice to just increment sequentially from the default: 8501, 8502, 8503, 8504, etc.

```bash
streamlit run scripts/pos_terminal_local.py --server.port 8502
```

![alt text](images/pos_terminal_local_terminal_8502.png)

![alt text](images/pos_terminal_local_streamlit_8502.png)
The second Streamlit application is now accessible in the browser via (http://localhost:8502).

📝 **Note for Team Simulations (Multiple Laptops):**
If you are doing the team simulation on different computers, your scripts need to know how to find the central Data Engineer's laptop!
1. Open your `.env` file.
2. Change `KAFKA_BROKER="localhost:9092"` to use the Data Engineer's IP address (for example: `KAFKA_BROKER="192.168.1.15:9092"`).
(Data Engineers: Remember to update your `docker-compose.yml` file with your IP address before starting Kafka, as outlined in the [B_SIMULATION.md](B_SIMULATION.md) guide!)

![alt text](images/fraud_detection_dashboard_localhost_8000.png)

![alt text](images/pos_terminal_local_streamlit_8501_start.png)
In the Streamlit application, click the "Start Transactions" button to start generating transactions.

![alt text](images/fraud_detection_dashboard_localhost_8000_auto.png)
In the Fraud Detection Dashboard, you will see that the transactions are being processed in real-time and automatically flagged as WHITELISTED OR FREEZE ACCOUNT.

![alt text](images/fraud_detection_dashboard_localhost_8000_manual.png)
You can also manually flag transactions as WHITELISTED OR FREEZE ACCOUNT if you disagree with the model prediction by clicking the **Override & Whitelist** or **Freeze Account** button.

### 🛑 3.4 Stop Local Pipeline (Crossing over to Cloud)

Before proceeding to the Enterprise Cloud Deployment, you need to free up your terminal windows and stop the local Python processes so they do not conflict with the cloud versions.

**Instruction:** Go back to the 3 terminal windows currently running your local Consumer, POS Terminal, and Fraud Detection Dashboard. Press `Ctrl+C` (Windows) or `Control+C` (Mac) in each terminal to stop the running Python processes. 

📝 **Note:**
For Mac users: Do not use `Cmd+C`, as that only copies text. You must use the `Control` key to stop the terminal. You can leave the Kafka Docker container running for now.

---

---

## <span id="stage-2"></span><span style="color:red">☁️ 4. Stage 2: Enterprise Cloud Deployment (GCP Serverless)</span> <span style="font-size: 14px; font-weight: normal;">[⬆️ Back to TOC](#toc)</span>

**Objective:** Understand how to deploy this real-time fraud detection pipeline (the streaming message broker and the ML inference consumer) into the cloud using Infrastructure as Code (Terraform) and Serverless Computing (Cloud Run Function of GCP).

In the real world, running Kafka locally does not scale. We will replace Apache Kafka with **Google Cloud Pub/Sub** and our local consumer script with a **Cloud Run Function**.

### 📦 4.1 Prepare the Deployment Package

First, copy your trained model into the `cloud_function` directory so Terraform can package it:
```bash
cd <your_stream_fraud_detection_project_path>

cp fraud_model.joblib cloud_function/
```

![alt text](images/copy_fraud_model_joblib.png)

### ⚙️ 4.2 Configure your GCP Environment

Create an .env file from the environment variables template file:

```bash
cp .env.example .env
```

Open `.env` (with TextEdit, Notepad, VS Code, or any text editor) and update `GCP_PROJECT_ID` to your GCP project ID.


![alt text](images/copy_env_example_terminal.png)

![alt text](images/nano_env.png)
Save the changes made. For nano in Mac, press `Ctrl-X` followed by `Y` key and then `Enter` key.

Next, navigate to the `terraform` directory and copy the example variables file:
```bash
cd terraform

cp terraform.tfvars.example terraform.tfvars
```

![alt text](images/copy_terraform_tfvars_terminal.png)

Open `terraform.tfvars` (with TextEdit, Notepad, VS Code, or any text editor) and update your GCP Project ID there as well.

![alt text](images/nano_terraform_tfvars_terminal.png)

![alt text](images/nano_terraform_tfvars.png)
Save the changes made. For nano in Mac, press `Ctrl-X` followed by `Y` key and then `Enter` key.

### 🚀 4.3 Deploy the Infrastructure

Before deploying, you must authenticate your terminal with Google Cloud so Terraform has permission to create resources on your behalf. Run these commands:
```bash
# Log in to Google Cloud
gcloud auth login

# Set your active project
gcloud config set project <YOUR_GCP_PROJECT_ID>

# Authenticate Terraform to use your Google Cloud credentials
gcloud auth application-default login
```
![alt text](images/gcloud_auth_login_terminal.png)
Running the `gcloud auth login` will open a browser window to authenticate your terminal with Google Cloud. This is to let you use gcloud CLI commands to access resources in Google Cloud.

![alt text](images/gcloud_config_set_project_terminal.png)
Running the `gcloud config set project <YOUR_GCP_PROJECT_ID>` will set the project you want to use when accessing Google Cloud.

![alt text](images/gcloud_auth_application_default_login_terminal.png)
Running the `gcloud auth application-default login` will authenticate Terraform to use your Google Cloud credentials.

Next, ensure the required Google Cloud APIs are enabled for your project. Run this command and wait 1-2 minutes for it to complete:
```bash
gcloud services enable cloudfunctions.googleapis.com cloudbuild.googleapis.com run.googleapis.com artifactregistry.googleapis.com pubsub.googleapis.com eventarc.googleapis.com
```

📝 **Note:**
Not all APIs may be enabled by default in your GCP project for security and billing reasons, so you may need to enable them first.

![alt text](images/gcloud_enable_api_terminal.png)

Run the following commands to provision the required resources (such as Pub/Sub topic, Cloud Run Function, etc) in GCP:
```bash
# Mac/WSL
terraform init

# Windows
./terraform.exe init
```

![alt text](images/terraform_init_terminal.png)

```bash
# Mac/WSL
terraform apply

# Windows
./terraform.exe apply
```

![alt text](images/terraform_apply_1_terminal.png)
The output of the `terraform apply` command is very long. This is just the first portion of it.

![alt text](images/terraform_apply_2_terminal.png)
Review the output of `terraform apply` and type `yes` and then `Enter` to deploy!

![alt text](images/terraform_apply_3_terminal.png)
The last line (green colour texts) of the output of the `terraform apply` command tells you the status of the deployment: there are 12 resources deployed to GCP using terraform.

### 🚀 4.4 Launch the POS Terminal & Cloud Dashboard

To test your newly deployed Cloud Run Function, you will run the dedicated Stage 2 Cloud scripts!

Ensure you have already pressed `Ctrl+C` (or `Control+C` on Mac) in your terminal windows to stop the local scripts from Stage 1.

💡 **Optional:** You can also shut down your local Kafka broker to free up memory by running `docker-compose down`.

Because we set up the `.env` file earlier, our scripts will automatically load your `GCP_PROJECT_ID`. You will only need **two terminal windows** for this stage. Just start the cloud-native interfaces:

#### 🏦 4.4.1 Terminal 1 (The Bank's Modern Fraud Detection Dashboard - Cloud):
```bash
uvicorn scripts.api_cloud:app --host 0.0.0.0 --reload
```

![alt text](images/uvicorn_cloud_localhost_8000.png)

Once running, open your browser to **http://localhost:8000** for the dashboard, and keep it visible on your screen.

#### 🛒 4.4.2 Terminal 2 (The Point of Sale Terminal - Cloud):
```bash
streamlit run scripts/pos_terminal_cloud.py
```

![alt text](images/pos_terminal_cloud_terminal_8501.png)

![alt text](images/pos_terminal_cloud_streamlit_8501.png)
Remember to click the **Start Transactions** button to start the event streaming.

💡 **Tip:**
Multi-Store Simulation: You can simulate multiple different stores simultaneously by opening new terminal windows and running the POS on different ports!
```bash
streamlit run scripts/pos_terminal_cloud.py --server.port 8502
```

![alt text](images/pos_terminal_cloud_terminal_8502.png)

![alt text](images/pos_terminal_cloud_streamlit_8502.png)

![alt text](images/fraud_detection_dashboard_cloud_localhost_8000_auto.png)
Fraud Detection Dashboard running in Auto-Mode. Transactions are auto-flagged as **FROZEN ACCOUNT** if the ML Confidence score is at least 70% (Default threshold set in the Dashboard Auto-Mode Settings).

![alt text](images/fraud_detection_dashboard_cloud_localhost_8000_manual.png)
Fraud Detection Dashboard running in Manual-Mode. Transactions have to be manually investigated if the ML Model prediction is not confident (less than 70%).

![alt text](images/fraud_detection_dashboard_cloud_localhost_8000_analytics.png)
Click the **Analytics** button in the left-hand menu to view the analytics and insights: which tells us how much time is taken from the time the transaction event is generated at POS terminal to when it is either approved or declined by the fraud detection system.

![alt text](images/fraud_detection_dashboard_cloud_localhost_8000_auto_settings.png)
Click the **Auto-Mode Settings** in the left-hand menu to set the threshold for the ML model confidence score. Fraud Detection Dashboard running in Auto-Mode will automatically flag the transaction as **FROZEN ACCOUNT** if the ML model confidence score is greater than or equal to the threshold (currently set to 70%). Otherwise, the transaction is auto-flagged as **WHITELISTED**.

⚠️ **Important:**
Where is the Cloud Consumer Script?
Notice that there is no consumer script to run for Stage 2! That is because the **Cloud Run Function** you just deployed via Terraform is now acting as your consumer. It automatically triggers on Pub/Sub messages and runs the ML model serverlessly in the cloud!

![alt text](images/gcp_cloud_run_functions_overview.png)
**💡 How to view your Cloud Consumer logs:**
1. In the GCP Console, type **"Cloud Run Functions"** in the top search bar and click on the service.
2. On the Overview page, click the name of the function you just deployed.
3. Navigate to the **Logs** tab to watch it process transactions in real-time!

![alt text](images/gcp_cloud_run_functions_log.png)

---

---

## <span id="collaboration"></span><span style="color:red">🤝 5. Collaboration Mode (Host the Bank HQ)</span> <span style="font-size: 14px; font-weight: normal;">[⬆️ Back to TOC](#toc)</span>

Want to make this interactive? You can turn your computer into the central "Bank HQ" and invite your colleagues to help you catch fraud in real-time!

**How it Works (The Bank HQ Concept)**
Instead of everyone running their own separate dashboards, you can host a single master dashboard. When you share your local network IP address with your team, anyone can open their browser and instantly see your live fraud alerts. If a colleague clicks "Freeze" on their phone or laptop, that command is instantly sent back to your computer and flashes on your Point of Sale (POS) terminal!

📝 **Note:**
To expand this into a structured exercise across a larger team, refer to Section 4.1 below.

**Step-by-Step Setup:**

1. **Find your Local IP Address:**
   First, you need to find out what your computer's "IP address" is on the local network (e.g. `192.168.10.111`).
   - **Mac:** Open your terminal and type: `ipconfig getifaddr en0`

   ![MacOS ipconfig](images/macos_ipconfig.png)

   - **Windows:** Open the **Command Prompt** terminal and type `ipconfig`. Look for the "IPv4 Address" under your active Wi-Fi connection.

   ![alt text](images/windows_ipconfig_ipv4.png)

2. **Start the Central Web Server (FastAPI):**
   Start your FastAPI web server just like before, but this time we will add `--host 0.0.0.0` to the command. This special flag tells your computer to "open the doors" and allow incoming connections from your local network.
   * **For Stage 1 (Local Kafka):**
     ```bash
     uvicorn scripts.api_local:app --host 0.0.0.0 --port 8000
     ```
   * **For Stage 2 (Cloud Pub/Sub):**
     ```bash
     uvicorn scripts.api_cloud:app --host 0.0.0.0 --port 8000
     ```

3. **Invite Your Team:**
   Tell your team members to connect to the same Wi-Fi network as you. Then, have them open their web browsers and type your IP address followed by `:8000`.
   Example: `http://192.168.10.111:8000`

That's it! Watch the transactions flow in and let your team help you resolve them! (If your team cannot connect, the local network might have a security block. See [Section 7.6 in the Troubleshooting Guide](07_TROUBLESHOOTING.md#7-6-networking) for easy workarounds!)

### 🧑‍🤝‍🧑 5.1 Organization Simulation Setup (Optional)

💡 **Tip:**
If you have extra time and a large enough team, you can expand this basic collaboration into a fully structured simulation by assigning specific "Roles" (Data Engineer, Retail Merchant, ML Engineer, Fraud Analyst) to different team members!
**[👉 Click here to view the Full Organization Simulation Guide & Architecture Chart!](B_SIMULATION.md)**

---

---

## <span id="clean-up"></span><span style="color:red">🏁 6. Clean-Up</span> <span style="font-size: 14px; font-weight: normal;">[⬆️ Back to TOC](#toc)</span>

**Cleanup:** When you are finished, clean up both your local and cloud environments to free up memory and avoid GCP charges:

1. **Local Cleanup:** Shut down your Kafka broker:
```bash
docker-compose down
```

![alt text](images/docker-compose-down-kafka.png)

2. **Cloud Cleanup:** Destroy your GCP infrastructure:
```bash
cd terraform

# Mac/WSL
terraform destroy

# Windows
./terraform.exe destroy
```

![alt text](images/terraform_destroy_1_terminal.png)

![alt text](images/terraform_destroy_2_terminal.png)

![alt text](images/terraform_destroy_3_terminal.png)


