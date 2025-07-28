# 🌀 Modular Data Ingestion Pipeline using Airflow, S3 & PostgreSQL (Hosted on Render)

## 📌 Project Overview

This project demonstrates a **modular, object-oriented data pipeline** built using **Apache Airflow**, **Python**, **AWS S3**, and a **free PostgreSQL database hosted on Render**. It simulates a real-world ETL scenario where raw JSON files are picked from a local directory, moved to cloud storage (S3), processed, transformed, and then loaded into a cloud-hosted database — all orchestrated through Airflow.

---

## 🚀 Architecture Flow

<img width="2040" height="701" alt="diagram-export-28-07-2025-20_54_21" src="https://github.com/user-attachments/assets/d5956a58-a223-4099-a3ae-f4746fafa993" />

---

## 🛠️ Tech Stack
Python (with OOP design)

Apache Airflow

AWS S3 (via boto3)

PostgreSQL (hosted on Render)

configparser for externalized credentials

Logging module for traceability and monitoring

Airflow UI for DAG scheduling and monitoring

---

### ⚙️ Features
✅ Local file availability check
✅ Upload JSON file to AWS S3
✅ Read from S3 and flatten JSON using Python
✅ Create tables dynamically in PostgreSQL (if not exist)
✅ Transform and load data into Render DB
✅ Airflow DAG to automate the workflow
✅ Configurable credentials via config.ini
✅ Modular, object-oriented codebase
✅ Comprehensive logging using Python loggers and Airflow task logs

---

### 🔄 Data Flow / Pipeline Stages
Data Availability Check
Checks for new raw JSON files in a local directory.
Upload to S3
If available, the file is uploaded to a configured S3 bucket.
Read & Transform
The JSON is read from S3, flattened into tabular format using pandas.
DB Table Check & Creation
The system checks if the destination table exists in the Render PostgreSQL DB; if not, it creates it.
Data Load
Transformed data is inserted into the target table.
Monitoring & Logging
Each task logs its process using Python loggers and can be monitored through Airflow’s UI.

---

### 📅 Airflow DAG Details
Primary Trigger: Scheduled DAG checks for the presence of a new local JSON file.
Flow:
Task 1: Check local file availability
Task 2: Upload file to S3
Task 3: Read file from S3 & transform
Task 4: Check/Create DB Table
Task 5: Load data into DB
Monitoring: All steps monitored through Airflow's UI & task logs
---

### 🔧 Configuration
All sensitive or environment-specific configurations are stored in a config.ini file, including:
AWS credentials
S3 bucket details
Database connection strings

---

### 📂 Project Structure
.
├── dags/
│   └── data_ingestion_dag.py      # Airflow DAG file
├── data_uploads/
│   └── sample_data.json           # Sample raw data
├── src/
│   ├── config/
│   │   └── config.ini             # Credentials and settings
│   ├── s3_utils.py                # Upload & download functions using boto3
│   ├── transform.py               # JSON transformation logic
│   ├── db_handler.py              # DB connection and table handling logic
│   └── logger.py                  # Custom Python logger
├── README.md                      # You are here

---

### 🌐 Deployment Details
Airflow: Locally hosted for DAG orchestration
S3: Used as the intermediate cloud storage
PostgreSQL: Free-tier database hosted on Render
Local Machine: Source for JSON files simulating raw data arrival

---

### 📈 Future Enhancements
Add unit tests and CI/CD integration
Migrate to Dockerized Airflow for cloud deployment
Integrate email alerts or Slack notifications
Add schema validation and error handling layers

---

### 🤝 Let's Connect
This project is part of my portfolio showcasing real-world data engineering skills.
Feel free to explore, clone, or reach out for collaboration or feedback.

📧 [Your Email]
🔗 [LinkedIn Profile]
🌍 [Portfolio or Blog]
