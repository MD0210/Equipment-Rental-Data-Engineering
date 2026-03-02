🚀 Equipment Rental Data Engineering Pipeline

Medallion Architecture + Custom Metadata-Driven Orchestrator

📌 Project Overview

This project implements a production-style data engineering pipeline using the Medallion Architecture (Bronze → Silver → Gold) combined with a custom-built metadata-driven Pipeline Manager.

The solution simulates a real-world equipment rental company facing:

Inconsistent reporting

Revenue reconciliation issues

Data quality problems

Multi-table ingestion requirements

Instead of hardcoding logic, the pipeline is fully driven by metadata tables (source, schedule, batch, task) stored in a SQLite orchestration database.

🏗 Architecture
🔹 Data Flow (Medallion)
Source → Bronze → Silver → Gold

Bronze → Raw ingestion

Silver → Data validation & cleansing

Gold → Aggregation & reporting-ready outputs

🔹 Orchestration Flow (Metadata-Driven)
Source
   ↓
Schedule (unique)
   ↓
Batch (per table)
   ↓
Task (per process step)

Each pipeline execution tracks:

Task status (running / success / failed)

Start & end timestamps

Duration (seconds)

Error messages

Next scheduled run time (auto-calculated)

✨ Key Features

✅ Interactive runtime input

✅ Unique schedule enforcement

✅ Multiple batches under one schedule

✅ Task-level duration tracking

✅ Automatic next_run_ts calculation

✅ Data quality validation

✅ Structured artifact storage

✅ Failure handling & logging

✅ Rerunnable batch design

▶ How to Run
1️⃣ Clone the Repository
git clone <your-repo-url>
cd Equipment-Rental-Data-Engineering
2️⃣ Create Virtual Environment
python -m venv venv
venv\Scripts\activate
3️⃣ Install Dependencies
pip install pandas openpyxl
4️⃣ Execute Pipeline
python main.py

You will be prompted for:

Enter tables (comma-separated)
Enter source name
Enter source type (excel/db/api)
Enter file path or connection string
Enter schedule name
Enter run timestamp
Enter timezone
Enter frequency (daily/weekly/hourly/monthly/manual)
Enter priority number
Enter active flag
Enter batch type (full/incremental)
📂 Output Structure
artifacts/
│
├── bronze/
├── silver/
├── gold/
└── pipeline_manager/
    └── pipeline_manager.db
🧠 Key Design Assumptions

schedule_name must be unique.

One schedule can contain multiple batches.

Each table is processed as an independent batch.

Bronze layer stores raw data.

Silver enforces validation and cleansing.

Gold prepares aggregated reporting tables.

next_run_ts is automatically calculated using:

daily → +1 day

weekly → +7 days

hourly → +1 hour

monthly → +30 days

🔍 Data Quality Issues Identified

During analysis of the Equipment Hire dataset:

1️⃣ Missing EndDate

Causes incorrect rental duration

Flagged during Silver validation

2️⃣ Negative Revenue

Financial reconciliation risk

Rejected or flagged

3️⃣ Inconsistent Date Formats

Standardized in Silver transformation

4️⃣ Duplicate Transactions

Deduplicated in Silver

5️⃣ Referential Integrity Issues

Orphan CustomerID / EquipmentID detected

⚙ Technology Stack

Python 3.9+

Pandas

SQLite (metadata store)

Logging module

OpenPyXL (Excel ingestion)

📊 Why This Project Is Production-Oriented

Unlike simple ETL scripts, this solution:

Separates orchestration from transformation

Uses metadata-driven execution

Tracks execution metrics

Prevents duplicate schedules

Supports multi-table batch execution

Enables reruns without recreating metadata

This design mirrors patterns used in:

Azure Data Factory

Microsoft Fabric Pipelines

Apache Airflow

Enterprise ETL systems

🚀 Future Improvements

Selective rerun of failed tasks

SLA monitoring

Email alerts on failure

DAG dependency management

Migration to Azure SQL / Fabric Lakehouse metadata

CI/CD integration