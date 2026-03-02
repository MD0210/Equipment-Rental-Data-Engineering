# 🚀 Equipment Rental Data Engineering Pipeline  
### Medallion Architecture + Metadata-Driven Orchestrator

---

## 📌 Project Overview

This project implements a production-style **Data Engineering Pipeline** using the **Medallion Architecture (Bronze → Silver → Gold)** combined with a custom-built **metadata-driven Pipeline Manager**.

The solution simulates a real-world equipment rental company facing:

- Inconsistent reporting  
- Revenue reconciliation issues  
- Data quality problems  
- Multi-table ingestion requirements  

Instead of hardcoding logic, the pipeline execution is fully driven by metadata tables (`source`, `schedule`, `batch`, `task`) stored in a SQLite orchestration database.

---

## 🏗 Architecture

### 🔹 Data Flow (Medallion)
# 🚀 Equipment Rental Data Engineering Pipeline  
### Medallion Architecture + Metadata-Driven Orchestrator

---

## 📌 Project Overview

This project implements a production-style **Data Engineering Pipeline** using the **Medallion Architecture (Bronze → Silver → Gold)** combined with a custom-built **metadata-driven Pipeline Manager**.

The solution simulates a real-world equipment rental company facing:

- Inconsistent reporting  
- Revenue reconciliation issues  
- Data quality problems  
- Multi-table ingestion requirements  

Instead of hardcoding logic, the pipeline execution is fully driven by metadata tables (`source`, `schedule`, `batch`, `task`) stored in a SQLite orchestration database.

---

## 🏗 Architecture

### 🔹 Data Flow (Medallion)
Source → Bronze → Silver → Gold

- **Bronze** → Raw ingestion layer (no transformations)
- **Silver** → Data validation, cleansing, standardization
- **Gold** → Aggregated, analytics-ready datasets

---

### 🔹 Orchestration Flow (Metadata Model)
Source
↓
Schedule (unique)
↓
Batch (per table)
↓
Task (per processing step)


Each execution tracks:

- Task status (running / success / failed)
- Start & end timestamps
- Execution duration (seconds)
- Error messages
- Automatically calculated next scheduled run

---

## ✨ Key Features

- ✅ Interactive runtime input
- ✅ Unique schedule enforcement
- ✅ Multiple batches under one schedule
- ✅ Task-level duration tracking
- ✅ Automatic `next_run_ts` calculation
- ✅ Data quality validation
- ✅ Structured artifact storage
- ✅ Failure handling & logging
- ✅ Rerunnable batch design

---

## ▶ How to Run

### 1️⃣ Clone the Repository

```bash
git clone <your-repository-url>
cd Equipment-Rental-Data-Engineering

### 2️⃣ Create Virtual Environment (Recommended)
python -m venv venv
venv\Scripts\activate

### 3️⃣ Install Dependencies
pip install pandas openpyxl

### 4️⃣ Execute the Pipeline
python main.py

You will be prompted to provide:
Enter tables (comma-separated)
Enter source name
Enter source type (excel/db/api)
Enter file path or connection string
Enter schedule name
Enter run timestamp (YYYY-MM-DD HH:MM:SS)
Enter timezone
Enter frequency (daily/weekly/hourly/monthly/manual)
Enter priority number
Enter active flag (1=active, 0=inactive)
Enter batch type (full/incremental)

## 📂 Output Structure

After execution:
artifacts/
│
├── bronze/
├── silver/quarantine
├── gold/
└── pipeline_manager/
    └── pipeline_manager.db

    🧠 Key Design Assumptions

schedule_name must be unique.

1.One schedule can contain multiple batches.

2. Each table is processed as an independent batch.

3. Bronze stores raw data without transformation.

4. Silver enforces validation, cleansing, and business rules.

5. Gold prepares aggregated reporting datasets.

6. next_run_ts is automatically calculated based on:

 - daily → +1 day

 - weekly → +7 days

 - hourly → +1 hour

 - monthly → +30 days

8. Batch-level reruns are allowed without recreating schedules.

## 🔍 Data Quality Issues Identified
During analysis of the Equipment Hire dataset, the following issues were observed:

1️⃣ Missing EndDate

- Causes incorrect rental duration calculations
- Flagged during Silver validation

2️⃣ Negative Revenue

- Causes reconciliation issues
- Flagged or rejected in Silver layer

3️⃣ Inconsistent Date Formats

- Converted to standardized datetime format in Silver

4️⃣ Duplicate Transactions

- Deduplicated during transformation

5️⃣ Referential Integrity Issues

- Orphan CustomerID / EquipmentID identified during joins

# 🚀 Equipment Rental Data Engineering Pipeline  
### Medallion Architecture + Metadata-Driven Orchestrator

---

## 📌 Project Overview

This project implements a production-style **Data Engineering Pipeline** using the **Medallion Architecture (Bronze → Silver → Gold)** combined with a custom-built **metadata-driven Pipeline Manager**.

The solution simulates a real-world equipment rental company facing:

- Inconsistent reporting  
- Revenue reconciliation issues  
- Data quality problems  
- Multi-table ingestion requirements  

Instead of hardcoding logic, the pipeline execution is fully driven by metadata tables (`source`, `schedule`, `batch`, `task`) stored in a SQLite orchestration database.

---

## 🏗 Architecture

### 🔹 Data Flow (Medallion)
# 🚀 Equipment Rental Data Engineering Pipeline  
### Medallion Architecture + Metadata-Driven Orchestrator

---

## 📌 Project Overview

This project implements a production-style **Data Engineering Pipeline** using the **Medallion Architecture (Bronze → Silver → Gold)** combined with a custom-built **metadata-driven Pipeline Manager**.

The solution simulates a real-world equipment rental company facing:

- Inconsistent reporting  
- Revenue reconciliation issues  
- Data quality problems  
- Multi-table ingestion requirements  

Instead of hardcoding logic, the pipeline execution is fully driven by metadata tables (`source`, `schedule`, `batch`, `task`) stored in a SQLite orchestration database.

---

## 🏗 Architecture

### 🔹 Data Flow (Medallion)
Source → Bronze → Silver → Gold

- **Bronze** → Raw ingestion layer (no transformations)
- **Silver** → Data validation, cleansing, standardization
- **Gold** → Aggregated, analytics-ready datasets

---

### 🔹 Orchestration Flow (Metadata Model)
Source → Schedule (unique) → Batch (per table) → Task (per processing step)

Each execution tracks:
- Task status (running / success / failed)
- Start & end timestamps
- Execution duration (seconds)
- Error messages
- Automatically calculated next scheduled run

---

## ✨ Key Features

✅ Interactive runtime input
✅ Unique schedule enforcement
✅ Multiple batches under one schedule
✅ Task-level duration tracking
✅ Automatic `next_run_ts` calculation
✅ Data quality validation
✅ Structured artifact storage
✅ Failure handling & logging
✅ Rerunnable batch design

---
---
## ▶ How to Run

### 1️⃣ Clone the Repository
```bash
git clone <your-repository-url>
cd Equipment-Rental-Data-Engineering
```

### 2️⃣ Create Virtual Environment (Recommended)
```bash
python -m venv venv
venv\Scripts\activate
```

### 3️⃣ Install Dependencies
```bash
pip install pandas openpyxl
```

### 4️⃣ Execute the Pipeline
```bash
python main.py
```

You will be prompted to provide:
```bash
Enter tables (comma-separated)
Enter source name
Enter source type (excel/db/api)
Enter file path or connection string
Enter schedule name
Enter run timestamp (YYYY-MM-DD HH:MM:SS)
Enter timezone
Enter frequency (daily/weekly/hourly/monthly/manual)
Enter priority number
Enter active flag (1=active, 0=inactive)
Enter batch type (full/incremental)
```

## 📂 Output Structure
```bash
After execution:
artifacts/
│
├── bronze/
├── silver/quarantine
├── gold/
└── pipeline_manager/
    └── pipeline_manager.db
```

## 🧠 Key Design Assumptions

1. schedule_name must be unique.
2. One schedule can contain multiple batches.
3. Each table is processed as an independent batch.
4. Bronze stores raw data without transformation.
5. Silver enforces validation, cleansing, and business rules.
6. Gold prepares aggregated reporting datasets.
7. next_run_ts is automatically calculated based on:
 - daily → +1 day
 - weekly → +7 days
 - hourly → +1 hour
 - monthly → +30 days
8. Batch-level reruns are allowed without recreating schedules.

## 🔍 Data Quality Issues Identified
During analysis of the Equipment Hire dataset, the following issues were observed:

1️⃣ Missing EndDate
- Causes incorrect rental duration calculations
- Flagged during Silver validation

2️⃣ Negative Revenue
- Causes reconciliation issues
- Flagged or rejected in Silver layer

3️⃣ Inconsistent Date Formats
- Converted to standardized datetime format in Silver

4️⃣ Duplicate Transactions
- Deduplicated during transformation

5️⃣ Referential Integrity Issues
- Orphan CustomerID
- Orphan EquipmentID
- Identified during Silver joins

## 🔁 Rerun Strategy

This pipeline supports selective reruns:
- Rerun only failed tasks
- Rerun specific batches
- Maintain historical execution records
- No need to recreate schedules
Each rerun generates new task entries while preserving historical metadata for auditability.

## 📦 Dependencies

- Python 3.9+
- pandas
- openpyxl
- sqlite3 (built-in)
Optional:
- Logging framework (custom logger included)

## 📈 Future Enhancements

- Add API ingestion support
- Implement incremental watermark tracking
- Add email/Slack failure notifications
- Deploy as containerized microservice
- Integrate with Azure Data Factory or Microsoft Fabric
- Add dashboard for pipeline monitoring