# Equipment Rental Medallion Pipeline

## Overview

This project demonstrates a **Medallion Architecture** (Bronze → Silver → Gold) for an equipment hire company. The pipeline addresses **data quality and consistency issues** in rental transaction data from multiple source systems.  

It is built in **Python** using **Pandas** and designed to simulate a Microsoft Fabric-like data engineering workflow.  

---
`````
equipment_rental/
├─ components/
│ ├─ bronze_ingestion.py
│ ├─ silver_validation.py
│ ├─ silver_transformation.py
│ ├─ gold_aggregation.py
│ └─ quarantine_handler.py
├─ pipeline/
│ ├─ medallion_pipeline.py
│ └─ pipeline_manager.py
├─ constants/
│ └─ constants.py
├─ exception/
│ └─ exception.py
├─ logger/
│ └─ logger.py
├─ utils/
│ ├─ common_utils.py
| └─ email_utils.py
artifacts/
├─ bronze/.keep
├─ silver/.keep
├─ silver/quarantine/.keep
├─ gold/.keep
└─ pipeline_manager/.keep
data/.keep
main.py
pm_config.py
requirements.txt
Dockerfile
.dockerignore
setup.py
`````

---

## Pre-requisites

- Python ≥ 3.9  
- Libraries:
  ```bash
  pip install -r requirements.txt
  ```
requirements.txt includes: pandas, openpyxl, and other dependencies.

## How to Run
1. Configure Pipeline Metadata

Run pm_config.py to add or update sources, schedules, and batches:
```bash
python pm_config.py
```
Follow prompts to:
- Add source files (CSV/Excel)
- Create schedules and batch configurations
- Set priorities and active flags

2. Run the Pipeline
```bash
python main.py
```
- Processes Bronze → Silver → Gold layers sequentially.
- Tracks task status, incremental watermarks, and SLA warnings.
- Outputs are saved in artifacts/bronze, artifacts/silver, and artifacts/gold.

Medallion Layer Details
| Layer      | Purpose                     | Key Features / Transformations                                                                                                                                                                                                                             |
| ---------- | --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Bronze** | Raw landing                 | - Ingest CSV/Excel<br>- Preserve original data<br>- Add `load_timestamp`, `source_file` audit columns<br>- Incremental load support with watermarks                                                                                                        |
| **Silver** | Cleaned / Conformed         | - Validate data quality (duplicates, nulls, invalid values)<br>- Standardize formats (dates, strings)<br>- Apply business rules (e.g., RentalDays ≥ 0)<br>- Calculate derived metrics (equipment utilisation, revenue)<br>- Quarantine or log invalid rows |
| **Gold**   | Aggregated / Business-ready | - Merge Silver tables for reporting<br>- Compute key metrics:<br>  - Total revenue per month<br>  - Equipment utilisation (%)<br>  - Completed vs cancelled transactions                                                                                   |

## Data Quality Issues Identified
- RentalDays can be negative → corrected or quarantined
- EquipmentUtilisation may exceed 100% → capped or flagged
- Missing CustomerID or EquipmentID → skipped and logged
- Duplicates in transactional data → removed during Silver layer
- Null or inconsistent date formats → standardized in Silver layer

## Assumptions
1. Incremental loads are based on a LastUpdated timestamp column.
2. Business rules for invalid data:
    - RentalDays < 0 → quarantined
    - Utilisation > 100% → capped
    - Null mandatory keys → skipped
3. Gold metrics assume fully cleaned Silver tables.
4. SLA thresholds:
    - Bronze: 30s
    - Silver: 60s
    - Gold: 120s
5. Tasks are tracked in SQLite database (pipeline_manager.db) to allow restart from failures.

## Key Features
- Incremental processing with watermarks
- Pipeline orchestration with dependency handling
- Error handling & logging for auditability
- Task-level SLA monitoring
- Separation of layers following Medallion architecture
- Modular and reusable Python components