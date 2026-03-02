# Equipment-Rental-Data-Engineeringequipment_rental/
├─ components/
│  ├─ bronze_ingestion.py
│  ├─ silver_validation.py
│  ├─ silver_transformation.py
│  ├─ gold_aggregation.py
│  └─ quarantine_handler.py
│
├─ pipeline/
│  ├─ medallion_pipeline.py
│  └─ pipeline_manager.py
│
├─ configuration/
│  └─ configuration.py
│
├─ entity/
│  ├─ config_entity.py
│  └─ artifact_entity.py
│
├─ constants/
│  └─ constants.py
│
├─ exception/
│  └─ exception.py
│
├─ logger/
│  └─ logger.py
│
├─ utils/
│  └─ common_utils.py
│
├─ artifacts/
│  ├─ bronze/
│  ├─ silver/
│  │  └─ quarantine/
│  └─ gold/
│
├─ data/                               ← **Excel source files stored here**
│  └─ Equipment_Hire_Dataset.xlsx
│     ├─ Equipment_master
│     ├─ Customer_Master
│     ├─ Rental_Transactions
│     └─ Date_Dimension
│
├─ config/
│  ├─ config.yaml
│  └─ schema.yaml
│
├─ main.py
├─ requirements.txt
├─ Dockerfile
├─ .dockerignore
└─ setup.py