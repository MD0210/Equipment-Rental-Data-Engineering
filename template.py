import os
from pathlib import Path

project_name = "equipment_rental"

list_of_files = [

    # Root package
    f"{project_name}/__init__.py",

    # ========================
    # COMPONENTS (Medallion)
    # ========================
    f"{project_name}/components/__init__.py",
    f"{project_name}/components/bronze_ingestion.py",
    f"{project_name}/components/silver_validation.py",
    f"{project_name}/components/silver_transformation.py",
    f"{project_name}/components/gold_aggregation.py",
    f"{project_name}/components/quarantine_handler.py",

    # ========================
    # PIPELINES
    # ========================
    f"{project_name}/pipeline/__init__.py",
    f"{project_name}/pipeline/medallion_pipeline.py",

    # ========================
    # CONFIGURATION
    # ========================
    f"{project_name}/configuration/__init__.py",
    f"{project_name}/configuration/configuration.py",

    # ========================
    # ENTITY (Config + Artifacts)
    # ========================
    f"{project_name}/entity/__init__.py",
    f"{project_name}/entity/config_entity.py",
    f"{project_name}/entity/artifact_entity.py",

    # ========================
    # CONSTANTS
    # ========================
    f"{project_name}/constants/__init__.py",
    f"{project_name}/constants/constants.py",

    # ========================
    # EXCEPTION & LOGGER
    # ========================
    f"{project_name}/exception/__init__.py",
    f"{project_name}/exception/exception.py",
    f"{project_name}/logger/__init__.py",
    f"{project_name}/logger/logger.py",

    # ========================
    # UTILS
    # ========================
    f"{project_name}/utils/__init__.py",
    f"{project_name}/utils/common_utils.py",

    # ========================
    # CONFIG FILES
    # ========================
    "config/config.yaml",
    "config/schema.yaml",

    # ========================
    # APPLICATION ENTRY
    # ========================
    "main.py",
    "requirements.txt",
    "Dockerfile",
    ".dockerignore",
    "setup.py",
]

for filepath in list_of_files:
    filepath = Path(filepath)
    filedir, filename = os.path.split(filepath)

    if filedir != "":
        os.makedirs(filedir, exist_ok=True)

    if (not os.path.exists(filepath)) or (os.path.getsize(filepath) == 0):
        with open(filepath, "w") as f:
            pass
    else:
        print(f"File already exists at: {filepath}")