import os

# ========================
# BASE DIRECTORIES
# ========================
BASE_DIR = os.getcwd()
ARTIFACT_DIR = os.path.join(BASE_DIR, "artifacts")

# ========================
# MEDALLION LAYERS
# ========================
BRONZE_DIR = os.path.join(ARTIFACT_DIR, "bronze")
SILVER_DIR = os.path.join(ARTIFACT_DIR, "silver")
GOLD_DIR = os.path.join(ARTIFACT_DIR, "gold")
QUARANTINE_DIR = os.path.join(SILVER_DIR, "quarantine")

# ========================
# PIPELINE MANAGER METADATA
# ========================
PIPELINE_DIR = os.path.join(ARTIFACT_DIR, "pipeline_manager")
SOURCE_FILE = os.path.join(PIPELINE_DIR, "source.csv")
SCHEDULE_FILE = os.path.join(PIPELINE_DIR, "schedule.csv")
BATCH_FILE = os.path.join(PIPELINE_DIR, "batch.csv")
TASK_FILE = os.path.join(PIPELINE_DIR, "task.csv")

# ========================
# ENSURE DIRECTORIES EXIST
# ========================
for folder in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, QUARANTINE_DIR, PIPELINE_DIR]:
    os.makedirs(folder, exist_ok=True)