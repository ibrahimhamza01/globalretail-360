import os
from pathlib import Path

# -----------------------------
# Project Base Directory
# -----------------------------
BASE_DIR = Path(__file__).resolve().parents[2]

# -----------------------------
# Data Directories
# -----------------------------
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# -----------------------------
# Environment
# -----------------------------
ENV = os.getenv("ENV", "dev")

# -----------------------------
# PostgreSQL Configuration
# -----------------------------
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "globalretail")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

POSTGRES_URI = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
