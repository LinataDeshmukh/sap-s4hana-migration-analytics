# config/settings_template.py
# Copy this file to settings.py and fill in your credentials

DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "YOUR_PASSWORD_HERE",
    "database": "precision_mfg_bronze",
    "charset":  "utf8mb4",
}

import os
BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DIR = os.path.join(BASE_DIR, "bronze_data")
LOG_DIR    = os.path.join(BASE_DIR, "logs")