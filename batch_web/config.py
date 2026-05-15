import os

BASE_DIR = os.environ.get("BATCH_WEB_BASE_DIR", "/var/www/html/batch-web")
LOG_DIR = f"{BASE_DIR}/logs"
JOB_LOG_DIR = f"{LOG_DIR}/jobs"
WEB_LOG = f"{LOG_DIR}/web.log"
ARCHIVE_DIR = f"{LOG_DIR}/archive"
DB_PATH = os.environ.get("BATCH_WEB_DB_PATH", f"{BASE_DIR}/batch_system.db")
DB_FILE = f"sqlite:///{DB_PATH}"
BACKUP_DIR = f"{BASE_DIR}/backups"
SECRET_KEY = os.environ.get("BATCH_WEB_SECRET_KEY", "GlobalBatchKey2026")


def ensure_directories():
    os.makedirs(JOB_LOG_DIR, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    if not os.path.exists(WEB_LOG):
        open(WEB_LOG, "a").close()
