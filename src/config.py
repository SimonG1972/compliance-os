from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

DB_PATH = Path(os.getenv("DB_PATH", "./compliance.db")).resolve()
DATA_DIR = Path(os.getenv("DATA_DIR", "./data")).resolve()
RAW_DIR = DATA_DIR / "raw"
NORM_DIR = DATA_DIR / "normalized"
DIFF_DIR = DATA_DIR / "diffs"
SCHEMAS_DIR = Path("./schemas").resolve()
USER_AGENT = os.getenv("USER_AGENT", "ComplianceOSBot/0.1")
RAW_DIR.mkdir(parents=True, exist_ok=True)
NORM_DIR.mkdir(parents=True, exist_ok=True)
DIFF_DIR.mkdir(parents=True, exist_ok=True)
