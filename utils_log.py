# utils_log.py
from datetime import datetime
import sys
import os

LOG_FILE = "scraper_log.txt"

def log_event(source, message, level="INFO"):
    """
    Registra un evento nel file di log scraper_log.txt
    Compatibile con emoji, PowerShell e UTF-8 (Windows).
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    source_upper = source.upper() if isinstance(source, str) else "GENERIC"
    line = f"[{timestamp}] [{level}] [{source_upper}] {message}\n"

    # ✅ Assicura che la directory esista
    try:
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    except Exception:
        pass

    # ✅ Scrive sempre in UTF-8 su file
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception as e:
        print(f"[LOG ERROR] impossibile scrivere il file di log: {e}")

    # ✅ Forza UTF-8 sulla console (Windows compatibile)
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    # ✅ Stampa in console (fallback in ASCII se necessario)
    try:
        print(line, end="")
    except UnicodeEncodeError:
        print(line.encode("ascii", errors="ignore").decode(), end="")
