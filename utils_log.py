# utils_log.py
# ============================================================
# Sistema di logging RetroFuture — versione 2025
# Sicuro su:
#   • Linux, macOS, Windows, WSL
#   • Docker
#   • Hosting multipli
#   • Cronjobs
# ============================================================

from datetime import datetime
import sys
import os

# ============================================================
# Percorso assoluto del file di log accanto allo script
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "scraper_log.txt")

# Tentiamo di configurare stdout UTF-8 una sola volta
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


def log_event(source, message, level="INFO"):
    """
    Log universale:
      - Scrive su file scraper_log.txt
      - Stampa su console in UTF-8 (fallback ASCII)
      - Compatibile PowerShell, Windows, Cron, Docker
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    source_upper = source.upper() if isinstance(source, str) else "GENERIC"
    line = f"[{timestamp}] [{level}] [{source_upper}] {message}\n"

    # ========================================================
    # Scrittura su file (UTF-8)
    # ========================================================
    try:
        # crea directory SOLO se necessaria
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)

    except Exception as e:
        print(f"[LOG ERROR] impossibile scrivere il file di log: {e}")

    # ========================================================
    # Stampa su console
    # ========================================================
    try:
        print(line, end="")
    except UnicodeEncodeError:
        # fallback ASCII (rimuove emoji)
        safe_line = line.encode("ascii", errors="ignore").decode()
        print(safe_line, end="")
