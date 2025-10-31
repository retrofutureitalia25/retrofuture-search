# utils_db.py
import os
from datetime import datetime
from pymongo import MongoClient
from utils_log import log_event

# ✅ Variabili ambiente da Render
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "retrofuture")  # fallback
COLLECTION_NAME = "annunci"

if not MONGO_URI:
    raise RuntimeError("❌ ERRORE: MONGO_URI non impostata nelle variabili ambiente!")

# ✅ Connessione persistente (Atlas gestisce pool)
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col = db[COLLECTION_NAME]

# ✅ Stats globali
last_db_stats = {
    "inserted": 0,
    "updated": 0,
    "skipped": 0,
    "errors": 0,
    "total": 0
}

def salva_annunci_mongo(items, source="unknown"):
    global last_db_stats

    tot = len(items)
    inseriti, aggiornati, skipped, errori = 0, 0, 0, 0

    log_event(source, f"🚀 Avvio salvataggio di {tot} annunci su MongoDB")

    for i, doc in enumerate(items, start=1):
        try:
            if not doc.get("hash"):
                skipped += 1
                continue

            insert_doc = doc.copy()
            insert_doc.pop("updated_at", None)

            res = col.update_one(
                {"hash": doc.get("hash")},
                {
                    "$setOnInsert": insert_doc,
                    "$set": {
                        "updated_at": doc.get("updated_at", datetime.utcnow().isoformat())
                    }
                },
                upsert=True
            )

            if res.upserted_id:
                inseriti += 1
            elif res.modified_count > 0:
                aggiornati += 1
            else:
                skipped += 1

        except Exception as e:
            errori += 1
            log_event(source, f"❌ Errore inserimento: {e}", "ERROR")

        if i % 100 == 0 or i == tot:
            log_event(source, f"📦 {i}/{tot} processati")

    # ✅ Stats aggiornate
    last_db_stats = {
        "inserted": inseriti,
        "updated": aggiornati,
        "skipped": skipped,
        "errors": errori,
        "total": tot
    }

    # ✅ Report finale
    log_event(source, "===== RISULTATO SALVATAGGIO =====")
    log_event(source, f"✅ Inseriti: {inseriti}")
    log_event(source, f"♻️ Aggiornati: {aggiornati}")
    log_event(source, f"⚪ Ignorati: {skipped}")
    log_event(source, f"❌ Errori: {errori}")
    log_event(source, f"📊 Totale: {tot}")
    log_event(source, "✅ Salvataggio concluso!")

    return inseriti, aggiornati, skipped, errori
