import os
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
from utils_log import log_event

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "database_vintage"
COLLECTION_NAME = "annunci"

# ✅ Stats globali inizializzate (evita errori al primo run)
last_db_stats = {
    "inserted": 0,
    "updated": 0,
    "skipped": 0,
    "errors": 0,
    "total": 0
}

def salva_annunci_mongo(items, source="unknown"):
    global last_db_stats

    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]

    tot = len(items)
    inseriti = 0
    aggiornati = 0
    errori = 0
    skipped = 0

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

        # ✅ Progresso ogni 100
        if i % 100 == 0 or i == tot:
            log_event(source, f"📦 {i}/{tot} processati")

    client.close()

    # ✅ Aggiorna stats globali (usate dagli scraper + scheduler)
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
    log_event(source, f"⚪ Ignorati (nessun cambiamento): {skipped}")
    log_event(source, f"❌ Errori: {errori}")
    log_event(source, f"📊 Totale annunci passati: {tot}")
    log_event(source, "✅ Salvataggio concluso!")

    return inseriti, aggiornati, skipped, errori
