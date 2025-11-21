# utils_db.py
import os
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
from utils_log import log_event

# 🔥 Import del classificatore categorie
from detect_category import detect_category

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "database_vintage"
COLLECTION_NAME = "annunci"
FALSE_POSITIVE_COLLECTION = "false_positives"

# Stats globali iniziali
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
    skipped = 0
    errori = 0

    log_event(source, f"🚀 Avvio salvataggio di {tot} annunci su MongoDB")

    for i, doc in enumerate(items, start=1):
        try:
            # Hash mancante → ignora
            if not doc.get("hash"):
                skipped += 1
                continue

            # -----------------------------
            # 🔥 CLASSIFICAZIONE CATEGORIA
            # -----------------------------
            doc["category"] = detect_category(doc)

            # Copia documento per l'inserimento
            insert_doc = doc.copy()
            insert_doc.pop("updated_at", None)

            # Flag default
            if "is_removed" not in insert_doc:
                insert_doc["is_removed"] = False

            # -----------------------------
            # 🔁 Upsert
            # -----------------------------
            res = col.update_one(
                {"hash": doc["hash"]},
                {
                    "$setOnInsert": insert_doc,  # usato solo se nuovo
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

    client.close()

    # Aggiorna stats globali
    last_db_stats = {
        "inserted": inseriti,
        "updated": aggiornati,
        "skipped": skipped,
        "errors": errori,
        "total": tot
    }

    # Log finale
    log_event(source, "===== RISULTATO SALVATAGGIO =====")
    log_event(source, f"✅ Inseriti: {inseriti}")
    log_event(source, f"♻️ Aggiornati: {aggiornati}")
    log_event(source, f"⚪ Ignorati (nessun cambiamento): {skipped}")
    log_event(source, f"❌ Errori: {errori}")
    log_event(source, f"📊 Totale annunci passati: {tot}")
    log_event(source, "✅ Salvataggio concluso!")

    return inseriti, aggiornati, skipped, errori


# ---------------------------------------------------------
# ❌ Funzione rimozione + auto-training anti-moderno
# ---------------------------------------------------------

def mark_as_removed_and_learn(item_hash, raw_title):
    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]
    fp_col = client[DB_NAME][FALSE_POSITIVE_COLLECTION]

    # Segna come rimosso
    col.update_one(
        {"hash": item_hash},
        {"$set": {"is_removed": True, "removed_at": datetime.utcnow().isoformat()}}
    )

    # Salva nei falsi positivi
    fp_col.update_one(
        {"hash": item_hash},
        {
            "$set": {
                "hash": item_hash,
                "title": raw_title,
                "added_at": datetime.utcnow().isoformat()
            }
        },
        upsert=True
    )

    # Auto-apprendimento anti moderno
    from utils_normalize import save_json, load_json

    data = load_json("modern_learned.json")
    if not isinstance(data, dict):
        data = {"phrases": []}
    if "phrases" not in data:
        data["phrases"] = []

    # estrai parole utili
    words = [w for w in raw_title.lower().split() if len(w) > 3]

    for w in words:
        if w not in data["phrases"]:
            data["phrases"].append(w)

    save_json("modern_learned.json", data)

    client.close()
    log_event("system", f"🧹 Rimosso manualmente + addestrato su: {raw_title}")

