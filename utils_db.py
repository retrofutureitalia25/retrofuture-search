# utils_db.py
# ============================================================
# Gestione MongoDB + salvataggio annunci RetroFuture (2025)
# ============================================================

import os
from datetime import datetime, UTC
from pymongo import MongoClient
from dotenv import load_dotenv

from utils_log import log_event
from detect_category import detect_category

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "database_vintage"
COLLECTION_NAME = "annunci"
FALSE_POSITIVE_COLLECTION = "false_positives"


# ============================================================
# Stats globali
# ============================================================

last_db_stats = {
    "inserted": 0,
    "updated": 0,
    "skipped": 0,
    "errors": 0,
    "total": 0
}


# ============================================================
# SALVATAGGIO ANNUNCI
# ============================================================

def salva_annunci_mongo(items, source="unknown"):
    """
    Salva o aggiorna gli annunci nel DB con upsert intelligente.
    """
    global last_db_stats

    try:
        client = MongoClient(MONGO_URI)
        col = client[DB_NAME][COLLECTION_NAME]
    except Exception as e:
        log_event(source, f"❌ Errore connessione MongoDB: {e}", "ERROR")
        return 0, 0, 0, 1

    tot = len(items)
    inseriti = aggiornati = skipped = errori = 0

    log_event(source, f"🚀 Avvio salvataggio di {tot} annunci su MongoDB")

    for i, doc in enumerate(items, start=1):
        try:
            # ---------------------------------------------------
            # HASH mancante → skip
            # ---------------------------------------------------
            if not doc.get("hash"):
                skipped += 1
                continue

            # ---------------------------------------------------
            # CATEGORIA (protetta)
            # ---------------------------------------------------
            try:
                doc["category"] = detect_category(doc)
            except Exception:
                doc["category"] = doc.get("category", "vario")

            # Documento da inserire
            insert_doc = doc.copy()

            # updated_at non deve stare nel setOnInsert
            insert_doc.pop("updated_at", None)

            if "is_removed" not in insert_doc:
                insert_doc["is_removed"] = False

            now_iso = datetime.now(UTC).isoformat()

            # ---------------------------------------------------
            # Upsert pulito
            # ---------------------------------------------------
            res = col.update_one(
                {"hash": doc["hash"]},
                {
                    "$setOnInsert": insert_doc,
                    "$set": {"updated_at": now_iso}
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

        # Log ogni 100
        if i % 100 == 0 or i == tot:
            log_event(source, f"📦 {i}/{tot} processati")

    client.close()

    # ======================================================
    # Stats globali aggiornate
    # ======================================================
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
    log_event(source, f"⚪ Ignorati: {skipped}")
    log_event(source, f"❌ Errori: {errori}")
    log_event(source, f"📊 Totale annunci passati: {tot}")
    log_event(source, "✅ Salvataggio concluso!")

    return inseriti, aggiornati, skipped, errori


# ============================================================
# RIMOZIONE + AUTO-TRAINING ANTI MODERNO
# ============================================================

def mark_as_removed_and_learn(item_hash, raw_title):
    try:
        client = MongoClient(MONGO_URI)
        col = client[DB_NAME][COLLECTION_NAME]
        fp_col = client[DB_NAME][FALSE_POSITIVE_COLLECTION]
    except Exception as e:
        log_event("system", f"❌ Errore connessione DB: {e}", "ERROR")
        return

    now_iso = datetime.now(UTC).isoformat()

    # --------------------------------------------------------
    # Segna come rimosso
    # --------------------------------------------------------
    col.update_one(
        {"hash": item_hash},
        {"$set": {"is_removed": True, "removed_at": now_iso}}
    )

    # --------------------------------------------------------
    # Salva nei falsi positivi
    # --------------------------------------------------------
    fp_col.update_one(
        {"hash": item_hash},
        {
            "$set": {
                "hash": item_hash,
                "title": raw_title,
                "added_at": now_iso
            }
        },
        upsert=True
    )

    # --------------------------------------------------------
    # Learning anti-moderno migliorato
    # --------------------------------------------------------
    from utils_normalize import load_json, save_json

    data = load_json("modern_learned.json")
    if not isinstance(data, dict):
        data = {"phrases": []}
    if "phrases" not in data:
        data["phrases"] = []

    words = [w for w in raw_title.lower().split() if len(w) >= 4]

    for w in words:
        data["phrases"].append(w)

    # rimuovi duplicati e ordina
    data["phrases"] = sorted(set(data["phrases"]))

    save_json("modern_learned.json", data)

    client.close()

    log_event("system", f"🧹 Rimosso manualmente + addestrato su: {raw_title}")
