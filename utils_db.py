# ============================================================
# utils_db.py
# Gestione MongoDB + salvataggio annunci RetroFuture (2026)
# Sistema professionale multi-marketplace
# ============================================================

import os
from datetime import datetime, UTC
from pymongo import MongoClient, UpdateOne
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
# SALVATAGGIO ANNUNCI (Versione Professionale + FULL SCORING)
# ============================================================

def salva_annunci_mongo(items, source="unknown"):

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

    now_iso = datetime.now(UTC).isoformat()

    # --------------------------------------------------------
    # Campi STATO (mai toccati)
    # --------------------------------------------------------
    STATE_FIELDS = {
        "status",
        "expired_reason",
        "expired_at",
        "needs_check",
        "needs_check_at",

        "noimage_hits",
        "noimage_last_at",
        "noimage_last_image",
        "noimage_last_ip",

        "deadlink_hits",
        "deadlink_last_check",
        "deadlink_last_at",

        "is_removed",
        "removed_at",
    }

    # --------------------------------------------------------
    # Campi CONTENUTO (incluso scoring vintage)
    # --------------------------------------------------------
    CONTENT_FIELDS = {
        # Base
        "title", "titolo",
        "description", "descrizione",

        # Prezzo
        "price_value", "price_display", "price_currency", "prezzo",

        # Media / URL
        "image", "immagine",
        "url", "link",

        # Meta
        "location", "condition",
        "keywords",
        "era",
        "category",

        # 🔥 RIPRISTINO SISTEMA VINTAGE
        "vintage_class",
        "vintage_score",
        "legacy_hash",

        # 🔎 SEARCH METADATA V2
        "item_type",
        "entity_family",
        "entity_model",
        "core_title_tokens",
        "title_phrase_norm",

        # Sorgente
        "source",
        "source_id",
        "logo",

        # Timestamp contenuto
        "scraped_at",
        "updated_at",
    }

    # --------------------------------------------------------
    # Categorie UI ufficiali
    # --------------------------------------------------------
    UI_CATEGORIES = {
        "tecnologia",
        "arredamento",
        "moda_accessori",
        "giochi_giocattoli",
        "musica_cinema",
        "auto_moto",
        "libri_fumetti",
        "cucina",
        "cartoleria",
        "collezionismo",
        "vario",
    }

    CATEGORY_MAP = {
        "moda": "moda_accessori",
        "giocattoli": "giochi_giocattoli",
        "libri": "libri_fumetti",
    }

    ops = []

    for i, doc in enumerate(items, start=1):
        try:
            if not isinstance(doc, dict):
                skipped += 1
                continue

            item_hash = doc.get("hash")
            if not item_hash:
                skipped += 1
                continue

            # ---------------------------------------------------
            # Categoria protetta
            # ---------------------------------------------------
            try:
                current = (doc.get("category") or "").strip().lower()

                if current not in UI_CATEGORIES:
                    guessed = (detect_category(doc) or "").strip().lower()
                    guessed = CATEGORY_MAP.get(guessed, guessed)
                    doc["category"] = guessed if guessed in UI_CATEGORIES else "vario"

            except Exception:
                c = (doc.get("category") or "").strip().lower()
                doc["category"] = c if c in UI_CATEGORIES else "vario"

            # ---------------------------------------------------
            # Placeholder immagini
            # ---------------------------------------------------
            for key in ("image", "immagine"):
                if key in doc and isinstance(doc[key], str):
                    if "No-Image-Placeholder" in doc[key]:
                        doc[key] = None

            # ---------------------------------------------------
            # $set sicuro
            # ---------------------------------------------------
            set_doc = {"updated_at": now_iso}

            for k in CONTENT_FIELDS:
                if k in doc and doc[k] is not None:
                    if k not in STATE_FIELDS and k != "created_at":
                        set_doc[k] = doc[k]

            # Safety extra
            for k in list(set_doc.keys()):
                if k in STATE_FIELDS or k == "created_at":
                    set_doc.pop(k, None)

            # ---------------------------------------------------
            # $setOnInsert MINIMO
            # ---------------------------------------------------
            insert_doc = {
                "hash": item_hash,
                "created_at": doc.get("created_at") or now_iso,
                "is_removed": False
            }

            ops.append(
                UpdateOne(
                    {"hash": item_hash},
                    {
                        "$set": set_doc,
                        "$setOnInsert": insert_doc
                    },
                    upsert=True
                )
            )

        except Exception as e:
            errori += 1
            log_event(source, f"❌ Errore preparazione doc: {e}", "ERROR")

        if i % 200 == 0 or i == tot:
            log_event(source, f"📦 Preparati {i}/{tot}")

    if not ops:
        client.close()
        last_db_stats = {
            "inserted": 0,
            "updated": 0,
            "skipped": skipped,
            "errors": errori,
            "total": tot
        }
        log_event(source, "ℹ️ Nessuna operazione DB da eseguire", "INFO")
        return 0, 0, skipped, errori

    try:
        res = col.bulk_write(ops, ordered=False)

        inseriti = len(getattr(res, "upserted_ids", {}) or {})
        aggiornati = int(getattr(res, "modified_count", 0) or 0)

    except Exception as e:
        errori += 1
        log_event(source, f"❌ Errore bulk_write: {e}", "ERROR")

    finally:
        client.close()

    last_db_stats = {
        "inserted": inseriti,
        "updated": aggiornati,
        "skipped": skipped,
        "errors": errori,
        "total": tot
    }

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

    col.update_one(
        {"hash": item_hash},
        {"$set": {"is_removed": True, "removed_at": now_iso}}
    )

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

    from utils_normalize import load_json, save_json

    data = load_json("modern_learned.json")
    if not isinstance(data, dict):
        data = {"phrases": []}
    if "phrases" not in data:
        data["phrases"] = []

    words = [w for w in raw_title.lower().split() if len(w) >= 4]

    for w in words:
        data["phrases"].append(w)

    data["phrases"] = sorted(set(data["phrases"]))

    save_json("modern_learned.json", data)

    client.close()

    log_event("system", f"🧹 Rimosso manualmente + addestrato su: {raw_title}")
