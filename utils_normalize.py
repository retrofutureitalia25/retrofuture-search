#############################################################
# ✅ utils_normalize.py — RetroFuture Italia 2025
#    • Filtro aste
#    • Filtro ricambi veicoli (no auto/moto, sì elettronica vintage)
#    • Filtri bici moderne
#    • Super filtro anti-moderno
#    • Supporto extended modern JSON
#    • BOOST vintage core
#    • Patch MERCATINO + anti-duplicati
#    • Sinonimi backend
#############################################################

import re
import hashlib
import json
import os
from hashlib import sha1
from datetime import datetime, UTC

from utils_log import log_event
from utils_synonyms import expand_with_synonyms


#############################################################
# JSON tools
#############################################################

def load_json(filename):
    path = os.path.join(os.path.dirname(__file__), filename)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_json(filename, data):
    path = os.path.join(os.path.dirname(__file__), filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


#############################################################
# Liste
#############################################################

vintage_memory = load_json("vintage_memory.json") or {
    "phrase_boosts": {},
    "phrase_penalties": {},
    "history": []
}

raw_ml = load_json("modern_learned.json")
modern_learned = set(raw_ml if isinstance(raw_ml, list) else raw_ml.get("phrases", []))

learn_queue = load_json("learn_queue.json") or {"candidates": []}

def add_to_learn_queue(term, context):
    term = term.strip().lower()
    if len(term) < 3:
        return
    for c in learn_queue["candidates"]:
        if c["term"] == term:
            return
    learn_queue["candidates"].append({
        "term": term,
        "context": context[:120],
        "added_at": datetime.now(UTC).isoformat()
    })
    save_json("learn_queue.json", learn_queue)

blacklist = set(load_json("vintage_blacklist.json").get("words_block", []))

keywords_raw = load_json("keywords.json")
vintage_terms = {k.lower() for k in keywords_raw}
retro_terms = {k for k in vintage_terms if "retro" in k or "retrò" in k or "vintage" in k}
retro_terms |= {"retro", "stile vintage", "look retrò"}


#############################################################
# Modern EXTENDED
#############################################################

modern_ext = load_json("modern_keywords_extended.json")
modern_ext_terms = set()

if isinstance(modern_ext, dict):
    for _, values in modern_ext.items():
        for v in values:
            modern_ext_terms.add(str(v).strip().lower())
elif isinstance(modern_ext, list):
    modern_ext_terms = {str(v).strip().lower() for v in modern_ext}


#############################################################
# ERA detection
#############################################################

def detect_era(text):
    text = (text or "").lower()

    m = re.search(r"19([5-9][0-9])", text)
    if m:
        decade = m.group(1)[0] + "0"
        return f"anni_{decade}"

    patterns = {
        r"anni '50|anni 50|50s": "anni_50",
        r"anni '60|anni 60|60s": "anni_60",
        r"anni '70|anni 70|70s": "anni_70",
        r"anni '80|anni 80|80s": "anni_80",
        r"anni '90|anni 90|90s": "anni_90",
    }

    for pat, era in patterns.items():
        if re.search(pat, text):
            return era

    return "vintage_generico"


#############################################################
# VINTAGE CLASSIFIER
#############################################################

def classify_vintage_status(raw_text, expanded_text=None):
    raw_text = raw_text or ""
    raw_low = raw_text.lower()
    text_low = (expanded_text or raw_text).lower()

    score = 0
    vclass = "vintage_generico"

    # ❌ Modern EXTENDED
    for term in modern_ext_terms:
        if term and term in raw_low:
            return "non_vintage", -30

    # ❌ Modern learned
    for term in modern_learned:
        if term and term in raw_low:
            return "non_vintage", -20

    # ❌ Recent years
    if re.search(r"\b20(0[5-9]|1[0-9]|2[0-5])\b", raw_low):
        return "non_vintage", -15

    # ❌ Bici moderne
    modern_bike_patterns = [
        "e-bike", "ebike", "bici elettrica",
        "mountain bike", "mtb",
        "shimano deore", "shimano xt", "shimano xtr",
        "carbonio", "telaio carbonio",
        "rockrider", "btwin",
        "29\"", "29 pollici", "27.5"
    ]
    if any(p in raw_low for p in modern_bike_patterns):
        return "non_vintage", -10

    # ❌ Modern tech/hard
    modern_patterns = [
        r"\biphone\b", r"\bipad\b", r"\bps5\b",
        r"\bsamsung\b",
        r"\bsmart tv\b", r"\b4k\b", r"\bfull hd\b",
        r"\bnintendo switch\b"
    ]
    for pat in modern_patterns:
        if re.search(pat, raw_low):
            return "non_vintage", -10

    # ❌ Modern auto/moto
    modern_auto = [
        "golf 7", "golf mk7", "golf 8",
        "audi a3", "audi a4", "bmw serie",
        "mercedes classe", "tmax", "smart fortwo"
    ]
    if any(m in raw_low for m in modern_auto):
        return "non_vintage", -10

    # ⭐ BOOST vintage core
    vintage_core_terms = {
        "polaroid", "polaroid sx-70", "polaroid 600",
        "land camera", "atari", "amiga", "commodore",
        "c64", "amiga 500", "game boy", "gameboy",
        "nintendo nes", "super nintendo", "snes",
        "sega megadrive", "mega drive", "sega saturn",
        "walkman", "sony walkman", "vhs", "videoregistratore vhs"
    }
    if any(t in text_low for t in vintage_core_terms):
        score += 3
        vclass = "vintage_originale"

    # ✔ Keywords vintage
    if any(k in text_low for k in vintage_terms):
        score += 3
        vclass = "vintage_originale"

    if any(k in text_low for k in retro_terms):
        score += 1
        vclass = "retro_moderno"

    # BOOST auto/bici vintage (solo estetica, non ricambi)
    if any(v in text_low for v in [
        "fiat 500 f", "fiat 500 r", "maggiolino",
        "vespa", "lambretta", "mini cooper classica"
    ]):
        score += 4
        vclass = "vintage_originale"

    if any(b in text_low for b in [
        "graziella", "bianchi epoca", "bianchi vintage",
        "atala vintage", "columbus", "tubazioni acciaio",
        "anni 60", "anni 70", "anni 80", "anni 90",
        "corsa vintage", "freni a bacchetta"
    ]):
        score += 4
        vclass = "vintage_originale"

    # Learning
    if 0 <= score < 3:
        for w in re.findall(r"[a-zA-Z0-9]{4,}", raw_low):
            if w not in vintage_terms and w not in retro_terms and w not in blacklist:
                add_to_learn_queue(w, raw_low)

    if score >= 3:
        vclass = "vintage_originale"

    return vclass, score


#############################################################
# Filtri aste
#############################################################

def is_auction(text):
    if not text:
        return False
    t = text.lower()
    return any(k in t for k in [
        "offerta corrente", "offerta attuale", "offerte",
        "auction", "bid", "rilancio", "puntata"
    ])


#############################################################
# Ricambi veicoli (auto/moto) — BLOCCARE SEMPRE
#############################################################

VEHICLE_RICAMBI_TERMS = [
    # ricambi veicoli
    "ricambi auto", "ricambi moto", "ricambi scooter",
    "ricambio auto", "ricambio moto", "ricambio scooter",
    # parti meccaniche
    "paraurti", "faro", "fanale", "carena", "parafango",
    "carrozzeria", "ammortizzatore", "catalogo ricambi",
    "scarico", "marmitta", "pastiglie", "freni",
    "dischi freno", "centralina", "turbina",
    "alternatore", "motorino avviamento",
    "radiatore", "frizione", "pistone", "cilindro",
    "cinghia", "cinghia distribuzione",
    # ruote
    "cerchi", "cerchio", "pneumatici", "gomme",
    # parole generiche che indicano veicolo
    "auto", "moto", "scooter", "vespa", "lambretta"
]

def is_ricambio_veicoli(text):
    t = (text or "").lower()
    for term in VEHICLE_RICAMBI_TERMS:
        if term in t:
            return term
    return None


#############################################################
# Duplicati
#############################################################

_hash_cache = set()


#############################################################
# NORMALIZZAZIONE
#############################################################

def normalizza_annuncio(raw, source_name):
    title = (raw.get("title") or raw.get("titolo") or "").strip()
    description = raw.get("description") or raw.get("descrizione") or title

    full_text_raw = f"{title} {description}".strip().lower()

    # TITOLI MANCANTI
    if not title or title.lower() in ("titolo non disponibile", "n/a", "none"):
        return None

    # URL
    url = raw.get("url") or raw.get("link") or ""
    if not url:
        return None

    # BLACKLIST
    if any(bad.lower() in full_text_raw for bad in blacklist):
        return None

    # Aste
    if is_auction(title) or is_auction(description):
        return None

    # ❌ Ricambi veicoli (per tutte le piattaforme)
    term = is_ricambio_veicoli(full_text_raw)
    if term:
        log_event(source_name, f"❌ Ricambio VEICOLI scartato: \"{title}\" — trovato: \"{term}\"")
        return None

    # Sinonimi
    try:
        full_text_expanded = expand_with_synonyms(full_text_raw)
    except Exception:
        full_text_expanded = full_text_raw

    # Vintage + era
    vintage_class, score = classify_vintage_status(full_text_raw, full_text_expanded)
    if vintage_class == "non_vintage" or score < 2:
        return None

    era = detect_era(full_text_expanded)

    # Prezzo
    prezzo_raw = str(raw.get("price") or raw.get("prezzo") or "").strip()
    clean = re.sub(r"[^\d,\.]", "", prezzo_raw)

    if "," in clean and "." in clean:
        clean = clean.replace(".", "").replace(",", ".")
    elif "," in clean:
        clean = clean.replace(",", ".")

    try:
        prezzo_val = float(clean) if clean else 0.0
    except:
        prezzo_val = 0.0

    image = raw.get("image") or raw.get("img") or raw.get("immagine") or ""
    location = raw.get("location") or ""
    category = raw.get("category") or raw.get("categoria") or "vario"
    condition = raw.get("condition") or raw.get("condizione")

    # Keywords
    tokens = re.findall(r"[a-zA-Z0-9àèéìòù]+", full_text_expanded.lower())
    keywords = list(set(tokens))

    # Hash
    source_id = raw.get("id") or sha1(url.encode("utf-8")).hexdigest()[:12]
    hash_value = hashlib.md5(f"{source_name}-{title}-{prezzo_val}-{url}".encode("utf-8")).hexdigest()

    if hash_value in _hash_cache:
        return None
    _hash_cache.add(hash_value)

    now_iso = datetime.now(UTC).isoformat()
    price_value = f"{prezzo_val:.2f}"
    price_display = f"{price_value} EUR"

    return {
        "source": source_name,
        "source_id": source_id,
        "title": title,
        "description": description,
        "price_value": price_value,
        "price_display": price_display,
        "price_currency": "EUR",
        "url": url,
        "image": image,
        "location": location,
        "category": category,
        "condition": condition,
        "era": era,
        "vintage_class": vintage_class,
        "vintage_score": score,
        "scraped_at": now_iso,
        "updated_at": now_iso,
        "hash": hash_value,
        "keywords": keywords,
    }
