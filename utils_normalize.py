#############################################################
# ✅ utils_normalize.py — versione aggiornata 2025 + SINONIMI
#    • Nessun filtro località
#    • Filtro aste
#    • Filtro ricambi/motori SOLO eBay
#    • Filtri bici moderne + boost bici vintage
#    • Supporto modern_keywords_extended.json
#    • Super filtro anti-moderno
#    • Patch MERCATINO:
#         1) blocco annunci senza titolo
#         2) blocco annunci senza URL
#         3) blocco duplicati tramite hash_cache
#    • NEW 2025:
#         🔥 Sinonimi backend per:
#             - vintage_score
#             - era detection
#             - keywords generate
#############################################################

import re
import hashlib
import json
import os
from hashlib import sha1
from datetime import datetime, UTC

from utils_log import log_event
from utils_synonyms import expand_with_synonyms   # 🔥 NEW

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
if isinstance(raw_ml, list):
    modern_learned = set(raw_ml)
else:
    modern_learned = set(raw_ml.get("phrases", []))

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
    for key, values in modern_ext.items():
        if isinstance(values, list):
            for v in values:
                modern_ext_terms.add(str(v).strip().lower())

elif isinstance(modern_ext, list):
    for v in modern_ext:
        modern_ext_terms.add(str(v).strip().lower())


#############################################################
# ERA detection
#############################################################

def detect_era(text):
    text = text.lower()

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
# 🔥 VINTAGE CLASSIFIER (ora usa i sinonimi in ingresso)
#############################################################

def classify_vintage_status(text):
    """
    ATTENZIONE:
      ✔ 'text' è già stato passato attraverso expand_with_synonyms()
      ✔ i filtri modern rimangono sicuri perché usano il testo grezzo
    """
    text_low = text.lower()
    score = 0
    vclass = "vintage_generico"

    #########################################################
    # ❌ Modern EXTENDED
    #########################################################
    for term in modern_ext_terms:
        if term and term in text_low:
            return "non_vintage", -30
        if " " in term and term in text_low:
            return "non_vintage", -25

    #########################################################
    # ❌ Modern learned
    #########################################################
    for term in modern_learned:
        if term in text_low:
            return "non_vintage", -20

    #########################################################
    # ❌ Bici moderne
    #########################################################
    modern_bike_patterns = [
        "e-bike", "ebike", "bici elettrica",
        "mountain bike", "mtb",
        "shimano deore", "shimano xt", "shimano xtr",
        "carbonio", "telaio carbonio",
        "rockrider", "btwin",
        "29\"", "29 pollici", "27.5"
    ]
    for pat in modern_bike_patterns:
        if pat in text_low:
            return "non_vintage", -10

    #########################################################
    # ❌ Modern tech/hard
    #########################################################
    modern_patterns = [
        r"\biphone\b", r"\bipad\b", r"\bps5\b",
        r"\bsamsung\b",
        r"\bsmart tv\b", r"\b4k\b", r"\bfull hd\b",
        r"\bnintendo switch\b"
    ]
    for pat in modern_patterns:
        if re.search(pat, text_low):
            return "non_vintage", -10

    modern_auto = [
        "golf 7", "golf mk7", "golf 8",
        "audi a3", "audi a4", "bmw serie",
        "mercedes classe", "tmax", "smart fortwo"
    ]
    for m in modern_auto:
        if m in text_low:
            return "non_vintage", -10

    #########################################################
    # ✔ Vintage detection (ora con sinonimi espansi)
    #########################################################

    for w in vintage_terms:
        if w in text_low:
            score += 3
            vclass = "vintage_originale"

    for w in retro_terms:
        if w in text_low:
            score += 1
            vclass = "retro_moderno"

    vintage_auto = [
        "fiat 500 f", "fiat 500 r", "lambretta",
        "vespa", "maggiolino", "mini cooper classica"
    ]
    for v in vintage_auto:
        if v in text_low:
            score += 4
            vclass = "vintage_originale"

    vintage_bike_terms = [
        "graziella",
        "bianchi epoca", "bianchi vintage",
        "atala vintage",
        "columbus",
        "tubazioni acciaio",
        "anni 60", "anni 70", "anni 80", "anni 90",
        "corsa vintage", "bici da corsa vintage",
        "freni a bacchetta",
        "ruote d'epoca"
    ]
    for term in vintage_bike_terms:
        if term in text_low:
            score += 4
            vclass = "vintage_originale"

    #########################################################
    # Learning
    #########################################################
    if 0 <= score < 3:
        words = re.findall(r"[a-zA-Z0-9]{4,}", text_low)
        for w in words:
            if w not in vintage_terms and w not in retro_terms and w not in blacklist:
                add_to_learn_queue(w, text_low)

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
        "offerta corrente",
        "offerta attuale",
        "offerte",
        "auction",
        "bid",
        "rilancio",
        "puntata"
    ])


#############################################################
# Ricambi eBay
#############################################################

EBAY_RICAMBI_TERMS = [
    "ricambi", "ricambio", "spare part", "componenti",
    "carrozzeria", "faro", "fanale", "ammortizzatore",
    "parafango", "freccia", "centralina", "alternatore",
    "carburatore", "motorino avviamento",
    "dischi freno", "pastiglie", "frizione",
    "paraurti", "radiatore", "turbina"
]

def is_ricambio_ebay(text):
    t = text.lower()
    for term in EBAY_RICAMBI_TERMS:
        if term in t:
            return term
    return None


#############################################################
# Duplicati
#############################################################

_hash_cache = set()


#############################################################
# NORMALIZZAZIONE ANNUNCIO
#############################################################

def normalizza_annuncio(raw, source_name):
    title = (raw.get("title") or raw.get("titolo") or "").strip()
    description = raw.get("description") or raw.get("descrizione") or title

    full_text_raw = f"{title} {description}"

    # TITOLI MANCANTI
    if not title or title.lower() in ("titolo non disponibile", "n/a", "none"):
        return None

    # URL mancante
    url = raw.get("url") or raw.get("link") or ""
    if not url:
        return None

    # Aste
    if is_auction(title) or is_auction(description):
        return None

    # Ricambi eBay
    full_text_low = full_text_raw.lower()
    if source_name == "ebay":
        term = is_ricambio_ebay(full_text_low)
        if term:
            log_event("ebay", f"❌ Ricambio scartato: \"{title}\" — trovato: \"{term}\"")
            return None

    #############################################################
    # 🔥 EXPAND WITH SYNONYMS
    #############################################################
    try:
        full_text_expanded = expand_with_synonyms(full_text_raw)
    except:
        full_text_expanded = full_text_raw

    #############################################################
    # VINTAGE + ERA
    #############################################################
    vintage_class, score = classify_vintage_status(full_text_expanded)
    if vintage_class == "non_vintage" or score < 0:
        return None

    era = detect_era(full_text_expanded)

    #############################################################
    # PREZZO
    #############################################################
    prezzo_raw = str(raw.get("price") or raw.get("prezzo") or "").strip()
    clean = re.sub(r"[^\d,\.]", "", prezzo_raw)

    if "," in clean and "." in clean:
        clean = clean.replace(".", "").replace(",", ".")
    elif "," in clean:
        clean = clean.replace(",", ".")

    if clean == "":
        prezzo_val = 0.0
    else:
        try:
            prezzo_val = float(clean)
        except:
            prezzo_val = 0.0

    image = raw.get("image") or raw.get("img") or raw.get("immagine") or ""
    location = raw.get("location") or ""
    category = raw.get("category") or raw.get("categoria") or "vario"
    condition = raw.get("condition") or raw.get("condizione")

    #############################################################
    # 🔥 KEYWORDS potenziate (con sinonimi)
    #############################################################
    try:
        kw_source = expand_with_synonyms(full_text_expanded.lower())
    except:
        kw_source = full_text_expanded.lower()

    tokens = re.findall(r"[a-zA-Z0-9àèéìòù]+", kw_source)
    keywords = list(set(tokens))

    #############################################################
    # HASH
    #############################################################
    source_id = raw.get("id") or (sha1(url.encode("utf-8")).hexdigest()[:12])
    hash_value = hashlib.md5(
        f"{source_name}-{title}-{prezzo_val}-{url}".encode("utf-8")
    ).hexdigest()

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
