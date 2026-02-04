#############################################################
# ✅ utils_normalize.py — RetroFuture Italia 2026
#    • Filtro aste
#    • Filtro ricambi veicoli (no ricambi auto/moto; sì oggetti vintage auto/moto NON ricambi)
#    • Filtri bici moderne
#    • Super filtro anti-moderno
#    • Supporto extended modern JSON
#    • BOOST vintage core
#    • Patch MERCATINO + anti-duplicati
#    • Sinonimi backend
#    • ✅ NORMALIZZAZIONE CATEGORIE (coerente con filtri barra UI)
#      -> tecnologia / arredamento / moda_accessori / giochi_giocattoli /
#         musica_cinema / auto_moto / libri_fumetti / cucina /
#         cartoleria / collezionismo / vario
#    • ✅ Prezzo: se mancante/invalid -> None (non 0.00)
#    • ✅ Category aliases: POTENZIATI + match "contiene" per categorie composte
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

    # 1950–1999 -> anni_50..anni_90
    m = re.search(r"\b19([5-9][0-9])\b", text)
    if m:
        decade = m.group(1)[0] + "0"
        return f"anni_{decade}"

    # 2000–2004 -> anni_2000
    # NB: 2005+ è considerato moderno (gestito nel classifier)
    if re.search(r"\b200[0-4]\b", text):
        return "anni_2000"

    patterns = {
        r"anni\s*'50|anni\s*50|\b50s\b": "anni_50",
        r"anni\s*'60|anni\s*60|\b60s\b": "anni_60",
        r"anni\s*'70|anni\s*70|\b70s\b": "anni_70",
        r"anni\s*'80|anni\s*80|\b80s\b": "anni_80",
        r"anni\s*'90|anni\s*90|\b90s\b": "anni_90",
        r"anni\s*2000|\b2000s\b": "anni_2000",
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

    # ❌ Recent years (2005–2025)
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

    # ❌ Modern auto/moto (modelli recenti specifici)
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

    # Retro moderno (se ti servirà in futuro)
    if any(k in text_low for k in retro_terms):
        score += 1
        vclass = "retro_moderno"

    # BOOST auto/bici vintage (oggetti vintage legati al mondo, non ricambi)
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
# Ricambi veicoli (auto/moto) — BLOCCARE, ma non aggressivo
#############################################################

VEHICLE_PART_TERMS = [
    "ricambi", "ricambio", "ricambistica",
    "paraurti", "faro", "fanale", "carena", "parafango",
    "carrozzeria", "ammortizzatore", "catalogo ricambi",
    "scarico", "marmitta", "pastiglie", "freni",
    "dischi freno", "centralina", "turbina",
    "alternatore", "motorino avviamento",
    "radiatore", "frizione", "pistone", "cilindro",
    "cinghia", "cinghia distribuzione",
    "cerchi", "cerchio", "pneumatici", "gomme",
    "kit", "set", "originale oem", "aftermarket"
]

VEHICLE_WORDS = [
    "auto", "moto", "scooter", "vespa", "lambretta",
    "motocicletta", "motorino"
]

def is_ricambio_veicoli(text):
    t = (text or "").lower()

    has_vehicle = any(w in t for w in VEHICLE_WORDS)
    has_part = any(p in t for p in VEHICLE_PART_TERMS)

    if has_part and has_vehicle:
        for p in VEHICLE_PART_TERMS:
            if p in t:
                return p
        return "ricambi_veicoli"

    if "ricambi auto" in t or "ricambi moto" in t or "ricambi scooter" in t:
        return "ricambi"

    return None


#############################################################
# Duplicati
#############################################################

_hash_cache = set()


#############################################################
# CATEGORIE — NORMALIZZAZIONE (ALLINEATA ALLA BARRA FILTRI UI)
#############################################################

ALLOWED_CATEGORIES = {
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

# 🔥 Alias potenziati (copre categorie "sporche" da marketplace)
CATEGORY_ALIASES = {
    # =========================
    # TECNOLOGIA
    # =========================
    "tech": "tecnologia",
    "tecnologia": "tecnologia",
    "informatica": "tecnologia",
    "computer": "tecnologia",
    "pc": "tecnologia",
    "mac": "tecnologia",
    "apple": "tecnologia",
    "windows": "tecnologia",
    "ms dos": "tecnologia",
    "software": "tecnologia",
    "hardware": "tecnologia",
    "componenti pc": "tecnologia",
    "periferiche": "tecnologia",
    "tastiere": "tecnologia",
    "mouse": "tecnologia",
    "monitor": "tecnologia",
    "stampanti": "tecnologia",
    "scanner": "tecnologia",
    "accessori pc": "tecnologia",
    "modem": "tecnologia",
    "router": "tecnologia",
    "telefonia": "tecnologia",
    "telefoni": "tecnologia",
    "telefono fisso": "tecnologia",
    "cellulari": "tecnologia",
    "mobile": "tecnologia",
    "smartphone": "tecnologia",
    "videogiochi": "tecnologia",
    "console": "tecnologia",
    "retrogaming": "tecnologia",
    "gaming": "tecnologia",
    "nintendo": "tecnologia",
    "playstation": "tecnologia",
    "xbox": "tecnologia",
    "sega": "tecnologia",
    "atari": "tecnologia",
    "commodore": "tecnologia",
    "amiga": "tecnologia",
    "c64": "tecnologia",
    "sinclair": "tecnologia",
    "zx spectrum": "tecnologia",
    "hi fi": "tecnologia",
    "hifi": "tecnologia",
    "audio": "tecnologia",
    "stereo": "tecnologia",
    "giradischi": "tecnologia",
    "walkman": "tecnologia",
    "radioline": "tecnologia",
    "radio": "tecnologia",
    "televisori": "tecnologia",
    "tv": "tecnologia",
    "videoregistratore": "tecnologia",
    "vcr": "tecnologia",
    "fotografia": "tecnologia",
    "macchine fotografiche": "tecnologia",
    "fotocamere": "tecnologia",
    "videocamere": "tecnologia",
    "cinepresa": "tecnologia",
    "polaroid": "tecnologia",
    "elettronica": "tecnologia",
    "elettronica di consumo": "tecnologia",
    "elettrodomestici": "tecnologia",

    # =========================
    # ARREDAMENTO
    # =========================
    "casa": "arredamento",
    "arredo": "arredamento",
    "arredamento": "arredamento",
    "mobili": "arredamento",
    "mobile": "arredamento",
    "sedie": "arredamento",
    "sedia": "arredamento",
    "tavoli": "arredamento",
    "tavolo": "arredamento",
    "divani": "arredamento",
    "divano": "arredamento",
    "poltrone": "arredamento",
    "poltrona": "arredamento",
    "letto": "arredamento",
    "armadi": "arredamento",
    "armadio": "arredamento",
    "credenze": "arredamento",
    "credenza": "arredamento",
    "librerie": "arredamento",
    "libreria": "arredamento",
    "complementi d'arredo": "arredamento",
    "complementi": "arredamento",
    "oggettistica casa": "arredamento",
    "oggetti per la casa": "arredamento",
    "design": "arredamento",
    "modernariato": "arredamento",
    "illuminazione": "arredamento",
    "lampade": "arredamento",
    "lampada": "arredamento",
    "lampadari": "arredamento",
    "applique": "arredamento",
    "abat jour": "arredamento",
    "decorazioni": "arredamento",
    "quadri": "arredamento",
    "stampe": "arredamento",
    "specchi": "arredamento",
    "tappeti": "arredamento",
    "tessili": "arredamento",
    "tende": "arredamento",
    "biancheria casa": "arredamento",
    "ceramiche": "arredamento",
    "porcellane": "arredamento",
    "vetro": "arredamento",
    "cristallo": "arredamento",

    # =========================
    # MODA & ACCESSORI
    # =========================
    "moda": "moda_accessori",
    "fashion": "moda_accessori",
    "abbigliamento": "moda_accessori",
    "vestiti": "moda_accessori",
    "uomo": "moda_accessori",
    "donna": "moda_accessori",
    "unisex": "moda_accessori",
    "maglie": "moda_accessori",
    "maglioni": "moda_accessori",
    "giacche": "moda_accessori",
    "giacca": "moda_accessori",
    "cappotti": "moda_accessori",
    "cappotto": "moda_accessori",
    "camicie": "moda_accessori",
    "camicia": "moda_accessori",
    "jeans": "moda_accessori",
    "pantaloni": "moda_accessori",
    "pantalone": "moda_accessori",
    "gonne": "moda_accessori",
    "gonna": "moda_accessori",
    "scarpe": "moda_accessori",
    "stivali": "moda_accessori",
    "sneakers": "moda_accessori",
    "borse": "moda_accessori",
    "borsa": "moda_accessori",
    "zaini": "moda_accessori",
    "zaino": "moda_accessori",
    "portafogli": "moda_accessori",
    "cinture": "moda_accessori",
    "cintura": "moda_accessori",
    "cappelli": "moda_accessori",
    "cappello": "moda_accessori",
    "occhiali": "moda_accessori",
    "occhiali da sole": "moda_accessori",
    "gioielli": "moda_accessori",
    "collane": "moda_accessori",
    "collana": "moda_accessori",
    "bracciali": "moda_accessori",
    "bracciale": "moda_accessori",
    "anelli": "moda_accessori",
    "anello": "moda_accessori",
    "orecchini": "moda_accessori",
    "orecchino": "moda_accessori",
    "orologi": "moda_accessori",
    "orologio": "moda_accessori",
    "accessori": "moda_accessori",
    "profumi": "moda_accessori",
    "cosmetici": "moda_accessori",
    "make up": "moda_accessori",

    # =========================
    # GIOCHI & GIOCATTOLI
    # =========================
    "giochi": "giochi_giocattoli",
    "giocattoli": "giochi_giocattoli",
    "toy": "giochi_giocattoli",
    "toys": "giochi_giocattoli",
    "bambini": "giochi_giocattoli",
    "lego": "giochi_giocattoli",
    "playmobil": "giochi_giocattoli",
    "modellini": "giochi_giocattoli",
    "trenini": "giochi_giocattoli",
    "macchinine": "giochi_giocattoli",
    "robot": "giochi_giocattoli",
    "action figure": "giochi_giocattoli",
    "bambole": "giochi_giocattoli",
    "barbie": "giochi_giocattoli",
    "puzzle": "giochi_giocattoli",
    "giochi da tavolo": "giochi_giocattoli",
    "monopoli": "giochi_giocattoli",
    "risiko": "giochi_giocattoli",
    "carte": "giochi_giocattoli",
    "carte collezionabili": "giochi_giocattoli",

    # =========================
    # MUSICA & CINEMA
    # =========================
    "musica": "musica_cinema",
    "cinema": "musica_cinema",
    "film": "musica_cinema",
    "dvd": "musica_cinema",
    "bluray": "musica_cinema",
    "blu ray": "musica_cinema",
    "vhs": "musica_cinema",
    "cassette": "musica_cinema",
    "musicassette": "musica_cinema",
    "cd": "musica_cinema",
    "vinile": "musica_cinema",
    "vinili": "musica_cinema",
    "lp": "musica_cinema",
    "45 giri": "musica_cinema",
    "33 giri": "musica_cinema",
    "concerti": "musica_cinema",
    "colonne sonore": "musica_cinema",

    # =========================
    # AUTO & MOTO (NO ricambi)
    # =========================
    "auto": "auto_moto",
    "moto": "auto_moto",
    "motori": "auto_moto",
    "scooter": "auto_moto",
    "vespa": "auto_moto",
    "lambretta": "auto_moto",
    "accessori auto": "auto_moto",
    "accessori moto": "auto_moto",
    "gadget auto": "auto_moto",
    "gadget moto": "auto_moto",
    "manuali auto": "auto_moto",
    "manuali moto": "auto_moto",
    "modellini auto": "auto_moto",
    "modellini moto": "auto_moto",
    "automobilia": "auto_moto",

    # =========================
    # LIBRI & FUMETTI
    # =========================
    "libri": "libri_fumetti",
    "fumetti": "libri_fumetti",
    "comic": "libri_fumetti",
    "comics": "libri_fumetti",
    "manga": "libri_fumetti",
    "riviste": "libri_fumetti",
    "giornali": "libri_fumetti",
    "enciclopedie": "libri_fumetti",
    "manuali": "libri_fumetti",
    "editoria": "libri_fumetti",

    # =========================
    # CUCINA
    # =========================
    "cucina": "cucina",
    "utensili": "cucina",
    "pentole": "cucina",
    "posate": "cucina",
    "piatti": "cucina",
    "bicchieri": "cucina",
    "tazze": "cucina",
    "servizio piatti": "cucina",
    "servizi piatti": "cucina",
    "servizio": "cucina",
    "caffettiera": "cucina",
    "moka": "cucina",

    # =========================
    # CARTOLERIA
    # =========================
    "cartoleria": "cartoleria",
    "penne": "cartoleria",
    "penna": "cartoleria",
    "matite": "cartoleria",
    "quaderni": "cartoleria",
    "quaderno": "cartoleria",
    "agenda": "cartoleria",
    "agende": "cartoleria",
    "diari": "cartoleria",
    "diario": "cartoleria",

    # =========================
    # COLLEZIONISMO
    # =========================
    "collezionismo": "collezionismo",
    "collezioni": "collezionismo",
    "figurine": "collezionismo",
    "album figurine": "collezionismo",
    "monete": "collezionismo",
    "banconote": "collezionismo",
    "francobolli": "collezionismo",
    "medaglie": "collezionismo",
    "souvenir": "collezionismo",
    "cartoline": "collezionismo",
    "poster": "collezionismo",
    "manifesti": "collezionismo",
    "locandine": "collezionismo",
    "militaria": "collezionismo",

    # =========================
    # VARIO
    # =========================
    "vario": "vario",
    "altro": "vario",
    "misc": "vario",
    "miscellanea": "vario",
}

def normalize_category(raw_category: str, text_hint: str = "") -> str:
    """
    NON scarta annunci.
    Se non riconosce la categoria -> 'vario'
    """
    c = (raw_category or "").strip().lower()
    t = (text_hint or "").strip().lower()

    # normalizza separatori e spazi
    c = c.replace("-", " ").replace("_", " ")
    c = re.sub(r"[\/\|\(\)\[\]\.\,\:\;]+", " ", c)
    c = re.sub(r"\s+", " ", c).strip()

    if c in ALLOWED_CATEGORIES:
        return c

    if c in CATEGORY_ALIASES:
        return CATEGORY_ALIASES[c]

    # match "contiene" per categorie composte (es. "tv e audio")
    for k, v in CATEGORY_ALIASES.items():
        if k and k in c:
            return v

    blob = f"{c} {t}".strip()

    if any(k in blob for k in ["tecn", "computer", "console", "audio", "hifi", "hi fi", "videog", "amiga", "commodore", "walkman", "vhs"]):
        return "tecnologia"
    if any(k in blob for k in ["arred", "mobili", "casa", "arredo", "lamp", "decor", "design", "tavolo", "sedia"]):
        return "arredamento"
    if any(k in blob for k in ["abbigli", "scarpe", "bors", "access", "gioiell", "orolog", "jeans", "giacca", "cappotto", "camicia"]):
        return "moda_accessori"
    if any(k in blob for k in ["giocatt", "giochi", "lego", "playmobil", "modellin", "action figure", "barbie"]):
        return "giochi_giocattoli"
    if any(k in blob for k in ["vinile", "vinili", "cd", "dvd", "vhs", "cassetta", "cassett", "tape", "film", "colonna sonora", "soundtrack"]):
        return "musica_cinema"
    if any(k in blob for k in ["vespa", "lambretta", "auto", "moto", "scooter", "fiat 500", "maggiolino", "automobilia"]):
        return "auto_moto"
    if any(k in blob for k in ["fumett", "libri", "rivist", "giornal", "manga", "topolino", "tex"]):
        return "libri_fumetti"
    if any(k in blob for k in ["cucin", "servizio", "piatti", "posate", "pentola", "caffettiera", "moka", "bicchier", "tazze"]):
        return "cucina"
    if any(k in blob for k in ["cartoler", "penna", "matita", "quaderno", "agenda", "diario", "grafica", "stampa"]):
        return "cartoleria"
    if any(k in blob for k in ["collez", "figur", "poster", "cartolin", "francoboll", "monet", "banconot", "medagli", "militaria", "locandina", "manifesto"]):
        return "collezionismo"

    return "vario"


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

    # ❌ Ricambi veicoli
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

    # Prezzo (se non valido -> None, così non inquina filtri/sort)
    prezzo_raw = str(raw.get("price") or raw.get("prezzo") or "").strip()
    clean = re.sub(r"[^\d,\.]", "", prezzo_raw)

    if "," in clean and "." in clean:
        clean = clean.replace(".", "").replace(",", ".")
    elif "," in clean:
        clean = clean.replace(",", ".")

    prezzo_val = None
    try:
        if clean:
            prezzo_val = float(clean)
            if prezzo_val < 0:
                prezzo_val = None
    except Exception:
        prezzo_val = None

    image = raw.get("image") or raw.get("img") or raw.get("immagine") or ""
    location = raw.get("location") or ""

    # ✅ Categoria normalizzata (MAI scarto per categoria) + hint testo
    category_raw = raw.get("category") or raw.get("categoria") or ""
    category = normalize_category(category_raw, text_hint=full_text_expanded)

    condition = raw.get("condition") or raw.get("condizione")

    # Keywords (limit + cleanup)
    tokens = re.findall(r"[a-zA-Z0-9àèéìòù]{3,}", full_text_expanded.lower())
    tokens = [t for t in tokens if len(t) <= 24]
    keywords = sorted(set(tokens))[:80]

    # Hash
    source_id = raw.get("id") or sha1(url.encode("utf-8")).hexdigest()[:12]
    pv_for_hash = "" if prezzo_val is None else f"{prezzo_val:.2f}"
    hash_value = hashlib.md5(f"{source_name}-{title}-{pv_for_hash}-{url}".encode("utf-8")).hexdigest()

    if hash_value in _hash_cache:
        return None
    _hash_cache.add(hash_value)

    now_iso = datetime.now(UTC).isoformat()

    if prezzo_val is None:
        price_value = None
        price_display = ""
    else:
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
