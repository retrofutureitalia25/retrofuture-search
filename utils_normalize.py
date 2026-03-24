# ✅ utils_normalize.py — RetroFuture Italia 2026 (500k-ready)
#    • Filtro aste
#    • Filtro veicoli reali (NO annunci veicoli)
#    • Filtro ricambi veicoli
#    • Supporto extended modern JSON (robusto dict/list)
#    • Sinonimi backend
#    • ✅ Classificatore V2 universale a segnali generici
#    • ✅ NORMALIZZAZIONE CATEGORIE (coerente con filtri barra UI)
#    • ✅ Prezzo: parsing robusto (migliaia/decimali) -> float o None
#    • ✅ price_display IT (99.999,00 EUR)
#    • ✅ HASH STABILE: source + url (no prezzo) + legacy_hash per migrazione
#    • ✅ BLACKLIST robusta (dict/list/altro) -> mai crash
#    • ✅ modern_learned: entries (titoli completi con similarità) + phrases (solo trigger forti)
#    • ✅ SEARCH METADATA V2:
#         - item_type
#         - entity_family
#         - entity_model
#         - core_title_tokens
#         - title_phrase_norm
#############################################################

import re
import hashlib
import json
import os
import difflib
from hashlib import sha1
from datetime import datetime, UTC
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

from utils_log import log_event
from utils_synonyms import expand_with_synonyms

# debug
DEBUG_NORMALIZE = True


def _debug_reject(source_name, title, reason):
    if DEBUG_NORMALIZE:
        log_event(source_name, f'FILTER ❌ {reason} → {title}')


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
# Helpers base
#############################################################

def _norm(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _tokenize(s: str) -> list:
    s = _norm(s)
    return re.findall(r"[a-z0-9àèéìòù]+", s)


def _contains_term(text: str, term: str) -> bool:
    """
    Match sicuro su parola o frase:
    - evita falsi positivi tipo 'modern' dentro 'modernariato'
    - supporta frasi multi-parola
    """
    text_n = _norm(text)
    term_n = _norm(term)
    if not text_n or not term_n:
        return False

    if " " in term_n:
        return f" {term_n} " in f" {text_n} "

    return bool(re.search(rf"(?<![a-z0-9àèéìòù]){re.escape(term_n)}(?![a-z0-9àèéìòù])", text_n))


def _count_term_hits(text: str, terms, cap=None) -> int:
    count = 0
    for term in terms:
        if _contains_term(text, term):
            count += 1
            if cap and count >= cap:
                break
    return count


def _first_term_hits(text: str, terms, cap=None) -> list:
    hits = []
    for term in terms:
        if _contains_term(text, term):
            hits.append(term)
            if cap and len(hits) >= cap:
                break
    return hits


def _is_measure_only(p: str) -> bool:
    p = _norm(p)
    return bool(re.match(r"^\d{1,4}\s?(mm|cm|hz|w|kg|gb|tb)$", p))


def _is_strong_phrase(p: str) -> bool:
    p = _norm(p)
    if not p or len(p) < 3:
        return False
    if _is_measure_only(p):
        return False

    toks = _tokenize(p)
    if len(toks) <= 3:
        return True
    if re.match(r"^\d{2,4}(gb|tb)$", p):
        return True

    return False


def _similar(a: str, b: str) -> float:
    a = _norm(a)
    b = _norm(b)
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()


#############################################################
# Liste
#############################################################

vintage_memory = load_json("vintage_memory.json") or {
    "phrase_boosts": {},
    "phrase_penalties": {},
    "history": []
}

# ============================================================
# modern_learned.json (supporta: list OR dict con "phrases" + "entries")
# - entries: match quasi-esatto con similarità sul titolo
# - phrases: SOLO trigger forti
# ============================================================

raw_ml = load_json("modern_learned.json")

modern_phrases_all = []
modern_entries_titles = []

if isinstance(raw_ml, list):
    modern_phrases_all = [str(x).strip().lower() for x in raw_ml if str(x).strip()]
elif isinstance(raw_ml, dict):
    modern_phrases_all = [str(x).strip().lower() for x in raw_ml.get("phrases", []) if str(x).strip()]
    for e in (raw_ml.get("entries", []) or []):
        if isinstance(e, dict):
            t = str(e.get("title") or "").strip()
            if t:
                modern_entries_titles.append(t)

modern_learned_strong = set([p for p in modern_phrases_all if _is_strong_phrase(p)])

_modern_entry_index = {}
for t in modern_entries_titles:
    toks = _tokenize(t)
    if not toks:
        continue
    k = toks[0]
    _modern_entry_index.setdefault(k, []).append(t)

modern_learned = modern_learned_strong

# learn_queue.json deve essere dict {"candidates":[...]}
learn_queue = load_json("learn_queue.json")
if not isinstance(learn_queue, dict):
    learn_queue = {"candidates": []}
if "candidates" not in learn_queue or not isinstance(learn_queue["candidates"], list):
    learn_queue["candidates"] = []


def add_to_learn_queue(term, context):
    term = (term or "").strip().lower()
    if len(term) < 3:
        return
    for c in learn_queue["candidates"]:
        if c.get("term") == term:
            return
    learn_queue["candidates"].append({
        "term": term,
        "context": (context or "")[:120],
        "added_at": datetime.now(UTC).isoformat()
    })
    save_json("learn_queue.json", learn_queue)


# ✅ BLACKLIST robusta
raw_bl = load_json("vintage_blacklist.json")
if isinstance(raw_bl, dict):
    blacklist = set([str(x).strip().lower() for x in raw_bl.get("words_block", []) if str(x).strip()])
elif isinstance(raw_bl, list):
    blacklist = set([str(x).strip().lower() for x in raw_bl if str(x).strip()])
else:
    blacklist = set()

# keywords.json può essere list o dict
keywords_raw = load_json("keywords.json")
if isinstance(keywords_raw, dict):
    vintage_terms = {str(k).strip().lower() for k in keywords_raw.keys() if str(k).strip()}
elif isinstance(keywords_raw, list):
    vintage_terms = {str(k).strip().lower() for k in keywords_raw if str(k).strip()}
else:
    vintage_terms = set()

retro_terms = {k for k in vintage_terms if ("retro" in k or "retrò" in k or "vintage" in k)}
retro_terms |= {"retro", "retrò", "stile vintage", "look retrò", "look retro", "vintage style"}


#############################################################
# Modern EXTENDED
#############################################################

modern_ext = load_json("modern_keywords_extended.json")
modern_ext_terms = set()

if isinstance(modern_ext, dict):
    for _, values in modern_ext.items():
        if isinstance(values, list):
            for v in values:
                s = str(v).strip().lower()
                if s:
                    modern_ext_terms.add(s)
elif isinstance(modern_ext, list):
    modern_ext_terms = {str(v).strip().lower() for v in modern_ext if str(v).strip()}


#############################################################
# ERA detection
#############################################################

def detect_era(text):
    text = (text or "").lower()

    m = re.search(r"\b19([5-9][0-9])\b", text)
    if m:
        decade = m.group(1)[0] + "0"
        return f"anni_{decade}"

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
# VINTAGE CLASSIFIER V2 — universale a segnali generici
#############################################################

def classify_vintage_status(raw_text, expanded_text=None, title_hint: str = ""):
    raw_text = raw_text or ""
    raw_low = _norm(raw_text)
    text_low = _norm(expanded_text or raw_text)
    title_raw = (title_hint or raw_text or "").strip()
    title_low = _norm(title_raw)

    #########################################################
    # 1. FAMILY SIGNALS
    #########################################################

    # -------------------------
    # Time / historical signals
    # -------------------------
    time_terms = [
        "anni 50", "anni 60", "anni 70", "anni 80", "anni 90",
        "50s", "60s", "70s", "80s", "90s",
        "d'epoca", "di epoca",
        "primi 900", "primo 900", "metà 900", "meta 900", "fine 900",
        "primi '900", "primo '900", "metà '900", "meta '900", "fine '900",
        "primi 800", "primo 800", "metà 800", "meta 800", "fine 800",
        "primi '800", "primo '800", "metà '800", "meta '800", "fine '800",
        "xix secolo", "xx secolo",
        "inizio xx secolo", "fine xix secolo",
        "prima metà 900", "seconda metà 900",
        "prima metà 800", "seconda metà 800",
        "novecento", "ottocento",
        "prima serie", "prima edizione", "edizione originale",
    ]

    time_hits = _first_term_hits(text_low, time_terms, cap=8)

    year_old_hits = re.findall(r"\b(19[0-9]{2}|200[0-4])\b", text_low)
    year_recent_hits = re.findall(r"\b(20(0[5-9]|1[0-9]|2[0-9]))\b", text_low)
    roman_century_hits = re.findall(r"\b(xix|xx)\s*secolo\b", text_low)

    time_signal = 0
    if time_hits:
        time_signal += 2
    if year_old_hits:
        time_signal += 2
    if roman_century_hits:
        time_signal += 2
    time_signal = min(time_signal, 4)

    # -------------------------
    # Authenticity signals
    # -------------------------
    authenticity_terms = [
        "vintage originale",
        "originale d'epoca",
        "originale di epoca",
        "da collezione",
        "collezionismo",
        "modernariato",
        "antiquariato",
        "manifattura",
        "edizione originale",
        "lotto d'epoca",
        "usato d'epoca",
        "vintage",
        "epoca",
        "antico",
        "antica",
        "antichi",
        "antiche",
    ]
    authenticity_hits = _first_term_hits(raw_low, authenticity_terms, cap=8)

    authenticity_signal = 0
    if authenticity_hits:
        authenticity_signal += 2
    if any(t in authenticity_hits for t in [
        "modernariato", "antiquariato", "manifattura",
        "originale d'epoca", "originale di epoca"
    ]):
        authenticity_signal += 1
    authenticity_signal = min(authenticity_signal, 3)

    # -------------------------
    # Object signals (trasversali)
    # -------------------------
    object_terms = [
        # tecnologia / media
        "computer", "console", "telefono", "cellulare", "monitor", "stampante",
        "tastiera", "mouse", "joystick", "registratore", "videoregistratore",
        "radio", "stereo", "giradischi", "lettore", "amplificatore", "sintoamplificatore",
        "receiver", "tuner", "walkman", "fotocamera", "macchina fotografica",
        "cinepresa", "videocamera", "televisore",

        # supporti / editoria / collezionismo
        "vinile", "lp", "vhs", "cassetta", "musicassetta", "floppy", "cartolina",
        "poster", "locandina", "manifesto", "rivista", "giornale", "fumetto",
        "libro", "manuale", "enciclopedia", "album figurine", "figurine",
        "francobolli", "monete", "banconote", "medaglie", "souvenir",

        # arredamento / casa
        "lampada", "lampadario", "applique", "sedia", "sedie", "tavolo", "tavolino",
        "poltrona", "divano", "credenza", "mobile", "armadio", "comodino", "libreria",
        "specchio", "mensola", "consolle", "colonna", "cassettone", "centrotavola",
        "vaso", "ciotola", "tappeto",

        # moda / accessori
        "borsa", "giacca", "abito", "vestito", "camicia", "cappotto", "scarpe",
        "stivali", "occhiali", "orologio", "collana", "bracciale", "anello", "cintura",

        # giochi / giocattoli
        "lego", "barbie", "playmobil", "trenino", "robot", "bambola", "modellino",
        "macchinina", "action figure", "puzzle",

        # cucina / cartoleria
        "caffettiera", "moka", "piatti", "bicchieri", "tazze", "pentole", "posate",
        "penna", "quaderno", "agenda", "diario", "matita",
    ]
    object_hits = _first_term_hits(text_low, object_terms, cap=10)

    # oggetto reale sì, ma da solo deve pesare meno
    object_signal = 2 if object_hits else 0

    # -------------------------
    # Specificity signals
    # -------------------------
    title_tokens_raw = re.findall(r"\b[\w\-\/']+\b", title_raw)
    first_token = title_tokens_raw[0] if title_tokens_raw else ""

    common_first_words = {
        "vintage", "anni", "lotto", "set", "coppia", "gruppo", "mobile", "sedia",
        "lampada", "borsa", "giacca", "poster", "manuale", "libro", "radio",
        "telefono", "giradischi", "orologio", "specchio", "credenza", "tavolo",
        "vaso", "piatto", "ciotola", "abito", "scarpe", "lampade"
    }

    has_brand_like = False
    if first_token and first_token[0].isupper():
        if _norm(first_token) not in common_first_words and len(first_token) >= 3:
            has_brand_like = True

    model_code_patterns = [
        r"\b[a-z]{1,4}[- ]?\d{2,4}[a-z]?\b",
        r"\b\d{2,4}[a-z]{0,2}\b",
        r"\bmk[1-9]\b",
        r"\bse\/30\b",
        r"\biie\b", r"\biic\b", r"\biigs\b",
    ]
    model_code_hits = 0
    for pat in model_code_patterns:
        try:
            if re.search(pat, title_low):
                model_code_hits += 1
        except re.error:
            continue

    keyword_hits = min(3, _count_term_hits(raw_low, vintage_terms, cap=3))
    legacy_hits = _count_term_hits(
        text_low,
        [
            "commodore", "amiga", "macintosh", "olivetti", "atari", "zx spectrum",
            "walkman", "polaroid", "vhs", "vinile", "musicassetta", "floppy", "crt",
            "tubo catodico", "ms dos", "ms-dos", "isa", "8 bit", "16 bit", "33 giri",
            "45 giri", "album figurine", "modernariato", "antiquariato"
        ],
        cap=6
    )

    # bonus leggero per nomi propri / brand / designer / manifatture plausibili
    brand_like_bonus_terms = [
        "artemide", "flos", "oluce", "fontana", "fontana arte", "knoll", "kartell",
        "castiglioni", "venosta", "mangani", "baccarat", "bohemia",
        "murano", "valabrega", "lumina", "sirrah", "steiner", "seletti",
        "nason", "vista alegre", "daum", "limoges", "mendel heit"
    ]
    brand_like_bonus_hits = _first_term_hits(title_low, brand_like_bonus_terms, cap=4)

    specificity_signal = 0
    if model_code_hits >= 1:
        specificity_signal += 2
    if has_brand_like:
        specificity_signal += 1
    if keyword_hits >= 1:
        specificity_signal += 1
    if legacy_hits >= 1:
        specificity_signal += 1
    if brand_like_bonus_hits:
        specificity_signal += 1
    specificity_signal = min(specificity_signal, 4)

    # -------------------------
    # Historical material / style signal
    # -------------------------
    historical_material_terms = [
        "liberty", "space age", "mid-century", "mid century", "deco", "art déco", "art deco",
        "noce", "ottone", "bronzo", "bronzo dorato", "porcellana", "maiolica",
        "vetro di bohemia", "cristallo", "intagliata", "intagliato", "dorata", "dorato",
        "murano", "baccarat", "limoges", "onice", "radica"
    ]
    historical_material_hits = _first_term_hits(text_low, historical_material_terms, cap=6)
    historical_material_signal = 1 if historical_material_hits else 0

    # -------------------------
    # Coherence signal
    # -------------------------
    coherence_signal = 0
    if object_signal and (time_signal or authenticity_signal):
        coherence_signal += 1
    if object_signal and specificity_signal >= 2:
        coherence_signal += 1
    if legacy_hits >= 1 and (object_signal or time_signal or authenticity_signal):
        coherence_signal += 1
    if historical_material_signal and object_signal:
        coherence_signal += 1

    # più fiducia se si combinano almeno 3 famiglie positive
    positive_families = 0
    positive_families += 1 if time_signal >= 2 else 0
    positive_families += 1 if authenticity_signal >= 2 else 0
    positive_families += 1 if object_signal >= 2 else 0
    positive_families += 1 if specificity_signal >= 2 else 0
    positive_families += 1 if legacy_hits >= 1 else 0
    positive_families += 1 if historical_material_signal >= 1 else 0

    if positive_families >= 3:
        coherence_signal += 1

    coherence_signal = min(coherence_signal, 4)

    # -------------------------
    # Modern signal
    # -------------------------
    modern_hard_terms = [
        "iphone", "ipad", "airpods", "smartphone", "tablet",
        "smart tv", "4k", "8k", "oled", "qled", "uhd", "full hd",
        "bluetooth", "wireless", "wifi 6", "usb c", "type c",
        "ps5", "playstation 5", "nintendo switch", "switch oled",
        "apple watch", "smartwatch", "android"
    ]
    hard_modern_hits = _first_term_hits(raw_low, modern_hard_terms, cap=8)

    modern_soft_hits = []
    for term in modern_ext_terms:
        if _contains_term(raw_low, term):
            modern_soft_hits.append(term)
            if len(modern_soft_hits) >= 8:
                break

    tf_toks = _tokenize(title_low)
    if tf_toks:
        key = tf_toks[0]
        candidates = _modern_entry_index.get(key, [])
        for t in candidates:
            if _similar(title_low, t) >= 0.88:
                modern_soft_hits.append(f"entry:{t[:50]}")
                break

    for term in modern_learned_strong:
        if _contains_term(raw_low, term):
            modern_soft_hits.append(f"learned:{term}")
            if len(modern_soft_hits) >= 10:
                break

    modern_signal = 0
    if hard_modern_hits:
        modern_signal += 4
    if modern_soft_hits:
        modern_signal += 2
    if year_recent_hits:
        modern_signal += 2
    modern_signal = min(modern_signal, 6)

    # -------------------------
    # Style signal
    # -------------------------
    style_terms = [
        "stile vintage", "look retrò", "look retro", "stile retrò", "stile retro",
        "vintage style", "retro design", "industrial vintage", "shabby", "replica",
        "ristampa", "nuova collezione", "inspired", "ispirato",
        "stile anni 50", "stile anni 60", "stile anni 70", "stile anni 80", "stile anni 90"
    ]
    style_hits = _first_term_hits(text_low, style_terms, cap=8)
    style_signal = 2 if style_hits else 0

    #########################################################
    # 2. SCORE GLOBALE
    #########################################################

    score = 0
    score += (time_signal * 2)
    score += (authenticity_signal * 2)

    # ridotto: l'oggetto da solo non basta
    score += object_signal

    score += (specificity_signal * 2)
    score += (coherence_signal * 2)

    # piccolo boost da materiali / stili storici
    score += historical_material_signal

    # boost moderato dal vocabolario vintage del progetto
    if keyword_hits >= 1:
        score += 2
    if keyword_hits >= 2:
        score += 1

    # boost legacy/domain
    if legacy_hits >= 1:
        score += 1
    if legacy_hits >= 2:
        score += 1

    # penalità moderne / style-only
    score -= (modern_signal * 2)
    score -= style_signal

    #########################################################
    # 3. HARD REJECT CHIARI
    #########################################################

    positive_core = time_signal + authenticity_signal + object_signal + specificity_signal + coherence_signal + historical_material_signal

    # moderno forte senza vera struttura vintage
    if modern_signal >= 4 and positive_core <= 2:
        return "non_vintage", min(score, -10)

    # solo stile, nessuna struttura storica
    if style_signal >= 2 and time_signal == 0 and authenticity_signal == 0 and specificity_signal == 0 and historical_material_signal == 0 and modern_signal >= 2:
        return "non_vintage", min(score, -8)

    # score molto negativo e quasi nessun segnale positivo
    if score <= -6 and positive_core <= 2:
        return "non_vintage", score

    #########################################################
    # 4. LEARNING
    #########################################################

    if 0 <= score < 3:
        for w in re.findall(r"[a-zA-Z0-9àèéìòù]{4,}", raw_low):
            if w not in vintage_terms and w not in retro_terms and w not in blacklist:
                add_to_learn_queue(w, raw_low)

    #########################################################
    # 5. CLASSE FINALE
    #########################################################

    # molto solido: buona storicità + buona coerenza
    if score >= 10 and (time_signal >= 2 or authenticity_signal >= 2 or legacy_hits >= 1 or historical_material_signal >= 1):
        return "vintage_originale", score

    # struttura credibile e coerente
    if score >= 5:
        return "vintage_generico", score

    # borderline ma plausibile
    if score >= 3:
        return "vintage_dubbio", score

    # fallback dubbio: struttura minima credibile, senza modernità forte
    if (
        positive_core >= 3
        and object_signal >= 2
        and (
            authenticity_signal >= 2
            or specificity_signal >= 2
            or time_signal >= 2
            or legacy_hits >= 1
            or historical_material_signal >= 1
        )
        and modern_signal < 4
    ):
        return "vintage_dubbio", score

    return "non_vintage", score


def is_auction(text):
    if not text:
        return False
    t = text.lower()
    return any(k in t for k in [
        "offerta corrente", "offerta attuale", "offerte",
        "auction", "bid", "rilancio", "puntata"
    ])


#############################################################
# Filtro veicoli reali (NO annunci veicoli)
#############################################################

VEHICLE_LISTING_TERMS = [
    "auto", "automobile", "moto", "motocicletta", "scooter",
    "vespa", "lambretta", "camper", "furgone", "pickup"
]

VEHICLE_MODEL_TERMS = [
    "golf", "audi a3", "audi a4", "bmw serie", "mercedes classe",
    "fiat panda", "tmax", "xmax", "sh 125", "sh125", "kymco"
]

VEHICLE_COLLECTOR_SAFE_TERMS = [
    "manuale", "libro", "rivista", "brochure", "catalogo", "depliant",
    "modellino", "modellini", "poster", "locandina", "manifesto",
    "pubblicità", "pubblicita", "targa", "badge", "adesivo", "sticker",
    "automobilia", "ricambio", "ricambi", "accessorio", "accessori"
]


def is_vehicle_listing(text):
    t = _norm(text)

    has_vehicle = any(_contains_term(t, w) for w in VEHICLE_LISTING_TERMS)
    has_model = any(_contains_term(t, w) for w in VEHICLE_MODEL_TERMS)
    has_safe = any(_contains_term(t, w) for w in VEHICLE_COLLECTOR_SAFE_TERMS)

    # se sembra un veicolo vero e non un oggetto da collezione/contorno -> scarta
    if (has_vehicle or has_model) and not has_safe:
        return True

    return False


#############################################################
# Ricambi veicoli — CONTEXT AWARE
#############################################################

VEHICLE_PART_TERMS = [
    "paraurti", "faro", "fanale", "carena", "parafango",
    "carrozzeria", "ammortizzatore",
    "scarico", "marmitta", "pastiglie", "freni",
    "dischi freno", "centralina", "turbina",
    "alternatore", "motorino avviamento",
    "radiatore", "frizione", "pistone", "cilindro",
    "cinghia", "cinghia distribuzione",
    "cerchi", "cerchio", "pneumatici", "gomme",
    "specchietto", "specchietti", "manubrio", "forcella",
    "serbatoio", "carburatore", "iniettore",
    "cambio", "trasmissione", "catena", "corona", "pignone",
]

GENERIC_PART_WORDS = [
    "ricambi", "ricambio", "ricambistica",
    "kit", "set",
    "originale oem", "oem", "aftermarket",
    "compatibile", "compatibilità"
]

VEHICLE_WORDS = [
    "auto", "moto", "scooter", "vespa", "lambretta",
    "motocicletta", "motorino"
]

VEHICLE_HINT_PATTERNS = [
    r"\b(50|80|90|100|110|125|150|180|200|220|250|300|350|400|450|500|550|600|650|700|750|800|850|900|1000|1100|1200)\b",
    r"\b(cbr|gsxr|tmax|sh|pcx|yz|wr|rm|kx|r1|r6|cb|mt\-|xmax)\b",
    r"\b(audi|bmw|mercedes|volkswagen|fiat|ford|opel|toyota|honda|yamaha|kawasaki|suzuki|ducati|aprilia|piaggio)\b",
]

TECH_WHITELIST = [
    "commodore", "commodore 64", "c64", "amiga", "atari",
    "sinclair", "zx spectrum", "amstrad", "cpc",
    "ms dos", "ms-dos", "dos", "isa",
    "macintosh", "apple ii", "apple iie", "apple iic", "apple iigs",
    "quadra", "performa", "powermac", "power mac", "lc ii", "lc iii",
    "floppy", "floppy disk", "crt", "tubo catodico",
    "walkman", "vhs", "betamax", "video8",
]


def is_ricambio_veicoli(text):
    t = _norm(text)

    if any(_contains_term(t, w) for w in TECH_WHITELIST):
        return None

    has_vehicle = any(_contains_term(t, w) for w in VEHICLE_WORDS)
    has_vehicle_hint = any(re.search(p, t) for p in VEHICLE_HINT_PATTERNS)

    has_part_specific = any(_contains_term(t, p) for p in VEHICLE_PART_TERMS)
    has_part_generic = any(_contains_term(t, p) for p in GENERIC_PART_WORDS)

    if (has_vehicle or has_vehicle_hint) and has_part_specific:
        for p in VEHICLE_PART_TERMS:
            if _contains_term(t, p):
                return p
        return "ricambi_veicoli"

    if (has_vehicle or has_vehicle_hint) and has_part_generic:
        for p in GENERIC_PART_WORDS:
            if _contains_term(t, p):
                return p
        return "ricambi_veicoli"

    if _contains_term(t, "ricambi auto") or _contains_term(t, "ricambi moto") or _contains_term(t, "ricambi scooter"):
        return "ricambi"

    return None


#############################################################
# Duplicati (solo per run corrente)
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

CATEGORY_ALIASES = {
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
    "tv e audio": "tecnologia",
    "audio e hi fi": "tecnologia",
    "audio e hifi": "tecnologia",
    "informatica e accessori": "tecnologia",

    "casa": "arredamento",
    "arredo": "arredamento",
    "arredamento": "arredamento",
    "mobili": "arredamento",
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
    "casa e arredamento": "arredamento",
    "arredamento e casalinghi": "arredamento",
    "oggetti d'arredamento": "arredamento",

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
    "profumi": "moda_accessori",
    "cosmetici": "moda_accessori",
    "make up": "moda_accessori",
    "abbigliamento e accessori": "moda_accessori",
    "moda e accessori": "moda_accessori",
    "scarpe e borse": "moda_accessori",

    "giochi": "giochi_giocattoli",
    "giocattoli": "giochi_giocattoli",
    "toy": "giochi_giocattoli",
    "toys": "giochi_giocattoli",
    "bambini": "giochi_giocattoli",
    "lego": "giochi_giocattoli",
    "playmobil": "giochi_giocattoli",
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
    "giochi e giocattoli": "giochi_giocattoli",
    "prima infanzia": "giochi_giocattoli",

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
    "musica e film": "musica_cinema",
    "cd dvd vhs": "musica_cinema",
    "film e musica": "musica_cinema",

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
    "auto e moto": "auto_moto",
    "moto e scooter": "auto_moto",

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
    "libri e riviste": "libri_fumetti",
    "libri e fumetti": "libri_fumetti",

    "cucina": "cucina",
    "utensili": "cucina",
    "pentole": "cucina",
    "posate": "cucina",
    "piatti": "cucina",
    "bicchieri": "cucina",
    "tazze": "cucina",
    "servizio piatti": "cucina",
    "servizi piatti": "cucina",
    "caffettiera": "cucina",
    "moka": "cucina",
    "casalinghi": "cucina",
    "cucina e casalinghi": "cucina",

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
    "cancelleria": "cartoleria",
    "carta e cancelleria": "cartoleria",

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
    "collezionismo e rarità": "collezionismo",
    "rarità": "collezionismo",

    "vario": "vario",
    "altro": "vario",
    "misc": "vario",
    "miscellanea": "vario",
}


def _canon_cat(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("-", " ").replace("_", " ")
    s = re.sub(r"[\/\|\(\)\[\]\.\,\:\;]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace(" ", "_")
    return s


def normalize_category(raw_category: str, text_hint: str = "") -> str:
    c_raw = (raw_category or "").strip().lower()
    t = (text_hint or "").strip().lower()

    c = _canon_cat(c_raw)

    if c in ALLOWED_CATEGORIES:
        return c

    if c_raw in CATEGORY_ALIASES:
        return CATEGORY_ALIASES[c_raw]
    if c in CATEGORY_ALIASES:
        return CATEGORY_ALIASES[c]

    c_space = c.replace("_", " ")
    for k, v in CATEGORY_ALIASES.items():
        kk = (k or "").strip().lower()
        if not kk:
            continue
        if kk in c_raw or kk in c_space:
            return v

    blob = f"{c_space} {t}".strip()

    if any(k in blob for k in ["tecn", "computer", "console", "audio", "hifi", "hi fi", "videog", "amiga", "commodore", "walkman",]):
        return "tecnologia"
    if any(k in blob for k in ["arred", "mobili", "casa", "arredo", "lamp", "decor", "design", "tavolo", "sedia", "poltrona", "divano", "credenza", "armadio", "libreria"]):
        return "arredamento"
    if any(k in blob for k in ["abbigli", "scarpe", "bors", "gioiell", "orolog", "jeans", "giacca", "cappotto", "camicia"]):
        return "moda_accessori"
    if any(k in blob for k in ["giocatt", "giochi", "lego", "playmobil", "action figure", "barbie"]):
        return "giochi_giocattoli"
    if any(k in blob for k in ["vinile", "vinili", "cd", "dvd", "vhs", "cassetta", "cassett", "tape", "film", "colonna sonora", "soundtrack", "locandina"]):
        return "musica_cinema"
    if any(k in blob for k in ["vespa", "lambretta", "auto", "moto", "scooter", "fiat 500", "maggiolino", "automobilia"]):
        return "auto_moto"
    if any(k in blob for k in ["fumett", "libri", "rivist", "giornal", "manga", "topolino", "tex"]):
        return "libri_fumetti"
    if any(k in blob for k in ["cucin", "servizio", "piatti", "posate", "pentola", "caffettiera", "moka", "bicchier", "tazze", "bialetti", "cappuccino"]):
        return "cucina"
    if any(k in blob for k in ["cartoler", "penna", "matita", "quaderno", "agenda", "diario", "grafica", "stampa"]):
        return "cartoleria"
    if any(k in blob for k in ["collez", "figur", "poster", "cartolin", "francoboll", "monet", "banconot", "medagli", "militaria", "manifesto"]):
        return "collezionismo"

    return "vario"


#############################################################
# URL normalize (per hash stabile)
#############################################################

_TRACKING_KEYS_PREFIX = ("utm_",)
_TRACKING_KEYS_EXACT = {"gclid", "fbclid", "msclkid", "mkt_tok"}


def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    try:
        parts = urlsplit(url)
        scheme = parts.scheme.lower() or "https"
        netloc = parts.netloc.lower()
        path = parts.path or ""
        fragment = ""
        q = []
        for k, v in parse_qsl(parts.query, keep_blank_values=True):
            kl = (k or "").lower()
            if kl in _TRACKING_KEYS_EXACT:
                continue
            if any(kl.startswith(p) for p in _TRACKING_KEYS_PREFIX):
                continue
            q.append((k, v))
        query = urlencode(q, doseq=True)
        return urlunsplit((scheme, netloc, path, query, fragment))
    except Exception:
        return url


#############################################################
# Prezzo: parsing robusto (migliaia/decimali)
#############################################################

def parse_price_eur(value):
    if value is None:
        return None

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            v = float(value)
            return v if v >= 0 else None
        except Exception:
            return None

    s = str(value).strip()
    if not s:
        return None

    s = re.sub(r"[^\d\.\,]", "", s).strip()
    if not s or not re.search(r"\d", s):
        return None

    if re.match(r"^\d{1,3}(\.\d{3})+(,\d{1,2})?$", s):
        s = s.replace(".", "").replace(",", ".")
        try:
            v = float(s)
            return v if v >= 0 else None
        except Exception:
            return None

    if re.match(r"^\d{1,3}(,\d{3})+(\.\d{1,2})?$", s):
        s = s.replace(",", "")
        try:
            v = float(s)
            return v if v >= 0 else None
        except Exception:
            return None

    if "." in s and "," in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
        try:
            v = float(s)
            return v if v >= 0 else None
        except Exception:
            return None

    if "," in s:
        m = re.match(r"^(\d+),(\d{1,2})$", s)
        if m:
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
        try:
            v = float(s)
            return v if v >= 0 else None
        except Exception:
            return None

    if "." in s:
        m = re.match(r"^(\d+)\.(\d{1,2})$", s)
        if not m:
            if re.match(r"^\d{1,3}(\.\d{3})+$", s):
                s = s.replace(".", "")
        try:
            v = float(s)
            return v if v >= 0 else None
        except Exception:
            return None

    try:
        v = float(s)
        return v if v >= 0 else None
    except Exception:
        return None


def format_price_it(v):
    if v is None:
        return ""
    try:
        s = f"{float(v):,.2f}"
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        return f"{s} EUR"
    except Exception:
        return ""


#############################################################
# SEARCH METADATA V2
#############################################################

TITLE_STOPWORDS = {
    "con", "per", "da", "di", "del", "della", "dello", "delle", "degli",
    "in", "su", "a", "e", "ed", "o",
    "il", "lo", "la", "i", "gli", "le",
    "un", "uno", "una",
    "vintage", "retrò", "retro", "epoca",
    "anni", "anno",
    "originale", "originali",
    "usato", "usata", "nuovo", "nuova",
    "lotto", "set", "stock", "gruppo", "blocco", "collezione",
}

ACCESSORIO_TERMS = {
    "accessorio", "accessori", "cavo", "cavi", "adattatore", "adattatori",
    "alimentatore", "alimentatori", "joystick", "controller", "telecomando",
    "custodia", "cover", "supporto", "base", "mouse", "tastiera", "casse",
    "cuffie", "antenna", "caricatore", "caricatori", "pad", "volante",
    "portachiavi", "keychain", "copri", "copricomputer", "copri-computer"
}

SUPPORTO_TERMS = {
    "cassetta", "cassette", "floppy", "disco", "dischi", "disk", "cartuccia",
    "cartucce", "vinile", "vinili", "vhs", "cd", "dvd", "bluray", "blu ray",
    "musicassetta", "musicassette", "audiocassetta", "audiocassette",
    "cartridge", "cartridges", "tape", "tapes"
}

MAIN_OBJECT_TERMS = {
    "computer", "home computer", "pc", "console",
    "radio", "giradischi", "walkman", "fotocamera", "cinepresa",
    "televisore", "tv", "monitor", "telefono",
    "lampada", "poltrona", "sedia", "tavolo", "credenza", "mobile",
    "borsa", "orologio",
}

GAME_TERMS = {
    "gioco", "giochi", "game", "games", "videogioco", "videogame"

}

MEDIA_SOFTWARE_TERMS = {
    "casseta", "cassetta", "cassette", "floppy", "disk", "disco", "dischi",
    "cartuccia", "cartucce", "cartridge", "cartridges",
}

EDITORIA_TERMS = {
    "manuale", "manuali", "catalogo", "cataloghi", "brochure", "depliant",
    "rivista", "riviste", "giornale", "giornali", "fumetto", "fumetti",
    "libro", "libri", "enciclopedia", "istruzioni", "volantino",
}

LOTTO_TERMS = {
    "lotto", "stock", "blocco", "gruppo", "set", "collezione", "assortimento",
}

RICAMBIO_TERMS = {
    "ricambio", "ricambi", "ricambistica", "per pezzi", "parti",
    "compatibile", "compatibili", "non funzionante", "da riparare",
}

CORRELATO_TERMS = {
    "ispirato", "stile", "inspired", "replica", "ristampa",
}

ENTITY_FAMILY_TERMS = [
    "commodore",
    "amiga",
    "atari",
    "sinclair",
    "zx spectrum",
    "spectrum",
    "amstrad",
    "olivetti",
    "nintendo",
    "sega",
    "sony",
    "polaroid",
    "brionvega",
    "kartell",
    "stilnovo",
    "artemide",
    "flos",
    "gucci",
    "louis vuitton",
    "swatch",
    "casio",
    "seiko",
    "topolino",
    "diabolik",
    "tex",
    "zagor",
]

MODEL_PATTERNS = [
    (r"\bcommodore\s*64\b", "commodore_64"),
    (r"\bc64\b", "commodore_64"),
    (r"\bamiga\s*500\b", "amiga_500"),
    (r"\bamiga\s*1200\b", "amiga_1200"),
    (r"\batari\s*2600\b", "atari_2600"),
    (r"\bolivetti\s*lettera\s*32\b", "olivetti_lettera_32"),
    (r"\bzx\s*spectrum\b", "zx_spectrum"),
]


def _normalize_title_phrase(title: str) -> str:
    t = _norm(title)
    t = t.replace("/", " ").replace("-", " ")
    t = re.sub(r"[^\w\sàèéìòù]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _extract_core_title_tokens(title: str, max_tokens: int = 8) -> list:
    t = _normalize_title_phrase(title)
    toks = re.findall(r"[a-z0-9àèéìòù]+", t)

    out = []
    seen = set()

    for tok in toks:
        if len(tok) < 2:
            continue
        if tok in TITLE_STOPWORDS:
            continue
        if tok not in seen:
            seen.add(tok)
            out.append(tok)
        if len(out) >= max_tokens:
            break

    return out


def _contains_token_phrase(text: str, phrase: str) -> bool:
    text_n = f" {_normalize_title_phrase(text)} "
    phrase_n = f" {_normalize_title_phrase(phrase)} "
    return phrase_n in text_n


def _detect_entity_family(title: str, core_tokens=None) -> str:
    title_n = _normalize_title_phrase(title)

    for fam in ENTITY_FAMILY_TERMS:
        if _contains_token_phrase(title_n, fam):
            return fam.replace(" ", "_")

    core_tokens = core_tokens or []
    if core_tokens:
        first = core_tokens[0]
        if len(first) >= 3 and first not in {
            "manuale", "catalogo", "lotto", "set", "gioco", "giochi",
            "accessorio", "accessori", "borsa", "libro", "rivista",
            "fumetto", "poster", "lampada", "sedia", "mobile"
        }:
            return first

    return ""


def _detect_entity_model(title: str) -> str:
    title_n = _normalize_title_phrase(title)

    for pattern, model in MODEL_PATTERNS:
        if re.search(pattern, title_n):
            return model

    return ""


def _detect_item_type(title: str, description: str = "", category: str = "") -> str:
    title_n = _normalize_title_phrase(title)
    desc_n = _normalize_title_phrase(description)
    cat_n = _normalize_title_phrase(category)

    full_n = f"{title_n} {desc_n} {cat_n}".strip()

    title_tokens = set(re.findall(r"[a-z0-9àèéìòù]+", title_n))
    full_tokens = set(re.findall(r"[a-z0-9àèéìòù]+", full_n))

    def has_any(term_set, in_title_only=False):
        source_text = title_n if in_title_only else full_n
        source_tokens = title_tokens if in_title_only else full_tokens

        for term in term_set:
            if " " in term:
                if _contains_token_phrase(source_text, term):
                    return True
            elif term in source_tokens:
                return True
        return False

    has_model = bool(_detect_entity_model(title))
    has_family = bool(_detect_entity_family(title, _extract_core_title_tokens(title)))

    has_main_object = has_any(MAIN_OBJECT_TERMS, in_title_only=True)
    has_accessorio = has_any(ACCESSORIO_TERMS, in_title_only=True)
    has_supporto = has_any(SUPPORTO_TERMS, in_title_only=True)
    has_editoria = has_any(EDITORIA_TERMS, in_title_only=True)
    has_lotto = has_any(LOTTO_TERMS, in_title_only=True)
    has_ricambio = has_any(RICAMBIO_TERMS, in_title_only=True)
    has_correlato = has_any(CORRELATO_TERMS, in_title_only=True)
    has_game = has_any(GAME_TERMS, in_title_only=True)
    has_media_software = has_any(MEDIA_SOFTWARE_TERMS, in_title_only=True)

    # blocco extra: termini che NON devono essere promossi a main_item
    title_block_terms = {
        "videogioco", "videogame", "gioco", "giochi",
        "guida", "manuale", "manuali", "corso",
        "rivista", "riviste", "pubblicita", "pubblicità",
        "catalogo", "cataloghi", "brochure", "depliant",
        "cassetta", "cassette", "floppy", "cartuccia", "cartucce",
        "quickstart", "istruzioni", "booklet", "software", "programma"
        "manager", "arcade", "decathlon", "strider"
    }
    has_title_block = any(term in title_tokens for term in title_block_terms)

    # ordine importante: prima i casi da abbassare
    if has_ricambio:
        return "ricambio"

    if has_lotto:
        return "lotto"

    if has_editoria:
        return "editoria"

    # giochi / software / supporti
    if has_game or has_media_software or has_supporto:
        return "supporto"

    # accessori SEMPRE accessori, anche se citano marca/modello
    if has_accessorio:
        return "accessorio"

    if has_correlato:
        return "correlato"

    # vero oggetto principale esplicito
    if has_main_object:
        return "main_item"

    # fallback più intelligente:
    # se il titolo identifica chiaramente una famiglia/modello forte
    # e NON contiene segnali da accessorio/supporto/editoria/lotto/ricambio/correlato
    # né parole-titolo tipiche di giochi/manuali/corsi/pubblicità/supporti,
    # promuovilo a main_item
    if (has_model or has_family) and not (
        has_accessorio or has_supporto or has_editoria or
        has_lotto or has_ricambio or has_correlato or
        has_game or has_media_software or has_title_block
    ):
        return "main_item"

    return "correlato"


#############################################################
# NORMALIZZAZIONE
#############################################################

def normalizza_annuncio(raw, source_name):
    title = (raw.get("title") or raw.get("titolo") or "").strip()
    description = raw.get("description") or raw.get("descrizione") or title

    full_text_raw = f"{title} {description}".strip().lower()

    if not title or title.lower() in ("titolo non disponibile", "n/a", "none"):
        _debug_reject(source_name, title or "[NO TITLE]", "Titolo mancante")
        return None

    url = (raw.get("url") or raw.get("link") or "").strip()
    if not url:
        _debug_reject(source_name, title, "URL mancante")
        return None

    url_norm = normalize_url(url)

    # BLACKLIST
    for bad in blacklist:
        if _contains_term(full_text_raw, bad):
            _debug_reject(source_name, title, f'Blacklist: "{bad}"')
            return None

    # Aste
    if is_auction(title) or is_auction(description):
        _debug_reject(source_name, title, "Asta")
        return None

    # ❌ Veicoli veri
    if is_vehicle_listing(full_text_raw):
        log_event(source_name, f'❌ Veicolo scartato: "{title}"')
        _debug_reject(source_name, title, "Veicolo")
        return None

    # ❌ Ricambi veicoli
    term = is_ricambio_veicoli(full_text_raw)
    if term:
        log_event(source_name, f'❌ Ricambio VEICOLI scartato: "{title}" — trovato: "{term}"')
        _debug_reject(source_name, title, f'Ricambio veicoli: "{term}"')
        return None

    # Sinonimi
    try:
        full_text_expanded = expand_with_synonyms(full_text_raw)
    except Exception:
        full_text_expanded = full_text_raw

    # Vintage classification
    vintage_class, score = classify_vintage_status(
        full_text_raw,
        full_text_expanded,
        title_hint=title
    )

    if vintage_class == "non_vintage":
        _debug_reject(source_name, title, f"Non vintage (score={score})")
        return None

    if score < 0:
        _debug_reject(source_name, title, f"Score negativo (score={score}, class={vintage_class})")
        return None

    era = detect_era(full_text_expanded)

    prezzo_raw = raw.get("price") or raw.get("prezzo")
    prezzo_val = parse_price_eur(prezzo_raw)

    image = (raw.get("image") or raw.get("img") or raw.get("immagine") or "").strip()
    location = (raw.get("location") or "").strip()

    category_raw = raw.get("category") or raw.get("categoria") or ""
    category = normalize_category(category_raw, text_hint=full_text_expanded)

    condition = raw.get("condition") or raw.get("condizione")

    tokens = re.findall(r"[a-zA-Z0-9àèéìòù]{3,}", full_text_expanded.lower())
    tokens = [t for t in tokens if len(t) <= 24]
    keywords = sorted(set(tokens))[:80]

    # fallback intelligente anti-vario
    if category == "vario":
        try:
            from detect_category import detect_category
            guessed_category = detect_category({
                "title": title,
                "description": description,
                "keywords": keywords,
                "category": category_raw,
           })
            if guessed_category and guessed_category != "vario":
               category = guessed_category
        except Exception:
            pass

    title_phrase_norm = _normalize_title_phrase(title)
    core_title_tokens = _extract_core_title_tokens(title)
    item_type = _detect_item_type(title, description, category)
    entity_family = _detect_entity_family(title, core_title_tokens)
    entity_model = _detect_entity_model(title)

    source_id = raw.get("id") or sha1(url_norm.encode("utf-8")).hexdigest()[:12]

    hash_value = hashlib.md5(f"{source_name}|{url_norm}".encode("utf-8")).hexdigest()

    pv_for_hash = "" if prezzo_val is None else f"{prezzo_val:.2f}"
    legacy_hash = hashlib.md5(f"{source_name}-{title}-{pv_for_hash}-{url}".encode("utf-8")).hexdigest()

    if hash_value in _hash_cache:
        _debug_reject(source_name, title, "Duplicato run corrente")
        return None
    _hash_cache.add(hash_value)

    now_iso = datetime.now(UTC).isoformat()

    price_value = prezzo_val
    price_display = format_price_it(prezzo_val) if prezzo_val is not None else ""

    if DEBUG_NORMALIZE and vintage_class == "vintage_dubbio":
        log_event(source_name, f"FILTER ⚠️ DUBBIO (score={score}) → {title}")


    return {
        "source": source_name,
        "source_id": source_id,
        "title": title,
        "description": description,
        "price_value": price_value,
        "price_display": price_display,
        "price_currency": "EUR",
        "url": url_norm,
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
        "legacy_hash": legacy_hash,
        "keywords": keywords,

        # SEARCH METADATA V2
        "item_type": item_type,
        "entity_family": entity_family,
        "entity_model": entity_model,
        "core_title_tokens": core_title_tokens,
        "title_phrase_norm": title_phrase_norm,
    }
