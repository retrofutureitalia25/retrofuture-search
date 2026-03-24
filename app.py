# app.py
import os
import json
import re
from datetime import datetime, timezone, timedelta

from flask import Flask, request, render_template, Response, jsonify, redirect
from pymongo import MongoClient
from dotenv import load_dotenv
from rapidfuzz import fuzz

from utils_learn_modern import extract_modern_terms
from utils_keyword_stats import register_query, register_click
from price_analyzer import analyze_prices

from price_snapshot_service import (
    should_save_snapshot,
    save_price_snapshot,
    calculate_price_trend,
)

# === Load ENV ===
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
SITE_URL = os.getenv("SITE_URL", "http://localhost:5000")

# === Flask ===
app = Flask(__name__)

DB_NAME = "database_vintage"
COLLECTION_NAME = "annunci"

if MONGO_URI:
    _bootstrap_client = MongoClient(MONGO_URI)
    _bootstrap_db = _bootstrap_client[DB_NAME]
    _bootstrap_price_snapshots = _bootstrap_db["price_snapshots"]
    _bootstrap_price_snapshots.create_index([("query", 1), ("created_at", -1)])
    _bootstrap_price_snapshots.create_index("created_at")
    _bootstrap_client.close()

# ============================================================
# CONFIG UI / LOGICA
# ============================================================
SOFT_HIDE_SOURCES = {"mercatinousato"}

# ============================================================
# CONFIG NOIMAGE (solo Mercatinousato)
# ============================================================
NOIMAGE_ENABLED = os.getenv("NOIMAGE_ENABLED", "1") != "0"
NOIMAGE_SOURCES = {"mercatinousato"}
NOIMAGE_HITS_REQUIRED = int(os.getenv("NOIMAGE_HITS_REQUIRED", "2"))
NOIMAGE_COOLDOWN_MINUTES = int(os.getenv("NOIMAGE_COOLDOWN_MINUTES", "60"))
NOIMAGE_MAX_BODY = 4096

# ============================================================
# CONFIG TYPO VOCAB CACHE
# ============================================================
TYPO_VOCAB_CACHE_FILE = os.getenv("TYPO_VOCAB_CACHE_FILE", "typo_vocab_cache.json")
TYPO_VOCAB_REBUILD_CACHE = os.getenv("TYPO_VOCAB_REBUILD_CACHE", "0") == "1"
TYPO_VOCAB_DB_LIMIT = int(os.getenv("TYPO_VOCAB_DB_LIMIT", "20000"))
TYPO_VOCAB_DB_MIN_FREQ = int(os.getenv("TYPO_VOCAB_DB_MIN_FREQ", "2"))

# ============================================================
# CONFIG PRICE ANALYZER
# ============================================================
PRICE_ANALYSIS_LIMIT = 150

# semplice rate limit in-memory
_NOIMAGE_RL = {}


# ============================================================
# 🔹 UTIL: normalizzazione testo
# ============================================================
def _norm_text(s: str) -> str:
    if not s:
        return ""
    s = str(s).lower()
    s = s.replace("’", "'").replace("‘", "'")
    s = s.replace("`", "'")
    while "  " in s:
        s = s.replace("  ", " ")
    return s.strip()


# ============================================================
# 🔹 STEMMING LEGGERO
# ============================================================
def _stem(word):
    if not word:
        return word

    if word.endswith("i") and len(word) > 3:
        return word[:-1]
    if word.endswith("e") and len(word) > 3:
        return word[:-1]

    for suf in ["ina", "ine", "ino", "ini", "one", "oni"]:
        if word.endswith(suf) and len(word) > 4:
            return word[:-len(suf)]

    return word


def _tokenize(s: str):
    return [_stem(t) for t in _norm_text(s).split() if t]


def _generate_ngrams(tokens, max_len=4):
    ngrams = set()
    n = len(tokens)
    for i in range(n):
        for j in range(i + 1, min(i + max_len, n) + 1):
            ngrams.add(" ".join(tokens[i:j]))
    return ngrams


def _raw_query_tokens(q: str):
    qn = _norm_text(q)
    if not qn:
        return []

    toks = re.findall(r"[a-z0-9àèéìòù]+", qn)

    seen = set()
    out = []
    for t in toks:
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


# ============================================================
# 🔹 Load synonyms
# ============================================================
def load_synonyms():
    try:
        with open("synonyms.json", "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return {}

    norm_map = {}
    for key, lst in raw.items():
        k_norm = _norm_text(key)
        vals_norm = [_norm_text(v) for v in lst if _norm_text(v)]
        if k_norm:
            norm_map[k_norm] = vals_norm
    return norm_map


SINONIMI = load_synonyms()


# ============================================================
# 🔹 SINONIMI BIDIREZIONALI
# ============================================================
def build_bidirectional_synonyms(syn):
    bio = {}

    for key, lst in syn.items():
        key_n = _norm_text(key)

        if key_n not in bio:
            bio[key_n] = set()

        for v in lst:
            v_n = _norm_text(v)
            if not v_n:
                continue

            bio[key_n].add(v_n)

            if v_n not in bio:
                bio[v_n] = set()
            bio[v_n].add(key_n)

            for other in lst:
                o_n = _norm_text(other)
                if o_n and o_n != v_n:
                    bio[v_n].add(o_n)

    return {k: list(v) for k, v in bio.items()}


SINONIMI = build_bidirectional_synonyms(SINONIMI)


# ============================================================
# 🔹 Vocabolario sinonimi / espansioni
# ============================================================
def _build_synonym_vocabulary(syn_map):
    vocab = set()

    for key, vals in syn_map.items():
        nk = _norm_text(key)
        if nk:
            vocab.add(nk)

        for v in vals:
            nv = _norm_text(v)
            if nv:
                vocab.add(nv)

    return sorted(vocab)


SYNONYM_VOCAB = _build_synonym_vocabulary(SINONIMI)


def _fuzzy_synonym_candidates(token: str, limit=6, min_score=84):
    """
    Cerca nel vocabolario sinonimi anche termini digitati male:
    es. lamada -> lampada
        potrone -> poltrone / poltrona
    """
    token_n = _norm_text(token)
    if not token_n or len(token_n) < 4:
        return []

    scored = []
    for cand in SYNONYM_VOCAB:
        if abs(len(cand) - len(token_n)) > 3:
            continue

        score = fuzz.ratio(token_n, cand)
        if score >= min_score:
            scored.append((cand, score))

    scored.sort(key=lambda x: x[1], reverse=True)

    out = []
    seen = set()
    for cand, _ in scored[:limit]:
        if cand not in seen:
            seen.add(cand)
            out.append(cand)

    return out


# ============================================================
# 🔹 Vocabolario typo separato per suggerimenti UI
# ============================================================
TYPO_STOPWORDS = {
    "vintage", "retro", "retrò", "epoca",
    "anni", "anno",
    "con", "per", "di", "da", "del", "della", "dello", "delle", "degli",
    "in", "su", "a", "e", "ed", "o",
    "il", "lo", "la", "i", "gli", "le",
    "un", "uno", "una",
    "originale", "originali",
    "lotto", "set", "coppia", "pezzo", "pezzi",
    "nuovo", "nuova", "usato", "usata",
}

TYPO_STATIC_TERMS = {
    # categorie
    "tecnologia", "arredamento", "moda", "accessori",
    "giochi", "giocattoli", "musica", "cinema",
    "auto", "moto", "libri", "fumetti",
    "cucina", "cartoleria", "collezionismo", "vario",

    # epoche / termini correlati
    "anni", "50", "60", "70", "80", "90", "2000",

    # termini generici utili
    "lampada", "lampade", "poltrona", "poltrone",
    "sedia", "sedie", "tavolo", "tavoli",
    "radio", "giradischi", "vinile", "vinili",
    "poster", "locandina", "locandine",
    "borsa", "borse", "orologio", "orologi",
    "credenza", "specchio", "specchi",
    "vhs", "walkman", "floppy", "crt",
}


def _is_good_typo_token(token: str) -> bool:
    t = _norm_text(token)
    if not t:
        return False
    if len(t) < 3:
        return False
    if t in TYPO_STOPWORDS:
        return False
    if re.fullmatch(r"\d+", t):
        return False
    return True


def _tokens_from_text_for_typo(text: str):
    toks = _raw_query_tokens(text)
    return [t for t in toks if _is_good_typo_token(t)]


def _build_typo_vocab_base():
    vocab = set()

    # 1) termini da synonyms.json
    for term in SYNONYM_VOCAB:
        for tok in _tokens_from_text_for_typo(term):
            vocab.add(tok)

    # 2) termini statici di sicurezza
    for term in TYPO_STATIC_TERMS:
        for tok in _tokens_from_text_for_typo(term):
            vocab.add(tok)

    return sorted(vocab)


def _load_typo_vocab_from_db(limit_docs=20000, min_freq=2):
    """
    Estrae token frequenti dai titoli degli annunci.
    Serve solo per suggerimenti typo UI, non per la search.
    """
    vocab_count = {}
    client = None

    try:
        if not MONGO_URI:
            return []

        client = MongoClient(MONGO_URI)
        col = client[DB_NAME][COLLECTION_NAME]

        cursor = col.find(
            {
                "title": {"$exists": True, "$ne": ""},
                "vintage_class": {"$ne": "non_vintage"}
            },
            {"title": 1}
        ).limit(limit_docs)

        for doc in cursor:
            title = doc.get("title") or ""
            for tok in _tokens_from_text_for_typo(title):
                vocab_count[tok] = vocab_count.get(tok, 0) + 1

    except Exception:
        return []
    finally:
        if client:
            try:
                client.close()
            except Exception:
                pass

    out = []
    for tok, freq in vocab_count.items():
        if freq >= min_freq:
            out.append(tok)

    return sorted(out)


def _load_typo_vocab_cache(filepath=TYPO_VOCAB_CACHE_FILE):
    try:
        if not os.path.exists(filepath):
            return []

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict):
            words = data.get("words", [])
        elif isinstance(data, list):
            words = data
        else:
            return []

        out = []
        seen = set()
        for w in words:
            nw = _norm_text(w)
            if _is_good_typo_token(nw) and nw not in seen:
                seen.add(nw)
                out.append(nw)

        return sorted(out)

    except Exception:
        return []


def _save_typo_vocab_cache(words, filepath=TYPO_VOCAB_CACHE_FILE):
    try:
        safe_words = []
        seen = set()

        for w in words or []:
            nw = _norm_text(w)
            if _is_good_typo_token(nw) and nw not in seen:
                seen.add(nw)
                safe_words.append(nw)

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "count": len(safe_words),
            "words": sorted(safe_words),
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        return True
    except Exception:
        return False


def _build_typo_vocab_with_cache():
    """
    Ordine logico:
    1) base interna sempre disponibile
    2) cache file se esiste e non forzi rebuild
    3) altrimenti build da DB e salvataggio cache
    """
    vocab = set(_build_typo_vocab_base())

    cached_words = []
    if not TYPO_VOCAB_REBUILD_CACHE:
        cached_words = _load_typo_vocab_cache()

    if cached_words:
        vocab.update(cached_words)
        return sorted(vocab)

    db_words = _load_typo_vocab_from_db(
        limit_docs=TYPO_VOCAB_DB_LIMIT,
        min_freq=TYPO_VOCAB_DB_MIN_FREQ,
    )
    vocab.update(db_words)

    _save_typo_vocab_cache(sorted(vocab))
    return sorted(vocab)


TYPO_VOCAB = _build_typo_vocab_with_cache()


# ============================================================
# 🔹 Espansione sinonimi + typo correction search
# ============================================================
def _espandi_sinonimi(query: str):
    q_norm = _norm_text(query)
    if not q_norm:
        return []

    tokens = _tokenize(q_norm)
    raw_tokens = _raw_query_tokens(q_norm)

    if not tokens and not raw_tokens:
        return []

    ngrams = _generate_ngrams(tokens, max_len=4) if tokens else set()
    candidates = []

    # 1) Match esatto su sinonimi/ngram
    for key, lst in SINONIMI.items():
        if key in ngrams:
            candidates.append(key)
            candidates.extend(lst)

        for s in lst:
            if s in ngrams:
                candidates.append(key)
                candidates.extend(lst)
                break

    # 2) Correzione typo token per token SOLO per espansione sinonimi
    for tok in raw_tokens:
        if len(tok) < 4:
            continue

        fuzzy_cands = _fuzzy_synonym_candidates(tok, limit=6, min_score=84)

        for fc in fuzzy_cands:
            candidates.append(fc)

            if fc in SINONIMI:
                candidates.extend(SINONIMI.get(fc, []))

            for key, vals in SINONIMI.items():
                if fc == key or fc in vals:
                    candidates.append(key)
                    candidates.extend(vals)

    # 3) dedup ordinato
    seen = set()
    result = []
    for s in candidates:
        s_norm = _norm_text(s)
        if s_norm and s_norm not in seen:
            seen.add(s_norm)
            result.append(s_norm)

    return result


# ============================================================
# 🔹 Suggerimento correzione query UI
# ============================================================
def suggest_query_correction(query):
    raw_tokens = _raw_query_tokens(query)
    if not raw_tokens:
        return None

    corrected_tokens = []
    changed = False

    for tok in raw_tokens:
        tok_n = _norm_text(tok)

        if not _is_good_typo_token(tok_n):
            corrected_tokens.append(tok_n)
            continue

        best = tok_n
        best_score = 0

        for cand in TYPO_VOCAB:
            if abs(len(cand) - len(tok_n)) > 3:
                continue

            score = fuzz.ratio(tok_n, cand)

            if cand[:1] == tok_n[:1]:
                score += 2

            if score > best_score:
                best_score = score
                best = cand

        if best_score >= 82 and best != tok_n:
            corrected_tokens.append(best)
            changed = True
        else:
            corrected_tokens.append(tok_n)

    suggested = " ".join(corrected_tokens).strip()

    if changed and suggested and suggested != _norm_text(query):
        return suggested

    return None


# ============================================================
# 🔹 Fuzzy helper
# ============================================================
def fuzzy_match(query, text, threshold=65):
    if not query or not text:
        return False
    return fuzz.partial_ratio(query.lower(), text.lower()) >= threshold


# ============================================================
# 🔹 Ranking helpers
# ============================================================
def _query_tokens_for_rank(q: str):
    qn = _norm_text(q)
    if not qn:
        return []

    toks = [t for t in _tokenize(qn) if t and len(t) >= 2]

    seen = set()
    out = []
    for t in toks:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _build_query_regex(tokens):
    parts = []
    for t in tokens:
        nt = _norm_text(t)
        if not nt:
            continue
        parts.append(re.escape(nt))
    return "|".join(parts) if parts else ""


def _looks_generic_title(title: str):
    t = _norm_text(title)
    if not t:
        return True

    generic_titles = {
        "lampada", "lampade",
        "borsa", "borse",
        "cartolina", "cartoline",
        "poster", "locandina", "locandine",
        "radio", "giradischi", "orologio",
        "mobile", "credenza", "specchio",
        "libro", "libri", "manuale",
        "giacca", "scarpe",
        "vinile", "vhs",
        "tavolo", "sedia", "sedie",
        "lampadario",
    }

    if t in generic_titles:
        return True

    toks = re.findall(r"[a-z0-9àèéìòù]+", t)
    return len(toks) <= 1


def _class_weight_py(vclass: str):
    v = (vclass or "").strip().lower()
    if v == "vintage_originale":
        return 30.0
    if v == "vintage_generico":
        return 18.0
    if v == "vintage_dubbio":
        return 8.0
    return 0.0


def _recency_bonus_from_dt(dt_val):
    if not dt_val:
        return 0.0

    try:
        if isinstance(dt_val, str):
            dtp = datetime.fromisoformat(dt_val.replace("Z", "+00:00"))
        else:
            dtp = dt_val

        if getattr(dtp, "tzinfo", None) is None:
            dtp = dtp.replace(tzinfo=timezone.utc)

        age_days = (datetime.now(timezone.utc) - dtp).total_seconds() / 86400.0
    except Exception:
        return 0.0

    if age_days <= 1:
        return 4.0
    if age_days <= 3:
        return 3.0
    if age_days <= 7:
        return 2.0
    if age_days <= 14:
        return 1.0
    if age_days <= 30:
        return 0.5
    return 0.0


def _keyword_match_count_py(query_tokens, keywords):
    if not query_tokens or not keywords:
        return 0
    qset = {_norm_text(t) for t in query_tokens if _norm_text(t)}
    kset = {_norm_text(k) for k in keywords if _norm_text(k)}
    return len(qset.intersection(kset))


def _text_match_count_py(query_tokens, text):
    if not query_tokens or not text:
        return 0
    tlow = _norm_text(text)
    count = 0
    for t in query_tokens:
        nt = _norm_text(t)
        if nt and nt in tlow:
            count += 1
    return count


# ============================================================
# 🔹 Query-head helpers
# ============================================================
def _extract_query_heads(query: str, expanded_terms=None):
    qn = _norm_text(query)
    stop_words = {
        "vintage", "retrò", "retro", "epoca",
        "anni", "anno", "originale", "originali",
        "da", "collezione", "collezionismo",
        "stile", "look", "design",
        "moderno", "moderna", "modernariato", "antiquariato",
        "usato", "lotto", "set", "coppia", "gruppo",
        "pezzo", "pezzi", "nuovo", "nuova",
        "con", "per", "in", "su", "del", "della", "dello", "delle", "degli",
        "di", "de", "d"
    }

    heads = []

    for term in (expanded_terms or []):
        for t in _raw_query_tokens(_norm_text(term)):
            if len(t) >= 3 and t not in stop_words:
                heads.append(t)

    raw_tokens = _raw_query_tokens(qn)
    for t in raw_tokens:
        if len(t) >= 3 and t not in stop_words:
            heads.append(t)

    seen = set()
    out = []
    for h in heads:
        if h and h not in seen:
            seen.add(h)
            out.append(h)

    return out[:6]


def _head_term_regex_variants(term: str):
    t = _norm_text(term)
    if not t:
        return []

    variants = [rf"\b{re.escape(t)}\b"]

    if t.endswith("a") and len(t) >= 4:
        stem = re.escape(t[:-1])
        variants.append(rf"\b{stem}(a|e)\b")

    if t.endswith("o") and len(t) >= 4:
        stem = re.escape(t[:-1])
        variants.append(rf"\b{stem}(o|i)\b")

    if t.endswith("e") and len(t) >= 4:
        stem = re.escape(t[:-1])
        variants.append(rf"\b{stem}(e|i)\b")

    seen = set()
    out = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _build_heads_regex(heads):
    parts = []
    for h in heads or []:
        parts.extend(_head_term_regex_variants(h))
    parts = [p for p in parts if p]
    return "|".join(parts)


def _use_strict_head_filter(query: str, rank_tokens):
    qn = _norm_text(query)
    if not qn:
        return False

    raw_tokens = _raw_query_tokens(qn)
    if not raw_tokens:
        return False

    if re.search(r"\d", qn):
        return False

    weak = {
        "vintage", "retrò", "retro", "epoca",
        "anni", "anno", "originale", "originali",
        "stile", "look", "design",
        "di", "de", "d", "da", "del", "della", "dello", "delle", "degli"
    }

    strong_terms = [t for t in raw_tokens if t not in weak and len(t) >= 3]

    if len(strong_terms) == 1:
        return True

    if len(strong_terms) == 2 and len(raw_tokens) <= 3:
        return True

    return False


def _is_model_like_query(query: str):
    qn = _norm_text(query)
    if not qn:
        return False

    if re.search(r"\d", qn):
        return True

    specific_patterns = [
        r"\bzx spectrum\b",
        r"\bgame boy\b",
        r"\bwalkman\b",
        r"\bolivetti lettera\b",
    ]

    for pat in specific_patterns:
        if re.search(pat, qn):
            return True

    return False


def _detect_query_family(query: str):
    qn = _norm_text(query)
    if not qn:
        return ""

    family_terms = [
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
    ]

    for fam in family_terms:
        if fam in qn:
            return fam.replace(" ", "_")

    return ""


def _score_fuzzy_item(item, query_tokens):
    base_score = float(item.get("vintage_score") or 0)
    class_weight = _class_weight_py(item.get("vintage_class"))
    era_weight = 1.0 if (item.get("era") and item.get("era") != "vintage_generico") else 0.0
    recency_bonus = _recency_bonus_from_dt(item.get("updated_at") or item.get("created_at"))

    title = item.get("title") or ""
    description = item.get("description") or ""
    keywords = item.get("keywords") or []

    title_match_count = _text_match_count_py(query_tokens, title)
    desc_match_count = _text_match_count_py(query_tokens, description)
    keyword_match_count = _keyword_match_count_py(query_tokens, keywords)

    query_match_bonus = (title_match_count * 6.0) + (keyword_match_count * 4.0) + (desc_match_count * 1.0)
    generic_penalty = 2.0 if _looks_generic_title(title) else 0.0

    return (
        base_score
        + class_weight
        + era_weight
        + recency_bonus
        + query_match_bonus
        - generic_penalty
    )


# ============================================================
# 🔹 Parse sicuro prezzi
# ============================================================
_THOUSANDS_DOT_RE = re.compile(r"^\d{1,3}(\.\d{3})+(\,\d+)?$")
_THOUSANDS_COMMA_RE = re.compile(r"^\d{1,3}(,\d{3})+(\.\d+)?$")


def _parse_price(x):
    if x is None:
        return None

    if isinstance(x, (int, float)) and not isinstance(x, bool):
        try:
            return float(x)
        except Exception:
            return None

    s = str(x).strip()
    if not s:
        return None

    s = re.sub(r"[^\d\.,]", "", s)
    if not s:
        return None

    if _THOUSANDS_DOT_RE.match(s):
        s = s.replace(".", "")
        s = s.replace(",", ".")
    elif _THOUSANDS_COMMA_RE.match(s):
        s = s.replace(",", "")
    else:
        if "," in s and "." not in s:
            s = s.replace(",", ".")
        elif "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")

    try:
        return float(s)
    except Exception:
        return None


def _format_price_it(n):
    if n is None:
        return ""

    try:
        n = float(n)
    except Exception:
        return ""

    s = f"{n:.2f}"
    int_part, dec_part = s.split(".")
    int_part = f"{int(int_part):,}".replace(",", ".")
    return f"{int_part},{dec_part} EUR"


def _now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0)


def _client_ip():
    xff = request.headers.get("X-Forwarded-For") or ""
    if xff:
        return xff.split(",")[0].strip()
    return request.remote_addr or "unknown"


@app.route("/")
def index():
    return render_template("index.html")


###############################################################################
# REPORT NOIMAGE
###############################################################################
@app.route("/report_noimage", methods=["POST"])
def report_noimage():
    if not NOIMAGE_ENABLED:
        return jsonify({"status": "disabled"}), 200

    if request.content_length and request.content_length > NOIMAGE_MAX_BODY:
        return jsonify({"status": "error", "msg": "payload too large"}), 413

    data = request.get_json(silent=True) or {}
    item_hash = (data.get("hash") or "").strip()
    src = _norm_text(data.get("source") or "")
    img = (data.get("image") or "").strip()
    page_url = (data.get("page_url") or "").strip()

    if not item_hash:
        return jsonify({"status": "error", "msg": "missing hash"}), 400

    if src == "mercatino":
        src = "mercatinousato"

    if src and src not in NOIMAGE_SOURCES:
        return jsonify({"status": "ignored", "msg": "source not supported"}), 200

    ip = _client_ip()
    key = (ip, item_hash)
    now = _now_utc()

    last = _NOIMAGE_RL.get(key)
    if last and (now - last) < timedelta(minutes=NOIMAGE_COOLDOWN_MINUTES):
        return jsonify({"status": "throttled"}), 200
    _NOIMAGE_RL[key] = now

    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]
    try:
        doc = col.find_one({"hash": item_hash}, {"_id": 1, "source": 1, "status": 1, "expired_reason": 1})
        if not doc:
            return jsonify({"status": "error", "msg": "not found"}), 404

        doc_src = _norm_text(doc.get("source") or "")
        if doc_src == "mercatino":
            doc_src = "mercatinousato"

        if doc_src not in NOIMAGE_SOURCES:
            return jsonify({"status": "ignored", "msg": "doc source not supported"}), 200

        if doc.get("status") == "expired" and doc.get("expired_reason") == "deadlink":
            return jsonify({"status": "ok", "msg": "already deadlink-expired"}), 200

        update = {
            "$inc": {"noimage_hits": 1},
            "$set": {
                "noimage_last_at": now.isoformat(),
                "noimage_last_ip": ip,
            },
        }

        sample = {}
        if img:
            sample["noimage_last_image"] = img[:500]
        if page_url:
            sample["noimage_last_page"] = page_url[:500]
        if sample:
            update["$set"].update(sample)

        res = col.update_one({"_id": doc["_id"]}, update)
        if res.matched_count == 0:
            return jsonify({"status": "error", "msg": "update failed"}), 500

        doc2 = col.find_one({"_id": doc["_id"]}, {"noimage_hits": 1}) or {}
        hits = int(doc2.get("noimage_hits") or 0)

        expired_now = False
        if hits >= NOIMAGE_HITS_REQUIRED:
            col.update_one(
                {"_id": doc["_id"], "status": {"$ne": "expired"}},
                {"$set": {
                    "status": "expired",
                    "expired_at": now.isoformat(),
                    "expired_reason": "noimage",
                }}
            )
            expired_now = True

        return jsonify({"status": "ok", "hits": hits, "expired": expired_now}), 200

    finally:
        client.close()


###############################################################################
# SEARCH
###############################################################################
@app.route("/search")
def search():
    q = (request.args.get("q") or "").strip()

    if q:
        register_query(q)

    sinonimi_preview = _espandi_sinonimi(q) if q else []

    rank_query_tokens = _query_tokens_for_rank(q)
    for s in sinonimi_preview:
        for tok in _query_tokens_for_rank(s):
            if tok not in rank_query_tokens:
                rank_query_tokens.append(tok)

    rank_query_regex = _build_query_regex(rank_query_tokens)

    rank_query_phrase = _norm_text(q)

    rank_query_models = []
    q_norm_underscored = rank_query_phrase.replace(" ", "_")
    if q_norm_underscored:
        rank_query_models.append(q_norm_underscored)

    for s in sinonimi_preview:
        sn = _norm_text(s).replace(" ", "_")
        if sn and sn not in rank_query_models:
            rank_query_models.append(sn)

    query_heads = _extract_query_heads(q, sinonimi_preview)
    query_head = query_heads[0] if query_heads else ""
    query_head_regex = _build_heads_regex(query_heads)
    use_strict_head_filter = _use_strict_head_filter(q, rank_query_tokens)
    model_like_query = _is_model_like_query(q)
    detected_query_family = _detect_query_family(q)

    era = (request.args.get("era") or "").strip()
    category = (request.args.get("category") or "").strip()
    source = (request.args.get("source") or "").strip().lower()

    sort = (request.args.get("sort") or "score").strip()
    scope = (request.args.get("scope") or "").strip().lower()

    price_min_raw = request.args.get("price_min")
    price_max_raw = request.args.get("price_max")
    page = max(int(request.args.get("page", 1) or 1), 1)
    per_page = 50

    allowed_era = {
        "anni_50", "anni_60", "anni_70", "anni_80", "anni_90", "anni_2000",
        "vintage_generico",
    }
    allowed_sort = {"score", "date", "price_asc", "price_desc"}
    allowed_source = {"ebay", "vinted", "subito", "mercatinousato", "mercatino"}
    allowed_category = {
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

    if era and era not in allowed_era:
        era = ""
    if sort not in allowed_sort:
        sort = "score"
    if source and source not in allowed_source:
        source = ""

    category_norm = _norm_text(category)
    if category_norm and category_norm not in allowed_category:
        category_norm = ""

    if source == "mercatino":
        source = "mercatinousato"

    price_min = _parse_price(price_min_raw)
    price_max = _parse_price(price_max_raw)
    if price_min is not None and price_max is not None and price_min > price_max:
        price_min, price_max = price_max, price_min

    price_filter = {}
    if price_min is not None:
        price_filter["$gte"] = price_min
    if price_max is not None:
        price_filter["$lte"] = price_max

    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]
    price_snapshots = client[DB_NAME]["price_snapshots"]

    hide_dead = {
        "is_removed": {"$ne": True},
        "$nor": [
            {"status": "expired", "expired_reason": "deadlink"},
            {"status": "expired", "expired_reason": "noimage"},
        ],
    }

    soft_hide = {
        "$nor": [
            {"source": {"$in": list(SOFT_HIDE_SOURCES)}, "needs_check": True},
        ]
    }

    fallback_used = False
    fuzzy_used = False

    def build_query(query_terms):
        norm_terms = []
        regex_parts = []

        for t in query_terms:
            nt = _norm_text(t)
            if not nt:
                continue
            if len(nt) < 2 and " " not in nt:
                continue

            norm_terms.append(nt)
            pattern = re.escape(nt).replace(r"\ ", r"\s+")
            regex_parts.append(pattern)

        if not regex_parts:
            return {}

        regex = "|".join(regex_parts)

        return {
            "$or": [
                {"title": {"$regex": regex, "$options": "i"}},
                {"description": {"$regex": regex, "$options": "i"}},
                {"keywords": {"$in": norm_terms}},
            ]
        }

    def build_match(strict_mode: bool):
        if scope == "tutti":
            match = {**hide_dead, **soft_hide}
        else:
            match = {
                **hide_dead,
                **soft_hide,
                "vintage_class": {"$ne": "non_vintage"},
            }

        if scope != "tutti" and q:
            q_norm = _norm_text(q)
            tokens = _tokenize(q)
            search_terms = [q_norm] + tokens + sinonimi_preview

            seen_terms = set()
            search_terms_clean = []
            for term in search_terms:
                nt = _norm_text(term)
                if nt and nt not in seen_terms:
                    seen_terms.add(nt)
                    search_terms_clean.append(nt)

            query_block = build_query(search_terms_clean)
            if query_block:
                match.update(query_block)

            if strict_mode and use_strict_head_filter and query_head_regex:
                existing_or = match.pop("$or", None)
                and_parts = []

                if existing_or:
                    and_parts.append({"$or": existing_or})

                and_parts.append({
                    "title": {
                        "$regex": query_head_regex,
                        "$options": "i"
                    }
                })

                if "$and" in match and isinstance(match["$and"], list):
                    match["$and"].extend(and_parts)
                else:
                    match["$and"] = and_parts

        if scope != "tutti":
            if era:
                match["era"] = era
            if category_norm:
                match["category"] = category_norm
            if source:
                match["source"] = source

        return match

    def run_pipeline(strict_mode: bool, skip_value=0, limit_value=per_page):
        local_match = build_match(strict_mode)

        base_pipeline = [
            {"$match": local_match},

            {"$addFields": {
                "price_str_raw": {
                    "$convert": {
                        "input": {"$ifNull": ["$price_value", ""]},
                        "to": "string",
                        "onError": "",
                        "onNull": ""
                    }
                }
            }},

            {"$addFields": {
                "price_str": {
                    "$ifNull": [
                        {"$getField": {"field": "match", "input": {
                            "$regexFind": {
                                "input": "$price_str_raw",
                                "regex": r"[0-9\.,]+"
                            }
                        }}},
                        ""
                    ]
                }
            }},

            {"$addFields": {
                "price_norm": {
                    "$let": {
                        "vars": {"s": {"$toString": {"$ifNull": ["$price_str", ""]}}},
                        "in": {
                            "$switch": {
                                "branches": [
                                    {
                                        "case": {"$and": [
                                            {"$ne": [{"$indexOfBytes": ["$$s", "."]}, -1]},
                                            {"$ne": [{"$indexOfBytes": ["$$s", ","]}, -1]},
                                        ]},
                                        "then": {
                                            "$replaceAll": {
                                                "input": {
                                                    "$replaceAll": {"input": "$$s", "find": ".", "replacement": ""}
                                                },
                                                "find": ",",
                                                "replacement": "."
                                            }
                                        }
                                    },
                                    {
                                        "case": {
                                            "$and": [
                                                {"$ne": [{"$indexOfBytes": ["$$s", "."]}, -1]},
                                                {"$eq": [{"$indexOfBytes": ["$$s", ","]}, -1]},
                                                {"$regexMatch": {"input": "$$s", "regex": r"^\d{1,3}(\.\d{3})+$"}}
                                            ]
                                        },
                                        "then": {"$replaceAll": {"input": "$$s", "find": ".", "replacement": ""}}
                                    },
                                    {
                                        "case": {
                                            "$and": [
                                                {"$eq": [{"$indexOfBytes": ["$$s", "."]}, -1]},
                                                {"$ne": [{"$indexOfBytes": ["$$s", ","]}, -1]},
                                            ]
                                        },
                                        "then": {"$replaceAll": {"input": "$$s", "find": ",", "replacement": "."}}
                                    },
                                ],
                                "default": "$$s"
                            }
                        }
                    }
                }
            }},

            {"$addFields": {
                "price_num": {
                    "$convert": {
                        "input": "$price_norm",
                        "to": "double",
                        "onError": None,
                        "onNull": None
                    }
                },
                "updated_dt": {
                    "$convert": {"input": "$updated_at", "to": "date", "onError": None, "onNull": None}
                },
                "created_dt": {
                    "$convert": {"input": "$created_at", "to": "date", "onError": None, "onNull": None}
                },
                "base_dt": {"$ifNull": ["$updated_dt", {"$ifNull": ["$created_dt", datetime(1970, 1, 1)]}]},
                "era_weight": {"$cond": [{"$ne": ["$era", "vintage_generico"]}, 1, 0]},
            }},

            {"$addFields": {
                "age_days": {
                    "$divide": [
                        {"$subtract": ["$$NOW", "$base_dt"]},
                        86400000
                    ]
                }
            }},

            {"$addFields": {
                "recency_bonus": {
                    "$switch": {
                        "branches": [
                            {"case": {"$lte": ["$age_days", 1]}, "then": 4},
                            {"case": {"$lte": ["$age_days", 3]}, "then": 3},
                            {"case": {"$lte": ["$age_days", 7]}, "then": 2},
                            {"case": {"$lte": ["$age_days", 14]}, "then": 1},
                            {"case": {"$lte": ["$age_days", 30]}, "then": 0.5},
                        ],
                        "default": 0
                    }
                },

                "class_weight": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$vintage_class", "vintage_originale"]}, "then": 30},
                            {"case": {"$eq": ["$vintage_class", "vintage_generico"]}, "then": 18},
                            {"case": {"$eq": ["$vintage_class", "vintage_dubbio"]}, "then": 8},
                        ],
                        "default": 0
                    }
                },

                "generic_title_penalty": {
                    "$cond": [
                        {"$lte": [{"$strLenCP": {"$ifNull": ["$title", ""]}}, 12]},
                        2,
                        0
                    ]
                }
            }},
        ]

        if scope != "tutti" and q and rank_query_regex:
            base_pipeline.append({
                "$addFields": {
                    "title_match_count": {
                        "$size": {
                            "$regexFindAll": {
                                "input": {"$toLower": {"$ifNull": ["$title", ""]}},
                                "regex": rank_query_regex
                            }
                        }
                    },
                    "desc_match_count": {
                        "$size": {
                            "$regexFindAll": {
                                "input": {"$toLower": {"$ifNull": ["$description", ""]}},
                                "regex": rank_query_regex
                            }
                        }
                    },
                    "keyword_match_count": {
                        "$size": {
                            "$filter": {
                                "input": {"$ifNull": ["$keywords", []]},
                                "as": "kw",
                                "cond": {
                                    "$in": [{"$toLower": "$$kw"}, rank_query_tokens]
                                }
                            }
                        }
                    }
                }
            })

            if strict_mode and use_strict_head_filter and query_head_regex and query_heads:
                base_pipeline.append({
                    "$addFields": {
                        "head_title_match_count": {
                            "$size": {
                                "$regexFindAll": {
                                    "input": {"$toLower": {"$ifNull": ["$title", ""]}},
                                    "regex": query_head_regex
                                }
                            }
                        },
                        "head_keyword_match_count": {
                            "$size": {
                                "$filter": {
                                    "input": {"$ifNull": ["$keywords", []]},
                                    "as": "kw",
                                    "cond": {
                                        "$in": [{"$toLower": "$$kw"}, query_heads]
                                    }
                                }
                            }
                        }
                    }
                })

                base_pipeline.append({
                    "$addFields": {
                        "query_match_bonus": {
                            "$add": [
                                {"$multiply": ["$title_match_count", 4]},
                                {"$multiply": ["$keyword_match_count", 3]},
                                {"$multiply": ["$desc_match_count", 1]},
                                {"$multiply": ["$head_title_match_count", 12]},
                                {"$multiply": ["$head_keyword_match_count", 8]}
                            ]
                        }
                    }
                })
            else:
                base_pipeline.append({
                    "$addFields": {
                        "head_title_match_count": 0,
                        "head_keyword_match_count": 0,
                        "query_match_bonus": {
                            "$add": [
                                {"$multiply": ["$title_match_count", 6]},
                                {"$multiply": ["$keyword_match_count", 4]},
                                {"$multiply": ["$desc_match_count", 1]}
                            ]
                        }
                    }
                })
        else:
            base_pipeline.append({
                "$addFields": {
                    "title_match_count": 0,
                    "desc_match_count": 0,
                    "keyword_match_count": 0,
                    "head_title_match_count": 0,
                    "head_keyword_match_count": 0,
                    "query_match_bonus": 0
                }
            })

        base_pipeline.append({
            "$addFields": {
                "entity_model_match": {
                    "$cond": [
                        {"$in": ["$entity_model", rank_query_models]},
                        1,
                        0
                    ]
                },
                "entity_family_match": {
                    "$cond": [
                        {"$in": ["$entity_family", rank_query_tokens]},
                        1,
                        0
                    ]
                },
                "wrong_family_penalty": {
                    "$cond": [
                        {
                            "$and": [
                                {"$eq": [model_like_query, True]},
                                {"$ne": [detected_query_family, ""]},
                                {"$ne": [{"$ifNull": ["$entity_family", ""]}, ""]},
                                {"$ne": ["$entity_family", detected_query_family]}
                            ]
                        },
                        45,
                        0
                    ]
                },
                "title_phrase_match": {
                    "$cond": [
                        {
                            "$and": [
                                {"$ne": [rank_query_phrase, ""]},
                                {
                                    "$regexMatch": {
                                        "input": {"$ifNull": ["$title_phrase_norm", ""]},
                                        "regex": re.escape(rank_query_phrase)
                                    }
                                }
                            ]
                        },
                        1,
                        0
                    ]
                },
                "core_tokens_match": {
                    "$size": {
                        "$filter": {
                            "input": {"$ifNull": ["$core_title_tokens", []]},
                            "as": "ct",
                            "cond": {
                                "$in": ["$$ct", rank_query_tokens]
                            }
                        }
                    }
                }
            }
        })

        base_pipeline.append({
            "$addFields": {
                "item_type_weight": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$item_type", "main_item"]}, "then": 120 if model_like_query else 40},
                            {"case": {"$eq": ["$item_type", "accessorio"]}, "then": -120 if model_like_query else -20},
                            {"case": {"$eq": ["$item_type", "supporto"]}, "then": -140 if model_like_query else -25},
                            {"case": {"$eq": ["$item_type", "editoria"]}, "then": -90 if model_like_query else -18},
                            {"case": {"$eq": ["$item_type", "ricambio"]}, "then": -120 if model_like_query else -30},
                            {"case": {"$eq": ["$item_type", "lotto"]}, "then": -80 if model_like_query else -15},
                            {"case": {"$eq": ["$item_type", "correlato"]}, "then": -100 if model_like_query else -10},
                        ],
                        "default": 0
                    }
                }
            }
        })

        base_pipeline.append({
            "$addFields": {
                "main_object_phrase_bonus": {
                    "$cond": [
                        {
                            "$and": [
                                {"$eq": ["$item_type", "main_item"]},
                                {
                                    "$or": [
                                        {
                                            "$regexMatch": {
                                                "input": {"$ifNull": ["$title", ""]},
                                                "regex": r"\bcomputer\b|\bhome computer\b|\bpc\b|\btelevisore\b|\btv\b|\bradio\b|\bmonitor\b",
                                                "options": "i"
                                            }
                                        },
                                        {
                                            "$and": [
                                                {"$ne": [rank_query_phrase, ""]},
                                                {
                                                    "$regexMatch": {
                                                        "input": {"$ifNull": ["$title_phrase_norm", ""]},
                                                        "regex": re.escape(rank_query_phrase),
                                                        "options": "i"
                                                    }
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        },
                        25 if model_like_query else 10,
                        0
                    ]
                }
            }
        })

        base_pipeline.append({
            "$addFields": {
                "score_final": {
                    "$subtract": [
                        {
                            "$add": [
                                {"$ifNull": ["$vintage_score", 0]},
                                "$class_weight",
                                "$query_match_bonus",
                                "$recency_bonus",
                                "$era_weight",
                                "$item_type_weight",
                                "$main_object_phrase_bonus",
                                {"$multiply": ["$entity_model_match", 70 if model_like_query else 50]},
                                {"$multiply": ["$entity_family_match", 25]},
                                {"$multiply": ["$title_phrase_match", 35]},
                                {"$multiply": ["$core_tokens_match", 8]}
                            ]
                        },
                        {"$add": ["$generic_title_penalty", "$wrong_family_penalty"]}
                    ]
                }
            }
        })

        if scope != "tutti" and price_filter:
            base_pipeline.append({"$match": {"price_num": price_filter}})

        base_pipeline.append({"$addFields": {"price_sort": {"$ifNull": ["$price_num", 999999999]}}})

        if scope == "tutti":
            sort_stage = {"$sort": {"created_dt": -1, "updated_dt": -1, "_id": -1}}
        else:
            if sort == "price_asc":
                sort_stage = {"$sort": {"price_sort": 1, "updated_dt": -1, "_id": -1}}
            elif sort == "price_desc":
                base_pipeline.append({"$addFields": {"price_sort_desc": {"$ifNull": ["$price_num", -1]}}})
                sort_stage = {"$sort": {"price_sort_desc": -1, "updated_dt": -1, "_id": -1}}
            elif sort == "date":
                sort_stage = {"$sort": {"updated_dt": -1, "_id": -1}}
            else:
                sort_stage = {"$sort": {
                    "score_final": -1,
                    "head_title_match_count": -1,
                    "head_keyword_match_count": -1,
                    "title_match_count": -1,
                    "keyword_match_count": -1,
                    "era_weight": -1,
                    "updated_dt": -1,
                    "_id": -1,
                }}

        pipeline = base_pipeline + [
            {
                "$facet": {
                    "results": [
                        sort_stage,
                        {"$skip": skip_value},
                        {"$limit": limit_value},
                    ],
                    "meta": [
                        {"$count": "total"}
                    ]
                }
            }
        ]

        agg = list(col.aggregate(pipeline))
        if not agg:
            return [], 0

        payload = agg[0]
        local_results = payload.get("results", [])
        meta = payload.get("meta", [])
        total = meta[0]["total"] if meta else 0

        if scope != "tutti" and q:
            filtered_results = []

            for it in local_results:
                title = it.get("title") or ""

                if _looks_generic_title(title):
                    it["score_final"] = float(it.get("score_final") or 0) - 2.0

                if strict_mode and use_strict_head_filter and query_head_regex:
                    title_low = _norm_text(title)
                    if not re.search(query_head_regex, title_low):
                        continue

                filtered_results.append(it)

            local_results = filtered_results

            if sort == "score":
                local_results.sort(
                    key=lambda x: (
                        float(x.get("score_final") or 0),
                        int(x.get("head_title_match_count") or 0),
                        int(x.get("head_keyword_match_count") or 0),
                        int(x.get("title_match_count") or 0),
                        int(x.get("keyword_match_count") or 0),
                        x.get("updated_at") or ""
                    ),
                    reverse=True
                )

        return local_results, total

    def build_price_analysis_results():
        if not q:
            return []

        q_norm = _norm_text(q)
        core_tokens = [t for t in _raw_query_tokens(q_norm) if t]

        if len(core_tokens) < 2:
            return []

        title_patterns = []

        title_patterns.append(re.escape(q_norm).replace(r"\ ", r"\s+"))

        if len(core_tokens) >= 2 and re.fullmatch(r"\d+", core_tokens[1]):
            compact = f"{core_tokens[0][:1]}{core_tokens[1]}"
            title_patterns.append(re.escape(compact))

        if len(core_tokens) == 2:
            reversed_q = f"{core_tokens[1]} {core_tokens[0]}"
            title_patterns.append(re.escape(reversed_q).replace(r"\ ", r"\s+"))

        title_regex = "|".join(title_patterns)

        source_limits = {
            "ebay": 60,
            "subito": 60,
            "vinted": 30,
            "mercatinousato": 30,
        }

        all_results = []

        for src, src_limit in source_limits.items():
            if source and src != source:
                continue

            price_match = {
                **hide_dead,
                **soft_hide,
                "vintage_class": {"$ne": "non_vintage"},
                "source": src,
                "title": {"$regex": title_regex, "$options": "i"},
            }

            if era:
                price_match["era"] = era
            if category_norm:
                price_match["category"] = category_norm

            pipeline = [
                {"$match": price_match},

                {"$addFields": {
                    "price_str_raw": {
                        "$convert": {
                            "input": {"$ifNull": ["$price_value", ""]},
                            "to": "string",
                            "onError": "",
                            "onNull": ""
                        }
                    }
                }},

                {"$addFields": {
                    "price_str": {
                        "$ifNull": [
                            {"$getField": {"field": "match", "input": {
                                "$regexFind": {
                                    "input": "$price_str_raw",
                                    "regex": r"[0-9\.,]+"
                                }
                            }}},
                            ""
                        ]
                    }
                }},

                {"$addFields": {
                    "price_norm": {
                        "$let": {
                            "vars": {"s": {"$toString": {"$ifNull": ["$price_str", ""]}}},
                            "in": {
                                "$switch": {
                                    "branches": [
                                        {
                                            "case": {"$and": [
                                                {"$ne": [{"$indexOfBytes": ["$$s", "."]}, -1]},
                                                {"$ne": [{"$indexOfBytes": ["$$s", ","]}, -1]},
                                            ]},
                                            "then": {
                                                "$replaceAll": {
                                                    "input": {
                                                        "$replaceAll": {"input": "$$s", "find": ".", "replacement": ""}
                                                    },
                                                    "find": ",",
                                                    "replacement": "."
                                                }
                                            }
                                        },
                                        {
                                            "case": {
                                                "$and": [
                                                    {"$ne": [{"$indexOfBytes": ["$$s", "."]}, -1]},
                                                    {"$eq": [{"$indexOfBytes": ["$$s", ","]}, -1]},
                                                    {"$regexMatch": {"input": "$$s", "regex": r"^\d{1,3}(\.\d{3})+$"}}
                                                ]
                                            },
                                            "then": {"$replaceAll": {"input": "$$s", "find": ".", "replacement": ""}}
                                        },
                                        {
                                            "case": {
                                                "$and": [
                                                    {"$eq": [{"$indexOfBytes": ["$$s", "."]}, -1]},
                                                    {"$ne": [{"$indexOfBytes": ["$$s", ","]}, -1]},
                                                ]
                                            },
                                            "then": {"$replaceAll": {"input": "$$s", "find": ",", "replacement": "."}}
                                        },
                                    ],
                                    "default": "$$s"
                                }
                            }
                        }
                    }
                }},

                {"$addFields": {
                    "price_num": {
                        "$convert": {
                            "input": "$price_norm",
                            "to": "double",
                            "onError": None,
                            "onNull": None
                        }
                    },
                    "updated_dt": {
                        "$convert": {"input": "$updated_at", "to": "date", "onError": None, "onNull": None}
                    }
                }},
            ]

            if price_filter:
                pipeline.append({"$match": {"price_num": price_filter}})

            pipeline.append({"$sort": {"updated_dt": -1, "_id": -1}})
            pipeline.append({"$limit": src_limit})

            src_results = list(col.aggregate(pipeline))
            all_results.extend(src_results)

        return all_results

    strict_mode = bool(use_strict_head_filter and query_head_regex)

    analysis_results = build_price_analysis_results()

    results, total_results = run_pipeline(
        strict_mode=strict_mode,
        skip_value=(page - 1) * per_page,
        limit_value=per_page
    )

    if strict_mode and total_results < 3:
        fallback_used = True
        relaxed_results, relaxed_total_results = run_pipeline(
            strict_mode=False,
            skip_value=(page - 1) * per_page,
            limit_value=per_page
        )
        if relaxed_total_results > total_results:
            results = relaxed_results
            total_results = relaxed_total_results
            strict_mode = False

    for it in results:
        pn = it.get("price_num")
        if pn is None:
            pn = _parse_price(it.get("price_value"))
        it["price_display"] = _format_price_it(pn) if pn is not None else (it.get("price_display") or "")

    if scope != "tutti" and q and len(results) < 5:
        fallback_terms = []
        seen_fb = set()

        for term in [q] + sinonimi_preview + query_heads:
            nt = _norm_text(term)
            if nt and nt not in seen_fb:
                seen_fb.add(nt)
                fallback_terms.append(nt)

        fallback_query_block = build_query(fallback_terms)

        prelim_match = {
            "vintage_class": {"$ne": "non_vintage"},
            **hide_dead,
            **soft_hide,
        }

        if fallback_query_block:
            prelim_match.update(fallback_query_block)

        if era:
            prelim_match["era"] = era
        if category_norm:
            prelim_match["category"] = category_norm
        if source:
            prelim_match["source"] = source

        prelim = list(col.find(
            prelim_match,
            {
                "title": 1, "description": 1, "url": 1,
                "image": 1, "price_display": 1, "price_value": 1,
                "source": 1, "hash": 1, "vintage_score": 1,
                "vintage_class": 1,
                "updated_at": 1, "created_at": 1, "era": 1, "category": 1,
                "keywords": 1
            }
        ).limit(2000))

        fuzzy_matches = []

        for item in prelim:
            pv = _parse_price(item.get("price_value"))
            if price_min is not None and (pv is None or pv < price_min):
                continue
            if price_max is not None and (pv is None or pv > price_max):
                continue

            text = (item.get("title", "") + " " + item.get("description", ""))
            if not fuzzy_match(q, text):
                continue

            if query_heads:
                title_low = _norm_text(item.get("title") or "")
                desc_low = _norm_text(item.get("description") or "")
                kw_low = {_norm_text(x) for x in (item.get("keywords") or []) if x}

                head_found = False
                for h in query_heads:
                    h_re = _build_heads_regex([h])
                    if (
                        (h_re and re.search(h_re, title_low))
                        or (h_re and re.search(h_re, desc_low))
                        or h in kw_low
                    ):
                        head_found = True
                        break

                if not head_found:
                    continue

            item["price_display"] = _format_price_it(pv) if pv is not None else (item.get("price_display") or "")
            item["score_final"] = _score_fuzzy_item(item, rank_query_tokens)
            item["title_match_count"] = _text_match_count_py(rank_query_tokens, item.get("title") or "")
            item["keyword_match_count"] = _keyword_match_count_py(rank_query_tokens, item.get("keywords") or [])
            item["head_title_match_count"] = _text_match_count_py(query_heads, item.get("title") or "") if query_heads else 0
            item["head_keyword_match_count"] = _keyword_match_count_py(query_heads, item.get("keywords") or []) if query_heads else 0
            fuzzy_matches.append(item)

        if len(fuzzy_matches) > total_results:
            fuzzy_used = True
            fuzzy_matches.sort(
                key=lambda it: (
                    float(it.get("score_final") or 0),
                    int(it.get("head_title_match_count") or 0),
                    int(it.get("head_keyword_match_count") or 0),
                    int(it.get("title_match_count") or 0),
                    int(it.get("keyword_match_count") or 0),
                    it.get("updated_at") or ""
                ),
                reverse=True
            )

            start = (page - 1) * per_page
            end = page * per_page
            results = fuzzy_matches[start:end]

    price_stats = None

    if scope != "tutti" and q:
        try:
            price_stats = analyze_prices(q, analysis_results)

            if price_stats:
                if should_save_snapshot(q, price_stats, price_snapshots):
                    save_price_snapshot(q, price_stats, price_snapshots)

                trend_data = calculate_price_trend(q, price_snapshots, days=365)
                price_stats["trend"] = trend_data

        except Exception as e:
            print("[PRICE_ANALYZER ERROR]", e)
            price_stats = None

    suggested_query = None

    if q:
        suggested_query = suggest_query_correction(q)

    client.close()

    return render_template(
        "results.html",
        query=q,
        risultati=results,
        total_results=total_results,
        per_page=per_page,
        suggested_query=suggested_query,
        era=era,
        category=category_norm or category,
        source=source,
        price_min=price_min_raw,
        price_max=price_max_raw,
        sort=sort,
        page=page,
        scope=scope,
        fallback_used=fallback_used,
        fuzzy_used=fuzzy_used,
        original_query=q,
        price_stats=price_stats,
    )


###############################################################################
# Robots
###############################################################################
@app.route("/robots.txt")
def robots_txt():
    return Response(
        "User-agent: *\nDisallow: /search\nSitemap: "
        + SITE_URL.rstrip("/")
        + "/sitemap.xml",
        mimetype="text/plain",
    )


###############################################################################
# Sitemap
###############################################################################
@app.route("/sitemap.xml")
def sitemap_xml():
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>{SITE_URL.rstrip('/')}/</loc>
    <lastmod>{datetime.now(timezone.utc).strftime('%Y-%m-%d')}</lastmod>
    <changefreq>daily</changefreq>
    <priority>1.0</priority>
  </url>
</urlset>"""
    return Response(xml, mimetype="application/xml")


###############################################################################
# Remove item + moderno
###############################################################################
@app.route("/remove_item", methods=["POST"])
def remove_item():
    data = request.get_json() or {}
    item_hash = data.get("hash")
    raw_title = data.get("title", "").strip()

    if not item_hash:
        return jsonify({"status": "error", "msg": "missing hash"}), 400

    try:
        client = MongoClient(MONGO_URI)
        col = client[DB_NAME][COLLECTION_NAME]

        res = col.delete_one({"hash": item_hash})

        try:
            extract_modern_terms(raw_title)
        except Exception as e:
            print("[WARN] modern auto-learn failed:", e)

        client.close()

        if res.deleted_count > 0:
            return jsonify({"status": "ok"})
        return jsonify({"status": "error", "msg": "item not found"})

    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)}), 500


###############################################################################
# TRACK CLICK
###############################################################################
@app.route("/track_click", methods=["POST"])
def track_click():
    try:
        data = request.get_json() or {}

        raw_query = (data.get("query") or "").strip().lower()
        raw_title = (data.get("title") or "").strip().lower()

        if not raw_query or not raw_title:
            return jsonify({"status": "error", "msg": "missing data"}), 400

        client = MongoClient(MONGO_URI)
        col = client[DB_NAME]["auto_synonyms"]

        col.insert_one({
            "query": raw_query,
            "title": raw_title,
            "created_at": datetime.now(timezone.utc)
        })

        client.close()
        return jsonify({"status": "ok"})

    except Exception as e:
        print("[TRACK_CLICK ERROR]", e)
        return jsonify({"status": "error", "msg": str(e)}), 500


###############################################################################
# GO / REGISTER CLICK FOR KEYWORD STATS
###############################################################################
@app.route("/go")
def go():
    url = (request.args.get("url") or "").strip()
    q = (request.args.get("q") or "").strip()

    if q:
        try:
            register_click(q)
        except Exception as e:
            print("[GO REGISTER_CLICK ERROR]", e)

    if not url:
        return redirect("/")

    return redirect(url)


###############################################################################
# RUN
###############################################################################
if __name__ == "__main__":
    app.run(debug=True)
