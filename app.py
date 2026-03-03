# app.py
import os, json, re
from datetime import datetime, timezone, timedelta
from flask import Flask, request, render_template, Response, jsonify
from pymongo import MongoClient
from dotenv import load_dotenv
from rapidfuzz import fuzz  # fuzzy

from utils_learn_modern import extract_modern_terms

# === Load ENV ===
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
SITE_URL = os.getenv("SITE_URL", "http://localhost:5000")

# === Flask ===
app = Flask(__name__)

DB_NAME = "database_vintage"
COLLECTION_NAME = "annunci"

# ============================================================
# CONFIG UI / LOGICA
# - Mercatinousato: se needs_check=True -> NASCONDI (soft-hide)
# - Expired: NASCONDI solo se expired_reason=deadlink / noimage (prove)
# ============================================================
SOFT_HIDE_SOURCES = {"mercatinousato"}  # chiave normalizzata nel DB

# ============================================================
# CONFIG NOIMAGE (solo Mercatinousato)
# ============================================================
NOIMAGE_ENABLED = os.getenv("NOIMAGE_ENABLED", "1") != "0"
NOIMAGE_SOURCES = {"mercatinousato"}  # per ora SOLO mercatino/mercatinousato
NOIMAGE_HITS_REQUIRED = int(os.getenv("NOIMAGE_HITS_REQUIRED", "2"))
NOIMAGE_COOLDOWN_MINUTES = int(os.getenv("NOIMAGE_COOLDOWN_MINUTES", "60"))
NOIMAGE_MAX_BODY = 4096

# semplice rate limit in-memory (ok per singola istanza)
_NOIMAGE_RL = {}  # key=(ip,hash) -> last_dt_utc


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
# 🔹 Espansione sinonimi
# ============================================================
def _espandi_sinonimi(query: str):
    q_norm = _norm_text(query)
    if not q_norm:
        return []

    tokens = _tokenize(q_norm)
    if not tokens:
        return []

    ngrams = _generate_ngrams(tokens, max_len=4)
    candidates = []

    for key, lst in SINONIMI.items():
        if key in ngrams:
            candidates.extend(lst)
        for s in lst:
            if s in ngrams:
                candidates.append(key)
                candidates.extend(lst)
                break

    seen = set()
    result = []
    for s in candidates:
        s_norm = _norm_text(s)
        if s_norm and s_norm not in seen:
            seen.add(s_norm)
            result.append(s_norm)

    return result


# ============================================================
# 🔹 Fuzzy helper
# ============================================================
def fuzzy_match(query, text, threshold=65):
    if not query or not text:
        return False
    return fuzz.partial_ratio(query.lower(), text.lower()) >= threshold


# ============================================================
# 🔹 Parse sicuro prezzi (robusto per 99.999 / 99.999,00 ecc.)
# ============================================================
_THOUSANDS_DOT_RE = re.compile(r"^\d{1,3}(\.\d{3})+(\,\d+)?$")
_THOUSANDS_COMMA_RE = re.compile(r"^\d{1,3}(,\d{3})+(\.\d+)?$")

def _parse_price(x):
    """
    Converte stringhe/num in float.
    Supporta:
      - 99.999      -> 99999
      - 99.999,00   -> 99999.00
      - 120,50      -> 120.50
      - 120.50      -> 120.50
      - 250         -> 250.0
    """
    if x is None:
        return None

    # già numero
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        try:
            return float(x)
        except Exception:
            return None

    s = str(x).strip()
    if not s:
        return None

    # tieni solo cifre e separatori
    s = re.sub(r"[^\d\.,]", "", s)
    if not s:
        return None

    # caso EU con punti migliaia (Subito): 99.999 o 99.999,00
    if _THOUSANDS_DOT_RE.match(s):
        s = s.replace(".", "")
        s = s.replace(",", ".")
    # caso US raro: 1,234.56
    elif _THOUSANDS_COMMA_RE.match(s):
        s = s.replace(",", "")
    else:
        # caso semplice: 120,50 -> 120.50
        if "," in s and "." not in s:
            s = s.replace(",", ".")
        # se ci sono entrambi, assume "." migliaia e "," decimali
        elif "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")

    try:
        return float(s)
    except Exception:
        return None


def _format_price_it(n):
    """
    Mostra:
      - 99999   -> 99.999 EUR
      - 60      -> 60.00 EUR
      - 120.5   -> 120.50 EUR
    """
    if n is None:
        return ""

    try:
        n = float(n)
    except Exception:
        return ""

    # sempre 2 decimali come il tuo UI attuale
    s = f"{n:.2f}"

    # separatore decimale "."
    int_part, dec_part = s.split(".")
    # migliaia con "."
    int_part = f"{int(int_part):,}".replace(",", ".")
    return f"{int_part}.{dec_part} EUR"


# ============================================================
# 🔹 Recency bonus (Python) — per fuzzy fallback
# ============================================================
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
        return 0.7
    if age_days <= 3:
        return 0.4
    if age_days <= 7:
        return 0.2
    if age_days <= 14:
        return 0.1
    return 0.0


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
# REPORT NOIMAGE (client-side proof) — SOLO mercatinousato
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

    # ✅ Filtri scelti (barra minimal)
    era = (request.args.get("era") or "").strip()
    category = (request.args.get("category") or "").strip()
    source = (request.args.get("source") or "").strip().lower()

    sort = (request.args.get("sort") or "score").strip()
    scope = (request.args.get("scope") or "").strip().lower()

    price_min_raw = request.args.get("price_min")
    price_max_raw = request.args.get("price_max")
    page = max(int(request.args.get("page", 1) or 1), 1)
    per_page = 50

    # -------------------------
    # ✅ WHITELIST
    # -------------------------
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

    # -------------------------
    # ✅ Parse prezzi (Python)
    # -------------------------
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

    # -------------------------
    # ✅ Match base
    # -------------------------
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

    if scope == "tutti":
        match = {**hide_dead, **soft_hide}
    else:
        match = {
            **hide_dead,
            **soft_hide,
            "vintage_class": {"$ne": "non_vintage"},
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

    # ------------ Query principale ----------------
    if scope != "tutti" and q:
        sinonimi = _espandi_sinonimi(q)
        q_norm = _norm_text(q)
        tokens = _tokenize(q)
        search_terms = [q_norm] + tokens + sinonimi

        query_block = build_query(search_terms)
        if query_block:
            match.update(query_block)

    # ------------ Filtri (solo se non "tutti") ------------
    if scope != "tutti":
        if era:
            match["era"] = era
        if category_norm:
            match["category"] = category_norm
        if source:
            match["source"] = source

    # ------------ Pipeline ------------
    pipeline = [
        {"$match": match},

        # ✅ price_value può essere: number OR string con migliaia (99.999) / decimali (120,50)
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

        # ✅ Estrai solo 0-9 . , (SEMPRE stringa, evita crash su $replaceAll)
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

        # normalizzazione EU:
        # - se contiene sia "." che "," => "." migliaia, "," decimali
        # - se contiene solo "." e matcha pattern migliaia (xx.xxx o x.xxx.xxx) => rimuovi "."
        # - se contiene solo "," => "," decimali => replace con "."
        {"$addFields": {
            "price_norm": {
                "$let": {
                    # ✅ cintura di sicurezza: $$s sempre stringa
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
            },
            "recency_bonus": {
                "$switch": {
                    "branches": [
                        {"case": {"$lte": ["$age_days", 1]},  "then": 0.7},
                        {"case": {"$lte": ["$age_days", 3]},  "then": 0.4},
                        {"case": {"$lte": ["$age_days", 7]},  "then": 0.2},
                        {"case": {"$lte": ["$age_days", 14]}, "then": 0.1},
                    ],
                    "default": 0
                }
            },
            "score_final": {"$add": [{"$ifNull": ["$vintage_score", 0]}, "$recency_bonus"]},
        }},
    ]

    if scope != "tutti" and price_filter:
        pipeline.append({"$match": {"price_num": price_filter}})

    pipeline.append({"$addFields": {"price_sort": {"$ifNull": ["$price_num", 999999999]}}})

    if scope == "tutti":
        pipeline.append({"$sort": {"created_dt": -1, "updated_dt": -1, "_id": -1}})
    else:
        if sort == "price_asc":
            pipeline.append({"$sort": {"price_sort": 1, "updated_dt": -1, "_id": -1}})
        elif sort == "price_desc":
            pipeline.append({"$addFields": {"price_sort_desc": {"$ifNull": ["$price_num", -1]}}})
            pipeline.append({"$sort": {"price_sort_desc": -1, "updated_dt": -1, "_id": -1}})
        elif sort == "date":
            pipeline.append({"$sort": {"updated_dt": -1, "_id": -1}})
        else:
            pipeline.append({"$sort": {
                "score_final": -1,
                "era_weight": -1,
                "updated_dt": -1,
                "_id": -1,
            }})

    pipeline += [
        {"$skip": (page - 1) * per_page},
        {"$limit": per_page},
    ]

    results = list(col.aggregate(pipeline))

    # ✅ Ricostruisci SEMPRE un display coerente dal numero (risolve subito casi tipo 99.999)
    for it in results:
        pn = it.get("price_num")
        if pn is None:
            pn = _parse_price(it.get("price_value"))
        it["price_display"] = _format_price_it(pn) if pn is not None else (it.get("price_display") or "")

    # =====================================================================
    # 🔥 Fuzzy fallback
    # =====================================================================
    if scope != "tutti" and q and len(results) < 5:
        q_clean = q.strip()
        q_prefix = q_clean[:5] if len(q_clean) >= 5 else q_clean
        q_prefix = re.escape(q_prefix)

        loose_regex = {"$or": [
            {"title": {"$regex": q_prefix, "$options": "i"}},
            {"description": {"$regex": q_prefix, "$options": "i"}},
        ]}

        prelim_match = {
            "vintage_class": {"$ne": "non_vintage"},
            **hide_dead,
            **soft_hide,
            **loose_regex
        }

        if era:
            prelim_match["era"] = era
        if category_norm:
            prelim_match["category"] = category_norm
        if source:
            prelim_match["source"] = source

        prelim = list(col.find(
            prelim_match,
            {"title": 1, "description": 1, "url": 1,
             "image": 1, "price_display": 1, "price_value": 1,
             "source": 1, "hash": 1, "vintage_score": 1,
             "updated_at": 1, "created_at": 1, "era": 1, "category": 1}
        ).limit(2000))

        fuzzy_matches = []
        for item in prelim:
            pv = _parse_price(item.get("price_value"))
            if price_min is not None and (pv is None or pv < price_min):
                continue
            if price_max is not None and (pv is None or pv > price_max):
                continue

            text = (item.get("title", "") + " " + item.get("description", ""))
            if fuzzy_match(q, text):
                # aggiorna display anche qui
                item["price_display"] = _format_price_it(pv) if pv is not None else (item.get("price_display") or "")
                fuzzy_matches.append(item)

        if len(fuzzy_matches) > len(results):
            fuzzy_used = True
            fuzzy_matches.sort(
                key=lambda it: float(it.get("vintage_score") or 0)
                               + _recency_bonus_from_dt(it.get("updated_at") or it.get("created_at")),
                reverse=True
            )

            start = (page - 1) * per_page
            end = page * per_page
            results = fuzzy_matches[start:end]

    client.close()

    return render_template(
        "results.html",
        query=q,
        risultati=results,
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
        else:
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
# RUN
###############################################################################
if __name__ == "__main__":
    app.run(debug=True)
