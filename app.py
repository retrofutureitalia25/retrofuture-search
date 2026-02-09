# app.py
import os, json, re
from datetime import datetime
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
        for j in range(i + 1, min(n, i + max_len) + 1):
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
# 🔹 Parse sicuro prezzi
# ============================================================
def _parse_price(x):
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


# ============================================================
# 🔹 Recency bonus (Python) — per fuzzy fallback
# ============================================================
def _recency_bonus_from_dt(dt_val):
    """
    Bonus leggero: massimo +0.7 (entro 1 giorno), poi decresce.
    dt_val può essere stringa ISO o datetime.
    """
    if not dt_val:
        return 0.0

    try:
        if isinstance(dt_val, str):
            dtp = datetime.fromisoformat(dt_val.replace("Z", "+00:00"))
        else:
            dtp = dt_val

        # se è aware, la rendiamo naive UTC-like
        if getattr(dtp, "tzinfo", None) is not None:
            dtp = dtp.replace(tzinfo=None)

        age_days = (datetime.utcnow() - dtp).total_seconds() / 86400.0
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


@app.route("/")
def index():
    return render_template("index.html")


###############################################################################
# SEARCH
###############################################################################
@app.route("/search")
def search():
    q = (request.args.get("q") or "").strip()

    # ✅ Filtri scelti (barra minimal)
    era = (request.args.get("era") or "").strip()
    category = (request.args.get("category") or "").strip()
    source = (request.args.get("source") or "").strip().lower()  # marketplace

    sort = (request.args.get("sort") or "score").strip()
    scope = (request.args.get("scope") or "").strip().lower()

    price_min_raw = request.args.get("price_min")
    price_max_raw = request.args.get("price_max")
    page = max(int(request.args.get("page", 1) or 1), 1)
    per_page = 50

    # -------------------------
    # ✅ WHITELIST (anti valori strani)
    # -------------------------
    allowed_era = {
        "anni_50", "anni_60", "anni_70", "anni_80", "anni_90", "anni_2000",
        # compatibilità DB/link vecchi (anche se non lo metti nel menu)
        "vintage_generico",
    }

    # ✅ Ordinamento UI deciso:
    # - Migliori        -> score
    # - Più recenti     -> date
    # - Prezzo ↑        -> price_asc
    # - Prezzo ↓        -> price_desc
    allowed_sort = {"score", "date", "price_asc", "price_desc"}

    # ✅ marketplace attuali
    allowed_source = {"ebay", "vinted", "subito", "mercatino"}

    # ✅ categorie brand (slug “puliti”)
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

    # -------------------------
    # ✅ Parse prezzi (safe)
    # -------------------------
    price_min = _parse_price(price_min_raw)
    price_max = _parse_price(price_max_raw)
    if price_min is not None and price_max is not None and price_min > price_max:
        price_min, price_max = price_max, price_min

    # ✅ range prezzo: lo applichiamo su price_num (dopo conversione)
    price_filter = {}
    if price_min is not None:
        price_filter["$gte"] = price_min
    if price_max is not None:
        price_filter["$lte"] = price_max

    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]

    # -------------------------
    # ✅ Match base (pulito + senza ambiguità)
    #   - sempre nascondi expired
    #   - se NON "tutti", nascondi anche non_vintage
    # -------------------------
    match = {"status": {"$ne": "expired"}}
    if scope != "tutti":
        match["vintage_class"] = {"$ne": "non_vintage"}

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

    # ------------ Pipeline (conversioni safe + recency bonus) ------------
    pipeline = [
        {"$match": match},
        {"$addFields": {
            # ✅ prezzo numerico robusto (gestisce anche "12,50")
            "price_num": {
                "$convert": {
                    "input": {
                        "$replaceAll": {
                            "input": {"$ifNull": ["$price_value", ""]},
                            "find": ",",
                            "replacement": "."
                        }
                    },
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

            # base_dt = updated_dt se presente, altrimenti created_dt, altrimenti epoch
            "base_dt": {"$ifNull": ["$updated_dt", {"$ifNull": ["$created_dt", datetime(1970, 1, 1)]}]},

            "era_weight": {"$cond": [{"$ne": ["$era", "vintage_generico"]}, 1, 0]},
        }},
        {"$addFields": {
            # età in giorni: (NOW - base_dt) / ms_giorno
            "age_days": {
                "$divide": [
                    {"$subtract": ["$$NOW", "$base_dt"]},
                    86400000
                ]
            },
            # micro-bonus recency (massimo +0.7)
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
            # score finale usato per "Migliori"
            "score_final": {"$add": [{"$ifNull": ["$vintage_score", 0]}, "$recency_bonus"]},
        }},
    ]

    # ✅ filtro prezzo applicato DOPO conversione
    if scope != "tutti" and price_filter:
        pipeline.append({"$match": {"price_num": price_filter}})

    # per sort prezzo: i null vanno in fondo (asc)
    pipeline.append({"$addFields": {"price_sort": {"$ifNull": ["$price_num", 999999999]}}})

    if scope == "tutti":
        pipeline.append({"$sort": {"created_dt": -1, "updated_dt": -1, "_id": -1}})
    else:
        if sort == "price_asc":
            pipeline.append({"$sort": {"price_sort": 1, "updated_dt": -1, "_id": -1}})
        elif sort == "price_desc":
            # per desc: null in fondo
            pipeline.append({"$addFields": {"price_sort_desc": {"$ifNull": ["$price_num", -1]}}})
            pipeline.append({"$sort": {"price_sort_desc": -1, "updated_dt": -1, "_id": -1}})
        elif sort == "date":
            pipeline.append({"$sort": {"updated_dt": -1, "_id": -1}})
        else:
            # "score" = migliori (score + recency bonus)
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

    # =====================================================================
    # 🔥 Fuzzy fallback (rispetta i filtri scelti)
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
            "status": {"$ne": "expired"},  # ✅ coerente con ricerca principale
            **loose_regex
        }

        # riapplica filtri
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
            # ✅ filtro prezzo in Python (perché price_value può essere stringa)
            pv = _parse_price(item.get("price_value"))
            if price_min is not None and (pv is None or pv < price_min):
                continue
            if price_max is not None and (pv is None or pv > price_max):
                continue

            text = (item.get("title", "") + " " + item.get("description", ""))
            if fuzzy_match(q, text):
                fuzzy_matches.append(item)

        if len(fuzzy_matches) > len(results):
            fuzzy_used = True

            # ✅ ordina fuzzy come "migliori": score + recency bonus
            fuzzy_matches.sort(
                key=lambda it: float(it.get("vintage_score") or 0)
                               + _recency_bonus_from_dt(it.get("updated_at") or it.get("created_at")),
                reverse=True
            )

            start = (page - 1) * per_page
            end = page * per_page
            results = fuzzy_matches[start:end]

    # =====================================================================

    client.close()

    return render_template(
        "results.html",
        query=q,
        risultati=results,
        era=era,
        category=category_norm or category,
        source=source,  # ✅ marketplace
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
    <lastmod>{datetime.utcnow().strftime('%Y-%m-%d')}</lastmod>
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
            "created_at": datetime.utcnow()
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
