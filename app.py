# app.py
import os, json, re
from datetime import datetime
from flask import Flask, request, render_template, Response, jsonify
from pymongo import MongoClient
from dotenv import load_dotenv
from rapidfuzz import fuzz

from utils_learn_modern import extract_modern_terms


# === Load ENV ===
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
SITE_URL = os.getenv("SITE_URL", "http://localhost:5000")

app = Flask(__name__)

DB_NAME = "database_vintage"
COLLECTION_NAME = "annunci"


# ============================================================
# 🔹 UTIL
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


def _tokenize(s: str):
    return [t for t in _norm_text(s).split() if t]


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
# 🔹 Fuzzy
# ============================================================
def fuzzy_match(query, text, threshold=65):
    if not query or not text:
        return False
    return fuzz.partial_ratio(query.lower(), text.lower()) >= threshold


@app.route("/")
def index():
    return render_template("index.html")


###############################################################################
# ✅ SEARCH
###############################################################################
@app.route("/search")
def search():
    q = (request.args.get("q") or "").strip()
    era = request.args.get("era") or ""
    vclass = request.args.get("vintage_class") or ""
    category = (request.args.get("category") or "").strip()
    sort = (request.args.get("sort") or "score").strip()
    scope = (request.args.get("scope") or "").strip().lower()

    price_min = request.args.get("price_min")
    price_max = request.args.get("price_max")
    page = max(int(request.args.get("page", 1)), 1)
    per_page = 50

    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]

    match = {} if scope == "tutti" else {"vintage_class": {"$ne": "non_vintage"}}

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

    if scope != "tutti" and q:
        sinonimi = _espandi_sinonimi(q)
        q_norm = _norm_text(q)
        tokens = _tokenize(q)
        search_terms = [q_norm] + tokens + sinonimi

        query_block = build_query(search_terms)
        if query_block:
            match.update(query_block)

    if scope != "tutti":
        if era:
            match["era"] = era
        if vclass:
            match["vintage_class"] = vclass
        if category:
            match["category"] = {"$regex": f"^{category}$", "$options": "i"}

        price_filter = {}
        if price_min:
            price_filter["$gte"] = float(price_min)
        if price_max:
            price_filter["$lte"] = float(price_max)
        if price_filter:
            match["price_value"] = price_filter

    pipeline = [
        {"$match": match},
        {
            "$addFields": {
                "price_num": {"$toDouble": "$price_value"},
                "updated_dt": {"$toDate": "$updated_at"},
                "created_dt": {"$toDate": "$created_at"},
                "era_weight": {
                    "$cond": [{"$ne": ["$era", "vintage_generico"]}, 1, 0]
                },
            }
        },
    ]

    # ordinamento
    if scope == "tutti":
        pipeline.append({"$sort": {"created_dt": -1, "updated_dt": -1, "_id": -1}})
    else:
        if sort == "price_asc":
            pipeline.append({"$sort": {"price_num": 1}})
        elif sort == "price_desc":
            pipeline.append({"$sort": {"price_num": -1}})
        elif sort == "date":
            pipeline.append({"$sort": {"updated_dt": -1}})
        elif sort == "added":
            pipeline.append({"$sort": {"created_dt": -1}})
        else:
            pipeline.append({
                "$sort": {
                    "vintage_score": -1,
                    "era_weight": -1,
                    "updated_dt": -1,
                }
            })

    pipeline += [
        {"$skip": (page - 1) * per_page},
        {"$limit": per_page},
    ]

    results = list(col.aggregate(pipeline))

    # Fallback sinonimi
    if scope != "tutti" and q and len(results) == 0:
        sinonimi = _espandi_sinonimi(q)
        if sinonimi:
            fallback_used = True
            q_norm = _norm_text(q)
            tokens = _tokenize(q)
            search_terms = [q_norm] + tokens + sinonimi
            query_block = build_query(search_terms)
            if query_block:
                match.update(query_block)
                pipeline[0] = {"$match": match}
                results = list(col.aggregate(pipeline))

    # Fuzzy
    if scope != "tutti" and q and len(results) < 5:
        prelim = list(col.find({"vintage_class": {"$ne": "non_vintage"}}))
        fuzzy_matches = []
        for item in prelim:
            text = (item.get("title", "") + " " + item.get("description", ""))
            if fuzzy_match(q, text):
                fuzzy_matches.append(item)

        if len(fuzzy_matches) > len(results):
            fuzzy_used = True
            start = (page - 1) * per_page
            end = page * per_page
            results = fuzzy_matches[start:end]

    client.close()

    return render_template(
        "results.html",
        query=q,
        risultati=results,
        era=era,
        vintage_class=vclass,
        category=category,
        price_min=price_min,
        price_max=price_max,
        sort=sort,
        page=page,
        scope=scope,
        fallback_used=fallback_used,
        fuzzy_used=fuzzy_used,
        original_query=q,
    )


###############################################################################
# ✅ Robots
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
# ✅ Sitemap
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
# ❌ Remove item (usa ancora Mongo)
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
# ✅ TRACK CLICK — SOLO FILE (NO MONGO)
###############################################################################
@app.route("/track_click", methods=["POST"])
def track_click():

    data = request.get_json() or {}

    raw_query = (data.get("query") or "").strip().lower()
    raw_title = (data.get("title") or "").strip().lower()

    if not raw_query or not raw_title:
        return jsonify({"status": "error", "msg": "missing data"}), 400

    entry = {
        "query": raw_query,
        "title": raw_title,
        "created_at": datetime.utcnow().isoformat()
    }

    LOG_FILE = "click_log.json"

    try:
        # leggo o creo
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                logs = json.load(f)
        else:
            logs = []

        logs.append(entry)

        with open(LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(logs, f, indent=2, ensure_ascii=False)

        return jsonify({"status": "ok"})

    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)}), 500


###############################################################################
# RUN
###############################################################################
if __name__ == "__main__":
    app.run(debug=True)

