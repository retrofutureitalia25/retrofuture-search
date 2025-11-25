# app.py
import os, json, re
from datetime import datetime
from flask import Flask, request, render_template, Response, jsonify
from pymongo import MongoClient
from dotenv import load_dotenv
from rapidfuzz import fuzz  # ✅ fuzzy

from utils_learn_modern import extract_modern_terms  # ✅ IMPORT INTELLIGENZA MODERNO

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
    # togli doppi spazi
    while "  " in s:
        s = s.replace("  ", " ")
    return s.strip()


def _tokenize(s: str):
    return [t for t in _norm_text(s).split() if t]


def _generate_ngrams(tokens, max_len=4):
    """
    Genera frasi (n-grammi) di lunghezza 1..max_len
    es: ["tv","vintage","philips"] -> "tv", "tv vintage", "tv vintage philips", "vintage", ...
    """
    ngrams = set()
    n = len(tokens)
    for i in range(n):
        for j in range(i + 1, min(n, i + max_len) + 1):
            ngrams.add(" ".join(tokens[i:j]))
    return ngrams


# ============================================================
# 🔹 Load synonyms (normalizzati)
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
# 🔹 Espansione sinonimi (frasi intere, niente substring sporche)
# ============================================================
def _espandi_sinonimi(query: str):
    """
    Espansione bidirezionale:
    - se la query contiene una chiave (come frase), aggiungo i suoi valori
    - se la query contiene uno dei valori (come frase), aggiungo la chiave + tutti i valori
    Matching SOLO su parole intere (ngram), NON su substring dentro altre parole.
    """
    q_norm = _norm_text(query)
    if not q_norm:
        return []

    tokens = _tokenize(q_norm)
    if not tokens:
        return []

    ngrams = _generate_ngrams(tokens, max_len=4)  # frasi fino a 4 parole

    candidates = []

    for key, lst in SINONIMI.items():
        # match se la chiave è una delle frasi della query
        if key in ngrams:
            candidates.extend(lst)

        # match se uno dei valori è una delle frasi della query
        for s in lst:
            if s in ngrams:
                candidates.append(key)
                candidates.extend(lst)
                break  # evitiamo duplicazioni inutili

    # dedup preservando l'ordine
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


@app.route("/")
def index():
    return render_template("index.html")


###############################################################################
# ✅ SEARCH — AGGIUNTA MODALITÀ "TUTTI"
###############################################################################
@app.route("/search")
def search():
    q = (request.args.get("q") or "").strip()
    era = request.args.get("era") or ""
    vclass = request.args.get("vintage_class") or ""
    category = (request.args.get("category") or "").strip()
    sort = (request.args.get("sort") or "score").strip()

    # ✅ nuovo parametro
    scope = (request.args.get("scope") or "").strip().lower()

    price_min = request.args.get("price_min")
    price_max = request.args.get("price_max")
    page = max(int(request.args.get("page", 1)), 1)
    per_page = 50

    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]

    # ✅ MODALITÀ TUTTI: nessun filtro
    if scope == "tutti":
        match = {}  # mostra tutto
    else:
        match = {"vintage_class": {"$ne": "non_vintage"}}

    fallback_used = False
    fuzzy_used = False

    # BUILD QUERY
    def build_query(query_terms):
        norm_terms = []
        regex_parts = []

        for t in query_terms:
            nt = _norm_text(t)
            if not nt:
                continue
            # opzionale: evita termini 1 lettera inutili
            if len(nt) < 2 and " " not in nt:
                continue

            norm_terms.append(nt)
            # escape sicuro + spazio flessibile
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

    # ✅ ricerca testuale SOLO se non è scope=tutti
    if scope != "tutti" and q:
        sinonimi = _espandi_sinonimi(q)
        # includo anche la query normalizzata e i token singoli
        q_norm = _norm_text(q)
        tokens = _tokenize(q)
        search_terms = [q_norm] + tokens + sinonimi

        query_block = build_query(search_terms)
        if query_block:
            match.update(query_block)

    # ✅ filtri normalizzati solo se non è "tutti"
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

    # ✅ pipeline base comune
    pipeline = [
        {"$match": match},
        {
            "$addFields": {
                "price_num": {"$toDouble": "$price_value"},
                "updated_dt": {"$toDate": "$updated_at"},
                "created_dt": {"$toDate": "$created_at"},  # ✅ fondamentale per "tutti"
                "era_weight": {
                    "$cond": [{"$ne": ["$era", "vintage_generico"]}, 1, 0]
                },
            }
        },
    ]

    # ✅ ordinamento MODALITÀ TUTTI
    if scope == "tutti":
        pipeline.append(
            {
                "$sort": {
                    "created_dt": -1,  # ✅ i più recenti
                    "updated_dt": -1,
                    "_id": -1,
                }
            }
        )

    else:
        # ordinamenti normali
        if sort == "price_asc":
            pipeline.append({"$sort": {"price_num": 1}})
        elif sort == "price_desc":
            pipeline.append({"$sort": {"price_num": -1}})
        elif sort == "date":
            pipeline.append({"$sort": {"updated_dt": -1}})
        elif sort == "added":
            pipeline.append({"$sort": {"created_dt": -1}})
        else:
            pipeline.append(
                {
                    "$sort": {
                        "vintage_score": -1,
                        "era_weight": -1,
                        "updated_dt": -1,
                    }
                }
            )

    pipeline += [
        {"$skip": (page - 1) * per_page},
        {"$limit": per_page},
    ]

    # ✅ esecuzione
    results = list(col.aggregate(pipeline))

    # ✅ fallback sinonimi & fuzzy SOLO se non è "tutti"
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
        scope=scope,  # ✅ lo passiamo al template
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
# ✅ Remove item + auto-learn moderno
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

        # auto-learn moderno
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
# ✅ RUN
###############################################################################
if __name__ == "__main__":
    app.run(debug=True)
