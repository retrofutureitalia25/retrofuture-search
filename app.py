import os
from datetime import datetime, timedelta
from flask import Flask, request, render_template, Response
from pymongo import MongoClient
from dotenv import load_dotenv

# === Load ENV ===
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
SITE_URL = os.getenv("SITE_URL", "http://localhost:5000")

# === Flask ===
app = Flask(__name__)

DB_NAME = "database_vintage"
COLLECTION_NAME = "annunci"

# === SINONIMI (completi) ===
SINONIMI = {
    "arredamento vintage": ["modernariato", "design vintage", "mobilio d epoca", "arredo retrò"],
    "lampada vintage": ["lampadario retrò", "lampada d epoca", "lampada anni 70", "lampada a lava"],
    "mobile vintage": ["cassettiera retrò", "comodino vintage", "tavolo vintage", "sedia vintage", "divano retrò"],
    "specchio vintage": ["cornice retrò", "specchiera d epoca"],
    "oggetto vintage": ["soprammobile retrò", "decorazione d epoca", "oggetti pubblicitari vintage"],
    "giradischi": ["piatto vinile", "turntable", "lettore vinile", "radio giradischi", "mangiadischi"],
    "vinile": ["disco lp", "record", "45 giri", "33 giri", "album vinile", "disco in vinile"],
    "radio vintage": ["radio d epoca", "radio retrò", "radio a valvole", "radiolina"],
    "jukebox": ["boombox", "musicassetta", "walkman", "lettore cd vintage"],
    "strumento musicale vintage": ["chitarra elettrica anni 70", "amplificatore valvolare", "microfono retrò", "cuffie d epoca"],
    "tv vintage": ["televisore d epoca", "televisore a tubo catodico", "televisore retrò", "tv anni 80", "decoder d epoca"],
    "videoregistratore": ["vhs", "lettore videocassette", "videocamera vintage", "camcorder analogico"],
    "fotocamera vintage": ["macchina fotografica vintage", "polaroid", "reflex analogica", "fotocamera d epoca", "analog camera"],
    "cinepresa vintage": ["super8", "proiettore super8", "proiettore diapositive"],
    "telefono vintage": ["telefono a disco", "telefono sip", "telefono fisso d epoca", "telefono anni 70"],
    "computer vintage": ["commodore 64", "amiga", "spectrum", "apple 2", "macintosh classico", "ibm 386", "monitor crt", "stampante ad aghi", "mouse a sfera"],
    "console vintage": ["game boy", "nintendo nes", "super nintendo", "sega megadrive", "atari", "joystick retrò", "flipper", "arcade"],
    "giocattolo vintage": ["barbie d epoca", "lego vintage", "big jim", "action man", "gi joe vintage", "robot giocattolo", "transformers vintage", "masters of the universe", "tartarughe ninja vintage", "ghostbusters vintage"],
    "auto d epoca": ["macchina vintage", "auto retrò", "modellino auto", "fiat 500 d epoca", "maggiolino", "mini cooper classica", "alfa romeo giulia"],
    "moto d epoca": ["vespa", "lambretta", "motocicletta vintage"],
    "accessori auto vintage": ["targa d epoca", "volante retrò", "contachilometri"],
    "abiti vintage": ["vestiti d epoca", "moda retrò", "camicia hawaiana vintage", "pantaloni a zampa", "minigonna anni 60", "maglione anni 80", "giubbotto jeans vintage"],
    "borsa vintage": ["borsetta anni 50", "handbag retrò", "pochette retrò", "zaino retrò"],
    "orologio vintage": ["swatch vintage", "seiko automatico", "casio anni 80", "omega seamaster", "citizen d epoca", "pendolo vintage", "sveglia d epoca"],
    "poster vintage": ["locandina film vintage", "manifesto retrò", "stampa anni 70", "insegna smaltata", "cartello in latta", "pubblicità d epoca", "insegna bar vintage"],
    "fumetti vintage": ["topolino anni 60", "diabolik", "tex willer", "alan ford", "martin mystere", "dylan dog", "zagor", "corriere dei piccoli"],
    "libro vintage": ["libro antico", "romanzo anni 50", "enciclopedia", "rivista vintage", "giornale d epoca"],
    "strumenti da cucina vintage": ["moka d epoca", "bilancia retrò", "macinacaffè antico", "pentola smaltata", "servizio da tè vintage"],
}

def _espandi_sinonimi(query):
    q = query.lower()
    lista = []
    for chiave, sinonimi in SINONIMI.items():
        if chiave in q or any(s in q for s in sinonimi):
            lista.append(chiave)
            lista.extend(sinonimi)
    return list(dict.fromkeys(lista))

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/search")
def search():
    query = request.args.get("q", "")
    fonte = request.args.get("fonte", "")
    categoria = request.args.get("categoria", "")
    prezzo_min = request.args.get("prezzo_min", "")
    prezzo_max = request.args.get("prezzo_max", "")
    sort = request.args.get("sort", "rilevanza")
    page = max(1, int(request.args.get("page", 1)))
    limit = 50
    skip = (page - 1) * limit

    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]

    pipeline = []

    # === Search engine ===
    if query:
        sinonimi = _espandi_sinonimi(query)
        pipeline.append({
            "$search": {
                "index": "default",
                "compound": {
                    "should": [
                        {"text": {"query": query, "path": "title", "fuzzy": {"maxEdits": 1}, "score": {"boost": {"value": 6}}}},
                        {"text": {"query": query, "path": "keywords", "fuzzy": {"maxEdits": 1}, "score": {"boost": {"value": 4}}}},
                        {"text": {"query": query, "path": "description", "fuzzy": {"maxEdits": 1}, "score": {"boost": {"value": 2}}}},
                    ] + (
                        [{"text": {"query": sinonimi, "path": ["title","keywords"], "fuzzy":{"maxEdits":1}, "score":{"boost":{"value":1.5}}}}]
                        if sinonimi else []
                    )
                }
            }
        })
    else:
        pipeline.append({"$sort": {"scraped_at": -1}})

    match = {}
    if fonte: match["source"] = fonte
    if categoria: match["category"] = categoria
    if match: pipeline.append({"$match": match})

    pipeline.append({
        "$match": {
            "title": {"$ne": "Titolo non disponibile"},
            "price_value": {"$ne": None},
            "price_currency": {"$exists": True}
        }
    })

    cutoff = datetime.utcnow() - timedelta(days=3)
    pipeline.append({
        "$addFields": {
            "priority_score": {
                "$add": [
                    {"$cond":[{"$ifNull":["$image",False]},1,0]},
                    {"$cond":[{"$ne":["$title","Titolo non disponibile"]},1,0]},
                    {"$cond":[{"$ne":["$price_value",None]},1,0]},
                    {"$cond":[{"$gte":["$scraped_at",cutoff.isoformat()]},0.5,0]}
                ]
            }
        }
    })

    pipeline.append({
        "$project": {
            "_id": 1,
            "title": 1, "price_value": 1, "price_currency": 1,
            "image": 1, "url": 1, "source": 1,
            "category": 1, "description": 1,
            "scraped_at": 1, "priority_score": 1
        }
    })

    if sort == "prezzo_asc":
        pipeline.append({"$sort": {"price_value": 1}})
    elif sort == "prezzo_desc":
        pipeline.append({"$sort": {"price_value": -1}})
    elif sort == "recenti":
        pipeline.append({"$sort": {"scraped_at": -1}})
    else:
        pipeline.append({"$sort": {"priority_score": -1, "scraped_at": -1}})

    pipeline += [{"$skip": skip}, {"$limit": limit}]

    results = list(col.aggregate(pipeline))
    client.close()

    return render_template("results.html",
        query=query, risultati=results,
        fonte=fonte, categoria=categoria,
        prezzo_min=prezzo_min, prezzo_max=prezzo_max,
        sort=sort, page=page
    )

@app.route("/robots.txt")
def robots_txt():
    return Response("User-agent: *\nDisallow: /search\nSitemap: "+SITE_URL.rstrip("/")+"/sitemap.xml", mimetype="text/plain")

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

if __name__ == "__main__":
    app.run(debug=True)
