# detect_category.py
"""
Sistema avanzato di classificazione categorie RetroFuture Italia.
Versione: OPZIONE A — Full vintage keywords + integrazione keywords.json + SINONIMI
Corretto e ottimizzato — Novembre 2025
"""

from typing import Dict, Any
import json
import os
import re

from utils_synonyms import expand_with_synonyms


# ======================================================
# 1) Categorie valide
# ======================================================

VALID_CATEGORIES = {
    "arredamento",
    "tecnologia",
    "moda_accessori",
    "giochi_giocattoli",
    "auto_moto",
    "musica_cinema",
    "libri_fumetti",
    "cucina",
    "cartoleria",
    "collezionismo",
    "vario",
}

GENERIC_CATEGORIES = {"", None, "vario", "altro", "uncategorized", "other"}


# CACHE per ottimizzare synonyms expansion
_CACHE_EXPANSION = {}


def _normalize_text(text: str) -> str:
    if not text:
        return ""
    text = str(text).lower()
    text = text.replace("’", "'").replace("‘", "'")
    text = text.replace("`", "'")
    while "  " in text:
        text = text.replace("  ", " ")
    return text.strip()


def _collect_text(doc: Dict[str, Any]) -> str:
    """
    Raccoglie titolo + descrizione + keywords,
    normalizza e poi ESPANDE con sinonimi backend.
    CON CACHE → molto più veloce per 100k annunci!
    """
    parts = [
        str(doc.get("title") or doc.get("titolo") or ""),
        str(doc.get("description") or doc.get("descrizione") or "")
    ]

    kws = doc.get("keywords") or []
    if isinstance(kws, list):
        parts.extend(str(k) for k in kws)

    base = _normalize_text(" ".join(parts))

    if base in _CACHE_EXPANSION:
        return _CACHE_EXPANSION[base]

    expanded = expand_with_synonyms(base)
    _CACHE_EXPANSION[base] = expanded
    return expanded


# ======================================================
# 2) CATEGORIE + KEYWORD STRONG / SOFT
# ======================================================

def _norm_kw_list(lst):
    return [_normalize_text(x) for x in lst]


CATS = {
    "arredamento": {
        "strong": _norm_kw_list([
            "credenza", "servomuto", "mobile cucina", "mobile soggiorno",
            "madia", "libreria", "cassettiera", "comodino", "mobile bar",
            "vetrina vintage", "divano anni", "poltrona anni", "poltrona vintage",
            "mobile in formica", "tavolino anni", "arco lampada", "stilnovo",
        ]),
        "soft": _norm_kw_list([
            "arredamento", "arredo", "sedia vintage", "sedie vintage",
            "lampada vintage", "lampadario retrò", "applique vintage",
            "specchio vintage", "panca vintage", "tavolo vintage",
            "sgabello retrò", "loft vintage"
        ])
    },

    "tecnologia": {
        "strong": _norm_kw_list([
            "commodore", "commodore 64", "amiga", "amiga 500",
            "atari", "atari 2600", "sega megadrive", "sega saturn",
            "nintendo nes", "super nintendo", "game boy",
            "walkman", "sony walkman", "trinitron",
            "fotocamera analogica", "reflex analogica",
            "videoregistratore", "vhs recorder",
            "monitor crt", "tv crt", "televisore a tubo catodico",
            "macchina fotografica polaroid", "polaroid vintage",
        ]),
        "soft": _norm_kw_list([
            "radio vintage", "radio d epoca", "giradischi", "lettore cd vintage",
            "stampante ad aghi", "mouse a sfera", "pc vintage", "computer vintage",
            "console vintage", "decoder d epoca", "camcorder analogico",
            "cinepresa vintage", "cinepresa super8"
        ])
    },

    "moda_accessori": {
        "strong": _norm_kw_list([
            "giacca pelle vintage", "giacca jeans vintage",
            "jeans levis vintage", "maglione anni 80",
            "borsetta anni", "borsa vintage", "occhiali anni 70",
            "orologio vintage", "seiko automatico anni",
            "swatch vintage", "casio anni 80"
        ]),
        "soft": _norm_kw_list([
            "zaino retrò", "cappello vintage", "scarpe anni 80",
            "cravatta anni 70", "cintura vintage",
            "maglia vintage", "felpa vintage", "camicia vintage"
        ])
    },

    "giochi_giocattoli": {
        "strong": _norm_kw_list([
            "big jim", "action man", "he-man", "masters of the universe",
            "soldatini vintage", "lego vintage", "robot giocattolo",
            "cicciobello vintage", "tartarughe ninja vintage",
            "gi joe vintage", "thundercats"
        ]),
        "soft": _norm_kw_list([
            "gioco retrò", "gioco d epoca", "pupazzo vintage",
            "peluche anni 80", "micro machines", "yo-yo vintage"
        ])
    },

    "auto_moto": {
        "strong": _norm_kw_list([
            "modellini auto epoca", "modellini moto epoca",
            "modellino vespa", "modellini fiat", "modellini alfa romeo",
            "modellini moto", "modellino rally", "diecast",
            "casco vespa", "casco lambretta"
        ]),
        "soft": _norm_kw_list([
            "targa smaltata", "ruote d epoca",
            "garage vintage", "manuale officina d epoca",
            "gadget rally", "gadget f1 vintage"
        ])
    },

    "musica_cinema": {
        "strong": _norm_kw_list([
            "vinile", "lp 33 giri", "45 giri", "33 giri",
            "musicassetta", "audiocassetta", "vhs",
            "locandina film vintage", "poster cinema", "manifesto pubblicitario d epoca"
        ]),
        "soft": _norm_kw_list([
            "cd musicali vintage", "mangiadischi",
            "rock anni 70", "disco funky anni", "disco dance anni 80"
        ])
    },

    "libri_fumetti": {
        "strong": _norm_kw_list([
            "tex willer", "tex willer vintage",
            "diabolik", "diabolik vintage",
            "dylan dog", "dylan dog vintage", "dylan dog prima serie",
            "topolino vintage", "topolino anni",
            "zagor", "zagor vintage", "lucky luke vintage"
        ]),
        "soft": _norm_kw_list([
            "fumetti vintage", "libro antico", "rivista vintage",
            "romanzo anni", "corriere dei piccoli"
        ])
    },

    "cucina": {
        "strong": _norm_kw_list([
            "caffettiera d epoca", "moka d epoca", "servizio piatti vintage",
            "bilancia cucina retrò", "pentola smaltata vintage"
        ]),
        "soft": _norm_kw_list([
            "piatto vintage", "bicchiere vintage", "tazza vintage",
            "teiera vintage", "set cucina vintage"
        ])
    },

    "cartoleria": {
        "strong": _norm_kw_list([
            "macchina da scrivere olivetti", "olivetti lettera 32",
            "penna stilografica vintage"
        ]),
        "soft": _norm_kw_list([
            "quaderno vintage", "agenda vintage",
            "block notes vintage", "calcolatrice vintage"
        ])
    },

    "collezionismo": {
        "strong": _norm_kw_list([
            "figurine panini", "monete lire", "gettoni sip",
            "francobolli", "spille vintage", "badge vintage",
            "poster vintage", "souvenir vintage"
        ]),
        "soft": _norm_kw_list([
            "miniature auto vintage", "miniature moto vintage",
            "gadget vintage", "memorabilia vintage"
        ])
    }
}


# ======================================================
# 3) keywords.json autocaricate e mappate
# ======================================================

KEYWORDS_FILE = os.path.join(os.path.dirname(__file__), "keywords.json")


def load_external_keywords():
    try:
        with open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [_normalize_text(x) for x in data]
    except Exception as e:
        print(f"[detect_category] Errore keywords.json: {e}")
    return []


def assign_keyword_to_category(kw: str) -> str:
    """
    Attribuzione intelligente categoria singola keyword.
    """
    k = _normalize_text(kw)

    # ARREDAMENTO
    if any(x in k for x in [
        "arredo", "credenza", "sedie", "lampada", "lampadario",
        "specchio", "formica", "libreria", "cassettiera", "comodino"
    ]):
        return "arredamento"

    # TECNOLOGIA
    if any(x in k for x in [
        "commodore", "amiga", "atari", "nintendo", "game boy",
        "walkman", "trinitron", "giradischi", "polaroid",
        "super8", "cinepresa", "console", "videoregistratore",
        "monitor crt", "tv crt"
    ]):
        return "tecnologia"

    # MODA / ACCESSORI
    if any(x in k for x in [
        "giacca", "borsa", "borsetta", "occhiali", "cravatta",
        "maglione", "scarpe", "zaino", "jeans", "abbigliamento",
        "orologio", "cintura", "camicia"
    ]):
        return "moda_accessori"

    # GIOCHI / GIOCATTOLI
    if any(x in k for x in [
        "big jim", "action man", "he-man", "soldatini",
        "lego", "cicciobello", "micro machines", "gi joe",
        "tartarughe", "toy", "thundercats", "barbie", "playmobil"
    ]):
        return "giochi_giocattoli"

    # AUTO / MOTO
    if any(x in k for x in [
        "modellino vespa", "modellini fiat", "modellini alfa romeo",
        "modellini moto", "modellini auto", "diecast",
        "vespa", "alfa romeo", "targa smaltata",
        "garage vintage", "manuale officina"
    ]):
        return "auto_moto"

    # MUSICA / CINEMA
    if any(x in k for x in [
        "vinile", "45 giri", "33 giri", "lp", "vhs",
        "manifesto", "locandina", "soundtrack",
        "musicassetta", "audiocassetta", "cd musicale",
        "poster cinema"
    ]):
        return "musica_cinema"

    # LIBRI / FUMETTI
    if any(x in k for x in [
        "tex", "diabolik", "dylan dog", "topolino",
        "zagor", "fumetti", "libro", "rivista", "romanzo",
        "corriere dei piccoli", "lucky luke"
    ]):
        return "libri_fumetti"

    # CUCINA
    if any(x in k for x in [
        "caffettiera", "moka", "bilancia", "pentola",
        "piatto", "teiera", "servizio piatti", "bicchiere", "tazza"
    ]):
        return "cucina"

    # CARTOLERIA
    if any(x in k for x in [
        "olivetti", "penna", "stilografica", "quaderno",
        "agenda", "block notes", "calcolatrice"
    ]):
        return "cartoleria"

    # Default
    return "collezionismo"


EXTERNAL_KEYWORDS = load_external_keywords()

for kw in EXTERNAL_KEYWORDS:
    cat = assign_keyword_to_category(kw)
    if cat in CATS:
        if any(x in kw for x in ["anni", "epoca", "d epoca", "antico", "retrò", "retro", "classic"]):
            CATS[cat]["strong"].append(kw)
        else:
            CATS[cat]["soft"].append(kw)


# ======================================================
# 4) FUNZIONE detect_category — FINALE CORRETTA
# ======================================================

def detect_category(doc: Dict[str, Any], debug: bool = False) -> str:
    """
    Flying classifier RetroFuture:
    - prende categoria esistente se valida
    - altrimenti analizza testo (con sinonimi)
    - usa punteggio strong/soft
    """

    # LEGGE SOLO LA CATEGORIA ORIGINALE DELLO SCRAPER
    existing_raw = str(doc.get("category") or "").lower().strip()

    if existing_raw in VALID_CATEGORIES and existing_raw not in GENERIC_CATEGORIES:
        return existing_raw

    text = _collect_text(doc)
    if not text:
        return "vario"

    scores = {cat: 0 for cat in CATS}

    for cat, rule in CATS.items():
        # STRONG
        for kw in rule["strong"]:
            if kw and re.search(rf"\b{re.escape(kw)}\b", text):
                scores[cat] += 3

        # SOFT
        for kw in rule["soft"]:
            if kw and kw in text:
                scores[cat] += 1

    best_cat = max(scores, key=lambda k: scores[k])
    best_score = scores[best_cat]

    if debug:
        print("\n==== DEBUG detect_category ====")
        print("Text:", text[:200], "...")
        print("Scores:", scores)
        print("Best:", best_cat, best_score)
        print("===============================\n")

    if best_score <= 0:
        return "vario"

    return best_cat
