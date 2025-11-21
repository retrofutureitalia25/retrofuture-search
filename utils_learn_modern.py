# ============================================================
# utils_learn_modern.py — Versione PRO (2025)
#
#  ✓ Integra modern_keywords_extended.json
#  ✓ Evita duplicati tra extended e learned
#  ✓ Rilevamento potenziato modelli moderni
#  ✓ Rafforzati i filtri smartphone / console / auto moderne
# ============================================================

import re, json, os
from datetime import datetime, UTC


# ------------------------------------------------------------
#  Funzioni utili JSON
# ------------------------------------------------------------
def load_json(filename, default):
    path = os.path.join(os.path.dirname(__file__), filename)
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return default


def save_json(filename, data):
    path = os.path.join(os.path.dirname(__file__), filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)



# ------------------------------------------------------------
#  CARICA LISTE ESTERNE
# ------------------------------------------------------------

# mega-lista PRO
modern_extended = load_json("modern_keywords_extended.json", {
    "modern_brands": [],
    "modern_devices": [],
    "modern_auto": [],
    "modern_gaming": [],
    "modern_tv": [],
    "modern_years": []
})

# flatten in un unico set
EXTENDED_TERMS = set()
for group in modern_extended.values():
    for item in group:
        EXTENDED_TERMS.add(item.lower())


# dati di apprendimento automatico
modern_data = load_json("modern_learned.json", {"phrases": [], "entries": []})
LEARNED_TERMS = set(modern_data.get("phrases", []))



# ------------------------------------------------------------
#  LISTE STORICHE (safe)
# ------------------------------------------------------------

# marchi storici che NON devono essere considerati moderni
SAFE_BRANDS = {
    "mercedes","benz","bmw","alfa","romeo","fiat","lancia",
    "porsche","audi","volkswagen","vw","toyota","honda",
    "ford","citroen","renault","opel","volvo","saab",
    "jaguar","mini","vespa","lambretta","brionvega",
    "olivetti","grundig","telefunken","panasonic"
}


# parole moderne inutili
IGNORE = {
    "turbo","benzina","diesel","airbag","abs","automatico",
    "fari","sport","android","hdr","uhd"
}



# ------------------------------------------------------------
#  PATTERN FORTI per modelli MODERNI CERTI
# ------------------------------------------------------------

MODERN_PATTERNS = [

    # smartphone moderni
    r"\biphone\s?(7|8|x|xr|xs|11|12|13|14|15)\b",
    r"\bsamsung\s?galaxy\b",
    r"\bgalaxy\s?s[5-24]\b",

    # console moderne
    r"\bps4\b",
    r"\bps5\b",
    r"\bxbox\s?one\b",
    r"\bxbox\s?series\b",
    r"\bnintendo\s?switch\b",

    # tv moderne
    r"\bsmart\s?tv\b",
    r"\b4k\b",
    r"\b8k\b",
    r"\bfd\b",

    # auto moderne
    r"\bgolf\s?(6|7|8|mk7|mk8)\b",
    r"\ba\d{1,2}\b",
    r"\bq[2-8]\b",
    r"\bgl[abcse]\b",
    r"\b\d\.\d\s?(tdi|tfsi|tfs|multijet|ecoboost)\b",
    r"\bhybrid\b",
    r"\bplug[- ]?in\b",
    r"\belectric\b",
]



# ------------------------------------------------------------
#  FUNZIONE PRINCIPALE
# ------------------------------------------------------------

def extract_modern_terms(title):
    text = title.lower()
    hits = []

    # --------------------------------------------------------
    # 1) RICONOSCIMENTO pattern moderni forti
    # --------------------------------------------------------
    for pat in MODERN_PATTERNS:
        m = re.search(pat, text)
        if m:
            hits.append(m.group().strip())


    # --------------------------------------------------------
    # 2) Tokenizzazione avanzata
    # --------------------------------------------------------
    tokens = re.findall(r"[a-z0-9\.-]+", text)

    for w in tokens:

        w = w.lower()

        if len(w) < 3:
            continue

        # marchi storici → ignora
        if w in SAFE_BRANDS:
            continue

        # parole ignorate
        if w in IGNORE:
            continue

        # evita numeri semplici
        if re.fullmatch(r"\d{2,4}", w):
            continue

        # modelli auto moderni (tipo 118i 320d 420i)
        if re.fullmatch(r"\d{2,4}[a-z]{1,2}", w):
            hits.append(w)
            continue

        # token moderni evidenti (gia' nella lista extended)
        if w in EXTENDED_TERMS:
            hits.append(w)
            continue



    # --------------------------------------------------------
    # 3) Rimuove duplicati
    # --------------------------------------------------------
    hits = sorted(set(hits))


    # --------------------------------------------------------
    # 4) Se nessun match → fallback
    # --------------------------------------------------------
    if not hits:
        full = text.strip()

        if full and full not in EXTENDED_TERMS and full not in LEARNED_TERMS:
            modern_data["phrases"].append(full)

        hits = ["fallback_full_title"]

    else:
        # salva ogni termine moderno trovato
        for h in hits:
            if h not in EXTENDED_TERMS and h not in LEARNED_TERMS:
                modern_data["phrases"].append(h)


    # --------------------------------------------------------
    # 5) Registra entry completa (debug + storico)
    # --------------------------------------------------------
    modern_data["entries"].append({
        "title": title,
        "detected": hits,
        "when": datetime.now(UTC).isoformat()
    })


    # --------------------------------------------------------
    # 6) SALVA
    # --------------------------------------------------------
    save_json("modern_learned.json", modern_data)

    return hits
