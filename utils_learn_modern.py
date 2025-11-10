# utils_learn_modern.py
import re, json, os
from datetime import datetime, UTC


def load_json(file, default):
    path = os.path.join(os.path.dirname(__file__), file)
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return default


def save_json(file, data):
    path = os.path.join(os.path.dirname(__file__), file)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)



# ✅ Marche storiche che NON devono essere considerate moderne
SAFE_BRANDS = {
    "mercedes","benz","bmw","alfa","romeo","fiat","lancia","porsche","audi",
    "volkswagen","vw","toyota","honda","ford","citroen","renault","opel",
    "volvo","saab","jaguar","mini","vespa","lambretta"
}

# ✅ Parole generiche NON utili
IGNORE = {"turbo","benzina","diesel","airbag","abs","automatico","fari","sport"}

# ✅ Pattern moderni certi
MODERN_PATTERNS = [
    r"\bgl[abce]\b",
    r"\bgla\b", r"\bglc\b", r"\bgle\b", r"\bgls\b",
    r"\bq[2-8]\b",
    r"\ba\d{1,2}\b", r"\bs\d{1,2}\b",
    r"\bgolf\s?(6|7|8|mk7|mk8)\b",
    r"\bamg\s?\d{2,3}\b",
    r"\b\d\.\d\s?(tdi|tfsi|tfs|multijet|ecoboost)\b",
    r"\b4matic\b",
    r"\bh?y?brid\b",
    r"\bplug[- ]?in\b",
    r"\bfull[- ]?electric\b",
]



def extract_modern_terms(title):
    text = title.lower()
    hits = []

    # ✅ 1) Pattern moderni sicuri
    for pat in MODERN_PATTERNS:
        m = re.search(pat, text)
        if m:
            hits.append(m.group().strip())

    # ✅ 2) Tokenizzazione intelligente
    tokens = re.findall(r"[a-z0-9\.]+", text)

    for w in tokens:
        if len(w) <= 2:
            continue
        if w in SAFE_BRANDS:
            continue
        if w in IGNORE:
            continue

        # ❌ Evita numeri puri (100 / 1000) — non sono moderni
        if re.fullmatch(r"\d{2,4}", w):
            continue

        # ✅ Modelli moderni tipo “200d”, “320d”, “118i”
        if re.fullmatch(r"\d{2,4}[a-z]{1,2}", w):
            hits.append(w)

    hits = sorted(set(hits))

    # ✅ CARICA FILE DI APPRENDIMENTO
    modern_data = load_json("modern_learned.json", {"phrases": [], "entries": []})

    # ✅ 3) Se NON ha trovato nulla → fallback: salva il titolo intero
    if not hits:
        full = text.strip()
        if full and full not in modern_data["phrases"]:
            modern_data["phrases"].append(full)
        hits = ["fallback_full_title"]  # indicatore per entries

    else:
        # ✅ Salva ogni hit moderna
        for h in hits:
            if h not in modern_data["phrases"] and h != "fallback_full_title":
                modern_data["phrases"].append(h)

    # ✅ 4) Aggiungi entry con dettagli
    modern_data["entries"].append({
        "title": title,
        "detected": hits,
        "when": datetime.now(UTC).isoformat()
    })

    # ✅ 5) Salva file
    save_json("modern_learned.json", modern_data)

    return hits
