# utils_normalize.py
import re
import hashlib
from hashlib import sha1
from datetime import datetime, UTC

def normalizza_annuncio(raw, source_name):
    """
    Converte un annuncio grezzo in formato standard e aggiunge hash + timestamp.
    Compatibile con Python 3.14 e MongoDB (date ISO stringhe).
    """

    # 1️⃣ Titolo
    title = (raw.get("title") or raw.get("titolo") or "").strip()

    # 2️⃣ Descrizione (fallback: titolo)
    description = raw.get("description") or raw.get("descrizione") or title

    # 3️⃣ Prezzo numerico
    prezzo_str = str(raw.get("price") or raw.get("prezzo") or "0").replace(",", ".")
    try:
        prezzo_val = float(re.findall(r"[\d\.]+", prezzo_str)[0])
    except Exception:
        prezzo_val = 0.0

    # 4️⃣ URL
    url = raw.get("url") or raw.get("link") or ""

    # 5️⃣ Immagine
    image = raw.get("image") or raw.get("img") or raw.get("immagine") or ""

    # 6️⃣ Luogo e categoria
    location = raw.get("location") or raw.get("luogo") or ""
    category = raw.get("category") or raw.get("categoria") or "vario"

    # 7️⃣ Condizione (nuovo/usato)
    condition = raw.get("condition") or raw.get("condizione") or None

    # 8️⃣ Parole chiave
    tokens = re.findall(r"[a-zA-Z0-9àèéìòù]+", title.lower())
    keywords = list(set(tokens))

    # 9️⃣ ID sorgente (da URL o ID esistente)
    source_id = raw.get("id") or raw.get("source_id")
    if not source_id and url:
        source_id = sha1(url.encode("utf-8")).hexdigest()[:12]

    # 🔟 Hash univoco per deduplicazione
    unique_key = f"{source_name}-{title}-{prezzo_val}-{url}"
    hash_value = hashlib.md5(unique_key.encode("utf-8")).hexdigest()

    # 🕒 Timestamp compatibile (UTC ISO string)
    now_iso = datetime.now(UTC).isoformat()

    # ✅ Format prezzo sempre 2 decimali
    price_value = f"{prezzo_val:.2f}" if prezzo_val else "0.00"

    # ✅ Format leggibile per UI
    price_display = f"{price_value} EUR"

    return {
        "source": source_name,
        "source_id": source_id,
        "title": title,
        "description": description,
        "price_value": price_value,
        "price_display": price_display,
        "price_currency": "EUR",
        "url": url,
        "image": image,
        "location": location,
        "category": category,
        "condition": condition,
        "scraped_at": now_iso,
        "updated_at": now_iso,
        "hash": hash_value,
        "keywords": keywords,
    }
