# utils_synonyms.py
# ============================================================
#  Sistema sinonimi backend RetroFuture (VERSIONE 2025)
#
#  MIGLIORAMENTI:
#    ✔ rilevamento sinonimi basato su token, non solo regex
#    ✔ espansione completa: canonica + varianti (limitate)
#    ✔ no duplicati
#    ✔ gestione gruppi vuoti
#    ✔ fallback sicuri
#    ✔ ✅ FIX 2026: underscore "_" -> spazio " " (query + keywords più pulite)
# ============================================================

import json
import os
import re

SYNONYMS_FILE = os.path.join(os.path.dirname(__file__), "synonyms_map.json")


# ============================================================
# Normalizzazione base
# ============================================================

def _norm_base(s: str) -> str:
    if not s:
        return ""
    s = str(s).lower()

    # ✅ FIX: evita sinonimi tipo "fotocamera_analogica"
    # (meglio per marketplace queries, keyword tokens, classifier)
    s = s.replace("_", " ")

    s = s.replace("’", "'").replace("‘", "'").replace("`", "'").replace("Â", "")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


# ============================================================
# Caricamento sinonimi
# ============================================================

def _load_synonyms_map():
    if not os.path.exists(SYNONYMS_FILE):
        print("[utils_synonyms] Nessun synonyms_map.json trovato.")
        return {}

    try:
        with open(SYNONYMS_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        print("[utils_synonyms] Errore lettura synonyms_map.json:", e)
        return {}

    syn_map = {}

    for canon, variants in raw.items():
        canon_norm = _norm_base(canon)
        if not canon_norm:
            continue

        clean_variants = []
        if isinstance(variants, list):
            for v in variants:
                v_norm = _norm_base(v)
                if v_norm and v_norm != canon_norm:
                    clean_variants.append(v_norm)

        syn_map[canon_norm] = clean_variants

    print(f"[utils_synonyms] Caricati {len(syn_map)} gruppi sinonimi.")
    return syn_map


SYN_MAP = _load_synonyms_map()


# ============================================================
# Espansione sinonimi (versione potenziata)
# ============================================================

def _tokenized_match(text: str, phrase: str) -> bool:
    """
    Match sicuro per frasi multi-parola basato su token.
    Evita falsi positivi e rileva anche parole parziali.
    """
    text_tokens = text.split()
    phrase_tokens = phrase.split()

    if len(phrase_tokens) == 1:
        return phrase_tokens[0] in text_tokens

    # match sequenziale multi-token
    for i in range(0, len(text_tokens) - len(phrase_tokens) + 1):
        if text_tokens[i:i + len(phrase_tokens)] == phrase_tokens:
            return True
    return False


def expand_with_synonyms(text: str) -> str:
    """
    Espansione completa 2025:
      - ritorna testo normalizzato
      - aggiunge canonica se compare una variante
      - aggiunge varianti se compare la canonica
      - NO duplicati
      - NO esplosione del testo
    """
    base = _norm_base(text)
    if not base or not SYN_MAP:
        return base

    out = set()
    out.add(base)

    for canon, variants in SYN_MAP.items():
        group_hit = False

        # 1) Se nel testo compare la canonica → aggiungi canonica e alcune varianti
        if _tokenized_match(base, canon):
            out.add(canon)
            for v in variants[:2]:  # limitiamo a 2 varianti
                out.add(v)
            continue

        # 2) Se compare una variante → aggiungi canonica
        for v in variants:
            if _tokenized_match(base, v):
                out.add(canon)
                out.add(v)
                group_hit = True
                break

        # 3) se gruppo riconosciuto → aggiungi max 1 altra variante
        if group_hit:
            extra = [vv for vv in variants if vv not in out]
            if extra:
                out.add(extra[0])

    return " ".join(sorted(out))


# ============================================================
# Espansione keyword per scraper
# ============================================================

def expand_keyword_for_scraper(keyword: str, max_synonyms: int = 2) -> list:
    """
    Output:
        keyword,
        keyword vintage,
        sinonimi (max N),
        sinonimi vintage
    """
    base = _norm_base(keyword)
    if not base:
        return []

    out = []

    # keyword base
    out.append(base)
    if "vintage" not in base:
        out.append(base + " vintage")

    # trova gruppo nel quale è contenuta
    for canon, variants in SYN_MAP.items():
        group = [canon] + variants
        if base in group:
            # prendi da canon+variants
            for v in group:
                if v not in out:
                    out.append(v)
                    if "vintage" not in v:
                        out.append(v + " vintage")
                if len(out) >= 2 + max_synonyms * 2:
                    break
            break

    # dedup con ordine preservato
    seen = set()
    final = []
    for x in out:
        if x not in seen:
            seen.add(x)
            final.append(x)

    return final
