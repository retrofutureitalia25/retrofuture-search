# price_analyzer.py
import math
import os
import re
from statistics import median
from typing import Any, Dict, List, Optional


# ============================================================
# CONFIG
# ============================================================

MAX_ANALYSIS_RESULTS = 150

MIN_VALID_PRICE = 3
MAX_VALID_PRICE = 2000

OUTLIER_TRIM_RATIO = 0.10
MIN_ITEMS_FOR_OUTLIER_TRIM = 10

# se la dispersione tra min e max è troppo alta, nascondiamo la quotazione
MAX_DISPLAY_SPREAD_RATIO = 12.0

PRIMARY_VINTAGE_STATUS = {
    "vintage_originale",
    "vintage_generico",
}

FALLBACK_VINTAGE_STATUS = {
    "vintage_dubbio",
}

EXCLUDED_VINTAGE_STATUS = {
    "retro_moderno",
    "non_vintage",
}

MIN_WORDS_BY_SOURCE = {
    "ebay": 2,
    "subito": 2,
    "mercatinousato": 2,
    "vinted": 3,
}

PRICE_ANALYZER_DEBUG = os.getenv("PRICE_ANALYZER_DEBUG", "1") == "1"

EXCLUDE_TERMS = {
    # condizioni / difetti
    "ricambi",
    "ricambio",
    "per pezzi",
    "per parti",
    "difettoso",
    "guasto",
    "non funzionante",
    "da riparare",

    # documentazione
    "manuale",
    "istruzioni",
    "brochure",
    "catalogo",

    # vendite multiple
    "lotto",
    "stock",
    "blocco",
    "bundle",

    # scatole vuote
    "solo scatola",
    "scatola vuota",
    "box vuoto",
    "solo box",

    # accessori generici
    "alimentatore",
    "cavo",
    "batteria",
    "custodia",
    "supporto",
    "stand",

    # adattatori
    "adattatore",
    "adapter",
    "convertitore",

    # periferiche
    "joystick",
    "controller",
    "mouse",
    "tastiera",

    # supporti
    "floppy",
    "disk",
    "disco",
    "cassette",
    "cassetta",
    "nastro",

    # giochi
    "gioco",
    "giochi",
    "cartuccia",
    "cartucce",

    # periferiche retro comuni
    "datasette",
    "c2n",

    # parti / riparazione / mod
    "kit",
    "repair",
    "riparazione",
    "ganci",
    "case",
    "chip",
    "ram",
    "socket",
    "modem",
    "wifi",
    "cartridge",
    "test",

    # gadget / decorazione
    "gadget",
    "decorations",
    "decoration",
    "christmas",
    "ball",

    # altri termini sporchi
    "swap",
}

ACCESSORY_START_TERMS = {
    "gioco",
    "game",
    "cartuccia",
    "cartucce",
    "cassette",
    "cassetta",
    "floppy",
    "disk",
    "disco",
    "joystick",
    "controller",
    "adattatore",
    "adapter",
    "lotto",
    "stock",
    "bundle",
    "datasette",
    "c2n",
    "kit",
    "repair",
    "gadget",
    "chip",
    "modem",
    "swap",
}

MAIN_OBJECT_HINTS = {
    "computer",
    "console",
    "macchina",
    "radio",
    "giradischi",
    "televisore",
    "tv",
    "monitor",
    "walkman",
    "typewriter",
    "biscottone",
    "personal",
}

SAFE_TRAILING_MODIFIERS = {
    "con",
    "box",
    "boxed",
    "boxato",
    "funzionante",
    "restaurato",
    "vintage",
    "originale",
    "originali",
    "completo",
    "completa",
    "complete",
    "retro",
    "no",
    "da",
    "collezione",
    "home",
    "personal",
    "computer",
}

LEADING_BRAND_WORDS = {
    "commodore",
    "sony",
    "philips",
    "olivetti",
    "nintendo",
    "sega",
    "atari",
    "geloso",
    "grundig",
    "panasonic",
    "sharp",
    "nec",
    "sinclair",
    "amstrad",
}

LEADING_SAFE_WORDS = {
    "computer",
    "vintage",
    "retro",
    "originale",
    "originali",
    "personal",
    "home",
}

MULTI_ITEM_TERMS = {
    "set",
    "lotto",
    "bundle",
    "blocco",
    "stock",
    "assortiti",
    "assortite",
    "accessori",
    "pezzi",
}

SECONDARY_MEDIA_TERMS = {
    # editoriale / stampa
    "manuale",
    "manuali",
    "istruzioni",
    "guida",
    "guide",
    "libro",
    "libri",
    "book",
    "books",
    "rivista",
    "riviste",
    "magazine",
    "catalogo",
    "cataloghi",
    "brochure",
    "poster",
    "locandina",
    "locandine",
    "stampa",
    "stampe",
    "print",
    "photo",
    "foto",
    "fotografia",
    "ad",
    "advert",
    "pubblicita",
    "pubblicità",

    # merch / gadget
    "tshirt",
    "shirt",
    "tee",
    "maglietta",
    "keychain",
    "portachiavi",
    "adesivo",
    "adesivi",
    "sticker",
    "stickers",
    "gadget",

    # software / giochi / supporti
    "software",
    "videogioco",
    "videogiochi",
    "gioco",
    "giochi",
    "disk",
    "disco",
    "floppy",
    "cassette",
    "cassetta",
    "cartuccia",
    "cartucce",
    "pal",
    "ntsc",

    # modernizzazioni / versioni non originali
    "mini",
    "remake",
    "swap",
    "righello",
    "calendario",
    "orologio",
}

BUNDLE_COMPANION_TERMS = {
    "monitor",
    "stampante",
    "printer",
    "datasette",
    "drive",
    "disk drive",
    "floppy drive",
    "lettore",
    "registratore",
    "mouse",
    "joystick",
    "alimentatore",
    "power supply",
    "cavo",
    "cavi",
    "accessori",
    "tastiera",
    "casse",
    "speaker",
    "speakers",
    "telecomando",
    "gamepad",
    "pad",
}

BUNDLE_CONNECTOR_PATTERNS = {
    " con ",
    " completo di ",
    " completa di ",
    " completi di ",
    " insieme a ",
    " assieme a ",
    " corredato da ",
    " corredata da ",
}


# ============================================================
# DEBUG
# ============================================================

def _dbg(*args):
    if PRICE_ANALYZER_DEBUG:
        print(*args)


# ============================================================
# NORMALIZATION HELPERS
# ============================================================

def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_text(text: str) -> str:
    text = _safe_str(text).lower()
    text = re.sub(r"[^\w\sàèéìòù\+\/\-]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _word_count(text: str) -> int:
    norm = _normalize_text(text)
    if not norm:
        return 0
    return len(norm.split())


def _extract_core_query_tokens(query: str) -> List[str]:
    q = _normalize_text(query)
    if not q:
        return []

    stop_tokens = {
        "vintage", "retro", "retrò", "epoca",
        "anni", "anno",
        "originale", "originali",
        "con", "per", "di", "da", "del", "della", "dello", "delle", "degli",
        "in", "su", "a", "e", "ed", "o",
        "il", "lo", "la", "i", "gli", "le",
        "un", "uno", "una",
    }

    return [t for t in q.split() if t and t not in stop_tokens]


def _is_specific_query(query: str) -> bool:
    qn = _normalize_text(query)
    if not qn:
        return False

    core = _extract_core_query_tokens(qn)
    if len(core) < 2:
        return False

    # se c'è un numero, di solito è una query specifica
    if re.search(r"\d", qn):
        return True

    # 3 o più token core = abbastanza specifica
    if len(core) >= 3:
        return True

    # qui restano soprattutto query a 2 token
    # molte di queste per RetroFuture sono valide:
    # "walkman sony", "radio geloso", "zx spectrum", ecc.
    GENERIC_OBJECT_TOKENS = {
        "borsa",
        "lampada",
        "computer",
        "giocattolo",
        "gioco",
        "oggetto",
        "articolo",
        "mobile",
        "mobili",
        "accessorio",
        "accessori",
        "prodotto",
        "prodotti",
        "cosa",
        "robe",
        "roba",
    }

    WEAK_DESCRIPTOR_TOKENS = {
        "vintage",
        "retro",
        "antico",
        "antica",
        "epoca",
        "vecchio",
        "vecchia",
        "usato",
        "usata",
        "anni",
    }

    # se è tipo "lampada vintage", "borsa vintage" → non specifica
    if len(core) == 2:
        a, b = core[0], core[1]

        if (a in GENERIC_OBJECT_TOKENS and b in WEAK_DESCRIPTOR_TOKENS) or \
           (b in GENERIC_OBJECT_TOKENS and a in WEAK_DESCRIPTOR_TOKENS):
            return False

        # altrimenti la consideriamo specifica
        return True

    return False


def _build_strong_query_forms(query: str) -> List[str]:
    qn = _normalize_text(query)
    core = _extract_core_query_tokens(qn)

    if not core:
        return []

    forms = set()
    forms.add(" ".join(core))

    if len(core) >= 2:
        forms.add(" ".join(core[-2:]))
        forms.add(" ".join(core[:2]))

        first = core[0]
        second = core[1]
        if re.fullmatch(r"\d+", second) and first:
            forms.add(f"{first[:1]}{second}")

    if len(core) == 2:
        forms.add(f"{core[1]} {core[0]}")

    cleaned = []
    seen = set()

    for f in forms:
        nf = _normalize_text(f)
        if not nf or len(nf) < 2:
            continue
        if nf not in seen:
            seen.add(nf)
            cleaned.append(nf)

    cleaned.sort(key=lambda x: (-len(x.split()), -len(x)))
    return cleaned


def _title_contains_strong_form_near_start(title: str, query: str, window_words: int = 4) -> bool:
    """
    Accetta anche titoli come:
    - commodore amiga 500
    - computer commodore amiga 500

    ma NON:
    - batman commodore 64
    - ace commodore 64
    """
    title_norm = _normalize_text(title)
    if not title_norm:
        return False

    title_words = title_norm.split()
    if not title_words:
        return False

    strong_forms = _build_strong_query_forms(query)

    for form in strong_forms:
        form_words = _normalize_text(form).split()
        if not form_words:
            continue

        max_start = max(0, min(window_words, len(title_words)) - len(form_words))
        for start_idx in range(max_start + 1):
            candidate = title_words[start_idx:start_idx + len(form_words)]
            if candidate != form_words:
                continue

            before_words = title_words[:start_idx]

            if not before_words:
                return True

            allowed_before = LEADING_BRAND_WORDS | LEADING_SAFE_WORDS | MAIN_OBJECT_HINTS
            if all(w in allowed_before for w in before_words):
                return True

    return False


def _title_starts_with_strong_form(title: str, query: str) -> bool:
    title_norm = _normalize_text(title)
    if not title_norm:
        return False

    title_words = title_norm.split()
    strong_forms = _build_strong_query_forms(query)

    if not strong_forms:
        return False

    allowed_hardware_terms = {
        "16k",
        "48k",
        "128k",
        "plus",
        "+",
        "plus2",
        "plus3",
        "+2",
        "+3",
        "computer",
        "personal",
        "home",
        "system",
    }

    for form in strong_forms:
        form_norm = _normalize_text(form)
        form_words = form_norm.split()

        if not form_words:
            continue

        if title_norm == form_norm:
            return True

        if title_norm.startswith(form_norm + " "):
            next_word_index = len(form_words)

            if next_word_index >= len(title_words):
                return True

            next_word = title_words[next_word_index]
            if next_word in ACCESSORY_START_TERMS:
                return False

            trailing = title_norm[len(form_norm):].strip()
            trailing_words = trailing.split()

            if not trailing_words:
                return True

            # se dopo la strong form compaiono subito termini accessorio/media, scarta
            if any(w in ACCESSORY_START_TERMS for w in trailing_words):
                return False

            if any(w in EXCLUDE_TERMS for w in trailing_words):
                return False

            if set(trailing_words).intersection(MAIN_OBJECT_HINTS):
                return True

            # caso tipico: "zx spectrum magic carpet"
            # 1-2 parole finali non hardware => probabilmente gioco/software
            if 1 <= len(trailing_words) <= 2:
                if not any(w in allowed_hardware_terms for w in trailing_words):
                    return False

            return True

    return _title_contains_strong_form_near_start(title, query, window_words=4)


def title_matches_query_core(title: str, query: str) -> bool:
    title_norm = _normalize_text(title)
    core_tokens = _extract_core_query_tokens(query)

    if not title_norm:
        return False

    if not core_tokens:
        return True

    if _is_specific_query(query):
        return _title_starts_with_strong_form(title, query)

    if len(core_tokens) >= 2:
        hits = sum(1 for tok in core_tokens if tok in title_norm)
        return hits >= 2

    return core_tokens[0] in title_norm


def title_starts_like_accessory(title: str) -> bool:
    title_norm = _normalize_text(title)
    if not title_norm:
        return False

    title_words = title_norm.split()
    if not title_words:
        return False

    return title_words[0] in ACCESSORY_START_TERMS


def title_looks_like_multi_item(title: str) -> bool:
    """
    Rileva annunci con più oggetti insieme.
    Esempi:
    - Commodore 64 + monitor
    - Amiga 500 / 1200 / 2000
    - Set accessori Commodore 64
    """
    raw_title = _safe_str(title)
    title_norm = _normalize_text(title)

    if not title_norm:
        return False

    if "+" in raw_title:
        return True

    slash_count = raw_title.count("/")
    if slash_count >= 2:
        return True

    words = title_norm.split()
    if any(w in MULTI_ITEM_TERMS for w in words):
        return True

    numbers = re.findall(r"\d+", title_norm)
    if len(set(numbers)) >= 3:
        return True

    return False


def title_looks_like_bundle_with_main_item(title: str) -> bool:
    """
    Rileva titoli con oggetto principale + periferica/accessorio importante.
    Esempi:
    - Commodore 64 con monitor
    - Amiga 500 completo di mouse
    - Radio vintage con casse
    """
    title_norm = f" {_normalize_text(title)} "
    if not title_norm.strip():
        return False

    for connector in BUNDLE_CONNECTOR_PATTERNS:
        for companion in BUNDLE_COMPANION_TERMS:
            companion_norm = _normalize_text(companion)
            if not companion_norm:
                continue

            pattern = f"{connector}{companion_norm}"
            if pattern in title_norm:
                return True

    return False


def title_looks_like_secondary_media_or_merch(title: str) -> bool:
    """
    Esclude titoli che sembrano:
    - editoriale / stampa
    - merchandising / gadget
    - giochi / software / supporti
    - versioni mini / remake
    """
    title_norm = _normalize_text(title)
    if not title_norm:
        return False

    words = set(title_norm.split())

    if words.intersection(SECONDARY_MEDIA_TERMS):
        return True

    if "print ad" in title_norm:
        return True

    if "mini ordinateur" in title_norm:
        return True

    if "retro games" in title_norm:
        return True

    if "t shirt" in title_norm:
        return True

    return False


def title_looks_like_main_object(title: str, query: str) -> bool:
    title_norm = _normalize_text(title)
    if not title_norm:
        return False

    if not _is_specific_query(query):
        return True

    title_words = title_norm.split()
    title_word_set = set(title_words)

    if title_word_set.intersection(MAIN_OBJECT_HINTS):
        return True

    strong_forms = _build_strong_query_forms(query)

    allowed_hardware_terms = {
        "16k",
        "48k",
        "128k",
        "plus",
        "+",
        "plus2",
        "plus3",
        "+2",
        "+3",
        "computer",
        "personal",
        "home",
        "system",
    }

    for form in strong_forms:
        form_norm = _normalize_text(form)
        if not form_norm:
            continue

        if title_norm == form_norm:
            return True

        if title_norm.startswith(form_norm + " "):
            trailing = title_norm[len(form_norm):].strip()
            trailing_words = trailing.split()

            if not trailing_words:
                return True

            for w in trailing_words:
                if w in EXCLUDE_TERMS:
                    return False

            if set(trailing_words).intersection(MAIN_OBJECT_HINTS):
                return True

            if len(trailing_words) > 5:
                return False

            if any(w in ACCESSORY_START_TERMS for w in trailing_words):
                return False

            if all(w in SAFE_TRAILING_MODIFIERS for w in trailing_words):
                return True

            # caso tipo: "zx spectrum magic carpet"
            # 1-2 parole finali non hardware => probabilmente gioco/software
            if 1 <= len(trailing_words) <= 2:
                if not any(w in allowed_hardware_terms for w in trailing_words):
                    return False

            return False

    if _title_contains_strong_form_near_start(title, query, window_words=4):
        allowed_before = LEADING_BRAND_WORDS | LEADING_SAFE_WORDS | MAIN_OBJECT_HINTS

        for form in strong_forms:
            form_norm = _normalize_text(form)
            if not form_norm:
                continue

            form_words = form_norm.split()
            if not form_words:
                continue

            for start_idx in range(min(4, len(title_words))):
                candidate = title_words[start_idx:start_idx + len(form_words)]
                if candidate != form_words:
                    continue

                before_words = title_words[:start_idx]
                if before_words and not all(w in allowed_before for w in before_words):
                    continue

                trailing_words = title_words[start_idx + len(form_words):]

                if not trailing_words:
                    return True

                if any(w in ACCESSORY_START_TERMS for w in trailing_words):
                    return False

                if any(w in EXCLUDE_TERMS for w in trailing_words):
                    return False

                if set(trailing_words).intersection(MAIN_OBJECT_HINTS):
                    return True

                if all(w in SAFE_TRAILING_MODIFIERS for w in trailing_words):
                    return True

                # caso tipo: "sinclair zx spectrum magic carpet"
                # dopo la strong form restano 1-2 parole non hardware => scarta
                if 1 <= len(trailing_words) <= 2:
                    if not any(w in allowed_hardware_terms for w in trailing_words):
                        return False

                return True

    return False


def _to_float_price(value: Any) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    raw = str(value).strip()
    if not raw:
        return None

    raw = raw.replace(".", "").replace(",", ".")
    raw = re.sub(r"[^\d.]", "", raw)

    if not raw:
        return None

    try:
        return float(raw)
    except ValueError:
        return None


# ============================================================
# VALIDATION HELPERS
# ============================================================

def is_valid_title(title: Any) -> bool:
    return bool(_safe_str(title))


def has_excluded_terms(title: str) -> bool:
    norm_title = _normalize_text(title)
    if not norm_title:
        return True

    for term in EXCLUDE_TERMS:
        term_norm = _normalize_text(term)
        if term_norm and term_norm in norm_title:
            return True

    return False


def is_valid_marketplace_title(title: str, source: str) -> bool:
    source_norm = _normalize_text(source)
    min_words = MIN_WORDS_BY_SOURCE.get(source_norm, 2)
    return _word_count(title) >= min_words


def is_valid_price(price_value: Any) -> bool:
    price = _to_float_price(price_value)
    if price is None:
        return False
    return MIN_VALID_PRICE <= price <= MAX_VALID_PRICE


def is_allowed_vintage_class(vintage_class: Any, allowed_status: set[str]) -> bool:
    vc = _safe_str(vintage_class)
    if not vc:
        return False
    return vc in allowed_status


# ============================================================
# STAT HELPERS
# ============================================================

def _percentile(sorted_values: List[float], pct: float) -> Optional[float]:
    if not sorted_values:
        return None

    if len(sorted_values) == 1:
        return sorted_values[0]

    k = (len(sorted_values) - 1) * (pct / 100.0)
    f = math.floor(k)
    c = math.ceil(k)

    if f == c:
        return sorted_values[int(k)]

    d0 = sorted_values[f] * (c - k)
    d1 = sorted_values[c] * (k - f)
    return d0 + d1


def remove_outliers(prices: List[float]) -> List[float]:
    if len(prices) < MIN_ITEMS_FOR_OUTLIER_TRIM:
        return prices[:]

    sorted_prices = sorted(prices)
    cut = int(len(sorted_prices) * OUTLIER_TRIM_RATIO)

    if cut <= 0:
        return sorted_prices

    trimmed = sorted_prices[cut: len(sorted_prices) - cut]

    if not trimmed:
        return sorted_prices

    return trimmed


def compute_statistics(prices: List[float]) -> Dict[str, Optional[float]]:
    if not prices:
        return {
            "count": 0,
            "median_price": None,
            "min_price": None,
            "max_price": None,
            "p25_price": None,
            "p75_price": None,
        }

    sorted_prices = sorted(prices)
    count = len(sorted_prices)

    stats = {
        "count": count,
        "median_price": float(median(sorted_prices)),
        "min_price": float(min(sorted_prices)),
        "max_price": float(max(sorted_prices)),
        "p25_price": None,
        "p75_price": None,
    }

    if count >= 20:
        stats["p25_price"] = _percentile(sorted_prices, 25)
        stats["p75_price"] = _percentile(sorted_prices, 75)

    return stats


def classify_estimate(count: int) -> str:
    if count >= 20:
        return "strong_estimate"
    if 3 <= count <= 19:
        return "preliminary_estimate"
    if count == 1:
        return "single_observation"
    return "no_data"


def compute_dominant_source_share(source_stats: Dict[str, Dict[str, Optional[float]]]) -> float:
    total = sum(int(data.get("count", 0) or 0) for data in source_stats.values())
    if total <= 0:
        return 0.0

    dominant = max(int(data.get("count", 0) or 0) for data in source_stats.values())
    return round(dominant / total, 4)


def decide_display_eligibility(
    query: str,
    raw_count_valid: int,
    clean_count_valid: int,
    min_price: Optional[float],
    max_price: Optional[float],
    source_stats: Dict[str, Dict[str, Optional[float]]],
) -> Dict[str, Any]:
    if not _is_specific_query(query):
        return {
            "show_price_box": False,
            "display_mode": "hide_quote",
            "display_reason": "query_not_specific",
        }

    if clean_count_valid == 0:
        return {
            "show_price_box": False,
            "display_mode": "hide_quote",
            "display_reason": "no_clean_data",
        }

    spread_ratio = None
    if min_price is not None and min_price > 0 and max_price is not None:
        spread_ratio = max_price / min_price

    if clean_count_valid >= 3 and spread_ratio is not None and spread_ratio >= MAX_DISPLAY_SPREAD_RATIO:
        return {
            "show_price_box": False,
            "display_mode": "hide_quote",
            "display_reason": "extreme_price_dispersion",
        }

    if clean_count_valid == 1:
        return {
            "show_price_box": True,
            "display_mode": "show_single_observation",
            "display_reason": "single_valid_observation",
        }

    if clean_count_valid == 2:
        return {
            "show_price_box": False,
            "display_mode": "hide_quote",
            "display_reason": "only_two_prices",
        }

    if 3 <= clean_count_valid <= 19:
        return {
            "show_price_box": True,
            "display_mode": "show_preliminary_estimate",
            "display_reason": "enough_for_preliminary_estimate",
        }

    return {
        "show_price_box": True,
        "display_mode": "show_strong_estimate",
        "display_reason": "enough_for_strong_estimate",
    }


# ============================================================
# COLLECTION HELPERS
# ============================================================

def _build_used_item(item: Dict[str, Any], price: float) -> Dict[str, Any]:
    return {
        "title": _safe_str(item.get("title")),
        "price_value": price,
        "source": _safe_str(item.get("source")).lower(),
        "vintage_class": _safe_str(item.get("vintage_class")),
        "url": _safe_str(item.get("url")),
    }


def collect_valid_items(
    query: str,
    results: List[Dict[str, Any]],
    allowed_status: set[str]
) -> List[Dict[str, Any]]:
    valid_items: List[Dict[str, Any]] = []

    reject_stats = {
        "title_matches_query_core": 0,
        "title_starts_like_accessory": 0,
        "title_looks_like_multi_item": 0,
        "title_looks_like_bundle_with_main_item": 0,
        "title_looks_like_secondary_media_or_merch": 0,
        "title_looks_like_main_object": 0,
        "vintage_class_not_allowed": 0,
        "excluded_vintage_status": 0,
        "invalid_title": 0,
        "has_excluded_terms": 0,
        "invalid_marketplace_title": 0,
        "price_none": 0,
        "invalid_price": 0,
    }

    for item in results:
        title = _safe_str(item.get("title"))
        source = _safe_str(item.get("source")).lower()
        vintage_class = _safe_str(item.get("vintage_class"))
        price = _to_float_price(item.get("price_value"))

        if not title_matches_query_core(title, query):
            reject_stats["title_matches_query_core"] += 1
            continue

        if title_starts_like_accessory(title):
            reject_stats["title_starts_like_accessory"] += 1
            continue

        if title_looks_like_multi_item(title):
            reject_stats["title_looks_like_multi_item"] += 1
            continue

        if title_looks_like_bundle_with_main_item(title):
            reject_stats["title_looks_like_bundle_with_main_item"] += 1
            continue

        if title_looks_like_secondary_media_or_merch(title):
            reject_stats["title_looks_like_secondary_media_or_merch"] += 1
            continue

        if not title_looks_like_main_object(title, query):
            reject_stats["title_looks_like_main_object"] += 1
            continue

        if not is_allowed_vintage_class(vintage_class, allowed_status):
            reject_stats["vintage_class_not_allowed"] += 1
            continue

        if vintage_class in EXCLUDED_VINTAGE_STATUS:
            reject_stats["excluded_vintage_status"] += 1
            continue

        if not is_valid_title(title):
            reject_stats["invalid_title"] += 1
            continue

        if has_excluded_terms(title):
            reject_stats["has_excluded_terms"] += 1
            continue

        if not is_valid_marketplace_title(title, source):
            reject_stats["invalid_marketplace_title"] += 1
            continue

        if price is None:
            reject_stats["price_none"] += 1
            continue

        if not is_valid_price(price):
            reject_stats["invalid_price"] += 1
            continue

        valid_items.append(_build_used_item(item, price))

    _dbg("\n--- COLLECT VALID ITEMS DEBUG ---")
    _dbg("QUERY:", query)
    _dbg("INPUT RESULTS:", len(results))
    _dbg("ALLOWED STATUS:", sorted(list(allowed_status)))
    _dbg("VALID ITEMS:", len(valid_items))
    _dbg("REJECT STATS:", reject_stats)

    if PRICE_ANALYZER_DEBUG:
        for idx, item in enumerate(results[:20]):
            _dbg(
                f"{idx+1:02d}. {_safe_str(item.get('source')).lower()} | "
                f"{_safe_str(item.get('vintage_class'))} | "
                f"{_safe_str(item.get('title'))}"
            )
        _dbg("---------------------------------\n")

    return valid_items


def compute_source_stats(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Optional[float]]]:
    by_source: Dict[str, List[float]] = {}

    for item in items:
        source = _safe_str(item.get("source")).lower()
        price = _to_float_price(item.get("price_value"))

        if not source or price is None:
            continue

        by_source.setdefault(source, []).append(price)

    source_stats: Dict[str, Dict[str, Optional[float]]] = {}

    for source, prices in by_source.items():
        if not prices:
            continue

        sorted_prices = sorted(prices)
        count = len(sorted_prices)

        source_stats[source] = {
            "count": count,
            "median_price": float(median(sorted_prices)) if count >= 2 else float(sorted_prices[0]),
            "min_price": float(min(sorted_prices)),
            "max_price": float(max(sorted_prices)),
        }

    return source_stats


# ============================================================
# OUTPUT BUILDERS
# ============================================================

def _base_output(query: str, used_fallback_dubbio: bool) -> Dict[str, Any]:
    return {
        "query": query,
        "mode": "no_data",
        "count_valid": 0,
        "raw_count_valid": 0,
        "clean_count_valid": 0,
        "median_price": None,
        "min_price": None,
        "max_price": None,
        "p25_price": None,
        "p75_price": None,
        "observed_price": None,
        "price_spread_ratio": None,
        "dominant_source_share": 0.0,
        "sources": [],
        "source_stats": {},
        "show_price_box": False,
        "display_mode": "hide_quote",
        "display_reason": "no_data",
        "used_fallback_dubbio": used_fallback_dubbio,
    }


def _round_money(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), 2)


# ============================================================
# PUBLIC API
# ============================================================

def analyze_prices(query: str, results: List[Dict[str, Any]]) -> Dict[str, Any]:
    query = _safe_str(query)

    _dbg("\n=== PRICE ANALYZER START ===")
    _dbg("QUERY:", query)
    _dbg("RESULTS IN:", len(results))

    core_tokens = _extract_core_query_tokens(query)
    _dbg("CORE TOKENS:", core_tokens)

    output = _base_output(query, False)

    if len(core_tokens) < 2:
        _dbg("RETURN EARLY: query too generic")
        output["display_reason"] = "query_not_specific"
        return output

    sampled_results = results[:MAX_ANALYSIS_RESULTS]

    primary_items = collect_valid_items(
        query,
        sampled_results,
        PRIMARY_VINTAGE_STATUS,
    )
    _dbg("PRIMARY ITEMS:", len(primary_items))

    used_fallback_dubbio = False
    valid_items = primary_items

    if len(primary_items) < 3:
        fallback_allowed = PRIMARY_VINTAGE_STATUS | FALLBACK_VINTAGE_STATUS
        fallback_items = collect_valid_items(
            query,
            sampled_results,
            fallback_allowed,
        )

        if len(fallback_items) > len(primary_items):
            valid_items = fallback_items
            used_fallback_dubbio = True

    _dbg("USED FALLBACK DUBBIO:", used_fallback_dubbio)
    _dbg("VALID ITEMS AFTER FALLBACK:", len(valid_items))

    output = _base_output(query, used_fallback_dubbio)

    if not valid_items:
        _dbg("RETURN EARLY: no valid_items")
        output["display_reason"] = "no_clean_data"
        return output

    _dbg("\n[PRICE ANALYZER DEBUG]")
    _dbg("query:", query)
    _dbg("valid_items count:", len(valid_items))

    if PRICE_ANALYZER_DEBUG:
        for idx, item in enumerate(sorted(valid_items, key=lambda x: float(x["price_value"]))):
            _dbg(
                f"{idx+1:02d}. €{item['price_value']} | {item['source']} | {item['title']}"
            )
        _dbg("")

    raw_prices = [float(item["price_value"]) for item in valid_items]
    cleaned_prices = remove_outliers(raw_prices)

    if not cleaned_prices:
        cleaned_prices = raw_prices[:]

    raw_count_valid = len(valid_items)
    clean_count_valid = len(cleaned_prices)

    stats = compute_statistics(cleaned_prices)
    count = int(stats["count"] or 0)
    mode = classify_estimate(count)

    observed_price = cleaned_prices[0] if len(cleaned_prices) == 1 else None

    source_stats = compute_source_stats(valid_items)
    sources = sorted(source_stats.keys())

    spread_ratio = None
    if stats["min_price"] is not None and stats["min_price"] > 0 and stats["max_price"] is not None:
        spread_ratio = float(stats["max_price"]) / float(stats["min_price"])

    dominant_source_share = compute_dominant_source_share(source_stats)

    display_decision = decide_display_eligibility(
        query=query,
        raw_count_valid=raw_count_valid,
        clean_count_valid=clean_count_valid,
        min_price=stats["min_price"],
        max_price=stats["max_price"],
        source_stats=source_stats,
    )

    output.update({
        "mode": mode,
        "count_valid": count,
        "raw_count_valid": raw_count_valid,
        "clean_count_valid": clean_count_valid,
        "median_price": _round_money(stats["median_price"]),
        "min_price": _round_money(stats["min_price"]),
        "max_price": _round_money(stats["max_price"]),
        "p25_price": _round_money(stats["p25_price"]),
        "p75_price": _round_money(stats["p75_price"]),
        "observed_price": _round_money(observed_price),
        "price_spread_ratio": _round_money(spread_ratio) if spread_ratio is not None else None,
        "dominant_source_share": dominant_source_share,
        "sources": sources,
        "source_stats": source_stats,
        "show_price_box": display_decision["show_price_box"],
        "display_mode": display_decision["display_mode"],
        "display_reason": display_decision["display_reason"],
    })

    if mode == "single_observation" and output["observed_price"] is None:
        output["observed_price"] = output["median_price"]

    _dbg("RETURN MODE:", mode)
    _dbg("RAW COUNT VALID:", raw_count_valid)
    _dbg("CLEAN COUNT VALID:", clean_count_valid)
    _dbg("SPREAD RATIO:", output["price_spread_ratio"])
    _dbg("DOMINANT SOURCE SHARE:", dominant_source_share)
    _dbg("SHOW PRICE BOX:", output["show_price_box"])
    _dbg("DISPLAY MODE:", output["display_mode"])
    _dbg("DISPLAY REASON:", output["display_reason"])

    return output
