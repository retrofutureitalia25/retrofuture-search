#price_snapshot_service.py
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional


SNAPSHOT_ALLOWED_MODES = {"strong_estimate", "preliminary_estimate"}
MIN_COUNT_TO_SAVE = 5
MIN_HOURS_BETWEEN_SNAPSHOTS = 6
MAX_ALLOWED_MEDIAN_CHANGE_RATIO = 0.50  # 50%


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_query(query: str) -> str:
    return _safe_str(query).lower()


def should_save_snapshot(
    query: str,
    price_data: Dict[str, Any],
    price_snapshots_collection
) -> bool:
    mode = _safe_str(price_data.get("mode"))
    count_valid = int(price_data.get("count_valid") or 0)
    median_price = price_data.get("median_price")

    if mode not in SNAPSHOT_ALLOWED_MODES:
        return False

    if count_valid < MIN_COUNT_TO_SAVE:
        return False

    if median_price is None:
        return False

    query_norm = _normalize_query(query)

    last_snapshot = price_snapshots_collection.find_one(
        {"query": query_norm},
        sort=[("created_at", -1)]
    )

    if not last_snapshot:
        return True

    last_created_at = last_snapshot.get("created_at")
    if isinstance(last_created_at, datetime):
        now_utc = datetime.now(timezone.utc)

        if last_created_at.tzinfo is None:
            last_created_at = last_created_at.replace(tzinfo=timezone.utc)

        if now_utc - last_created_at < timedelta(hours=MIN_HOURS_BETWEEN_SNAPSHOTS):
            return False

    old_median = last_snapshot.get("median_price")
    if old_median is None:
        return True

    try:
        old_median = float(old_median)
        new_median = float(median_price)
    except (TypeError, ValueError):
        return False

    if old_median <= 0:
        return True

    change_ratio = abs(new_median - old_median) / old_median
    if change_ratio > MAX_ALLOWED_MEDIAN_CHANGE_RATIO:
        return False

    return True


def save_price_snapshot(
    query: str,
    price_data: Dict[str, Any],
    price_snapshots_collection
) -> Optional[str]:
    query_norm = _normalize_query(query)

    doc = {
        "query": query_norm,
        "median_price": price_data.get("median_price"),
        "min_price": price_data.get("min_price"),
        "max_price": price_data.get("max_price"),
        "p25_price": price_data.get("p25_price"),
        "p75_price": price_data.get("p75_price"),
        "count_valid": int(price_data.get("count_valid") or 0),
        "mode": _safe_str(price_data.get("mode")),
        "sources": list(price_data.get("sources") or []),
        "used_fallback_dubbio": bool(price_data.get("used_fallback_dubbio")),
        "created_at": datetime.now(timezone.utc),
    }

    result = price_snapshots_collection.insert_one(doc)
    return str(result.inserted_id)


def calculate_price_trend(
    query: str,
    price_snapshots_collection,
    days: int = 365
) -> Optional[Dict[str, Any]]:
    query_norm = _normalize_query(query)
    now_utc = datetime.now(timezone.utc)
    date_from = now_utc - timedelta(days=days)

    snapshots = list(
        price_snapshots_collection.find(
            {
                "query": query_norm,
                "created_at": {"$gte": date_from}
            },
            {
                "_id": 0,
                "median_price": 1,
                "created_at": 1,
                "count_valid": 1,
                "mode": 1,
            }
        ).sort("created_at", 1)
    )

    if len(snapshots) < 2:
        return None

    first = snapshots[0]
    last = snapshots[-1]

    first_price = first.get("median_price")
    last_price = last.get("median_price")

    try:
        first_price = float(first_price)
        last_price = float(last_price)
    except (TypeError, ValueError):
        return None

    if first_price <= 0:
        return None

    change_pct = ((last_price - first_price) / first_price) * 100.0

    return {
        "days": days,
        "first_price": round(first_price, 2),
        "last_price": round(last_price, 2),
        "change_pct": round(change_pct, 1),
        "direction": "up" if change_pct > 0 else "down" if change_pct < 0 else "flat",
        "snapshots_count": len(snapshots),
    }
