"""Состояние пользователей VK-бота: подписки и порог уверенности.

Хранилище — единый JSON-файл в каталоге артефактов. Доступ сериализуется
блокировкой, поэтому модуль безопасно использовать из обработчиков и
фоновой рассылки.

Структура файла::

    {"users": {"12345": {"subscribed": true, "min_confidence": 0.7}}}
"""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any

from btc_forecast.config import get_settings

logger = logging.getLogger(__name__)

# Порог уверенности по умолчанию (доля, не проценты).
DEFAULT_MIN_CONFIDENCE = 0.6
MIN_CONFIDENCE_FLOOR = 0.5
MIN_CONFIDENCE_CEIL = 0.95

_lock = threading.RLock()


def _store_path() -> Path:
    path = get_settings().artifacts_dir / "vk_userstate.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _load() -> dict[str, Any]:
    path = _store_path()
    if not path.exists():
        return {"users": {}}
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Состояние пользователей повреждено (%s) — сбрасываем", exc)
        return {"users": {}}
    data.setdefault("users", {})
    return data


def _save(data: dict[str, Any]) -> None:
    path = _store_path()
    tmp = path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _default_record() -> dict[str, Any]:
    return {"subscribed": False, "min_confidence": DEFAULT_MIN_CONFIDENCE}


def get_user(user_id: int) -> dict[str, Any]:
    """Запись пользователя (с дефолтами, если её ещё нет)."""
    with _lock:
        rec = _load()["users"].get(str(user_id))
    merged = _default_record()
    if rec:
        merged.update(rec)
    return merged


def subscribe(user_id: int) -> bool:
    """Подписывает пользователя. True, если статус изменился."""
    with _lock:
        data = _load()
        rec = data["users"].get(str(user_id)) or _default_record()
        changed = not rec.get("subscribed", False)
        rec["subscribed"] = True
        rec.setdefault("min_confidence", DEFAULT_MIN_CONFIDENCE)
        data["users"][str(user_id)] = rec
        _save(data)
    return changed


def unsubscribe(user_id: int) -> bool:
    """Снимает подписку. True, если статус изменился."""
    with _lock:
        data = _load()
        rec = data["users"].get(str(user_id))
        if not rec or not rec.get("subscribed", False):
            return False
        rec["subscribed"] = False
        data["users"][str(user_id)] = rec
        _save(data)
    return True


def set_min_confidence(user_id: int, value: float) -> float:
    """Задаёт порог уверенности (зажат в диапазон). Возвращает сохранённое значение."""
    value = max(MIN_CONFIDENCE_FLOOR, min(MIN_CONFIDENCE_CEIL, float(value)))
    with _lock:
        data = _load()
        rec = data["users"].get(str(user_id)) or _default_record()
        rec["min_confidence"] = value
        data["users"][str(user_id)] = rec
        _save(data)
    return value


def list_subscribers() -> list[dict[str, Any]]:
    """Подписчики с их порогами: [{"user_id": int, "min_confidence": float}]."""
    with _lock:
        users = _load()["users"]
    out = []
    for uid, rec in users.items():
        if not rec.get("subscribed", False):
            continue
        out.append({
            "user_id": int(uid),
            "min_confidence": float(rec.get("min_confidence", DEFAULT_MIN_CONFIDENCE)),
        })
    return out


def get_last_broadcast() -> str | None:
    """Ключ последнего разосланного сигнала (для защиты от дублей)."""
    with _lock:
        return _load().get("last_broadcast")


def set_last_broadcast(key: str) -> None:
    """Запоминает ключ последнего разосланного сигнала."""
    with _lock:
        data = _load()
        data["last_broadcast"] = key
        _save(data)
