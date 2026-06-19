"""Отбор и форматирование уверенных сигналов для рассылки подписчикам."""

from __future__ import annotations

from dataclasses import dataclass

from btc_forecast.models.inference import ForecastResult

_LABELS = {1: "UP", -1: "DOWN", 0: "FLAT"}


@dataclass
class BroadcastSignal:
    tf: str
    signal: int
    confidence: float
    p_move: float
    price: float
    bar_ts: str
    dedup_key: str


def select_broadcast(result: ForecastResult) -> BroadcastSignal | None:
    """Самый уверенный активный сигнал среди таймфреймов (или None)."""
    best: BroadcastSignal | None = None
    for tf, forecast in result.forecasts.items():
        live = forecast.live
        if live.signal == 0 or live.confidence <= 0:
            continue
        bar_ts = live.timestamp.isoformat() if live.timestamp else ""
        candidate = BroadcastSignal(
            tf=tf,
            signal=live.signal,
            confidence=float(live.confidence),
            p_move=float(live.p_move),
            price=float(forecast.last_price),
            bar_ts=bar_ts,
            dedup_key=f"{tf}|{bar_ts}|{live.signal}",
        )
        if best is None or candidate.confidence > best.confidence:
            best = candidate
    return best


def format_broadcast(signal: BroadcastSignal) -> str:
    label = _LABELS.get(signal.signal, "?")
    bar = signal.bar_ts[:16].replace("T", " ")
    price = f"{signal.price:,.2f}".replace(",", " ")
    return (
        f"Сигнал BTC/USDT ({signal.tf.upper()}): {label}\n"
        f"Уверенность: {signal.confidence * 100:.0f}% | "
        f"P(move)={signal.p_move:.0%}\n"
        f"Цена: {price}\n"
        f"Бар: {bar} MSK\n\n"
        "Рассылка по вашему порогу уверенности. "
        "Не является финансовой рекомендацией."
    )
