from datetime import datetime

from btc_forecast.bot.broadcast import format_broadcast, select_broadcast
from btc_forecast.models.inference import (
    BarSignal,
    ForecastResult,
    TimeframeForecast,
)


def _result(sig_1h, conf_1h, sig_6h, conf_6h) -> ForecastResult:
    ts = datetime(2024, 6, 1, 12, 0)
    return ForecastResult(
        price=61000.0,
        updated_at=ts,
        forecasts={
            "1h": TimeframeForecast(
                tf="1h",
                live=BarSignal(sig_1h, 0.7, 0.02, conf_1h, ts),
                last_price=61000.0,
            ),
            "6h": TimeframeForecast(
                tf="6h",
                live=BarSignal(sig_6h, 0.6, 0.03, conf_6h, ts),
                last_price=61000.0,
            ),
        },
    )


def test_select_picks_highest_confidence():
    signal = select_broadcast(_result(1, 0.6, -1, 0.8))
    assert signal is not None
    assert signal.tf == "6h"
    assert signal.signal == -1
    assert signal.confidence == 0.8


def test_select_none_when_all_flat():
    assert select_broadcast(_result(0, 0.0, 0, 0.0)) is None


def test_select_skips_zero_confidence():
    signal = select_broadcast(_result(1, 0.0, 0, 0.0))
    assert signal is None


def test_dedup_key_changes_with_bar():
    signal = select_broadcast(_result(1, 0.72, 0, 0.0))
    assert signal.dedup_key == "1h|2024-06-01T12:00:00|1"


def test_format_contains_fields():
    signal = select_broadcast(_result(1, 0.72, 0, 0.0))
    text = format_broadcast(signal)
    assert "BTC/USDT" in text
    assert "UP" in text
    assert "72%" in text
