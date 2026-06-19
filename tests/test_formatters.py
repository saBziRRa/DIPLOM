from datetime import datetime

from btc_forecast.bot.formatters import format_summary
from btc_forecast.models.inference import BarSignal, ForecastResult, TimeframeForecast
from btc_forecast.models.bundle import BundleMeta


def test_format_summary():
    result = ForecastResult(
        price=100000.0,
        updated_at=datetime(2024, 6, 1, 12, 0),
        forecasts={
            "1h": TimeframeForecast(
                tf="1h",
                live=BarSignal(1, 0.6, 0.02, 0.4),
                last_price=100000.0,
            ),
            "6h": TimeframeForecast(
                tf="6h",
                live=BarSignal(0, 0.4, 0.0, 0.0),
                last_price=100000.0,
            ),
        },
        bundle_meta=BundleMeta(
            version="1.0.0",
            feature_cols_hash="x",
            trained_at="2024-05-28",
            metrics={"1h": {"cascade_mcc": 0.2}, "6h": {"cascade_mcc": 0.24}},
        ),
    )
    text = format_summary(result)
    assert "BTC/USDT" in text
    assert "1H" in text
    assert "MCC" in text
