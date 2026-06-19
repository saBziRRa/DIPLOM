import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier, DummyRegressor

from btc_forecast.models.bundle import BundleMeta, ModelBundle
from btc_forecast.models.inference import ForecastEngine


def _bundle(x: pd.DataFrame, price_full: pd.Series | None = None) -> ModelBundle:
    g = DummyClassifier(strategy="constant", constant=1)
    g.fit(x[["f1", "f2"]], [1, 0] * (len(x) // 2))
    r = DummyRegressor()
    r.fit(x[["f1", "f2"]], np.linspace(-0.02, 0.02, len(x)))
    tf_data = {
        "g_deploy": g,
        "r_deploy": r,
        "taus": {"static": 0.001, "dynamic": 0.001},
        "feat_cols": ["f1", "f2"],
        "feat_cols_jump": ["f1", "f2"],
        "X_full": x[["f1", "f2"]],
        "price_full": price_full,
    }
    return ModelBundle(
        meta=BundleMeta("1.0.0", "abc", "2024-01-01", {}),
        timeframes={"1h": dict(tf_data), "6h": dict(tf_data)},
    )


def test_price_from_price_log():
    idx = pd.date_range("2024-01-01", periods=30, freq="h")
    price = 61432.0
    x = pd.DataFrame(
        {"f1": 1.0, "f2": 2.0, "price_log": np.log(price)}, index=idx
    )
    result = ForecastEngine(_bundle(x)).run(x, x)
    assert abs(result.price - price) < 1.0
    assert abs(result.forecasts["1h"].last_price - price) < 1.0


def test_price_from_c_close_preferred():
    idx = pd.date_range("2024-01-01", periods=30, freq="h")
    x = pd.DataFrame(
        {"f1": 1.0, "f2": 2.0, "c_close": 55000.0, "price_log": np.log(99999.0)},
        index=idx,
    )
    result = ForecastEngine(_bundle(x)).run(x, x)
    assert abs(result.price - 55000.0) < 1.0


def test_price_from_bundle_when_features_lack_price():
    idx = pd.date_range("2024-01-01", periods=30, freq="h")
    x = pd.DataFrame({"f1": 1.0, "f2": 2.0}, index=idx)
    price_full = pd.Series(np.linspace(50000, 60000, 30), index=idx)
    result = ForecastEngine(_bundle(x, price_full)).run(x, x)
    assert abs(result.forecasts["1h"].last_price - 60000.0) < 1.0
