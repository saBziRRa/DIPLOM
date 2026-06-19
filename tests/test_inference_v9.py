import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier, DummyRegressor

from btc_forecast.models.bundle import BundleMeta, ModelBundle
from btc_forecast.models.inference import ForecastEngine, cascade_signal


def test_cascade_signal():
    sig, conf = cascade_signal(0.8, 0.02, tau=0.01)
    assert sig == 1
    assert conf > 0
    sig_flat, _ = cascade_signal(0.3, 0.02, tau=0.01)
    assert sig_flat == 0


def test_live_and_horizon():
    idx = pd.date_range("2024-01-01", periods=30, freq="h")
    x = pd.DataFrame({"f1": 1.0, "f2": 2.0, "c_close": 50000.0}, index=idx)

    g = DummyClassifier(strategy="constant", constant=1)
    g.fit(x[["f1", "f2"]], [1, 0] * 15)
    r = DummyRegressor()
    r.fit(x[["f1", "f2"]], np.linspace(-0.02, 0.02, 30))

    bundle = ModelBundle(
        meta=BundleMeta(
            version="1.0.0",
            feature_cols_hash="abc",
            trained_at="2024-01-01",
            metrics={"1h": {"cascade_mcc": 0.2, "gate_auc": 0.7}},
        ),
        timeframes={
            "1h": {
                "g_deploy": g,
                "r_deploy": r,
                "taus": {"static": 0.001, "dynamic": 0.001},
                "feat_cols": ["f1", "f2"],
                "feat_cols_jump": ["f1", "f2"],
                "X_full": x[["f1", "f2"]],
                "price_full": x["c_close"],
            }
        },
    )
    engine = ForecastEngine(bundle)
    live = engine.predict_live("1h", x)
    assert live.signal in (-1, 0, 1)
    horizon = engine.predict_horizon("1h", n_bars=5)
    assert len(horizon) == 5
    assert "signal_label" in horizon.columns
