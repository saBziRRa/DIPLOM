from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier, DummyRegressor

from btc_forecast.models.bundle import (
    BundleMeta,
    feature_cols_hash,
    save_bundle,
    load_bundle,
)


def test_bundle_roundtrip(tmp_path):
    cols = ["f1", "f2", "c_close"]
    x = pd.DataFrame(
        np.random.randn(20, 3),
        columns=cols,
        index=pd.date_range("2024-01-01", periods=20, freq="h"),
    )
    results = {
        "1h": {
            "g_deploy": DummyClassifier(strategy="most_frequent"),
            "r_deploy": DummyRegressor(),
            "taus": {"static": 0.01, "dynamic": 0.01},
            "gate_model_name": "dummy",
            "dir_model_name": "dummy",
            "feat_cols": ["f1", "f2"],
            "feat_cols_jump": ["f1", "f2"],
            "X_full": x[["f1", "f2"]],
            "price_full": x["c_close"],
            "m_gate": {"auc": 0.75},
            "cascade": {"static": {"mcc": 0.2}},
        }
    }
    path = save_bundle(results, tmp_path / "bundle.pkl")
    loaded = load_bundle(path)
    assert loaded.meta.version
    assert "1h" in loaded.timeframes


def test_feature_cols_hash_stable():
    assert feature_cols_hash(["b", "a"]) == feature_cols_hash(["a", "b"])
