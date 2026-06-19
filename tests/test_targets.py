import numpy as np
import pandas as pd

from btc_forecast.features.targets import add_static_target, build_jump_dataset, build_targets


def test_static_target_and_jump():
    df = pd.DataFrame(
        {
            "fwd_ret": [0.02, 0.0, -0.03, 0.001, 0.015],
            "volatility_1d": [0.01] * 5,
        },
        index=pd.date_range("2024-01-01", periods=5, freq="h"),
    )
    out = build_targets(df, static_thresh=0.008, vol_indicator="volatility_1d")
    assert "t_static" in out.columns
    assert "t_dynamic" in out.columns
    jump = build_jump_dataset(out)
    assert len(jump) >= 2
    assert jump["t_static"].isin([-1, 1]).all()
