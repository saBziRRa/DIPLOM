"""Smoke tests for publication-lag alignment helpers."""

import pandas as pd

from btc_forecast.data.loader import _align_intrabar, _MSK


def test_align_intrabar_shifts_one_bar():
    master = pd.date_range("2024-01-01", periods=5, freq="h", tz=None)
    master = master.tz_localize(_MSK).tz_localize(None)
    src = pd.DataFrame({"v": [10, 20, 30, 40, 50]}, index=master)
    aligned = _align_intrabar(src, master)
    assert aligned.iloc[0]["v"] != 10 or pd.isna(aligned.iloc[0]["v"])
