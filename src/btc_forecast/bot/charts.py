"""Matplotlib рендеринг графика"""


from __future__ import annotations

import io

import matplotlib

matplotlib.use("Agg")  # headless-бэкенд, без GUI и подпроцессов

import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter

from btc_forecast.models.inference import ForecastResult

plt.rcParams["font.family"] = "DejaVu Sans"  # поддержка кириллицы

COLORS = {
    "background": "#121212",
    "history": "#ffffff",
    "up": "#00e676",
    "down": "#ff5252",
    "text": "#e0e0e0",
    "grid": "#333333",
}


def render_chart(result: ForecastResult, tf: str, lookback: int = 100) -> bytes:
    fc = result.forecasts.get(tf.lower())
    if not fc or fc.horizon.empty:
        raise ValueError(f"No horizon data for {tf}")

    price = fc.last_price
    res = fc.horizon

    hist_len = min(lookback, 50)
    hist_x = [result.updated_at] * hist_len
    hist_y = [price] * hist_len

    up = res[res["signal"] == 1]
    dn = res[res["signal"] == -1]

    fig, ax = plt.subplots(figsize=(10, 5.5), dpi=100)
    fig.patch.set_facecolor(COLORS["background"])
    ax.set_facecolor(COLORS["background"])

    ax.plot(
        hist_x,
        hist_y,
        color=COLORS["history"],
        linewidth=2,
        label="Цена",
    )

    if not up.empty:
        ax.scatter(
            up.index,
            [price] * len(up),
            color=COLORS["up"],
            marker="^",
            s=180,
            label="UP",
            zorder=3,
        )
    if not dn.empty:
        ax.scatter(
            dn.index,
            [price] * len(dn),
            color=COLORS["down"],
            marker="v",
            s=180,
            label="DOWN",
            zorder=3,
        )

    unit = res.attrs.get("unit", "ч")
    horizon = float(res["units_ahead"].iloc[-1])
    ax.set_title(
        f"{tf.upper()} прогноз на {horizon:.0f} {unit} (frozen-features)",
        color=COLORS["text"],
        fontsize=13,
    )

    ax.tick_params(colors=COLORS["text"])
    ax.xaxis.set_major_formatter(DateFormatter("%d.%m %H:%M"))
    fig.autofmt_xdate(rotation=30)

    for spine in ax.spines.values():
        spine.set_color(COLORS["grid"])
    ax.grid(color=COLORS["grid"], linewidth=0.5, alpha=0.6)

    ax.legend(
        facecolor=COLORS["background"],
        edgecolor=COLORS["grid"],
        labelcolor=COLORS["text"],
    )

    fig.tight_layout()

    buf = io.BytesIO()
    try:
        fig.savefig(buf, format="png", facecolor=COLORS["background"])
    finally:
        plt.close(fig)

    return buf.getvalue()