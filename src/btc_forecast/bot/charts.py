"""Plotly chart export for Telegram."""

from __future__ import annotations

import plotly.graph_objects as go

from btc_forecast.models.inference import ForecastResult

COLORS = {
    "history": "#ffffff",
    "anchor": "#9e9e9e",
    "up": "#00e676",
    "down": "#ff5252",
    "now": "#ffd600",
}


def render_chart(result: ForecastResult, tf: str, lookback: int = 100) -> bytes:
    fc = result.forecasts.get(tf.lower())
    if not fc or fc.horizon.empty:
        raise ValueError(f"No horizon data for {tf}")

    price = fc.last_price
    res = fc.horizon
    hist_idx = [str(result.updated_at)] * min(lookback, 50)
    hist_y = [price] * len(hist_idx)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=hist_idx,
            y=hist_y,
            mode="lines",
            name="Цена",
            line=dict(color=COLORS["history"], width=2),
        )
    )

    up = res[res["signal"] == 1]
    dn = res[res["signal"] == -1]
    if not up.empty:
        fig.add_trace(
            go.Scatter(
                x=up.index.astype(str),
                y=[price] * len(up),
                mode="markers",
                name="UP",
                marker=dict(color=COLORS["up"], size=14, symbol="triangle-up"),
            )
        )
    if not dn.empty:
        fig.add_trace(
            go.Scatter(
                x=dn.index.astype(str),
                y=[price] * len(dn),
                mode="markers",
                name="DOWN",
                marker=dict(color=COLORS["down"], size=14, symbol="triangle-down"),
            )
        )

    unit = res.attrs.get("unit", "ч")
    horizon = float(res["units_ahead"].iloc[-1])
    fig.update_layout(
        title=f"{tf.upper()} прогноз на {horizon:.0f} {unit} (frozen-features)",
        template="plotly_dark",
        height=500,
    )

    try:
        return fig.to_image(format="png", engine="kaleido")
    except Exception as exc:
        # Не подменяем PNG на HTML-байты: VK/Telegram грузят их как
        # forecast.png и получают битое изображение. Лучше явная ошибка —
        # её перехватит хендлер команды и пришлёт текстом.
        raise RuntimeError(
            "Не удалось отрисовать график. Нужен kaleido 0.2.1 "
            "(pip install \"kaleido==0.2.1\"); версии 1.x требуют Chrome."
        ) from exc
