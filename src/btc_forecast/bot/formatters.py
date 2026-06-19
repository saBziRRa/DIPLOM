"""Команды бота."""

from __future__ import annotations

from btc_forecast.models.inference import ForecastResult


def _signal_label(signal: int) -> str:
    return {1: "UP", -1: "DOWN", 0: "FLAT"}.get(signal, "?")


def format_summary(result: ForecastResult) -> str:
    lines = []
    price = result.price
    ts = result.updated_at.strftime("%d.%m %H:%M MSK") if result.updated_at else "—"
    lines.append(f"BTC/USDT: {price:,.2f} (обновлено {ts})")
    lines.append("")

    for tf in ("1h", "6h"):
        fc = result.forecasts.get(tf)
        if not fc:
            continue
        live = fc.live
        conf = f"{live.confidence * 100:.0f}%" if live.signal != 0 else "—"
        lines.append(
            f"{tf.upper()} (ближайший бар): {_signal_label(live.signal)} | "
            f"P(move)={live.p_move:.0%} | уверенность={conf}"
        )

    lines.append("")
    lines.append(f"Согласованность: {result.agreement}")

    meta = result.bundle_meta
    if meta:
        mcc_1h = meta.metrics.get("1h", {}).get("cascade_mcc")
        mcc_6h = meta.metrics.get("6h", {}).get("cascade_mcc")
        trained = (meta.trained_at or "")[:10]
        lines.append("")
        lines.append(f"Модель: v{meta.version} | обучена {trained}")
        if mcc_1h is not None and mcc_6h is not None:
            lines.append(f"Качество (hold-out): 1H MCC={mcc_1h:.2f} | 6H MCC={mcc_6h:.2f}")

    lines.append("")
    lines.append("Горизонт детализации: /forecast")
    return "\n".join(lines)


def format_forecast(result: ForecastResult, tf: str | None = None) -> str:
    lines = [
        "⚠ Ориентир на основе текущего снимка рынка; качество падает с горизонтом.",
        "",
    ]
    tfs = [tf.lower()] if tf else ["1h", "6h"]
    for name in tfs:
        fc = result.forecasts.get(name)
        if not fc or fc.horizon.empty:
            continue
        unit = fc.horizon.attrs.get("unit", "ч")
        lines.append(f"── {name.upper()} ──")
        for idx, row in fc.horizon.head(12).iterrows():
            conf = (
                f"{row['confidence_pct']:.0f}%"
                if row["signal"] != 0
                else "—"
            )
            lines.append(
                f"{str(idx)[:16]:<16} +{row['units_ahead']:.1f}{unit} "
                f"{row['signal_label']:<5} P={row['p_move']:.0%} {conf}"
            )
        if len(fc.horizon) > 12:
            lines.append(f"... ещё {len(fc.horizon) - 12} баров")
        lines.append("")
    return "\n".join(lines).strip()


def format_start() -> str:
    return (
        "Бот прогноза скачков BTC/USDT (каскад GATE + регрессор).\n\n"
        "Быстрые команды:\n"
        "/summary — сводка 1H + 6H\n"
        "/chart [1h|6h] — график сигналов\n"
        "/forecast [1h|6h] — таблица на горизонт\n"
        "/info - общая информация\n"
        "/help - список всех команд \n\n"
        "Не является финансовой рекомендацией."
    )


AUTHOR = "Коржавин Артём Александрович. Московский университет имени С.Ю.Витте."


def format_help() -> str:
    """Список команд VK-бота с кратким описанием."""
    return (
        "Команды:\n"
        "/start - начальная страница\n"
        "/summary — сводка 1H + 6H на ближайший бар\n"
        "/forecast [1h|6h] — прогноз на выбранный горизонт\n"
        "/chart [1h|6h] — график сигналов\n"
        "/subscribe [0.5–0.95] — подписка на сигналы с заданной уверенностью\n"
        "/unsubscribe — отмена подписки\n"
        "/settings — меню быстрых действий\n"
        "/info — общая информация о системе и авторе\n"
        "/refresh - запустить цикл переобучения (только для админа)\n"
        "/help — этот список команд"
    )


def format_info() -> str:
    """Общая информация о системе и авторе."""
    return (
        "Бот прогноза направления BTC/USDT на основе машинного обучения.\n"
        "Каскад GATE + регрессор, горизонты предсказания 1 час и 6 часов.\n"
        "Сигнал: UP(вверх) / FLAT(без движения) / DOWN(вниз) + его вероятность — направление движения, не цена OHLC.\n\n"
        f"Автор: {AUTHOR}\n"
        "Не является финансовой рекомендацией, все решения принимаются самостоятельно и автор не несёт ответсвенности за возможные финансовые убытки."
    )


def format_subscription(subscribed: bool, changed: bool, threshold: float) -> str:
    """Ответ на /subscribe и /unsubscribe."""
    if subscribed:
        status = "Подписка оформлена" if changed else "Подписка обновлена"
        return (
            f"{status}. Будете получать сигналы с уверенностью "
            f"не ниже {threshold * 100:.0f}%.\n"
            "Изменить порог: /subscribe 0.7 (диапазон 0.5–0.95).\n"
            "Отписаться: /unsubscribe"
        )
    if changed:
        return "Подписка отменена."
    return "Вы не были подписаны на рассылку."


def format_settings(record: dict) -> str:
    """Текущие настройки пользователя и подсказка по изменению."""
    sub = "активна" if record.get("subscribed") else "не активна"
    thr = record.get("min_confidence", 0.6)
    return (
        "Настройки\n\n"
        f"Подписка: {sub}\n"
        f"Порог уверенности: {thr * 100:.0f}%\n\n"
        "Выберите действие на клавиатуре ниже.\n"
        "Подписка с порогом: /subscribe 0.7 (диапазон 0.5–0.95)."
    )