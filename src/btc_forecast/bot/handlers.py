"""Telegram command handlers."""

from __future__ import annotations

import logging

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, Message

from btc_forecast.bot import charts, formatters
from btc_forecast.bot.cache import clear_cache, get_cached_predict
from btc_forecast.config import get_settings
from btc_forecast.pipeline.predict import run_predict
from btc_forecast.pipeline.train import run_train
from btc_forecast.pipeline.ingest import run_ingest
from btc_forecast.pipeline.features import run_features

logger = logging.getLogger(__name__)
router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    await message.answer(formatters.format_start())


@router.message(Command("summary"))
async def cmd_summary(message: Message) -> None:
    try:
        result = get_cached_predict(run_predict)
        await message.answer(formatters.format_summary(result))
    except Exception as exc:
        logger.exception("summary failed")
        await message.answer(f"Ошибка прогноза: {exc}")


@router.message(Command("forecast"))
async def cmd_forecast(message: Message) -> None:
    tf = None
    parts = (message.text or "").split()
    if len(parts) > 1:
        tf = parts[1].lower()
    try:
        result = get_cached_predict(run_predict)
        text = formatters.format_forecast(result, tf=tf)
        await message.answer(text)
    except Exception as exc:
        await message.answer(f"Ошибка: {exc}")


@router.message(Command("chart"))
async def cmd_chart(message: Message) -> None:
    tf = "1h"
    parts = (message.text or "").split()
    if len(parts) > 1:
        tf = parts[1].lower()
    try:
        result = get_cached_predict(run_predict)
        png = charts.render_chart(result, tf)
        await message.answer_photo(
            BufferedInputFile(png, filename=f"chart_{tf}.png")
        )
    except Exception as exc:
        await message.answer(f"Ошибка графика: {exc}")


@router.message(Command("refresh"))
async def cmd_refresh(message: Message) -> None:
    settings = get_settings()
    if message.from_user and message.from_user.id not in settings.admin_ids:
        await message.answer("Команда доступна только администраторам.")
        return
    await message.answer("Запуск переобучения...")
    try:
        await message.bot.send_chat_action(message.chat.id, "typing")

        def _run():
            run_ingest(force=True)
            run_features()
            run_train(tune=True)

        import asyncio

        await asyncio.to_thread(_run)
        clear_cache()
        await message.answer("Переобучение завершено.")
    except Exception as exc:
        await message.answer(f"Ошибка переобучения: {exc}")
