"""Telegram bot application entry."""

from __future__ import annotations

import asyncio
import logging

from aiogram import Bot, Dispatcher

from btc_forecast.bot.cache import clear_cache
from btc_forecast.bot.handlers import router
from btc_forecast.bot.scheduler import BotScheduler
from btc_forecast.config import get_settings
from btc_forecast.debug.server import create_app

logger = logging.getLogger(__name__)


async def _start_debug_server() -> object | None:
    settings = get_settings()
    if not settings.debug_enabled_resolved:
        return None
    from aiohttp import web

    app = create_app(mode="dashboard")
    runner = web.AppRunner(app)
    await runner.setup()
    host = settings.debug_host_resolved
    port = settings.debug_port_resolved
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info("Dashboard UI http://%s:%d/", host, port)
    return runner


async def run_bot() -> None:
    settings = get_settings()
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    bot = Bot(token=settings.telegram_bot_token)
    dp = Dispatcher()
    dp.include_router(router)

    async def reload_bundle() -> None:
        clear_cache()
        logger.info("Forecast cache cleared after retrain")

    async def admin_msg(text: str) -> None:
        for chat_id in settings.admin_ids:
            try:
                await bot.send_message(chat_id, text)
            except Exception as exc:
                logger.warning("Admin message failed: %s", exc)

    scheduler = BotScheduler(
        on_bundle_reload=reload_bundle,
        on_admin_message=admin_msg,
    )
    scheduler.start()

    debug_runner = await _start_debug_server()

    try:
        logger.info("Bot starting (long polling)")
        await dp.start_polling(bot)
    finally:
        scheduler.shutdown()
        if debug_runner is not None:
            await debug_runner.cleanup()
        await bot.session.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(run_bot())
