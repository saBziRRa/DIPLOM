"""Точка входа VK-бота."""

from __future__ import annotations

import asyncio
import io
import logging
from typing import Any

import vk_api
from vk_api import VkUpload
from vk_api.bot_longpoll import VkBotEventType, VkBotLongPoll
from vk_api.keyboard import VkKeyboard, VkKeyboardColor
from vk_api.utils import get_random_id

from btc_forecast.bot import charts, formatters, userstate
from btc_forecast.bot.cache import clear_cache, get_cached_predict
from btc_forecast.bot.scheduler import BotScheduler
from btc_forecast.config import get_settings
from btc_forecast.debug.server import create_app
from btc_forecast.pipeline.features import run_features
from btc_forecast.pipeline.ingest import run_ingest
from btc_forecast.pipeline.predict import run_predict
from btc_forecast.pipeline.train import run_train

logger = logging.getLogger(__name__)

EVENT_TIMEOUT_SEC = 60.0
GENERIC_ERROR = "Произошла ошибка при обработке команды. Попробуйте позже."

_COMMANDS = {
    "start",
    "help",
    "info",
    "summary",
    "forecast",
    "chart",
    "subscribe",
    "unsubscribe",
    "settings",
    "refresh",
}

# Алиасы: латиница и русские названия.
_ALIASES = {
    "помощь": "help",
    "команды": "help",
    "инфо": "info",
    "сводка": "summary",
    "прогноз": "forecast",
    "график": "chart",
    "подписка": "subscribe",
    "подписаться": "subscribe",
    "отписка": "unsubscribe",
    "отписаться": "unsubscribe",
    "настройки": "settings",
}


def parse_command(text: str) -> tuple[str | None, str | None]:
    raw = (text or "").strip()
    if not raw:
        return None, None
    parts = raw.split()
    cmd = parts[0].lower()
    if cmd.startswith("/"):
        cmd = cmd[1:].split("@", 1)[0]
    cmd = _ALIASES.get(cmd, cmd)
    arg = parts[1].lower().lstrip("@/") if len(parts) > 1 else None
    if cmd not in _COMMANDS:
        return None, None
    return cmd, arg


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


def _send_text(vk: Any, peer_id: int, text: str) -> None:
    vk.messages.send(peer_id=peer_id, random_id=get_random_id(), message=text)


def _send_photo(
    vk: Any, upload: VkUpload, peer_id: int, image_bytes: bytes
) -> None:
    stream = io.BytesIO(image_bytes)
    stream.name = "forecast.png"
    photo = upload.photo_messages([stream], peer_id=peer_id)[0]
    attachment = f"photo{photo['owner_id']}_{photo['id']}"
    vk.messages.send(
        peer_id=peer_id, random_id=get_random_id(), attachment=attachment
    )


async def _a_send_text(vk: Any, peer_id: int, text: str) -> None:
    await asyncio.to_thread(_send_text, vk, peer_id, text)


async def _a_send_photo(
    vk: Any, upload: VkUpload, peer_id: int, image_bytes: bytes
) -> None:
    await asyncio.to_thread(_send_photo, vk, upload, peer_id, image_bytes)


def _extract_message(event: Any) -> tuple[int, int, str]:
    msg = getattr(event, "message", None)
    if msg:
        return int(msg.peer_id), int(msg.from_id), str(msg.text or "")
    obj = getattr(event, "obj", None)
    if obj and getattr(obj, "message", None):
        m = obj.message
        return int(m.peer_id), int(m.from_id), str(m.text or "")
    raise ValueError("Unsupported VK event payload")


def _parse_threshold(arg: str) -> float | None:
    try:
        value = float(arg.replace(",", ".").rstrip("%"))
    except ValueError:
        return None
    if value > 1:
        value /= 100.0
    return value


def _normalize_tf(arg: str | None) -> str | None:
    if not arg:
        return None
    return arg if arg in {"1h", "6h"} else None


def _settings_keyboard() -> str:
    keyboard = VkKeyboard(inline=True)
    keyboard.add_button("Сводка", color=VkKeyboardColor.PRIMARY)
    keyboard.add_button("Прогноз", color=VkKeyboardColor.SECONDARY)
    keyboard.add_button("График", color=VkKeyboardColor.SECONDARY)
    keyboard.add_line()
    keyboard.add_button("Подписка", color=VkKeyboardColor.POSITIVE)
    keyboard.add_button("Отписка", color=VkKeyboardColor.NEGATIVE)
    return keyboard.get_keyboard()


def _send_menu(vk: Any, peer_id: int, text: str, keyboard: str) -> None:
    vk.messages.send(
        peer_id=peer_id,
        random_id=get_random_id(),
        message=text,
        keyboard=keyboard,
    )


async def _a_send_menu(
    vk: Any, peer_id: int, text: str, keyboard: str
) -> None:
    await asyncio.to_thread(_send_menu, vk, peer_id, text, keyboard)


async def _handle_refresh(vk: Any, peer_id: int) -> None:
    await _a_send_text(vk, peer_id, "Запуск переобучения...")
    try:
        def _run() -> None:
            run_ingest(force=True)
            run_features()
            run_train(tune=True)

        await asyncio.to_thread(_run)
        clear_cache()
        await _a_send_text(vk, peer_id, "Переобучение завершено.")
    except Exception:
        logger.exception("refresh failed")
        await _a_send_text(
            vk, peer_id, "Не удалось выполнить переобучение. Попробуйте позже."
        )


async def _handle_event(vk: Any, upload: VkUpload, event: Any) -> None:
    peer_id, from_id, text = _extract_message(event)
    cmd, arg = parse_command(text)
    if not cmd:
        return

    try:
        if cmd == "start":
            await _a_send_text(vk, peer_id, formatters.format_start())
            return
        if cmd == "help":
            await _a_send_text(vk, peer_id, formatters.format_help())
            return
        if cmd == "info":
            await _a_send_text(vk, peer_id, formatters.format_info())
            return
        if cmd == "summary":
            result = await asyncio.to_thread(get_cached_predict, run_predict)
            await _a_send_text(vk, peer_id, formatters.format_summary(result))
            return
        if cmd == "forecast":
            if arg and _normalize_tf(arg) is None:
                await _a_send_text(
                    vk,
                    peer_id,
                    "Укажите таймфрейм 1h или 6h, например: /forecast 1h",
                )
                return
            result = await asyncio.to_thread(get_cached_predict, run_predict)
            await _a_send_text(
                vk,
                peer_id,
                formatters.format_forecast(result, tf=_normalize_tf(arg)),
            )
            return
        if cmd == "chart":
            if arg and _normalize_tf(arg) is None:
                await _a_send_text(
                    vk,
                    peer_id,
                    "Укажите таймфрейм 1h или 6h, например: /chart 1h",
                )
                return
            tf = _normalize_tf(arg) or "1h"
            result = await asyncio.to_thread(get_cached_predict, run_predict)
            try:
                img = await asyncio.to_thread(charts.render_chart, result, tf)
            except Exception as exc:
                logger.exception("chart render failed")
                await _a_send_text(
                    vk, peer_id, f"Не удалось построить график: {exc}"
                )
                return
            await _a_send_photo(vk, upload, peer_id, img)
            return
        if cmd == "subscribe":
            threshold = None
            if arg is not None:
                threshold = _parse_threshold(arg)
                if threshold is None:
                    await _a_send_text(
                        vk,
                        peer_id,
                        "Укажите число от 0.5 до 0.95, например: /subscribe 0.7",
                    )
                    return
                await asyncio.to_thread(
                    userstate.set_min_confidence, from_id, threshold
                )
            changed = await asyncio.to_thread(userstate.subscribe, from_id)
            rec = await asyncio.to_thread(userstate.get_user, from_id)
            await _a_send_text(
                vk,
                peer_id,
                formatters.format_subscription(
                    True, changed, rec["min_confidence"]
                ),
            )
            return
        if cmd == "unsubscribe":
            changed = await asyncio.to_thread(userstate.unsubscribe, from_id)
            rec = await asyncio.to_thread(userstate.get_user, from_id)
            await _a_send_text(
                vk,
                peer_id,
                formatters.format_subscription(
                    False, changed, rec["min_confidence"]
                ),
            )
            return
        if cmd == "settings":
            rec = await asyncio.to_thread(userstate.get_user, from_id)
            await _a_send_menu(
                vk,
                peer_id,
                formatters.format_settings(rec),
                _settings_keyboard(),
            )
            return
        if cmd == "refresh":
            settings = get_settings()
            if from_id not in settings.vk_admin_id_set:
                await _a_send_text(
                    vk, peer_id, "Команда доступна только администраторам."
                )
                return
            asyncio.create_task(_handle_refresh(vk, peer_id))
            return
    except Exception:
        logger.exception("vk command failed: %s", cmd)
        await _a_send_text(vk, peer_id, GENERIC_ERROR)


async def _guarded_handle(vk: Any, upload: VkUpload, event: Any) -> None:
    try:
        await asyncio.wait_for(
            _handle_event(vk, upload, event), timeout=EVENT_TIMEOUT_SEC
        )
    except asyncio.TimeoutError:
        logger.warning(
            "Обработка VK-события превысила таймаут %.0fs", EVENT_TIMEOUT_SEC
        )
    except Exception:
        logger.exception("Ошибка обработки VK-события — пропускаем")


def _listen_forever(
    longpoll: VkBotLongPoll,
    vk: Any,
    upload: VkUpload,
    loop: asyncio.AbstractEventLoop,
) -> None:
    for event in longpoll.listen():
        if event.type != VkBotEventType.MESSAGE_NEW:
            continue
        asyncio.run_coroutine_threadsafe(
            _guarded_handle(vk, upload, event), loop
        )


async def run_vk_bot() -> None:
    settings = get_settings()
    if not settings.vk_group_token:
        raise RuntimeError("VK_GROUP_TOKEN is not set")
    if not settings.vk_group_id:
        raise RuntimeError("VK_GROUP_ID is not set")

    vk_session = vk_api.VkApi(token=settings.vk_group_token)
    vk = vk_session.get_api()
    upload = VkUpload(vk_session)
    longpoll = VkBotLongPoll(vk_session, settings.vk_group_id)

    async def reload_bundle() -> None:
        clear_cache()
        logger.info("Forecast cache cleared after retrain")

    async def admin_msg(text: str) -> None:
        for peer_id in settings.vk_admin_id_set:
            try:
                await asyncio.to_thread(_send_text, vk, int(peer_id), text)
            except Exception as exc:  # noqa: BLE001
                logger.warning("VK admin message failed: %s", exc)

    async def user_msg(user_id: int, text: str) -> None:
        await asyncio.to_thread(_send_text, vk, int(user_id), text)

    scheduler = BotScheduler(
        on_bundle_reload=reload_bundle,
        on_admin_message=admin_msg,
        on_user_message=user_msg,
    )
    scheduler.start()
    debug_runner = await _start_debug_server()
    loop = asyncio.get_running_loop()

    try:
        logger.info("VK bot starting (group_id=%s)", settings.vk_group_id)
        await asyncio.to_thread(_listen_forever, longpoll, vk, upload, loop)
    finally:
        scheduler.shutdown()
        if debug_runner is not None:
            await debug_runner.cleanup()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(run_vk_bot())


if __name__ == "__main__":
    main()