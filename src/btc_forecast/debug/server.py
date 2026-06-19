"""Admin debug web UI (aiohttp)."""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import logging
import math
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from aiohttp import web

from btc_forecast.bot.cache import clear_cache
from btc_forecast.config import get_settings
from btc_forecast.debug.state import collect_status
from btc_forecast.pipeline.benchmark import load_benchmark_history, run_benchmark
from btc_forecast.pipeline.features import run_features
from btc_forecast.pipeline.ingest import run_ingest
from btc_forecast.pipeline.predict import run_predict
from btc_forecast.pipeline.train import run_train

logger = logging.getLogger(__name__)

_DASHBOARD_HTML = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")
_DEBUG_HTML = (Path(__file__).parent / "debug.html").read_text(encoding="utf-8")
_BENCHMARK_HTML = (Path(__file__).parent / "benchmark.html").read_text(encoding="utf-8")
_APP_STANDALONE_MODE = web.AppKey("standalone_mode", bool)
_APP_ACTION_MANAGER = web.AppKey("action_manager", Any)
_APP_MODE = web.AppKey("mode", str)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


@dataclass
class ActionState:
    name: str
    status: str = "idle"
    started_at: float | None = None
    finished_at: float | None = None
    duration_sec: float | None = None
    run_count: int = 0
    last_error: str | None = None
    last_result: str | None = None
    log: str = ""
    task: asyncio.Task | None = field(default=None, repr=False)

    def as_dict(self) -> dict[str, Any]:
        now = time.time()
        return {
            "name": self.name,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "running_for_sec": (
                round(now - self.started_at, 1)
                if self.status == "running" and self.started_at
                else None
            ),
            "duration_sec": self.duration_sec,
            "run_count": self.run_count,
            "last_error": self.last_error,
            "last_result": self.last_result,
            "log": self.log[-8000:],
        }


class ActionManager:
    def __init__(self) -> None:
        self._states = {
            "ingest": ActionState(name="ingest"),
            "train": ActionState(name="train"),
            "predict": ActionState(name="predict"),
            "benchmark": ActionState(name="benchmark"),
        }

    def snapshot(self) -> dict[str, Any]:
        return {name: state.as_dict() for name, state in self._states.items()}

    async def trigger(self, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        if action not in self._states:
            raise web.HTTPBadRequest(text=f"Unknown action: {action}")
        state = self._states[action]
        if state.task and not state.task.done():
            raise web.HTTPConflict(text=f"Action '{action}' already running")

        state.status = "running"
        state.started_at = time.time()
        state.finished_at = None
        state.duration_sec = None
        state.last_error = None
        state.last_result = None
        state.log = ""
        state.run_count += 1
        state.task = asyncio.create_task(self._run_action(state, payload))
        return state.as_dict()

    async def _run_action(self, state: ActionState, payload: dict[str, Any]) -> None:
        log_buffer = io.StringIO()
        try:
            result = await asyncio.to_thread(self._execute_action, state.name, payload, log_buffer)
            state.status = "ok"
            state.last_result = result
        except Exception as exc:  # noqa: BLE001
            state.status = "error"
            state.last_error = str(exc)
            log_buffer.write("\n" + traceback.format_exc())
        finally:
            state.finished_at = time.time()
            if state.started_at:
                state.duration_sec = round(state.finished_at - state.started_at, 2)
            state.log = log_buffer.getvalue()
            clear_cache()

    def _execute_action(self, action: str, payload: dict[str, Any], log_buffer: io.StringIO) -> str:
        with contextlib.redirect_stdout(log_buffer), contextlib.redirect_stderr(log_buffer):
            if action == "ingest":
                force = bool(payload.get("force", False))
                out = run_ingest(force=force)
                return f"ingest done: {len(out)} sources"
            if action == "train":
                skip_features = bool(payload.get("skip_features", False))
                skip_tune = bool(payload.get("skip_tune", False))
                if not skip_features:
                    run_features()
                bundle = run_train(tune=not skip_tune)
                return f"train done: bundle={bundle}"
            if action == "predict":
                refresh = bool(payload.get("refresh", True))
                result = run_predict(use_cached_final=not refresh)
                return (
                    f"predict done: price={result.price:.2f}, "
                    f"agreement={result.agreement}, tf={list(result.forecasts)}"
                )
            if action == "benchmark":
                name = str(payload.get("name", "")).strip()
                description = str(payload.get("description", "")).strip()
                result = run_benchmark(name=name, description=description)
                run_id = result["run_id"].iloc[0] if len(result) else "n/a"
                return f"benchmark done: run_id={run_id}, rows={len(result)}"
        raise RuntimeError(f"Unsupported action: {action}")


def _check_auth(request: web.Request) -> bool:
    settings = get_settings()
    token = settings.admin_debug_token
    if not token:
        # No token: allow only localhost
        peer = request.remote or ""
        return peer in ("127.0.0.1", "::1", "localhost") or peer.startswith("127.")
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer ") and auth[7:] == token:
        return True
    return request.query.get("token") == token


def _auth_middleware(handler):
    async def wrapper(request: web.Request) -> web.StreamResponse:
        if not _check_auth(request):
            raise web.HTTPUnauthorized(
                text="Unauthorized",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return await handler(request)

    return wrapper


@_auth_middleware
async def handle_index(request: web.Request) -> web.Response:
    mode = request.app.get(_APP_MODE, "dashboard")
    html = _DASHBOARD_HTML if mode == "dashboard" else _DEBUG_HTML
    return web.Response(text=html, content_type="text/html")


@_auth_middleware
async def handle_benchmark_page(_request: web.Request) -> web.Response:
    return web.Response(text=_BENCHMARK_HTML, content_type="text/html")


@_auth_middleware
async def handle_api_status(request: web.Request) -> web.Response:
    refresh = request.query.get("refresh", "1") != "0"
    standalone_mode = bool(request.app.get(_APP_STANDALONE_MODE, False))
    status = collect_status(
        # Прогноз считаем и в standalone (btc-debug), чтобы админ видел
        # ровно те же данные, что получат пользователи бота.
        include_forecast=refresh,
        standalone_mode=standalone_mode,
    )
    mode = request.app.get(_APP_MODE, "dashboard")
    if mode == "dashboard":
        manager = request.app[_APP_ACTION_MANAGER]
        status["actions"] = manager.snapshot()
    else:
        status["actions"] = {}
    status = _json_safe(status)
    return web.json_response(
        status,
        dumps=lambda obj: json.dumps(obj, ensure_ascii=False, default=str, allow_nan=False),
    )


@_auth_middleware
async def handle_action(request: web.Request) -> web.Response:
    action = request.match_info["action"]
    manager = request.app[_APP_ACTION_MANAGER]
    payload = {}
    if request.can_read_body:
        try:
            payload = await request.json()
        except Exception:  # noqa: BLE001
            payload = {}
    state = await manager.trigger(action, payload)
    return web.json_response({"ok": True, "action": action, "state": state}, status=202)


@_auth_middleware
async def handle_api_benchmark(request: web.Request) -> web.Response:
    limit = int(request.query.get("limit", "300"))
    mode = request.app.get(_APP_MODE, "dashboard")
    rows = load_benchmark_history(limit=limit)
    payload = _json_safe({"mode": mode, "rows": rows})
    return web.json_response(
        payload,
        dumps=lambda obj: json.dumps(obj, ensure_ascii=False, default=str, allow_nan=False),
    )


def create_app(
    standalone_mode: bool = False,
    mode: str = "dashboard",
) -> web.Application:
    if mode not in {"debug", "dashboard"}:
        raise ValueError(f"Unsupported mode: {mode}")
    app = web.Application()
    app[_APP_STANDALONE_MODE] = standalone_mode
    app[_APP_MODE] = mode
    app[_APP_ACTION_MANAGER] = ActionManager()
    app.router.add_get("/", handle_index)
    app.router.add_get("/benchmark", handle_benchmark_page)
    app.router.add_get("/api/status", handle_api_status)
    app.router.add_get("/api/benchmark", handle_api_benchmark)
    if mode == "dashboard":
        app.router.add_post("/api/actions/{action}", handle_action)
    return app


async def run_debug_server(
    host: str | None = None,
    port: int | None = None,
    standalone_mode: bool = False,
    mode: str = "dashboard",
) -> None:
    settings = get_settings()
    host = host or settings.debug_host_resolved
    port = port or settings.debug_port_resolved
    app = create_app(standalone_mode=standalone_mode, mode=mode)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info("%s UI http://%s:%d/", mode.capitalize(), host, port)
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await runner.cleanup()


def main(mode: str = "debug") -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    asyncio.run(run_debug_server(standalone_mode=True, mode=mode))
