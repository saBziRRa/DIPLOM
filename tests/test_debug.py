import asyncio

from aiohttp.test_utils import TestClient, TestServer

from btc_forecast.config import get_settings
from btc_forecast.debug.server import create_app
from btc_forecast.debug.state import collect_status


def test_collect_status_minimal():
    status = collect_status(include_forecast=False)
    assert "scheduler" in status
    assert "artifacts" in status
    assert "cache" in status


def test_debug_api_localhost():
    async def _run() -> None:
        app = create_app(mode="dashboard")
        settings = get_settings()
        token = settings.admin_debug_token
        suffix = f"&token={token}" if token else ""
        async with TestClient(TestServer(app)) as client:
            resp = await client.get(f"/api/status?refresh=0{suffix}")
            assert resp.status == 200
            data = await resp.json()
            assert "timestamp" in data
            assert "actions" in data

            html = await client.get(f"/?token={token}" if token else "/")
            assert html.status == 200
            text = await html.text()
            assert "Admin Dashboard" in text

            bm_page = await client.get(f"/benchmark?token={token}" if token else "/benchmark")
            assert bm_page.status == 200

            bm_api = await client.get(f"/api/benchmark?limit=5{suffix}")
            assert bm_api.status == 200
            bm_data = await bm_api.json()
            assert "rows" in bm_data
            assert bm_data.get("mode") == "dashboard"

    asyncio.run(_run())


def test_debug_action_unknown():
    async def _run() -> None:
        app = create_app(standalone_mode=True, mode="dashboard")
        settings = get_settings()
        token = settings.admin_debug_token
        suffix = f"?token={token}" if token else ""
        async with TestClient(TestServer(app)) as client:
            resp = await client.post(f"/api/actions/unknown{suffix}", json={})
            assert resp.status == 400

    asyncio.run(_run())


def test_debug_api_standalone_mode_forecast_attempt():
    async def _run() -> None:
        app = create_app(standalone_mode=True, mode="debug")
        settings = get_settings()
        token = settings.admin_debug_token
        suffix = f"&token={token}" if token else ""
        async with TestClient(TestServer(app)) as client:
            resp = await client.get(f"/api/status?refresh=1{suffix}")
            assert resp.status == 200
            data = await resp.json()
            assert data.get("standalone_mode") is True
            assert "bot_preview" in data
            # В standalone scheduler не запущен, но preview формируется через тот же pipeline.
            assert "retrain" in data.get("scheduler", {})
            assert data.get("actions") == {}

    asyncio.run(_run())


def test_debug_mode_has_no_actions_endpoint():
    async def _run() -> None:
        app = create_app(standalone_mode=True, mode="debug")
        settings = get_settings()
        token = settings.admin_debug_token
        suffix = f"?token={token}" if token else ""
        async with TestClient(TestServer(app)) as client:
            resp = await client.post(f"/api/actions/predict{suffix}", json={})
            assert resp.status == 404
            bm_api = await client.get(f"/api/benchmark{suffix}")
            assert bm_api.status == 200
            bm_data = await bm_api.json()
            assert bm_data.get("mode") == "debug"

    asyncio.run(_run())
