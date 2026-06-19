"""Application configuration loaded from YAML and environment."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    telegram_bot_token: str = ""
    vk_group_token: str = ""
    vk_group_id: int = 0
    vk_admin_ids: str = ""
    cryptopanic_token: str = ""
    admin_chat_ids: str = ""
    btc_forecast_config: str = "config/default.yaml"

    retrain_interval_sec: int = 86400
    drift_window_bars_1h: int = 500
    drift_window_bars_6h: int = 100
    drift_threshold: float = 0.05
    drift_check_interval_sec: int = 3600
    broadcast_interval_sec: int = 1800
    inference_cache_ttl_sec: int = 900

    debug_enabled: bool = True
    debug_host: str = "127.0.0.1"
    debug_port: int = 8765
    admin_debug_token: str = ""

    @property
    def root(self) -> Path:
        return _project_root()

    @property
    def yaml_config(self) -> dict[str, Any]:
        cfg_path = self.root / self.btc_forecast_config
        return _load_yaml(cfg_path)

    def path(self, key: str) -> Path:
        paths = self.yaml_config.get("paths", {})
        rel = paths.get(key, f"artifacts/{key}")
        return self.root / rel

    @property
    def artifacts_dir(self) -> Path:
        return self.path("artifacts")

    @property
    def models_dir(self) -> Path:
        return self.path("models")

    @property
    def features_dir(self) -> Path:
        return self.path("features")

    @property
    def raw_dir(self) -> Path:
        return self.path("raw")

    @property
    def reports_dir(self) -> Path:
        return self.path("reports")

    @property
    def admin_ids(self) -> set[int]:
        if not self.admin_chat_ids.strip():
            return set()
        return {int(x.strip()) for x in self.admin_chat_ids.split(",") if x.strip()}

    @property
    def vk_admin_id_set(self) -> set[int]:
        if self.vk_admin_ids.strip():
            return {int(x.strip()) for x in self.vk_admin_ids.split(",") if x.strip()}
        return self.admin_ids

    def get(self, *keys: str, default: Any = None) -> Any:
        node: Any = self.yaml_config
        for key in keys:
            if not isinstance(node, dict):
                return default
            node = node.get(key, default)
        return node

    @property
    def debug_cfg(self) -> dict[str, Any]:
        return self.yaml_config.get("debug", {})

    @property
    def debug_enabled_resolved(self) -> bool:
        if "enabled" in self.debug_cfg:
            return bool(self.debug_cfg["enabled"])
        return self.debug_enabled

    @property
    def debug_host_resolved(self) -> str:
        return str(self.debug_cfg.get("host", self.debug_host))

    @property
    def debug_port_resolved(self) -> int:
        return int(self.debug_cfg.get("port", self.debug_port))


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    if settings.cryptopanic_token:
        os.environ.setdefault("CRYPTOPANIC_TOKEN", settings.cryptopanic_token)
    return settings
