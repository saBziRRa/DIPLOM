# BTC Telegram/VK Bot

Прогноз направления BTC/USDT на таймфреймах 1H и 6H с каскадом **GATE + регрессор (v9)**.

## Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
# Заполните TELEGRAM_BOT_TOKEN (или VK_GROUP_TOKEN+VK_GROUP_ID) и ADMIN_CHAT_IDS/VK_ADMIN_IDS
```

## Быстрый старт

```bash
# 1. Загрузка данных (Bybit, yfinance, …)
btc-ingest

# 2. Фичи + обучение (Optuna + walk-forward)
btc-train

# 3. Telegram-бот
btc-bot

# 3b. VK-бот
btc-vk-bot
```

CLI-прогноз без бота:

```bash
btc-predict
```

## Команды бота
- `/start` — начало
- `/help` — список команд (алиасы: помощь, команды)
- `/info` — о системе и авторе (инфо)
- `/summary` — сводка 1H + 6H (сводка)
- `/forecast [1h|6h] ` — прогноз на горизонт (прогноз)
- `/chart [1h|6h]` — график сигналов (график)
- `/subscribe [0.5-0.95]` — подписка на уверенные сигналы (подписка, подписаться)
- `/unsubscribe` — отмена подписки (отписка, отписаться)
- `/settings ` — входв меню настроек с кнопками (настройки)
- `/refresh` — переобучение, только для админа

## Конфигурация

- `config/default.yaml` — горизонты, пороги, пути артефактов
- `.env` — `TELEGRAM_BOT_TOKEN` или `VK_GROUP_TOKEN`+`VK_GROUP_ID`, `CRYPTOPANIC_TOKEN`, `ADMIN_CHAT_IDS`/`VK_ADMIN_IDS`
- `RETRAIN_INTERVAL_SEC` (default 86400) — авто-переобучение в фоне бота

## Admin debug UI

```bash
btc-bot        # встроенный dashboard на http://127.0.0.1:8765/ (если DEBUG_ENABLED=true)
btc-debug      # headless read-only мониторинг
btc-dashboard  # standalone управление + мониторинг
btc-benchmark --name "manual-check" --description "после retrain"
```

Подробнее: [docs/ADMIN.md](docs/ADMIN.md)

## Структура

```
src/btc_forecast/
  data/loader.py       # загрузка с anti-leakage alignment
  features/            # engineering, selection, targets
  models/              # tuning, training, bundle, inference
  bot/                 # aiogram + scheduler + drift alerts
  pipeline/            # ingest → features → train → predict
```

## Тесты

```bash
pytest
```

## Дисклеймер

Не является финансовой рекомендацией. Прогноз — направление (UP/DOWN/FLAT), не цена OHLC.
