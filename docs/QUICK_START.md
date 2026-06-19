# Быстрый старт

Telegram/VK-бот прогнозирует **направление** BTC/USDT (UP / DOWN / FLAT) на таймфреймах **1H** и **6H** с помощью каскада GATE + регрессор.

> Бот не предсказывает цену OHLC — только сигнал на ближайший бар и ориентир на горизонте.

## Требования

- Python **3.11+**
- Доступ в интернет (Bybit, yfinance, alternative.me)
- Токен Telegram-бота ([@BotFather](https://t.me/BotFather)) или токен VK сообщества

Опционально:

- `CRYPTOPANIC_TOKEN` — новостной сентимент
- `pip install -e ".[sentiment]"` — CryptoBERT / VADER для сентимента

## Установка

```bash
cd /path/to/atm2
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
cp .env.example .env
```

Заполните `.env`:

```env
TELEGRAM_BOT_TOKEN=123456:ABC...
VK_GROUP_TOKEN=vk1.a....
VK_GROUP_ID=123456789
ADMIN_CHAT_IDS=your_telegram_user_id
VK_ADMIN_IDS=123456789            # необязательно, fallback на ADMIN_CHAT_IDS
CRYPTOPANIC_TOKEN=          # необязательно
BTC_FORECAST_CONFIG=config/default.yaml
```

Узнать свой Telegram ID: [@userinfobot](https://t.me/userinfobot).

## Первый запуск (полный цикл)

### 1. Загрузка данных

```bash
btc-ingest
```

Скачивает ~720 дней истории в `artifacts/raw/` (Bybit, макро, OI, funding и др.).

Принудительная перезагрузка:

```bash
btc-ingest --force
```

### 2. Обучение модели

```bash
btc-train
```

Выполняет:

1. Построение признаков и отбор фич
2. Optuna-тюнинг (gate + регрессор)
3. Walk-forward обучение и сохранение `artifacts/models/cascade_bundle.pkl`

**Первый запуск может занять 1–3+ часа** (зависит от CPU и сети).

Ускорение для разработки — уменьшите в `config/default.yaml`:

```yaml
download_days: 90
tuning:
  budget: light   # 40 trials; для ещё быстрее — правьте models/tuning.py
```

Пропустить этапы:

```bash
btc-train --skip-tune      # без Optuna (нужны best_params*.json)
btc-train --skip-features  # только обучение на готовых CSV
```

### 3. Проверка без бота

```bash
btc-predict
```

Выведет сводку `/summary` и краткий `/forecast` в терминал.

### 4. Запуск бота

```bash
btc-bot
```

Бот работает в режиме long-polling. В Telegram отправьте `/start`.

Для VK:

```bash
btc-vk-bot
```

В сообщения сообщества отправьте `/start` (или `start`).

## Команды бота

| Команда | Что делает |
|---------|------------|
| `/start` | Справка и дисклеймер |
| `/summary` | Сводка 1H + 6H на **ближайший бар** (актуальные фичи) |
| `/forecast` | Таблица на горизонт (~24ч / ~28 дней) |
| `/forecast 1h` | Только 1H |
| `/forecast 6h` | Только 6H |
| `/chart 1h` | PNG-график сигналов |
| `/chart 6h` | PNG-график сигналов |
| `/refresh` | Переобучение (**только admin**) |

### Пример `/summary`

```
BTC/USDT: 97 450.00 (обновлено 01.06 21:00 MSK)

1H (ближайший бар): DOWN | P(move)=62% | уверенность=41%
6H (ближайший бар): FLAT

Согласованность: частичная (1H активен)

Модель: v1.0.0 | обучена 2026-05-28
Качество (hold-out): 1H MCC=0.20 | 6H MCC=0.24

Горизонт детализации: /forecast
```

## Что означают сигналы

| Сигнал | Значение |
|--------|----------|
| **UP** | Каскад ожидает рост на следующем баре |
| **DOWN** | Каскад ожидает падение |
| **FLAT** | Движение незначимо или gate не сработал |

Частые FLAT — нормально: модель консервативна (active rate на hold-out ~0.1–2%).

## Режимы прогноза

| Режим | Где используется | Доверие |
|-------|------------------|---------|
| **live_signal** | `/summary` | Высокое — все рыночные фичи свежие |
| **horizon_table** | `/forecast`, `/chart` | Среднее — фичи «заморожены», обновляются только календарь и лаги |

На длинном горизонте качество `/forecast` **падает** — это ориентир, не точный прогноз.

## Тесты

```bash
pytest
```

## Типичные ошибки

| Ошибка | Решение |
|--------|---------|
| `Bundle not found` | Выполните `btc-train` |
| `No data — run btc-ingest` | Выполните `btc-ingest` |
| `TELEGRAM_BOT_TOKEN is not set` | Заполните `.env` |
| Долгое обучение | Уменьшите `download_days` и `tuning.budget` |

## Admin debug UI

При запуске `btc-bot` или `btc-vk-bot` (если `DEBUG_ENABLED=true`) доступна панель:

```
http://127.0.0.1:8765/?token=YOUR_ADMIN_DEBUG_TOKEN
```

Показывает: время до retrain, метрики модели, drift, превью `/summary` и `/forecast`.
Также из панели можно вручную запускать `ingest`, `train`, `predict`, `benchmark`.

Отдельно:

- `btc-debug` — **только мониторинг** (read-only) без управления;
- `btc-dashboard` — **управление + мониторинг** (run ingest/train/predict).
- `btc-benchmark --name "manual-check" --description "after retrain"` — прогон benchmark и запись в `artifacts/benchmark.csv`.

В standalone-режиме (`btc-debug`) блоки runtime-процесса показываются как placeholders:

- scheduler/bot status: `standalone_mode: bot/scheduler not running`

При этом `/summary` и `/forecast` считаются тем же pipeline, что и в боте, на основе текущих артефактов.

## Дальше

- Администрирование, планировщик, drift, debug UI — [ADMIN.md](ADMIN.md)
- Общее описание проекта — [../README.md](../README.md)
