# Руководство администратора

Документ для развёртывания, обслуживания и мониторинга BTC Forecast Bot.

## Роли и доступ

| Параметр | Назначение |
|----------|------------|
| `TELEGRAM_BOT_TOKEN` | Токен бота (обязателен для `btc-bot`) |
| `VK_GROUP_TOKEN` | Токен сообщества VK (обязателен для `btc-vk-bot`) |
| `VK_GROUP_ID` | ID сообщества VK для Bots Long Poll |
| `ADMIN_CHAT_IDS` | Telegram user ID через запятую — доступ к `/refresh` и алертам |
| `VK_ADMIN_IDS` | VK user ID через запятую — доступ к `refresh` и алертам |

Пример `.env`:

```env
TELEGRAM_BOT_TOKEN=123456789:AA...
VK_GROUP_TOKEN=vk1.a....
VK_GROUP_ID=123456789
ADMIN_CHAT_IDS=111111111,222222222
VK_ADMIN_IDS=111111111,222222222
CRYPTOPANIC_TOKEN=optional_token
BTC_FORECAST_CONFIG=config/default.yamlы
```

Переменные из `.env` с префиксом (pydantic-settings):

| Переменная | Default | Описание |
|------------|---------|----------|
| `RETRAIN_INTERVAL_SEC` | `43200` | Интервал авто-переобучения (сек) |
| `DRIFT_WINDOW_BARS_1H` | `500` | Окно drift для 1H |
| `DRIFT_WINDOW_BARS_6H` | `100` | Окно drift для 6H |
| `DRIFT_THRESHOLD` | `0.05` | Падение AUC для алерта |
| `DRIFT_CHECK_INTERVAL_SEC` | `3600` | Интервал проверки drift |
| `INFERENCE_CACHE_TTL_SEC` | `900` | TTL кэша прогноза в боте |
| `ADMIN_DEBUG_TOKEN` | — | Токен доступа к debug UI |
| `DEBUG_ENABLED` | `true` | Включить веб-интерфейс при `btc-bot` |
| `DEBUG_HOST` | `127.0.0.1` | Адрес debug-сервера |
| `DEBUG_PORT` | `8765` | Порт debug-сервера |

Те же значения можно задать в `config/default.yaml` (секции `scheduler`, `drift`, `inference`, `debug`).

## Debug-интерфейс (веб)

Админ-панель для мониторинга состояния бота, метрик и превью сообщений пользователям.

### Запуск

**Вместе с ботом** (по умолчанию, если `DEBUG_ENABLED=true`):

```bash
btc-vk-bot
# → http://127.0.0.1:8765/
```

**Отдельно** (без ВК):

```bash
btc-debug      # headless read-only мониторинг
btc-dashboard  # управление + мониторинг
```

`btc-debug` запускает headless debug-monitor в `standalone_mode` (только read-only):

- бот и scheduler не поднимаются,
- API возвращает placeholders для bot runtime данных,
- это удобно для проверки артефактов/конфига без токена.

### Доступ

- Без токена: только с `127.0.0.1` (localhost)
- С токеном (`ADMIN_DEBUG_TOKEN` в `.env`):

```
http://127.0.0.1:8765/?token=your_debug_token
```

или заголовок `Authorization: Bearer your_debug_token`

### Что отображается

| Блок | Содержание |
|------|------------|
| Планировщик | Время до retrain / drift check, последний запуск, ошибки |
| Управление | Только в `btc-dashboard`: кнопки запуска `ingest/train/predict/benchmark` on-demand, статусы и логи |
| Кэш | TTL прогноза, время до инвалидации |
| Модель | Версия bundle, hold-out MCC/AUC |
| Drift | Текущий vs baseline gate AUC |
| Артефакты | Возраст bundle, датасетов, сырых данных |
| `/summary`, `/forecast` | Текст, который увидят пользователи в боте |

Страница обновляется автоматически каждые 5 секунд. JSON API: `GET /api/status`.

### Standalone placeholders (`btc-debug`)

При запуске `btc-debug` в ответе `GET /api/status`:

- `standalone_mode: true`
- `scheduler.*.last_error = "standalone_mode: bot/scheduler not running"`
- `bot_preview.summary/forecast` считаются pipeline-ом прогноза (как в боте) из текущих артефактов
- placeholders используются только если прогноз не удалось посчитать (например, нет bundle)

Это ожидаемое поведение: в этом режиме нет live-сессии бота и нет runtime-данных polling/scheduler.

### API управления из dashboard (`btc-dashboard`)

Для ручного запуска задач из браузера используется endpoint:

```http
POST /api/actions/{action}
```

Где `{action}`: `ingest`, `train`, `predict`, `benchmark`.
Также доступен `benchmark` (записывает метрики в `artifacts/benchmark.csv`).

Примеры payload:

```json
{ "force": true }                 // ingest
{ "skip_tune": true }             // train
{ "refresh": true }               // predict
{ "name": "manual-check", "description": "after retrain" } // benchmark
```

Статус и логи задач возвращаются в `GET /api/status` → `actions`.

Отдельный интерфейс сравнения benchmark-результатов: `GET /benchmark`.

## Артефакты

```
artifacts/
├── raw/           # btc_*_*.csv — сырые данные
├── features/      # features_*.csv, final_dataset_*.csv
├── models/        # cascade_bundle.pkl, best_params*.json
└── reports/       # отчёты selection (JSON)
```

Ключевые файлы:

| Файл | Содержание |
|------|------------|
| `artifacts/models/cascade_bundle.pkl` | Обученные модели + метаданные |
| `artifacts/models/best_params.json` | Гиперпараметры gate (Optuna) |
| `artifacts/models/best_params_reg.json` | Гиперпараметры регрессора |
| `artifacts/features/final_dataset_1h.csv` | Финальный датасет 1H |
| `artifacts/features/final_dataset_1h_jump.csv` | Jump-строки для регрессора |

### Метаданные bundle

`cascade_bundle.pkl` содержит `BundleMeta`:

- `version` — версия схемы bundle
- `feature_cols_hash` — hash списка признаков
- `trained_at` — дата обучения (UTC)
- `metrics` — hold-out MCC/AUC по 1H и 6H

При несовпадении `feature_cols_hash` после изменения фич нужно переобучить: `btc-train`.

## CLI-операции

### Загрузка данных

```bash
btc-ingest          # инкрементально (пропуск, если кэш есть)
btc-ingest --force  # принудительная перезапись CSV
```

### Обучение

```bash
btc-train                    # features + tune + train
btc-train --skip-tune        # без Optuna
btc-train --skip-features    # только train на готовых CSV
```

Пайплайн обучения:

```
ingest → features → selection → targets (jump) → Optuna → walk-forward → bundle.pkl
```

### Прогноз (отладка)

```bash
btc-predict
```

### Бот

```bash
btc-bot
btc-vk-bot
```

При старте бота автоматически запускается планировщик (см. ниже).

## Планировщик (фоновые задачи)

Реализован в `src/btc_forecast/bot/scheduler.py`, стартует вместе с `btc-bot`.

| Задача | Интервал (default) | Действие |
|--------|-------------------|----------|
| **retrain** | 86400 сек (24 ч) | `ingest` → `features` → `train` → сброс кэша прогноза |
| **drift** | 3600 сек (1 ч) | Проверка gate AUC на последних N барах |

При ошибке retrain:

- Ошибка пишется в лог
- Админам отправляется: `❌ Retrain failed: ...`
- Бот продолжает работать на **старом** bundle

Изменить интервал retrain:

```yaml
# config/default.yaml
scheduler:
  retrain_interval_sec: 43200   # каждые 12 часов
```

или в `.env`:

```env
RETRAIN_INTERVAL_SEC=43200
```

## Drift-мониторинг

Модуль: `src/btc_forecast/monitoring/drift.py`

Логика:

1. Берёт последние N баров из `final_dataset_{tf}.csv`
2. Считает gate AUC на этих барах
3. Сравнивает с baseline из `BundleMeta.metrics` (hold-out)
4. Если `auc_recent < auc_baseline - threshold` → алерт админу

Пример алерта:

```
⚠ Drift: gate AUC 1H упал до 0.71 (было 0.77, окно 500 баров). Рекомендуется проверить данные.
```

Настройка:

```yaml
drift:
  window_bars_1h: 500
  window_bars_6h: 100
  threshold: 0.05
  check_interval_sec: 3600
```

## Команда `/refresh`

Доступна только пользователям из `ADMIN_CHAT_IDS`.

Выполняет вручную тот же цикл, что и планировщик retrain:

1. `btc-ingest --force`
2. Построение фич
3. `btc-train` (с Optuna)
4. Сброс кэша прогноза

Используйте после:

- изменения `config/default.yaml`
- обновления кода feature engineering
- подозрения на деградацию модели

## Конфигурация модели

Основной файл: `config/default.yaml`

### Таймфреймы

```yaml
timeframes:
  1h:
    threshold_static: 0.008    # ±0.8% для t_static
    future_bars: 24            # горизонт /forecast (~24ч)
    embargo_bars: 1
  6h:
    threshold_static: 0.015    # ±1.5%
    future_bars: 28            # ~28 дней на 6H барах
    embargo_bars: 6
```

### Тюнинг

```yaml
tuning:
  budget: light    # light | standard | heavy (в models/tuning.py)
  n_wf_splits: 5
```

Бюджеты Optuna (`models/tuning.py`):

| Бюджет | Trials/study | Timeout |
|--------|--------------|---------|
| light | 40 | 8 мин |
| standard | 80 | 18 мин |
| heavy | 150 | 35 мин |

### Инференс

```yaml
inference:
  cache_ttl_sec: 900       # кэш /summary и /forecast в памяти бота
  gate_threshold: 0.5

training:
  eval_mode: static        # static | dynamic — режим tau в каскаде
```

## Развёртывание (production)

### systemd (пример)

```ini
[Unit]
Description=BTC Forecast Telegram Bot
After=network-online.target

[Service]
Type=simple
User=btc
WorkingDirectory=/opt/atm2
EnvironmentFile=/opt/atm2/.env
ExecStart=/opt/atm2/.venv/bin/btc-bot
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

Перед первым запуском сервиса:

```bash
btc-ingest && btc-train
```

### Cron (альтернатива встроенному планировщику)

Если бот запущен без планировщика или нужен отдельный retrain:

```cron
0 3 * * * cd /opt/atm2 && .venv/bin/btc-ingest && .venv/bin/btc-train >> /var/log/btc-train.log 2>&1
```

При использовании cron отключите дублирование — установите `RETRAIN_INTERVAL_SEC` очень большим или отключите job в `scheduler.py`.

## Логирование

CLI и бот пишут в stdout:

```
2026-06-08 12:00:00 [INFO] Scheduled retrain started
```

Для production перенаправьте вывод в journald или файл.

## Диагностика

| Симптом | Проверка |
|---------|----------|
| Бот не отвечает | `TELEGRAM_BOT_TOKEN`, сеть, процесс `btc-bot` |
| Всегда FLAT | Нормально при консервативной модели; проверьте `gate_threshold` |
| `/chart` не работает | Установлен `kaleido`; проверьте логи plotly |
| Drift-алерты часто | Увеличьте `drift.threshold` или `window_bars` |
| Обучение падает | Проверьте наличие всех 4 CSV в `artifacts/features/` |
| Сентимент пустой | Нормально без `CRYPTOPANIC_TOKEN`; CryptoCompare может быть пуст |

### Проверка артефактов

```bash
ls -la artifacts/models/cascade_bundle.pkl
ls artifacts/features/final_dataset_*.csv
.venv/bin/python -c "from btc_forecast.models.bundle import load_bundle; b=load_bundle(); print(b.meta)"
```

### Тесты

```bash
pytest -q
```

## Обновление кода


```bash
git pull
source .venv/bin/activate
pip install -e ".[dev]"
btc-train          # переобучение обязательно при смене фич
systemctl restart btc-forecast-bot
```

## Безопасность

- Не коммитьте `.env` в git (уже в `.gitignore`)
- `ADMIN_CHAT_IDS` — только доверенные user ID
- `CRYPTOPANIC_TOKEN` и `TELEGRAM_BOT_TOKEN` — секреты
- Бот не исполняет сделки и не хранит ключи биржи

## Дисклеймер

Система предназначена для аналитики и исследований. Сигналы UP/DOWN/FLAT **не являются** инвестиционной рекомендацией. Горизонтальный прогноз (`/forecast`) использует frozen-features и деградирует с расстоянием от текущего момента.
