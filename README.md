# Статистика ресторана

Продуктовая страница статистики на Node.js + TypeScript + Express.

## Что умеет

- Выбор ресторана (если доступно политикой env)
- Периоды: `все время`, `месяц`, `день`
- Интерактивный архивный график: выручка и продажи с drilldown по клику (месяц → день → часы)
- Сводка: заказы, выручка, средний чек, оплаты, маржа
- Статусы, топ товаров, последние заказы

## Запуск

```bash
npm install
cp .env.example .env
npm run dev
```

Откройте: `http://localhost:3000`

## Docker Compose (сервер)

Цель: на сервере не поднимать PostgreSQL отдельно, а запускать `app + db` вместе, при этом `db` стартует уже с вашими локальными данными.

### 1) Подготовка локального SQL-дампа

```bash
cp .env.docker.example .env.docker
./scripts/export_local_pg_dump.sh
```

После выполнения появится файл:
`docker/initdb/01-local-seed.sql.gz`

### 2) Запуск на сервере

```bash
docker compose --env-file .env.docker up -d --build
```

Откройте: `http://<server-host>:3000`

### 3) Важный момент про повторный импорт

Init-скрипты из `docker/initdb` выполняются только при первом создании volume `pgdata`.
Если нужно перезалить БД из нового дампа, удалите volume:

```bash
docker compose --env-file .env.docker down -v
docker compose --env-file .env.docker up -d --build
```

## Фоновый бот догрузки заказов

Бот продолжает опрашивать API `http://api.maidah.kz/orders/{id}` и обновляет заказы до финального статуса.

Ключевые свойства:

- Не доверяет HTTP status: если в теле есть валидный заказ с нужным `id`, запись сохраняется.
- Пишет данные только через `UPSERT` в транзакции (`orders`, `order_products`, `order_transactions`).
- Не удаляет существующие записи (безопасно для накопленных данных).
- Работает медленно с паузами и джиттером, чтобы снизить риск блокировки по IP.
- Ошибки сохраняет в `fetch_errors`, успешные повторы очищают ошибку по `id`.
- Каждый цикл обязательно проверяет «живое окно» последних заказов + несколько новых `id` вперед (`BOT_LIVE_TAIL_WINDOW`, `BOT_NEW_IDS_PER_CYCLE`).

Запуск:

```bash
npm run bot:archive
```

Полезные режимы:

- `BOT_DRY_RUN=true` — только проверка API без записи в БД.
- `BOT_RUN_ONCE=true` — один цикл без бесконечного режима.

## Настройка доступа к ресторанам

- `RESTAURANT_VISIBILITY=all` — показывать все рестораны
- `RESTAURANT_VISIBILITY=company` — показывать только рестораны `DEFAULT_COMPANY_ID`
- `RESTAURANT_VISIBILITY=store` — показывать только `DEFAULT_STORE_ID`

Текущее значение по умолчанию: `company` + `sheker-coffee`.

## Политика аналитики

- `INCLUDED_ORDER_STATUSES=archived` — в расчеты попадают только архивные заказы
- `ANALYTICS_TIME_ZONE=Asia/Almaty` — группировка дат/часов по UTC+5
