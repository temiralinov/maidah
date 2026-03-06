# Init seeds for PostgreSQL container

Папка монтируется в `/docker-entrypoint-initdb.d` для сервиса `db` в `docker-compose.yml`.

Как это работает:

1. Если volume `pgdata` пустой (первый запуск), Postgres выполнит все `*.sql` / `*.sql.gz` из этой папки.
2. Если volume уже существует, init-скрипты повторно не запускаются.

Рекомендуемый путь:

1. Сгенерировать дамп из локальной PostgreSQL:
   `./scripts/export_local_pg_dump.sh`
2. Получится файл: `docker/initdb/01-local-seed.sql.gz`
3. Запустить compose:
   `docker compose --env-file .env.docker up -d --build`
