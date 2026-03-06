# Init seeds for PostgreSQL container

Папка копируется в `/docker-entrypoint-initdb.d` при сборке `db`-образа (`docker/postgres/Dockerfile`).

Как это работает:

1. Если volume `pgdata` пустой (первый запуск), Postgres выполнит все `*.sql` / `*.sql.gz` из этой папки.
2. Если volume уже существует, init-скрипты повторно не запускаются.

Рекомендуемый путь:

1. Сгенерировать дамп из локальной PostgreSQL:
   `./scripts/export_local_pg_dump.sh`
2. Получится файл: `docker/initdb/01-local-seed.sql.gz`
3. Закоммитить и запушить обновленный сид.
4. Запустить compose:
   `docker compose --env-file .env.docker up -d --build`
