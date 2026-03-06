#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${ROOT_DIR}/docker/initdb"
OUTPUT_FILE="${OUTPUT_DIR}/01-local-seed.sql.gz"
DEFAULT_ENV_FILE="${ROOT_DIR}/.env"
ENV_FILE="${ENV_FILE:-$DEFAULT_ENV_FILE}"

if ! command -v pg_dump >/dev/null 2>&1; then
  echo "error: pg_dump не найден. Установите PostgreSQL client tools." >&2
  exit 1
fi

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$ENV_FILE"
  set +a
fi

LOCAL_PGHOST="${LOCAL_PGHOST:-${PGHOST:-localhost}}"
LOCAL_PGPORT="${LOCAL_PGPORT:-${PGPORT:-5432}}"
LOCAL_PGUSER="${LOCAL_PGUSER:-${PGUSER:-}}"
LOCAL_PGPASSWORD="${LOCAL_PGPASSWORD:-${PGPASSWORD:-}}"
LOCAL_PGDATABASE="${LOCAL_PGDATABASE:-${PGDATABASE:-}}"

if [[ -z "$LOCAL_PGUSER" ]]; then
  echo "error: не задан пользователь БД. Укажите LOCAL_PGUSER или PGUSER." >&2
  exit 1
fi

if [[ -z "$LOCAL_PGDATABASE" ]]; then
  echo "error: не задана БД. Укажите LOCAL_PGDATABASE или PGDATABASE." >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

echo "Экспорт из PostgreSQL ${LOCAL_PGHOST}:${LOCAL_PGPORT}/${LOCAL_PGDATABASE} ..."

export PGPASSWORD="$LOCAL_PGPASSWORD"

pg_dump \
  --host="$LOCAL_PGHOST" \
  --port="$LOCAL_PGPORT" \
  --username="$LOCAL_PGUSER" \
  --dbname="$LOCAL_PGDATABASE" \
  --clean \
  --if-exists \
  --no-owner \
  --no-privileges \
  --inserts \
  | gzip -9 >"$OUTPUT_FILE"

gzip -t "$OUTPUT_FILE"
echo "OK: создан $OUTPUT_FILE"
