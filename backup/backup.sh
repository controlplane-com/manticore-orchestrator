#!/usr/bin/env bash
set -euo pipefail

TIMESTAMP="$(date -u +"%Y-%m-%dT%H-%M-%SZ")"
FILENAME="manticore-${TIMESTAMP}.sql.gz"

echo "[INFO] Starting ManticoreSearch backup (${TIMESTAMP})"

mysqldump \
  --host="${MANTICORE_HOST}" \
  --port="${MANTICORE_PORT:-9306}" \
  > /tmp/manticore.sql

gzip /tmp/manticore.sql

if [ "${BACKUP_PROVIDER}" = "gcp" ]; then
  gsutil cp /tmp/manticore.sql.gz \
    "gs://${BACKUP_BUCKET}/${BACKUP_PREFIX}/${FILENAME}"

elif [ "${BACKUP_PROVIDER}" = "aws" ]; then
  aws s3 cp /tmp/manticore.sql.gz \
    "s3://${BACKUP_BUCKET}/${BACKUP_PREFIX}/${FILENAME}"

else
  echo "[ERROR] Unsupported BACKUP_PROVIDER: ${BACKUP_PROVIDER}"
  exit 1
fi

echo "[INFO] Backup completed: ${FILENAME}"
