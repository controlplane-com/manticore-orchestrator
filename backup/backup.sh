#!/usr/bin/env bash
set -euo pipefail

if [ -z "${ACTION:-}" ]; then
  echo "[ERROR] ACTION env var is required"
  exit 1
fi

if [ "${ACTION}" = "backup" ]; then
  DELTA_TABLE="${DATASET}_delta"
  TIMESTAMP="$(date -u +"%Y-%m-%dT%H-%M-%SZ")"
  FILENAME="${DELTA_TABLE}-${TIMESTAMP}.sql.gz"

  echo "[INFO] Starting backup of ${DELTA_TABLE} (${TIMESTAMP})"

  mysqldump \
    --host="${MANTICORE_HOST}" \
    --port="${MANTICORE_PORT:-9306}" \
    Manticore "${DELTA_TABLE}" \
    > /tmp/delta.sql

  gzip /tmp/delta.sql

  if [ "${BACKUP_PROVIDER}" = "gcp" ]; then
    gsutil cp /tmp/delta.sql.gz \
      "gs://${BACKUP_BUCKET}/${BACKUP_PREFIX}/${FILENAME}"

  elif [ "${BACKUP_PROVIDER}" = "aws" ]; then
    aws s3 cp /tmp/delta.sql.gz \
      "s3://${BACKUP_BUCKET}/${BACKUP_PREFIX}/${FILENAME}"

  else
    echo "[ERROR] Unsupported BACKUP_PROVIDER: ${BACKUP_PROVIDER}"
    exit 1
  fi

  echo "[INFO] Backup completed: ${FILENAME}"

else
  echo "[ERROR] Unsupported ACTION: ${ACTION}"
  exit 1
fi
