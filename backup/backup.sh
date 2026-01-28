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

  # Use --no-create-info to skip CREATE TABLE (can't drop/recreate clustered tables)
  # Use --skip-add-drop-table to skip DROP TABLE IF EXISTS
  mysqldump \
    --host="${MANTICORE_HOST}" \
    --port="${MANTICORE_PORT:-9306}" \
    --skip-lock-tables \
    --no-create-info \
    --skip-add-drop-table \
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

elif [ "${ACTION}" = "restore" ]; then
  if [ -z "${RESTORE_FILE:-}" ]; then
    echo "[ERROR] RESTORE_FILE env var is required for restore action"
    exit 1
  fi

  DELTA_TABLE="${DATASET}_delta"

  echo "[INFO] Starting restore of ${DELTA_TABLE} from ${RESTORE_FILE}"

  # Download backup file from cloud storage
  if [ "${BACKUP_PROVIDER}" = "gcp" ]; then
    gsutil cp "gs://${BACKUP_BUCKET}/${BACKUP_PREFIX}/${RESTORE_FILE}" /tmp/restore.sql.gz

  elif [ "${BACKUP_PROVIDER}" = "aws" ]; then
    aws s3 cp "s3://${BACKUP_BUCKET}/${BACKUP_PREFIX}/${RESTORE_FILE}" /tmp/restore.sql.gz

  else
    echo "[ERROR] Unsupported BACKUP_PROVIDER: ${BACKUP_PROVIDER}"
    exit 1
  fi

  # Decompress backup
  gunzip /tmp/restore.sql.gz

  # Clustered tables require cluster:table format
  CLUSTER_NAME="${CLUSTER_NAME:-manticore}"
  CLUSTER_TABLE="${CLUSTER_NAME}:${DELTA_TABLE}"

  # Clear existing data from the delta table first
  # Note: TRUNCATE doesn't work on clustered tables, so we use DELETE
  echo "[INFO] Clearing existing data from ${CLUSTER_TABLE}"
  mysql \
    --host="${MANTICORE_HOST}" \
    --port="${MANTICORE_PORT:-9306}" \
    -e "DELETE FROM ${CLUSTER_TABLE} WHERE id > 0"

  # Replace table name with cluster-prefixed version in the SQL file
  # mysqldump outputs: INSERT INTO `tablename` ...
  # We need: INSERT INTO `cluster:tablename` ...
  echo "[INFO] Adding cluster prefix to SQL statements"
  sed -i "s/INSERT INTO \`${DELTA_TABLE}\`/INSERT INTO \`${CLUSTER_TABLE}\`/g" /tmp/restore.sql

  # Restore to Manticore delta table
  # The backup contains only INSERT statements (no DROP/CREATE)
  mysql \
    --host="${MANTICORE_HOST}" \
    --port="${MANTICORE_PORT:-9306}" \
    < /tmp/restore.sql

  echo "[INFO] Restore completed: ${RESTORE_FILE} -> ${DELTA_TABLE}"

else
  echo "[ERROR] Unsupported ACTION: ${ACTION}"
  exit 1
fi
