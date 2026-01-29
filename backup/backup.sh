#!/usr/bin/env bash
set -euo pipefail

# Defaults
AGENT_PORT="${AGENT_PORT:-8080}"
SHARED_VOLUME_MOUNT="${SHARED_VOLUME_MOUNT:-/mnt/shared}"
TYPE="${TYPE:-delta}"

if [ -z "${ACTION:-}" ]; then
  echo "[ERROR] ACTION env var is required"
  exit 1
fi

if [ -z "${AUTH_TOKEN:-}" ]; then
  echo "[ERROR] AUTH_TOKEN env var is required"
  exit 1
fi

# Derive table name from TYPE
if [ "${TYPE}" = "delta" ]; then
  TABLE_NAME="${DATASET}_delta"
elif [ "${TYPE}" = "main" ]; then
  if [ -z "${SLOT:-}" ]; then
    echo "[ERROR] SLOT env var is required when TYPE=main"
    exit 1
  fi
  TABLE_NAME="${DATASET}_main_${SLOT}"
else
  echo "[ERROR] Unsupported TYPE: ${TYPE} (expected 'delta' or 'main')"
  exit 1
fi

AGENT_URL="http://${MANTICORE_HOST}:${AGENT_PORT}"

# poll_agent_job polls an agent job endpoint until completion
# Usage: poll_agent_job <endpoint> <jobId>
# Returns 0 on success, 1 on failure
poll_agent_job() {
  local endpoint="$1"
  local job_id="$2"
  local poll_interval=5

  while true; do
    local response
    response=$(curl -sf \
      -H "Authorization: Bearer ${AUTH_TOKEN}" \
      "${AGENT_URL}/api/${endpoint}/${job_id}" 2>/dev/null) || {
      echo "[WARN] Failed to poll job status, retrying in ${poll_interval}s..."
      sleep "${poll_interval}"
      continue
    }

    local status
    status=$(echo "${response}" | jq -r '.job.status')

    case "${status}" in
      completed)
        echo "[INFO] Job ${job_id} completed successfully"
        return 0
        ;;
      failed)
        local error
        error=$(echo "${response}" | jq -r '.job.error // "unknown error"')
        echo "[ERROR] Job ${job_id} failed: ${error}"
        return 1
        ;;
      pending|running)
        echo "[INFO] Job ${job_id} status: ${status}"
        sleep "${poll_interval}"
        ;;
      *)
        echo "[WARN] Unknown job status: ${status}, retrying..."
        sleep "${poll_interval}"
        ;;
    esac
  done
}

if [ "${ACTION}" = "backup" ]; then
  TIMESTAMP="$(date -u +"%Y-%m-%dT%H-%M-%SZ")"
  FILENAME="${TABLE_NAME}-${TIMESTAMP}.tar.gz"
  BACKUP_DIR="${SHARED_VOLUME_MOUNT}/backups/${DATASET}/${TIMESTAMP}"

  echo "[INFO] Starting physical backup of ${TABLE_NAME} (${TIMESTAMP})"
  echo "[INFO] Backup directory: ${BACKUP_DIR}"

  # Step 1: Call agent to run manticore-backup
  echo "[INFO] Requesting agent to run manticore-backup..."
  RESPONSE=$(curl -sf -X POST \
    -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"table\":\"${TABLE_NAME}\",\"backupDir\":\"${BACKUP_DIR}\"}" \
    "${AGENT_URL}/api/backup") || {
    echo "[ERROR] Failed to start backup on agent"
    exit 1
  }

  JOB_ID=$(echo "${RESPONSE}" | jq -r '.jobId')
  if [ -z "${JOB_ID}" ] || [ "${JOB_ID}" = "null" ]; then
    echo "[ERROR] Failed to get job ID from agent response: ${RESPONSE}"
    exit 1
  fi

  echo "[INFO] Backup job started: ${JOB_ID}"

  # Step 2: Poll agent until backup completes
  if ! poll_agent_job "backup" "${JOB_ID}"; then
    echo "[ERROR] Backup failed"
    rm -rf "${BACKUP_DIR}" 2>/dev/null || true
    exit 1
  fi

  # Step 3: Create tar.gz from backup directory
  echo "[INFO] Creating archive: ${FILENAME}"
  tar -czf "/tmp/${FILENAME}" -C "${BACKUP_DIR}" .

  # Step 4: Upload to cloud storage
  if [ "${BACKUP_PROVIDER}" = "gcp" ]; then
    gsutil cp "/tmp/${FILENAME}" \
      "gs://${BACKUP_BUCKET}/${BACKUP_PREFIX}/${FILENAME}"

  elif [ "${BACKUP_PROVIDER}" = "aws" ]; then
    aws s3 cp "/tmp/${FILENAME}" \
      "s3://${BACKUP_BUCKET}/${BACKUP_PREFIX}/${FILENAME}"

  else
    echo "[ERROR] Unsupported BACKUP_PROVIDER: ${BACKUP_PROVIDER}"
    rm -rf "${BACKUP_DIR}" 2>/dev/null || true
    exit 1
  fi

  # Step 5: Cleanup
  echo "[INFO] Cleaning up backup directory..."
  rm -rf "${BACKUP_DIR}"
  rm -f "/tmp/${FILENAME}"

  echo "[INFO] Backup completed: ${FILENAME}"

elif [ "${ACTION}" = "restore" ]; then
  if [ -z "${RESTORE_FILE:-}" ]; then
    echo "[ERROR] RESTORE_FILE env var is required for restore action"
    exit 1
  fi

  TIMESTAMP="$(date -u +"%Y-%m-%dT%H-%M-%SZ")"
  RESTORE_DIR="${SHARED_VOLUME_MOUNT}/backups/${DATASET}/restore-${TIMESTAMP}"

  echo "[INFO] Starting physical restore of ${TABLE_NAME} from ${RESTORE_FILE}"

  # Step 1: Download backup file from cloud storage
  if [ "${BACKUP_PROVIDER}" = "gcp" ]; then
    gsutil cp "gs://${BACKUP_BUCKET}/${BACKUP_PREFIX}/${RESTORE_FILE}" /tmp/restore.tar.gz

  elif [ "${BACKUP_PROVIDER}" = "aws" ]; then
    aws s3 cp "s3://${BACKUP_BUCKET}/${BACKUP_PREFIX}/${RESTORE_FILE}" /tmp/restore.tar.gz

  else
    echo "[ERROR] Unsupported BACKUP_PROVIDER: ${BACKUP_PROVIDER}"
    exit 1
  fi

  # Step 2: Extract to shared volume
  echo "[INFO] Extracting backup to ${RESTORE_DIR}"
  mkdir -p "${RESTORE_DIR}"
  tar -xzf /tmp/restore.tar.gz -C "${RESTORE_DIR}"

  # Step 3: Call agent to restore (handles cluster ops + manticore-backup --restore)
  echo "[INFO] Requesting agent to restore from backup..."
  RESPONSE=$(curl -sf -X POST \
    -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"table\":\"${TABLE_NAME}\",\"backupDir\":\"${RESTORE_DIR}\"}" \
    "${AGENT_URL}/api/restore") || {
    echo "[ERROR] Failed to start restore on agent"
    rm -rf "${RESTORE_DIR}" 2>/dev/null || true
    exit 1
  }

  JOB_ID=$(echo "${RESPONSE}" | jq -r '.jobId')
  if [ -z "${JOB_ID}" ] || [ "${JOB_ID}" = "null" ]; then
    echo "[ERROR] Failed to get job ID from agent response: ${RESPONSE}"
    rm -rf "${RESTORE_DIR}" 2>/dev/null || true
    exit 1
  fi

  echo "[INFO] Restore job started: ${JOB_ID}"

  # Step 4: Poll agent until restore completes
  if ! poll_agent_job "restore" "${JOB_ID}"; then
    echo "[ERROR] Restore failed"
    rm -rf "${RESTORE_DIR}" 2>/dev/null || true
    exit 1
  fi

  # Step 5: Cleanup
  echo "[INFO] Cleaning up restore directory..."
  rm -rf "${RESTORE_DIR}"
  rm -f /tmp/restore.tar.gz

  echo "[INFO] Restore completed: ${RESTORE_FILE} -> ${TABLE_NAME}"

else
  echo "[ERROR] Unsupported ACTION: ${ACTION}"
  exit 1
fi
