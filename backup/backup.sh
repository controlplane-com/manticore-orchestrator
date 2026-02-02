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
      --connect-timeout 5 \
      --max-time 30 \
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

  # Create backup directory on shared volume (manticore-backup expects it to exist)
  mkdir -p "${BACKUP_DIR}"

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

  # Step 2: Poll agent until backup completes (initial delay to let job start)
  sleep 15
  if ! poll_agent_job "backup" "${JOB_ID}"; then
    echo "[ERROR] Backup failed"
    rm -rf "${BACKUP_DIR}" 2>/dev/null || true
    exit 1
  fi

  # Step 3: Compress and stream directly to cloud storage (no intermediate file)
  echo "[INFO] Compressing and uploading backup: ${FILENAME}"
  if [ "${BACKUP_PROVIDER}" = "gcp" ]; then
    tar -czf - -C "${BACKUP_DIR}" . \
      | gsutil cp - "gs://${BACKUP_BUCKET}/${BACKUP_PREFIX}/${FILENAME}"

  elif [ "${BACKUP_PROVIDER}" = "aws" ]; then
    tar -czf - -C "${BACKUP_DIR}" . \
      | aws s3 cp - "s3://${BACKUP_BUCKET}/${BACKUP_PREFIX}/${FILENAME}"

  else
    echo "[ERROR] Unsupported BACKUP_PROVIDER: ${BACKUP_PROVIDER}"
    rm -rf "${BACKUP_DIR}" 2>/dev/null || true
    exit 1
  fi

  # Step 4: Cleanup
  echo "[INFO] Upload complete, cleaning up backup directory..."
  rm -rf "${BACKUP_DIR}"

  echo "[INFO] Backup completed: ${FILENAME}"

elif [ "${ACTION}" = "restore" ]; then
  if [ -z "${RESTORE_FILE:-}" ]; then
    echo "[ERROR] RESTORE_FILE env var is required for restore action"
    exit 1
  fi

  # For blue-green main restore: BACKUP_SLOT is the slot the backup was taken from
  # TABLE_NAME is already set to the target (inactive) slot via SLOT env var
  SOURCE_TABLE=""
  if [ -n "${BACKUP_SLOT:-}" ]; then
    SOURCE_TABLE="${DATASET}_main_${BACKUP_SLOT}"
    echo "[INFO] Blue-green restore: backup from ${SOURCE_TABLE}, restoring to ${TABLE_NAME}"
  fi

  TIMESTAMP="$(date -u +"%Y-%m-%dT%H-%M-%SZ")"
  RESTORE_DIR="${SHARED_VOLUME_MOUNT}/backups/${DATASET}/restore-${TIMESTAMP}"

  echo "[INFO] Starting physical restore of ${TABLE_NAME} from ${RESTORE_FILE}"

  # Step 1: Download and extract backup directly to shared volume (no intermediate file)
  echo "[INFO] Downloading and extracting backup to ${RESTORE_DIR}"
  mkdir -p "${RESTORE_DIR}"
  if [ "${BACKUP_PROVIDER}" = "gcp" ]; then
    gsutil cp "gs://${BACKUP_BUCKET}/${BACKUP_PREFIX}/${RESTORE_FILE}" - \
      | tar -xzf - -C "${RESTORE_DIR}"

  elif [ "${BACKUP_PROVIDER}" = "aws" ]; then
    aws s3 cp "s3://${BACKUP_BUCKET}/${BACKUP_PREFIX}/${RESTORE_FILE}" - \
      | tar -xzf - -C "${RESTORE_DIR}"

  else
    echo "[ERROR] Unsupported BACKUP_PROVIDER: ${BACKUP_PROVIDER}"
    exit 1
  fi

  # Step 2: Call agent to restore (handles cluster ops + IMPORT TABLE)
  # Include sourceTable when restoring to a different slot (blue-green)
  RESTORE_PAYLOAD="{\"table\":\"${TABLE_NAME}\",\"backupDir\":\"${RESTORE_DIR}\"}"
  if [ -n "${SOURCE_TABLE}" ]; then
    RESTORE_PAYLOAD="{\"table\":\"${TABLE_NAME}\",\"backupDir\":\"${RESTORE_DIR}\",\"sourceTable\":\"${SOURCE_TABLE}\"}"
  fi

  echo "[INFO] Requesting agent to restore from backup..."
  RESPONSE=$(curl -sf -X POST \
    -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "${RESTORE_PAYLOAD}" \
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

  # Step 3: Poll agent until restore completes (initial delay to let job start)
  sleep 15
  if ! poll_agent_job "restore" "${JOB_ID}"; then
    echo "[ERROR] Restore failed"
    rm -rf "${RESTORE_DIR}" 2>/dev/null || true
    exit 1
  fi

  # Step 4: Blue-green rotation (main tables only)
  # Call orchestrator API to rotate distributed table and clean up old slot
  if [ -n "${SOURCE_TABLE}" ] && [ -n "${ORCHESTRATOR_API_URL:-}" ]; then
    OLD_SLOT="${BACKUP_SLOT}"
    NEW_SLOT="${SLOT}"
    echo "[INFO] Calling orchestrator to rotate main table: ${DATASET} from slot ${OLD_SLOT} to ${NEW_SLOT}"

    ROTATE_TMPFILE=$(mktemp)
    ROTATE_HTTP_CODE=$(curl -s -o "${ROTATE_TMPFILE}" -w "%{http_code}" -X POST \
      --connect-timeout 10 \
      --max-time 120 \
      -H "Authorization: Bearer ${AUTH_TOKEN}" \
      -H "Content-Type: application/json" \
      -d "{\"tableName\":\"${DATASET}\",\"newSlot\":\"${NEW_SLOT}\",\"oldSlot\":\"${OLD_SLOT}\"}" \
      "${ORCHESTRATOR_API_URL}/api/rotate-main" 2>&1) || {
      echo "[ERROR] Failed to connect to orchestrator API at ${ORCHESTRATOR_API_URL}/api/rotate-main (curl exit code: $?)"
      cat "${ROTATE_TMPFILE}" 2>/dev/null
      rm -f "${ROTATE_TMPFILE}"
      echo "[WARN] Restore data is in ${TABLE_NAME} but distributed table has NOT been rotated"
      echo "[WARN] Manual rotation required: POST ${ORCHESTRATOR_API_URL}/api/rotate-main with {\"tableName\":\"${DATASET}\",\"newSlot\":\"${NEW_SLOT}\",\"oldSlot\":\"${OLD_SLOT}\"}"
      rm -rf "${RESTORE_DIR}" 2>/dev/null || true
      exit 1
    }
    ROTATE_RESPONSE=$(cat "${ROTATE_TMPFILE}")
    rm -f "${ROTATE_TMPFILE}"

    if [ "${ROTATE_HTTP_CODE}" -lt 200 ] || [ "${ROTATE_HTTP_CODE}" -ge 300 ]; then
      echo "[ERROR] Orchestrator rotation failed with HTTP ${ROTATE_HTTP_CODE}: ${ROTATE_RESPONSE}"
      echo "[WARN] Restore data is in ${TABLE_NAME} but distributed table has NOT been rotated"
      echo "[WARN] Manual rotation required: POST ${ORCHESTRATOR_API_URL}/api/rotate-main with {\"tableName\":\"${DATASET}\",\"newSlot\":\"${NEW_SLOT}\",\"oldSlot\":\"${OLD_SLOT}\"}"
      rm -rf "${RESTORE_DIR}" 2>/dev/null || true
      exit 1
    fi

    echo "[INFO] Rotation response: ${ROTATE_RESPONSE}"
  fi

  # Step 5: Cleanup
  echo "[INFO] Cleaning up restore directory..."
  rm -rf "${RESTORE_DIR}"

  echo "[INFO] Restore completed: ${RESTORE_FILE} -> ${TABLE_NAME}"

else
  echo "[ERROR] Unsupported ACTION: ${ACTION}"
  exit 1
fi
