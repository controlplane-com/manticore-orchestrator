# Control Plane Manticore Orchestrator
This project makes it dead simple to run and operate [Manticore Search](https://manticoresearch.com/) on [Control Plane](controlplane.com).

> [!IMPORTANT]
 This project assumes you are running Manticore Search on [Control Plane](controlplane.com). It will not work with any other hosting solution.

## Features
### Clustering Without the Hassle ###
The Orchestrator makes intelligent decisions about cluster formation based on the state of all nodes.
### Non-Invasive Architecture ###
The Orchestrator is designed to work with the community Manticore docker images. It operates Manticore much like a human would, primarily through the MySql interface.
### Zero-Downtime Data Imports ###
The Orchestrator uses Manticore's `indexer` tool to quickly ingest data from .tsv or .csv files. It uses a blue/green import pattern which has many beneficial properties:
1. While an import runs, the old data remains indexed and accessible. This is key for large imports which often take more than an hour.
2. Once the import has finished, the Orchestrator transitions between old and new data instantaneously, across all running nodes.
3. To save space, the old data is deleted in the background.
### First-Class Support For the Idiomatic Main+Delta Pattern ###
Define a table schema using JSON, and instantly get a distributed table that:
1. Has a "main" child table, intended to be changed only by the import process (although that's up to you).
2. Has a "delta" child table. The delta table is always part of the Manticore cluster, leveraging Manticore's native replication capability.
3. Load balances queries between all healthy nodes in the cluster
4. Optionally, includes the main table in the cluster. This is not recommended, but helps in cases where you need to make changes to the main table between imports.
### A Helpful UI ###
<img width="1908" height="774" alt="image" src="https://github.com/user-attachments/assets/dc2a5514-5d4e-4ec2-a253-d2a72b70ac13" />
Here you can
- View the health of the cluster
- See the status of the tables managed by the Orchestrator
- Execute queries against your cluster, either to a specific node, or broadcast to all nodes at once
- Start an import and track its progress
- Repair the cluster in the event of a split brain with a single click
- Backup delta tables to cloud storage (S3/GCS)
- Restore delta tables from backup with file selection
- View import, repair, backup, and restore operation history

### Physical Backup & Restore ###
The Orchestrator provides physical backup and restore for both delta and main tables using `manticore-backup`:
- **Backup**: Physical backup of table data to S3 or GCS as compressed tar.gz archives
- **Restore**: Download and restore from any backup, including blue-green slot rotation for main tables
- **Scheduled Backups**: Configure cron schedules per table/type directly on the API server
- **UI Integration**: Manage backups and restores directly from the Dashboard

## Backup and Restore

### How It Works

**Backup Process (delta or main):**
1. User triggers backup from the Dashboard UI, API, or via cron schedule
2. API spawns a Control Plane cron workload with `ACTION=backup`
3. Backup binary calls the Manticore agent to run `manticore-backup` for a physical backup
4. Agent creates a backup in a shared volume directory
5. Backup binary compresses the backup directory into a tar.gz archive and streams it to cloud storage (S3 or GCS)
6. File naming: `{tableName}-{timestamp}.tar.gz` (e.g., `addresses_delta-2025-01-28T22-50-49Z.tar.gz`)

**Restore Process (delta):**
1. User selects a table and backup file from the Dashboard UI or API
2. API spawns a cron workload with `ACTION=restore` and `RESTORE_FILE={filename}`
3. Backup binary downloads and extracts the tar.gz archive from cloud storage to a shared volume
4. Agent removes the table from the cluster, drops it, and uses `IMPORT TABLE` to restore from backup files
5. Agent re-adds the table to the cluster

**Restore Process (main - blue-green):**
1. Same as delta steps 1-4, except the restore targets the **inactive** slot (e.g., if slot `a` is active, restore goes to slot `b`)
2. If `BACKUP_SLOT` is set and the backup came from a different slot, the agent handles file name remapping automatically via `sourceTable`
3. After the agent restore completes, the backup binary calls the orchestrator's `/api/rotate-main` endpoint
4. The orchestrator atomically rotates the distributed table to point at the newly restored slot and drops the old slot

### Backup Scheduling

The orchestrator API server includes a built-in cron scheduler for automated backups. This is useful because each table may need multiple backup schedules (e.g., delta every 6 hours, main once daily), but CPLN cron workloads only support a single schedule.

#### Configuration

Set the `BACKUP_SCHEDULES` environment variable on the API server as a JSON array:

```json
[
  {"table": "addresses", "type": "delta", "schedule": "0 */6 * * *"},
  {"table": "addresses", "type": "main", "schedule": "0 2 * * *"}
]
```

Each entry has:

| Field | Description | Example |
|-------|-------------|---------|
| `table` | Table base name (without `_delta`/`_main_` suffix) | `addresses` |
| `type` | Backup type: `delta` or `main` | `delta` |
| `schedule` | Standard 5-field cron expression | `0 */6 * * *` |

The scheduler uses standard 5-field cron format: `minute hour day-of-month month day-of-week`.

**Common schedule examples:**
- `0 */6 * * *` - Every 6 hours
- `0 2 * * *` - Daily at 2:00 AM
- `0 0 * * 0` - Weekly on Sunday at midnight
- `*/30 * * * *` - Every 30 minutes

#### Behavior

- If `BACKUP_SCHEDULES` is empty or unset, no scheduler is created (zero overhead)
- Invalid cron expressions are logged and skipped; other schedules still work
- The scheduler checks for active backup conflicts before triggering (if a backup is already running for the same table, it skips)
- On `SIGTERM`, the scheduler stops cleanly before the API server shuts down
- Manual backups via `POST /api/backup` continue to work alongside scheduled backups

### Backup Container Environment Variables

The backup binary (runs as a CPLN cron workload) reads these environment variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ACTION` | Yes | | `backup` or `restore` |
| `AUTH_TOKEN` | Yes | | Bearer token for agent API authentication |
| `DATASET` | Yes | | Table base name (e.g., `addresses`) |
| `TYPE` | No | `delta` | `delta` or `main` |
| `SLOT` | When TYPE=main | | Active slot: `a` or `b` |
| `MANTICORE_HOST` | Yes | | Agent hostname |
| `AGENT_PORT` | No | `8080` | Agent API port |
| `SHARED_VOLUME_MOUNT` | No | `/mnt/shared` | Shared volume mount path |
| `BACKUP_PROVIDER` | Yes | | `aws` or `gcp` |
| `BACKUP_BUCKET` | Yes | | Cloud storage bucket name |
| `BACKUP_PREFIX` | Yes | | Path prefix within bucket |
| `BACKUP_REGION` | No | | AWS region (required for S3) |
| `RESTORE_FILE` | For restore | | Backup filename to restore from |
| `BACKUP_SLOT` | For blue-green | | Slot the backup was taken from |
| `ORCHESTRATOR_API_URL` | For blue-green | | Orchestrator URL for rotation call |

### API Server Backup Environment Variables

The orchestrator API server needs these for backup operations:

| Variable | Description | Example |
|----------|-------------|---------|
| `BACKUP_PROVIDER` | Cloud provider (`aws` or `gcp`) | `aws` |
| `BACKUP_BUCKET` | Storage bucket name | `my-backup-bucket` |
| `BACKUP_PREFIX` | Prefix/folder for backup files | `manticore-backups` |
| `BACKUP_REGION` | AWS region (if using S3) | `us-east-1` |
| `BACKUP_WORKLOAD` | Name of the CPLN cron workload for backups | `manticore-backup` |
| `BACKUP_SCHEDULES` | JSON array of scheduled backups (optional) | See above |

## API Reference

### Backup & Restore Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/backups` | Get active backup/restore operations |
| GET | `/api/backups/files?tableName={name}` | List backup files for a table in cloud storage |
| POST | `/api/backup` | Trigger a backup for a table |
| POST | `/api/restore` | Restore a table from a backup file |
| POST | `/api/rotate-main` | Rotate the distributed table to a new main slot (called by backup binary) |

**Backup Request:**
```json
{
  "tableName": "addresses",
  "type": "delta"
}
```

**Restore Request:**
```json
{
  "tableName": "addresses",
  "filename": "addresses_delta-2025-01-28T22-50-49Z.tar.gz"
}
```

**Rotate Main Request** (typically called by the backup binary, not manually):
```json
{
  "tableName": "addresses",
  "newSlot": "b",
  "oldSlot": "a"
}
```

## Supported Manticore Versions ##
The orchestrator has been tested with version `15.1.0`

## Ready To Deploy? ##
There is a helm template available [here](https://github.com/controlplane-com/templates/tree/main/manticore) which can be used to deploy this project to Control Plane. [Click here](https://docs.controlplane.com/guides/cpln-helm#manage-helm-releases) to learn about managing helm releases on Control Plane. 
