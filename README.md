# Control Plane Manticore Orchestrator
This project makes it dead simple to run and operate [Manticore Search](https://manticoresearch.com/) on [Control Plane](controlplane.com).

> [IMPORTANT]
 This project assumes you are running Manticore Search on [Control Plane](controlplane.com). It will not work with any other hosting solution.

## Features
### Integrated Clustering ###
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
Here you can:

- View the health of the cluster
- See the status of the tables managed by the Orchestrator
- Execute queries against your cluster, either to a specific node, or broadcast to all nodes at once
- Start an import and track its progress
- Repair the cluster in the event of a split brain with a single click
- Backup tables to cloud storage (S3/GCS)
- Restore tables from backup with file selection
- View import, repair, backup, and restore operation history

### Physical Backup & Restore ###
The Orchestrator provides physical backup and restore for both delta and main tables using `manticore-backup`:
- **Backup**: Physical backup of table data to S3 or GCS as compressed tar.gz archives
- **Restore**: Download and restore from any backup, including blue-green slot rotation for main tables
- **Scheduled Backups**: Configure cron schedules per table/type directly on the API server
- **UI Integration**: Manage backups and restores directly from the Dashboard

## Backup and Restore

The Orchestrator supports physical backup and restore operations for both delta and main tables. Backups are stored as compressed archives in cloud storage (S3 or GCS). Restore operations are supported for both table types, with automatic blue-green slot rotation for main tables.

Backups and restores can be triggered from the Dashboard UI or the API.

### Backup Scheduling

The API server includes a built-in cron scheduler for automated backups. Configure it by setting the `BACKUP_SCHEDULES` environment variable on the API server as a JSON array:

```json
[
  {"table": "addresses", "type": "delta", "schedule": "0 */6 * * *"},
  {"table": "addresses", "type": "main", "schedule": "0 2 * * *"}
]
```

Each entry specifies a `table` name (without `_delta`/`_main_` suffix), a `type` (`delta` or `main`), and a `schedule` in [standard cron format](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#schedule-syntax). If `BACKUP_SCHEDULES` is empty or unset, no scheduler is created.

### API Server Environment Variables

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
