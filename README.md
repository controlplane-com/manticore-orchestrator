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

When `importMethod: indexer` is used, the Orchestrator preprocesses source CSV/TSV files in Go before passing them to the `indexer` tool. This step normalizes delimiters, handles boolean values, skips blank lines, and accommodates very long rows — improving compatibility with a wide range of input formats without relying on external shell utilities.
### First-Class Support For the Idiomatic Main+Delta Pattern ###
Define a table schema using JSON, and instantly get a distributed table that:
1. Has a "main" child table, intended to be changed only by the import process (although that's up to you).
2. Has a "delta" child table. The delta table is always part of the Manticore cluster, leveraging Manticore's native replication capability.
3. Load balances queries between all healthy nodes in the cluster
4. Optionally, includes the main table in the cluster. This is not recommended, but helps in cases where you need to make changes to the main table between imports.

### Multi-Segment Sharding ###
For very large datasets that benefit from being split across multiple index shards, the Orchestrator supports a `segmentCount` option. Each segment maintains its own main+delta pair, and the distributed table fans queries out across all of them automatically.

**Configure `segmentCount` in your schema config:**
```yaml
addresses:
  schema:
    columns: [...]
  config:
    segmentCount: 2   # default: 1 (original behavior, no naming change)
    importMethod: bulk
```

With `segmentCount: 2`, the Orchestrator manages:
- `addresses_main_a_1`, `addresses_main_a_2` — active-slot main tables (one per segment)
- `addresses_delta_1`, `addresses_delta_2` — replicated delta tables (one per segment)
- `addresses` — distributed table spanning all segments and all replicas

Setting `segmentCount: 1` (the default) preserves the original naming (`addresses_main_a`, `addresses_delta`) with no migration required.

**Multi-segment `TABLES_CONFIG`:**

The `TABLES_CONFIG` environment variable accepts either a single CSV path or an array of paths per table. Both formats are backward-compatible:
```json
{"addresses": "addresses.csv"}
{"addresses": ["addresses_1.csv", "addresses_2.csv"]}
```
When an array is provided, each path maps to the corresponding segment (path 1 → segment 1, path 2 → segment 2, etc.).
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
- **Multi-segment aware**: For tables with `segmentCount > 1`, backup creates a single archive containing all segment data. Restore automatically loops over each segment, restoring all from the same archive in sequence.

## Backup and Restore

The Orchestrator supports physical backup and restore operations for both delta and main tables. Backups are stored as compressed archives in cloud storage (S3 or GCS). Restore operations are supported for both table types, with automatic blue-green slot rotation for main tables.

Backups and restores can be triggered from the Dashboard UI or the API.

### Multi-Segment Backup & Restore

When a table is configured with `segmentCount > 1`, backup and restore operations handle all segments automatically:

- **Backup**: All segment main tables (`addresses_main_b_1`, `addresses_main_b_2`, …) are included in a single `manticore-backup` call and stored in one `.tar.gz` archive. Only the active slot is backed up.
- **Restore**: The archive is downloaded once. The restore process then iterates over each segment sequentially, calling the agent's `IMPORT TABLE` for each (`addresses_main_a_1`, `addresses_main_a_2`, …). After all segments are restored, the distributed table is rotated to the new slot in a single operation.
- **Rotation retry**: If the Control Plane API is transiently unavailable during the blue-green slot rotation at the end of a restore, the Orchestrator retries the rotation up to 5 times with increasing backoff (10 s, 20 s, …). If all retries fail, a warning is logged with instructions for manual rotation.

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
