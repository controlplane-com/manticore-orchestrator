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

### Delta Table Backup & Restore ###
The Orchestrator provides cloud storage backup and restore for delta tables:
- **Backup**: Export delta table data to S3 or GCS with timestamped filenames
- **Restore**: Select from available backups and restore data with a single click
- **UI Integration**: Manage backups and restores directly from the Dashboard

## Backup and Restore

### How It Works

**Backup Process:**
1. User triggers backup from the Dashboard UI or API
2. API spawns a Control Plane cron workload with `ACTION=backup`
3. Backup container connects to a Manticore replica via MySQL protocol
4. `mysqldump` exports the delta table (INSERT statements only, no DROP/CREATE)
5. SQL file is gzipped and uploaded to cloud storage
6. File naming: `{tableName}_delta-{timestamp}.sql.gz`

**Restore Process:**
1. User selects a table and backup file from the Dashboard UI
2. API spawns a cron workload with `ACTION=restore` and `RESTORE_FILE={filename}`
3. Backup container downloads the backup file from cloud storage
4. Existing delta table data is cleared (`DELETE FROM cluster:table WHERE id > 0`)
5. SQL file is modified to add cluster prefix to table names
6. Data is restored via MySQL protocol

### Configuration

The backup system requires these environment variables:

| Variable | Description | Example |
|----------|-------------|---------|
| `BACKUP_PROVIDER` | Cloud provider (`aws` or `gcp`) | `aws` |
| `BACKUP_BUCKET` | Storage bucket name | `my-backup-bucket` |
| `BACKUP_PREFIX` | Prefix/folder for backup files | `manticore-backups` |
| `BACKUP_REGION` | AWS region (if using S3) | `us-east-1` |
| `DATASET` | Table name (without `_delta` suffix) | `addresses_full` |
| `MANTICORE_HOST` | Manticore replica hostname | `manticore-0.manticore` |
| `MANTICORE_PORT` | MySQL protocol port | `9306` |
| `CLUSTER_NAME` | Manticore cluster name (default: `manticore`) | `manticore` |

### Clustered Table Considerations

ManticoreSearch clustered tables require special handling:

- **Cluster prefix required**: All DML operations must use `cluster:tablename` format (e.g., `manticore:addresses_full_delta`)
- **No DROP/CREATE**: Clustered tables cannot be dropped or recreated; backups contain only INSERT statements
- **No TRUNCATE**: Use `DELETE FROM table WHERE id > 0` to clear data
- **Single-node writes**: Write operations should target one replica; data replicates automatically

## API Reference

### Backup & Restore Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/backups` | Get active backup operations |
| GET | `/api/backups/files?tableName={name}` | List backup files for a table |
| POST | `/api/backup` | Trigger backup for a table's delta |
| POST | `/api/restore` | Restore a table from backup |

**Backup Request:**
```json
{
  "tableName": "addresses_full"
}
```

**Restore Request:**
```json
{
  "tableName": "addresses_full",
  "filename": "addresses_full_delta-2024-01-28T22-50-49Z.sql.gz"
}
```

## Supported Manticore Versions ##
The orchestrator has been tested with version `15.1.0`

## Ready To Deploy? ##
There is a helm template available [here](https://github.com/controlplane-com/templates/tree/main/manticore) which can be used to deploy this project to Control Plane. [Click here](https://docs.controlplane.com/guides/cpln-helm#manage-helm-releases) to learn about managing helm releases on Control Plane. 
