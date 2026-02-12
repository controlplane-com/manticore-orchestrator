// API response types for the Manticore orchestrator

export interface ApiResponse {
  status: string;
  message?: string;
  error?: string;
}

export interface HealthResponse extends ApiResponse {
  // Add health-specific fields as needed
}

export interface ActionRequest {
  tableName?: string;
  forceOverride?: boolean;
  sourceReplica?: number;
}

export interface ImportRequest {
  tableName: string;
}

export interface RepairRequest {
  forceOverride?: boolean;
  sourceReplica?: number;
}

// Table configuration from TABLES_CONFIG
export interface TableConfig {
  name: string;
  csvPath: string;
}

// Config response from /api/config
export interface ConfigResponse {
  replicaCount: number;
  workloadName: string;
  gvc: string;
  location: string;
  tables: TableConfig[];
}

// Replica status from /api/cluster
export interface ReplicaStatus {
  index: number;
  endpoint: string;
  status: 'online' | 'offline' | 'error' | 'not_in_use';
  clusterStatus: string | null;
  nodeState: string | null;
  error: string | null;
  deploymentMessage?: string;
}

// Cluster response from /api/cluster
export interface ClusterResponse {
  status: 'healthy' | 'degraded' | 'uninitialized';
  replicas: ReplicaStatus[];
}

// Cluster group for split-brain detection
export interface ClusterGroup {
  uuid: string;
  replicas: number[];
  nodeCount: number;
}

// Cluster discovery response from /api/cluster/discover
export interface ClusterDiscoveryResponse {
  cluster: {
    uuid: string;
    sourceAddr: string;
    sourceIdx: number;
    nodeCount: number;
  } | null;
  splitBrain: boolean;
  groups?: ClusterGroup[];
}

// Table component status (main, delta, distributed)
export interface TableComponentStatus {
  present: boolean;
  inCluster: boolean;
}

// Per-replica table status
export interface TableReplicaStatus {
  index: number;
  online: boolean;
  mainTable: TableComponentStatus;
  deltaTable: TableComponentStatus;
  distributedTable: TableComponentStatus;
  error?: string;
}

// Table status entry from /api/tables/status
export interface TableStatusEntry {
  name: string;
  csvPath: string;
  clusterMain: boolean; // Whether main table should be in cluster
  replicas: TableReplicaStatus[];
}

// Tables status response from /api/tables/status
export interface TablesStatusResponse {
  tableSlots: Record<string, string>;
  tables: TableStatusEntry[];
}

// Column schema from DESCRIBE command
export interface ColumnSchema {
  field: string;
  type: string;
  props?: string;
}

// Table schema response from /api/tables/{name}/schema
export interface TableSchemaResponse {
  table: string;
  columns: ColumnSchema[];
}

// Import status from /api/imports
export interface ImportStatus {
  tableName: string;
  commandId?: string;
  lifecycleStage: 'scaling' | 'starting' | 'pending' | 'running' | 'completed' | 'failed';
}

// Imports response from /api/imports
export interface ImportsResponse {
  imports: ImportStatus[];
}

// Command history entry from /api/commands
export interface CommandHistoryEntry {
  id: string;
  action: 'import' | 'repair' | 'backup' | 'restore';
  tableName?: string; // for imports, backups, and restores
  type?: 'delta' | 'main'; // for backups and restores
  filename?: string; // restore filename for retry
  sourceReplica?: number; // only for repairs
  lifecycleStage: 'pending' | 'running' | 'completed' | 'failed';
  created: string; // ISO timestamp
}

// Command history response from /api/commands
export interface CommandHistoryResponse {
  commands: CommandHistoryEntry[];
}

// Backup request - sent when triggering backup
export interface BackupRequest {
  tableName: string;
  type?: 'delta' | 'main';
}

// Backup status from /api/backups
export interface BackupStatus {
  tableName: string;
  commandId?: string;
  lifecycleStage: 'scaling' | 'starting' | 'pending' | 'running' | 'completed' | 'failed';
  action: 'backup' | 'restore';
}

// Backups response from /api/backups
export interface BackupsResponse {
  backups: BackupStatus[];
}

// Repair status from /api/repairs
export interface RepairStatus {
  commandId: string;
  sourceReplica?: number;
  lifecycleStage: 'pending' | 'running' | 'completed' | 'failed';
}

// Repairs response from /api/repairs
export interface RepairsResponse {
  repairs: RepairStatus[];
}

// SQL query request
export interface SqlQueryRequest {
  query: string;
  replicaIndex?: number; // Optional: target specific replica
  broadcast?: boolean; // Execute on all replicas
}

// SQL query column metadata
export interface SqlColumnMeta {
  name: string;
  type: string;
}

// SQL query response for single replica
export interface SqlQueryResponse extends ApiResponse {
  columns?: SqlColumnMeta[];
  rows?: Record<string, any>[];
  rowCount?: number;
  executionTimeMs?: number;
  replicaIndex?: number; // Which replica responded
}

// Per-replica result for broadcast mode
export interface SqlReplicaResult {
  replicaIndex: number;
  status: 'success' | 'error';
  columns?: SqlColumnMeta[];
  rows?: Record<string, any>[];
  rowCount?: number;
  executionTimeMs?: number;
  error?: string;
}

// SQL broadcast response (results from each replica)
export interface SqlBroadcastResponse extends ApiResponse {
  results: SqlReplicaResult[];
}

// Query count for a single replica
export interface QueryCountEntry {
  index: number;
  endpoint: string;
  queryCount?: number;
  error?: string;
}

// Query counts response from /api/cluster/query-counts
export interface QueryCountsResponse extends Array<QueryCountEntry> {}

// Backup file entry from /api/backups/files
export interface BackupFile {
  filename: string;
  key: string;
  size: number;
  lastModified: string; // ISO timestamp
}

// Backup files response from /api/backups/files
export interface BackupFilesResponse {
  backups: BackupFile[];
}

// Restore request - sent when triggering restore
export interface RestoreRequest {
  tableName: string;
  filename: string;
  type?: 'delta' | 'main';
}
