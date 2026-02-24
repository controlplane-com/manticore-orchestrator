import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { PageHeader } from '../components/PageHeader';
import { Card } from '../components/Card';
import { Badge } from '../components/Badge';
import { Button } from '../components/Button';
import { LoadingSpinner, LoadingPage } from '../components/LoadingSpinner';
import { SchemaModal } from '../components/SchemaModal';
import { ConfirmActionModal } from '../components/ConfirmActionModal';
import { getTablesStatus, getConfig, getImports, setTableLock } from '../api/orchestrator';
import { getLockedTables, lockTable, unlockTable } from '../utils/tableLocks';
import type { TableStatusEntry, TableReplicaStatus, ImportStatus } from '../types/api';
import {
  TableCellsIcon,
  ChevronDownIcon,
  ChevronRightIcon,
  CheckCircleIcon,
  XCircleIcon,
  ExclamationTriangleIcon,
  ArrowPathIcon,
  DocumentTextIcon,
  LockClosedIcon,
  LockOpenIcon,
} from '@heroicons/react/24/outline';

// Get overall table health status
const getTableHealth = (table: TableStatusEntry): 'healthy' | 'partial' | 'offline' | 'empty' => {
  const onlineReplicas = table.replicas.filter(r => r.online);
  if (onlineReplicas.length === 0) return 'offline';

  const allComplete = onlineReplicas.every(r => {
    const tablesPresent = r.mainTable.present && r.deltaTable.present && r.distributedTable.present;
    // If main table should be clustered, check that it is
    const mainClusterOk = !table.clusterMain || r.mainTable.inCluster;
    // Delta should always be clustered when present
    const deltaClusterOk = !r.deltaTable.present || r.deltaTable.inCluster;
    return tablesPresent && mainClusterOk && deltaClusterOk;
  });

  if (allComplete) return 'healthy';

  const anyPresent = onlineReplicas.some(r =>
    r.mainTable.present || r.deltaTable.present
  );

  return anyPresent ? 'partial' : 'empty';
};

// Get badge for table health
const getTableHealthBadge = (health: string) => {
  switch (health) {
    case 'healthy':
      return (
        <Badge variant="success">
          <CheckCircleIcon className="h-4 w-4 mr-1" />
          Healthy
        </Badge>
      );
    case 'partial':
      return (
        <Badge variant="warning">
          <ExclamationTriangleIcon className="h-4 w-4 mr-1" />
          Partial
        </Badge>
      );
    case 'offline':
      return (
        <Badge variant="error">
          <XCircleIcon className="h-4 w-4 mr-1" />
          Offline
        </Badge>
      );
    case 'empty':
      return (
        <Badge variant="info">
          Not Initialized
        </Badge>
      );
    default:
      return <Badge variant="warning">Unknown</Badge>;
  }
};

// Component status indicator
const ComponentStatus = ({ present, inCluster, shouldBeInCluster, label }: { present: boolean; inCluster: boolean; shouldBeInCluster: boolean; label: string }) => {
  if (!present) {
    return (
      <span className="inline-flex items-center text-gray-400">
        <XCircleIcon className="h-4 w-4 mr-1" />
        {label}
      </span>
    );
  }

  // Only show warning if the table should be in cluster but isn't
  if (shouldBeInCluster && !inCluster) {
    return (
      <span className="inline-flex items-center text-yellow-600">
        <ExclamationTriangleIcon className="h-4 w-4 mr-1" />
        {label}
      </span>
    );
  }

  return (
    <span className="inline-flex items-center text-green-600">
      <CheckCircleIcon className="h-4 w-4 mr-1" />
      {label}
    </span>
  );
};

// Replica row in expanded table
const ReplicaRow = ({ replica, clusterMain }: { replica: TableReplicaStatus; clusterMain: boolean }) => {
  if (!replica.online) {
    return (
      <div className="flex items-center justify-between py-2 px-4 bg-gray-100 dark:bg-stone-700 rounded text-gray-400">
        <span>Replica {replica.index}</span>
        <Badge variant="muted">Offline</Badge>
      </div>
    );
  }

  return (
    <div className="flex items-center justify-between py-2 px-4 bg-gray-50 dark:bg-stone-700 rounded">
      <span className="font-medium text-gray-900 dark:text-white">Replica {replica.index}</span>
      <div className="flex items-center gap-4 text-sm">
        <ComponentStatus present={replica.mainTable.present} inCluster={replica.mainTable.inCluster} shouldBeInCluster={clusterMain} label="Main" />
        <ComponentStatus present={replica.deltaTable.present} inCluster={replica.deltaTable.inCluster} shouldBeInCluster={true} label="Delta" />
        <ComponentStatus present={replica.distributedTable.present} inCluster={true} shouldBeInCluster={false} label="Dist" />
      </div>
    </div>
  );
};

// Expandable table row
const TableRow = ({ table, slot, importStatus, locked, onViewSchema, onToggleLock }: {
  table: TableStatusEntry;
  slot: string;
  importStatus?: ImportStatus;
  locked: boolean;
  onViewSchema: (tableName: string) => void;
  onToggleLock: (tableName: string, lock: boolean) => void;
}) => {
  const [expanded, setExpanded] = useState(false);
  const [showUnlockConfirm, setShowUnlockConfirm] = useState(false);
  const health = getTableHealth(table);
  const onlineCount = table.replicas.filter(r => r.online).length;

  const handleLockClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (locked) {
      setShowUnlockConfirm(true);
    } else {
      onToggleLock(table.name, true);
    }
  };

  return (
    <>
    <Card className="overflow-hidden">
      <div
        className="p-4 flex items-center justify-between cursor-pointer hover:bg-gray-50 dark:hover:bg-stone-700"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-center gap-4">
          <button className="text-gray-400 hover:text-gray-600">
            {expanded ? (
              <ChevronDownIcon className="h-5 w-5" />
            ) : (
              <ChevronRightIcon className="h-5 w-5" />
            )}
          </button>
          <div className="flex items-center gap-3">
            <TableCellsIcon className="h-6 w-6 text-cpln-cyan" />
            <div>
              <div className="flex items-center gap-2">
                <h4 className="font-medium text-gray-900 dark:text-white">{table.name}</h4>
                {locked && (
                  <Badge variant="warning">
                    <LockClosedIcon className="h-3 w-3 mr-1" />
                    Locked
                  </Badge>
                )}
                {importStatus && (
                  <span className="flex items-center gap-1 text-sm text-blue-600 dark:text-blue-400">
                    <ArrowPathIcon className="h-4 w-4 animate-spin" />
                    {importStatus.lifecycleStage === 'pending' ? 'Queued' : 'Importing'}
                  </span>
                )}
              </div>
              <p className="text-xs text-gray-500 dark:text-gray-400 font-mono">
                {table.csvPath}
              </p>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-4">
          <button
            onClick={handleLockClick}
            className={`p-1 ${locked ? 'text-yellow-500 hover:text-yellow-600' : 'text-gray-400 hover:text-gray-600 dark:hover:text-gray-300'}`}
            title={locked ? 'Unlock table' : 'Lock table'}
          >
            {locked ? <LockClosedIcon className="h-5 w-5" /> : <LockOpenIcon className="h-5 w-5" />}
          </button>
          <button
            onClick={(e) => {
              e.stopPropagation();
              onViewSchema(table.name);
            }}
            className="p-1 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
            title="View Schema"
          >
            <DocumentTextIcon className="h-5 w-5" />
          </button>
          <span className="text-sm text-gray-500 dark:text-gray-400">
            {onlineCount}/{table.replicas.length} replicas
          </span>
          <span className="text-xs text-gray-400 font-mono">
            slot: {slot}
          </span>
          {getTableHealthBadge(health)}
        </div>
      </div>

      {expanded && (
        <div className="border-t border-gray-200 dark:border-stone-600 p-4 space-y-2">
          <h5 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-3">
            Per-Replica Status
          </h5>
          {table.replicas.map((replica) => (
            <ReplicaRow key={replica.index} replica={replica} clusterMain={table.clusterMain} />
          ))}

          <div className="mt-4 pt-3 border-t border-gray-200 dark:border-stone-600">
            <p className="text-xs text-gray-500 dark:text-gray-400">
              <strong>Main:</strong> Active main table ({table.name}_main_{slot}){' '}
              <strong>Delta:</strong> Delta table for incremental updates{' '}
              <strong>Dist:</strong> Distributed table for cross-replica queries
            </p>
          </div>
        </div>
      )}
    </Card>

    <ConfirmActionModal
      isOpen={showUnlockConfirm}
      onClose={() => setShowUnlockConfirm(false)}
      onConfirm={() => {
        onToggleLock(table.name, false);
        setShowUnlockConfirm(false);
      }}
      title="Unlock Table"
      message={`Unlocking "${table.name}" will allow import and restore actions to be performed on it. Are you sure you want to unlock this table?`}
      confirmText="Unlock"
      confirmButtonClass="bg-yellow-500 text-white hover:bg-yellow-600"
    />
    </>
  );
};

export const Tables = () => {
  const [schemaModal, setSchemaModal] = useState<{ isOpen: boolean; tableName: string }>({
    isOpen: false,
    tableName: '',
  });

  const {
    data: tablesData,
    isLoading: tablesLoading,
    error: tablesError,
    refetch: refetchTables,
  } = useQuery({
    queryKey: ['tables-status'],
    queryFn: getTablesStatus,
    refetchInterval: 5000,
  });

  const {
    isLoading: configLoading,
  } = useQuery({
    queryKey: ['config'],
    queryFn: getConfig,
    refetchInterval: 60000,
  });

  // Fetch active imports with dynamic polling
  const { data: importsData } = useQuery({
    queryKey: ['imports'],
    queryFn: getImports,
    refetchInterval: (query) => {
      // Poll every 2 seconds when imports are active, otherwise every 30 seconds
      const hasActiveImports = (query.state.data?.imports?.length ?? 0) > 0;
      return hasActiveImports ? 2000 : 30000;
    },
  });

  // Lock state lives in localStorage — instant, consistent, no server round-trip races
  const [lockedTables, setLockedTables] = useState<string[]>(getLockedTables);

  const isTableLocked = (tableName: string) => lockedTables.includes(tableName);

  const handleToggleLock = (tableName: string, lock: boolean) => {
    const next = lock ? lockTable(tableName) : unlockTable(tableName);
    setLockedTables(next);
    // Best-effort sync to server (enforces lock on single-replica deployments)
    setTableLock(tableName, lock).catch(() => {});
  };

  // Helper to get import status for a table
  const getImportForTable = (tableName: string) => {
    return importsData?.imports?.find(i => i.tableName === tableName);
  };

  const handleViewSchema = (tableName: string) => {
    setSchemaModal({ isOpen: true, tableName });
  };

  const handleCloseSchema = () => {
    setSchemaModal({ isOpen: false, tableName: '' });
  };

  const [isRefreshing, setIsRefreshing] = useState(false);

  const handleRefresh = () => {
    setIsRefreshing(true);
    refetchTables();
    // Reset animation after a short delay
    setTimeout(() => setIsRefreshing(false), 600);
  };

  if (tablesLoading && configLoading) {
    return <LoadingPage message="Loading tables..." />;
  }

  const tableSlots = tablesData?.tableSlots || {};
  const tables = tablesData?.tables || [];

  // Helper to get slot for a table (default to 'a')
  const getSlot = (tableName: string) => tableSlots[tableName] || 'a';

  // Calculate summary stats
  const healthyCount = tables.filter(t => getTableHealth(t) === 'healthy').length;
  const totalCount = tables.length;

  // Count tables per slot
  const slotACounts = tables.filter(t => getSlot(t.name) === 'a').length;
  const slotBCounts = tables.filter(t => getSlot(t.name) === 'b').length;

  return (
    <div>
      <PageHeader
        title="Tables"
        description="Manage Manticore search tables"
        actions={
          <Button variant="secondary" onClick={handleRefresh}>
            <ArrowPathIcon className={`h-4 w-4 mr-2 transition-transform duration-500 ${isRefreshing ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
        }
      />

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
        <Card className="p-6">
          <div className="flex items-center gap-4 mb-4">
            <TableCellsIcon className="h-8 w-8 text-cpln-cyan" />
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Total Tables</h3>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-sm text-gray-500 dark:text-gray-400">Count</span>
            <span className="text-2xl font-semibold text-gray-900 dark:text-white">
              {totalCount}
            </span>
          </div>
        </Card>

        <Card className="p-6">
          <div className="flex items-center gap-4 mb-4">
            <CheckCircleIcon className="h-8 w-8 text-green-500" />
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Healthy Tables</h3>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-sm text-gray-500 dark:text-gray-400">Status</span>
            <span className="text-2xl font-semibold">
              <span className={healthyCount === totalCount ? 'text-green-600' : 'text-yellow-600'}>
                {healthyCount}
              </span>
              <span className="text-gray-400 text-lg">/{totalCount}</span>
            </span>
          </div>
        </Card>

        <Card className="p-6">
          <div className="flex items-center gap-4 mb-4">
            <ArrowPathIcon className="h-8 w-8 text-cpln-cyan" />
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Slot Distribution</h3>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-sm text-gray-500 dark:text-gray-400">A / B</span>
            <span className="text-2xl font-semibold text-gray-900 dark:text-white font-mono">
              {slotACounts} / {slotBCounts}
            </span>
          </div>
        </Card>
      </div>

      {/* Tables List */}
      {tablesLoading ? (
        <Card className="p-8">
          <div className="flex justify-center">
            <LoadingSpinner size="lg" />
          </div>
        </Card>
      ) : tablesError ? (
        <Card className="p-8 text-center text-red-600 dark:text-red-400">
          Failed to load tables status
        </Card>
      ) : tables.length === 0 ? (
        <Card className="p-8 text-center text-gray-500 dark:text-gray-400">
          No tables configured. Add tables to your values.yaml and redeploy.
        </Card>
      ) : (
        <div className="space-y-4">
          {tables.map((table) => (
            <TableRow
              key={table.name}
              table={table}
              slot={getSlot(table.name)}
              importStatus={getImportForTable(table.name)}
              locked={isTableLocked(table.name)}
              onViewSchema={handleViewSchema}
              onToggleLock={handleToggleLock}
            />
          ))}
        </div>
      )}

      {/* Legend */}
      <Card className="p-4 mt-6">
        <div className="flex flex-wrap gap-6 text-sm">
          <div className="flex items-center gap-2">
            <CheckCircleIcon className="h-4 w-4 text-green-600" />
            <span className="text-gray-600 dark:text-gray-400">Present & In Cluster</span>
          </div>
          <div className="flex items-center gap-2">
            <ExclamationTriangleIcon className="h-4 w-4 text-yellow-600" />
            <span className="text-gray-600 dark:text-gray-400">Present but Not in Cluster</span>
          </div>
          <div className="flex items-center gap-2">
            <XCircleIcon className="h-4 w-4 text-gray-400" />
            <span className="text-gray-600 dark:text-gray-400">Not Present</span>
          </div>
        </div>
      </Card>

      {/* Schema Modal */}
      <SchemaModal
        isOpen={schemaModal.isOpen}
        onClose={handleCloseSchema}
        tableName={schemaModal.tableName}
      />
    </div>
  );
};
