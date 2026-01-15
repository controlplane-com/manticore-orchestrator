import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { PageHeader } from '../components/PageHeader';
import { Card } from '../components/Card';
import { Badge } from '../components/Badge';
import { Button } from '../components/Button';
import { LoadingSpinner, LoadingPage } from '../components/LoadingSpinner';
import { SchemaModal } from '../components/SchemaModal';
import { getTablesStatus, getConfig, getImports } from '../api/orchestrator';
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
} from '@heroicons/react/24/outline';

// Get overall table health status
const getTableHealth = (table: TableStatusEntry): 'healthy' | 'partial' | 'offline' | 'empty' => {
  const onlineReplicas = table.replicas.filter(r => r.online);
  if (onlineReplicas.length === 0) return 'offline';

  const allComplete = onlineReplicas.every(r =>
    r.mainTable.present && r.deltaTable.present && r.distributedTable.present
  );

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
const ComponentStatus = ({ present, inCluster, label }: { present: boolean; inCluster: boolean; label: string }) => {
  if (!present) {
    return (
      <span className="inline-flex items-center text-gray-400">
        <XCircleIcon className="h-4 w-4 mr-1" />
        {label}
      </span>
    );
  }

  if (!inCluster && label !== 'Dist') {
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
const ReplicaRow = ({ replica }: { replica: TableReplicaStatus }) => {
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
        <ComponentStatus present={replica.mainTable.present} inCluster={replica.mainTable.inCluster} label="Main" />
        <ComponentStatus present={replica.deltaTable.present} inCluster={replica.deltaTable.inCluster} label="Delta" />
        <ComponentStatus present={replica.distributedTable.present} inCluster={true} label="Dist" />
      </div>
    </div>
  );
};

// Expandable table row
const TableRow = ({ table, slot, importStatus, onViewSchema }: { table: TableStatusEntry; slot: string; importStatus?: ImportStatus; onViewSchema: (tableName: string) => void }) => {
  const [expanded, setExpanded] = useState(false);
  const health = getTableHealth(table);
  const onlineCount = table.replicas.filter(r => r.online).length;

  return (
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
                {importStatus && (
                  <Badge variant="info">
                    <ArrowPathIcon className="h-3 w-3 mr-1 animate-spin" />
                    {importStatus.lifecycleStage === 'pending' ? 'Queued' : 'Importing'}
                  </Badge>
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
            <ReplicaRow key={replica.index} replica={replica} />
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
          <Button variant="secondary" onClick={() => refetchTables()}>
            <ArrowPathIcon className="h-4 w-4 mr-2" />
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
              onViewSchema={handleViewSchema}
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
