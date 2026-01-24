import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { PageHeader } from '../components/PageHeader';
import { Card } from '../components/Card';
import { Button } from '../components/Button';
import { Badge } from '../components/Badge';
import { LoadingSpinner } from '../components/LoadingSpinner';
import { FormSelect } from '../components/FormSelect';
import { ConfirmActionModal } from '../components/ConfirmActionModal';
import { useToast } from '../hooks/useToast';
import { getStatus, getConfig, getCluster, getClusterDiscovery, getImports, getRepairs, getCommandHistory, importTable, repairCluster } from '../api/orchestrator';
import {
  HeartIcon,
  TableCellsIcon,
  ArrowPathIcon,
  WrenchScrewdriverIcon,
  ServerStackIcon,
  ExclamationTriangleIcon,
  ClockIcon,
  ChevronDownIcon,
  ChevronUpIcon,
} from '@heroicons/react/24/outline';

export const Dashboard = () => {
  const toast = useToast();
  const queryClient = useQueryClient();
  const [selectedTable, setSelectedTable] = useState('');
  const [selectedSourceReplica, setSelectedSourceReplica] = useState('');
  const [expandedMessages, setExpandedMessages] = useState<Set<string>>(new Set());
  const [confirmModal, setConfirmModal] = useState<{
    isOpen: boolean;
    action: 'import' | 'repair';
    title: string;
    message: string;
  }>({ isOpen: false, action: 'import', title: '', message: '' });

  // Fetch config (tables list)
  const { data: configData, isLoading: configLoading, error: configError } = useQuery({
    queryKey: ['config'],
    queryFn: getConfig,
    refetchInterval: 60000, // Refresh every minute
  });

  // Fetch cluster status
  const { data: clusterData, isLoading: clusterLoading, error: clusterError } = useQuery({
    queryKey: ['cluster'],
    queryFn: getCluster,
    refetchInterval: 30000, // Refresh every 30 seconds
  });

  // Fetch status
  const { data: statusData, isLoading: statusLoading } = useQuery({
    queryKey: ['status'],
    queryFn: getStatus,
    refetchInterval: 10000, // Refresh every 10 seconds
  });

  // Fetch cluster discovery for split-brain detection
  const { data: discoveryData } = useQuery({
    queryKey: ['cluster-discovery'],
    queryFn: getClusterDiscovery,
    refetchInterval: 30000, // Refresh every 30 seconds
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

  // Fetch active repairs with dynamic polling
  const { data: repairsData } = useQuery({
    queryKey: ['repairs'],
    queryFn: getRepairs,
    refetchInterval: (query) => {
      // Poll every 2 seconds when repairs are active, otherwise every 30 seconds
      const hasActiveRepairs = (query.state.data?.repairs?.length ?? 0) > 0;
      return hasActiveRepairs ? 2000 : 30000;
    },
  });

  // Fetch command history
  const { data: commandHistoryData } = useQuery({
    queryKey: ['command-history'],
    queryFn: getCommandHistory,
    refetchInterval: () => {
      // Poll faster when there are active operations (import or repair)
      const hasActiveImports = (importsData?.imports?.length ?? 0) > 0;
      const hasActiveRepairs = (repairsData?.repairs?.length ?? 0) > 0;
      return (hasActiveImports || hasActiveRepairs) ? 2000 : 60000;
    },
  });

  // Set default selected table when config loads
  useEffect(() => {
    if (configData?.tables?.length && !selectedTable) {
      setSelectedTable(configData.tables[0].name);
    }
  }, [configData, selectedTable]);

  // Build table options for select
  const tableOptions = configData?.tables?.map(t => ({
    value: t.name,
    label: t.name.charAt(0).toUpperCase() + t.name.slice(1),
  })) || [];

  // Check if an import is in progress for a specific table
  const getImportForTable = (tableName: string) => {
    return importsData?.imports?.find(i => i.tableName === tableName);
  };

  // Check if the selected table has an import in progress
  const selectedTableImport = selectedTable ? getImportForTable(selectedTable) : undefined;

  // Mutations
  const importMutation = useMutation({
    mutationFn: () => importTable({ tableName: selectedTable }),
    onSuccess: (data) => {
      toast.success('Import started', data.message);
      queryClient.invalidateQueries({ queryKey: ['imports'] });
      queryClient.invalidateQueries({ queryKey: ['cluster'] });
      queryClient.invalidateQueries({ queryKey: ['tables-status'] });
    },
    onError: (error: any) => {
      toast.error('Import failed', error.response?.data?.error || error.message);
    },
  });

  const repairMutation = useMutation({
    mutationFn: () => repairCluster(
      selectedSourceReplica ? { sourceReplica: parseInt(selectedSourceReplica, 10) } : undefined
    ),
    onSuccess: (data) => {
      toast.success('Repair completed', data.message);
      queryClient.invalidateQueries({ queryKey: ['cluster'] });
      queryClient.invalidateQueries({ queryKey: ['cluster-discovery'] });
    },
    onError: (error: any) => {
      toast.error('Repair failed', error.response?.data?.error || error.message);
    },
  });

  const handleConfirmAction = () => {
    setConfirmModal({ ...confirmModal, isOpen: false });

    switch (confirmModal.action) {
      case 'import':
        importMutation.mutate();
        break;
      case 'repair':
        repairMutation.mutate();
        break;
    }
  };

  const openConfirmModal = (action: 'import' | 'repair') => {
    const configs = {
      import: {
        title: 'Import Data',
        message: `Are you sure you want to import data into the "${selectedTable}" table? This uses blue-green deployment and may take some time.`,
      },
      repair: {
        title: 'Repair Cluster',
        message: selectedSourceReplica
          ? `Repair cluster using Replica ${selectedSourceReplica} as source? All other replicas will rejoin this node.`
          : 'Repair cluster using auto-selected source replica? The system will choose the best source automatically.',
      },
    };

    setConfirmModal({
      isOpen: true,
      action,
      ...configs[action],
    });
  };

  const isAnyMutationLoading = importMutation.isPending || repairMutation.isPending;

  // Get cluster health badge
  const getClusterBadge = () => {
    if (clusterLoading) return <LoadingSpinner size="sm" />;
    if (clusterError) return <Badge variant="error">Error</Badge>;
    if (!clusterData) return <Badge variant="warning">Unknown</Badge>;

    switch (clusterData.status) {
      case 'healthy':
        return <Badge variant="success">Healthy</Badge>;
      case 'degraded':
        return <Badge variant="warning">Degraded</Badge>;
      default:
        return <Badge variant="warning">Unknown</Badge>;
    }
  };

  // Count online replicas (excluding not_in_use)
  const onlineReplicas = clusterData?.replicas?.filter(r => r.status === 'online').length || 0;
  const inUseReplicas = clusterData?.replicas?.filter(r => r.status !== 'not_in_use').length || configData?.replicaCount || 0;

  return (
    <div>
      <PageHeader
        title="Dashboard"
        description="Manticore cluster management and monitoring"
      />

      {/* Status Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        {/* Cluster Health */}
        <Card className="p-6">
          <div className="flex items-center gap-4 mb-4">
            <HeartIcon className="h-8 w-8 text-cpln-cyan" />
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Cluster Health</h3>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-sm text-gray-500 dark:text-gray-400">Status</span>
            {getClusterBadge()}
          </div>
        </Card>

        {/* Replicas Status */}
        <Card className="p-6">
          <div className="flex items-center gap-4 mb-4">
            <ServerStackIcon className="h-8 w-8 text-cpln-cyan" />
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Replicas</h3>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-sm text-gray-500 dark:text-gray-400">Online</span>
            {clusterLoading ? (
              <LoadingSpinner size="sm" />
            ) : (
              <span className="text-2xl font-semibold">
                <span className={onlineReplicas === inUseReplicas ? 'text-green-600' : 'text-yellow-600'}>
                  {onlineReplicas}
                </span>
                <span className="text-gray-400 text-lg">/{inUseReplicas}</span>
              </span>
            )}
          </div>
        </Card>

        {/* Service Status */}
        <Card className="p-6">
          <div className="flex items-center gap-4 mb-4">
            <ArrowPathIcon className="h-8 w-8 text-cpln-cyan" />
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Service Status</h3>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-sm text-gray-500 dark:text-gray-400">Status</span>
            {statusLoading ? (
              <LoadingSpinner size="sm" />
            ) : statusData?.status === 'ok' ? (
              <Badge variant="success">Running</Badge>
            ) : (
              <Badge variant="error">Unavailable</Badge>
            )}
          </div>
        </Card>

        {/* Tables Count */}
        <Card className="p-6">
          <div className="flex items-center gap-4 mb-4">
            <TableCellsIcon className="h-8 w-8 text-cpln-cyan" />
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Configured Tables</h3>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-sm text-gray-500 dark:text-gray-400">Count</span>
            {configLoading ? (
              <LoadingSpinner size="sm" />
            ) : configError ? (
              <span className="text-2xl font-semibold text-gray-900 dark:text-white">?</span>
            ) : (
              <span className="text-2xl font-semibold text-gray-900 dark:text-white">
                {configData?.tables?.length || 0}
              </span>
            )}
          </div>
        </Card>
      </div>

      {/* Actions */}
      <div>
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Actions</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Import Panel */}
          <Card className="p-6">
            <div className="flex items-center gap-2 mb-4">
              <ArrowPathIcon className="h-5 w-5 text-cpln-cyan" />
              <h3 className="text-base font-semibold text-gray-900 dark:text-white">Import Data</h3>
            </div>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
              Import data for a specific table using blue-green deployment.
            </p>

            <div className="space-y-4">
              <div className="max-w-sm">
                {configLoading ? (
                  <LoadingSpinner size="sm" />
                ) : tableOptions.length > 0 ? (
                  <FormSelect
                    label="Select Table"
                    value={selectedTable}
                    onChange={setSelectedTable}
                    options={tableOptions}
                  />
                ) : (
                  <p className="text-gray-500 dark:text-gray-400">No tables configured</p>
                )}
              </div>

              {/* Import status indicator */}
              {selectedTableImport && (
                <div className="flex items-center gap-2 p-3 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-md">
                  <ArrowPathIcon className="h-4 w-4 text-blue-600 dark:text-blue-400 animate-spin" />
                  <span className="text-sm text-blue-700 dark:text-blue-300">
                    Import {selectedTableImport.lifecycleStage} for "{selectedTable}"
                  </span>
                  <Badge variant="info">{selectedTableImport.lifecycleStage}</Badge>
                </div>
              )}

              <Button
                variant="secondary"
                onClick={() => openConfirmModal('import')}
                disabled={isAnyMutationLoading || !selectedTable || !!selectedTableImport}
              >
                <ArrowPathIcon className="h-4 w-4 mr-2" />
                {importMutation.isPending ? 'Starting...' : selectedTableImport ? 'Import in Progress' : 'Import Table'}
              </Button>
            </div>
          </Card>

          {/* Repair Panel */}
          <Card className="p-6">
            <div className="flex items-center gap-2 mb-4">
              <WrenchScrewdriverIcon className="h-5 w-5 text-cpln-cyan" />
              <h3 className="text-base font-semibold text-gray-900 dark:text-white">Cluster Repair</h3>
            </div>

            {/* Split-brain warning */}
            {discoveryData?.splitBrain && (
              <div className="mb-4 p-3 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-md">
                <div className="flex items-start gap-2">
                  <ExclamationTriangleIcon className="h-5 w-5 text-yellow-600 dark:text-yellow-500 flex-shrink-0 mt-0.5" />
                  <div>
                    <p className="text-sm font-medium text-yellow-800 dark:text-yellow-200">
                      Split-brain detected
                    </p>
                    <p className="text-sm text-yellow-700 dark:text-yellow-300 mt-1">
                      {discoveryData.groups?.length} cluster groups found:
                    </p>
                    <ul className="text-sm text-yellow-700 dark:text-yellow-300 mt-1 list-disc list-inside">
                      {discoveryData.groups?.map((group, idx) => (
                        <li key={idx}>
                          Replicas {group.replicas.join(', ')} ({group.nodeCount} nodes)
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              </div>
            )}

            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
              Repair cluster by rejoining all replicas to a single source node.
            </p>

            <div className="space-y-4">
              {/* Repair status indicator */}
              {(repairsData?.repairs?.length ?? 0) > 0 && (
                <div className="flex items-center gap-2 p-3 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-md">
                  <WrenchScrewdriverIcon className="h-4 w-4 text-yellow-600 dark:text-yellow-400 animate-spin" />
                  <span className="text-sm text-yellow-700 dark:text-yellow-300">
                    Repair {repairsData!.repairs[0].lifecycleStage}
                    {repairsData!.repairs[0].sourceReplica !== undefined && ` (source: replica ${repairsData!.repairs[0].sourceReplica})`}
                  </span>
                  <Badge variant="warning">{repairsData!.repairs[0].lifecycleStage}</Badge>
                </div>
              )}

              {/* Source Replica Selection */}
              <div className="max-w-sm">
                <FormSelect
                  label="Source Replica (optional)"
                  value={selectedSourceReplica}
                  onChange={setSelectedSourceReplica}
                  options={[
                    { value: '', label: 'Auto-select (recommended)' },
                    ...(clusterData?.replicas
                      ?.filter(r => r.status === 'online')
                      .map(r => ({
                        value: String(r.index),
                        label: `Replica ${r.index}${r.clusterStatus ? ` (${r.clusterStatus})` : ''}`,
                      })) || []),
                  ]}
                />
              </div>

              <Button
                variant="danger"
                onClick={() => openConfirmModal('repair')}
                disabled={isAnyMutationLoading || (repairsData?.repairs?.length ?? 0) > 0}
              >
                <WrenchScrewdriverIcon className="h-4 w-4 mr-2" />
                {repairMutation.isPending ? 'Repairing...' : (repairsData?.repairs?.length ?? 0) > 0 ? 'Repair in Progress' : 'Repair Cluster'}
              </Button>
            </div>
          </Card>
        </div>
      </div>

      {/* Command History */}
      <div className="mt-8">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Command History</h2>
        <Card className="p-6">
          <div className="flex items-center gap-2 mb-4">
            <ClockIcon className="h-5 w-5 text-cpln-cyan" />
            <h3 className="text-base font-semibold text-gray-900 dark:text-white">Recent Commands</h3>
          </div>

          {commandHistoryData?.commands?.length === 0 ? (
            <p className="text-sm text-gray-500 dark:text-gray-400">No commands have been run yet.</p>
          ) : (
            <div className="space-y-2">
              {commandHistoryData?.commands?.slice(0, 15).map((cmd) => {
                const isExpanded = expandedMessages.has(cmd.id);
                const hasMessage = cmd.message && cmd.message.trim() !== '';
                
                return (
                  <div key={cmd.id}>
                    <div className="flex items-center justify-between py-2 px-3 bg-gray-50 dark:bg-stone-700 rounded">
                      <div className="flex items-center gap-3">
                        <span className="text-xs text-gray-500 dark:text-gray-400 font-mono">
                          {new Date(cmd.created).toISOString().replace('T', ' ').replace(/\.\d{3}Z$/, 'Z')}
                        </span>
                        <Badge variant={cmd.action === 'import' ? 'info' : 'warning'}>
                          {cmd.action}
                        </Badge>
                        <span className="font-medium text-gray-900 dark:text-white">
                          {cmd.action === 'import'
                            ? cmd.tableName
                            : `Replica ${cmd.sourceReplica ?? 'auto'}`}
                        </span>
                        <span className="text-xs text-gray-400 font-mono">{cmd.id}</span>
                      </div>
                      <div className="flex items-center gap-2">
                        <Badge
                          variant={
                            cmd.lifecycleStage === 'completed'
                              ? 'success'
                              : cmd.lifecycleStage === 'failed'
                              ? 'error'
                              : 'info'
                          }
                        >
                          {(cmd.lifecycleStage === 'running' || cmd.lifecycleStage === 'pending') && (
                            <ArrowPathIcon className="h-3 w-3 mr-1 animate-spin" />
                          )}
                          {cmd.lifecycleStage}
                        </Badge>
                        {hasMessage && (
                          <button
                            onClick={() => {
                              const newExpanded = new Set(expandedMessages);
                              if (isExpanded) {
                                newExpanded.delete(cmd.id);
                              } else {
                                newExpanded.add(cmd.id);
                              }
                              setExpandedMessages(newExpanded);
                            }}
                            className="p-1 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 transition-colors"
                            title={isExpanded ? 'Hide message' : 'Show message'}
                          >
                            {isExpanded ? (
                              <ChevronUpIcon className="h-4 w-4" />
                            ) : (
                              <ChevronDownIcon className="h-4 w-4" />
                            )}
                          </button>
                        )}
                      </div>
                    </div>
                    {hasMessage && isExpanded && (
                      <div className="mt-1 px-3 py-2 bg-red-50 dark:bg-red-900/20 rounded text-sm text-red-700 dark:text-red-300 whitespace-pre-wrap">
                        {cmd.message}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </Card>
      </div>

      {/* Confirm Modal */}
      <ConfirmActionModal
        isOpen={confirmModal.isOpen}
        onClose={() => setConfirmModal({ ...confirmModal, isOpen: false })}
        onConfirm={handleConfirmAction}
        title={confirmModal.title}
        message={confirmModal.message}
        confirmText={confirmModal.action === 'repair' ? 'Repair' : 'Confirm'}
        confirmButtonClass={
          confirmModal.action === 'repair'
            ? 'bg-red-600 text-white hover:bg-red-700'
            : 'bg-cpln-cyan text-white hover:bg-cpln-cyan-dark'
        }
        isLoading={isAnyMutationLoading}
      />
    </div>
  );
};
