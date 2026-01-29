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
import { getStatus, getConfig, getCluster, getClusterDiscovery, getImports, getBackups, getRepairs, getCommandHistory, importTable, backupTable, repairCluster, getBackupFiles, restoreTable } from '../api/orchestrator';
import {
  HeartIcon,
  TableCellsIcon,
  ArrowPathIcon,
  WrenchScrewdriverIcon,
  ServerStackIcon,
  ExclamationTriangleIcon,
  ClockIcon,
  CloudArrowUpIcon,
  CloudArrowDownIcon,
} from '@heroicons/react/24/outline';

export const Dashboard = () => {
  const toast = useToast();
  const queryClient = useQueryClient();
  const [selectedTable, setSelectedTable] = useState('');
  const [selectedSourceReplica, setSelectedSourceReplica] = useState('');
  const [selectedBackupTable, setSelectedBackupTable] = useState('');
  const [selectedRestoreTable, setSelectedRestoreTable] = useState('');
  const [selectedBackupFile, setSelectedBackupFile] = useState('');
  const [backupType, setBackupType] = useState<'delta' | 'main'>('delta');
  const [restoreType, setRestoreType] = useState<'delta' | 'main'>('delta');
  const [commandFilter, setCommandFilter] = useState<'all' | 'import' | 'repair' | 'backup' | 'restore'>('all');
  const [confirmModal, setConfirmModal] = useState<{
    isOpen: boolean;
    action: 'import' | 'repair' | 'backup' | 'restore';
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

  // Fetch active backups with dynamic polling
  const { data: backupsData } = useQuery({
    queryKey: ['backups'],
    queryFn: getBackups,
    refetchInterval: (query) => {
      const hasActiveBackups = (query.state.data?.backups?.length ?? 0) > 0;
      return hasActiveBackups ? 2000 : 30000;
    },
  });

  // Fetch command history
  const { data: commandHistoryData } = useQuery({
    queryKey: ['command-history'],
    queryFn: getCommandHistory,
    refetchInterval: () => {
      // Poll faster when there are active operations
      const hasActiveImports = (importsData?.imports?.length ?? 0) > 0;
      const hasActiveRepairs = (repairsData?.repairs?.length ?? 0) > 0;
      const hasActiveBackups = (backupsData?.backups?.length ?? 0) > 0;
      return (hasActiveImports || hasActiveRepairs || hasActiveBackups) ? 2000 : 60000;
    },
  });

  // Fetch backup files for the selected restore table
  const { data: backupFilesData, isLoading: backupFilesLoading } = useQuery({
    queryKey: ['backup-files', selectedRestoreTable, restoreType],
    queryFn: () => getBackupFiles(selectedRestoreTable, restoreType),
    enabled: !!selectedRestoreTable,
    refetchInterval: 60000, // Refresh every minute
  });

  // Set default selected table when config loads
  useEffect(() => {
    if (configData?.tables?.length && !selectedTable) {
      setSelectedTable(configData.tables[0].name);
    }
    if (configData?.tables?.length && !selectedBackupTable) {
      setSelectedBackupTable(configData.tables[0].name);
    }
    if (configData?.tables?.length && !selectedRestoreTable) {
      setSelectedRestoreTable(configData.tables[0].name);
    }
  }, [configData, selectedTable, selectedBackupTable, selectedRestoreTable]);

  // Clear selected backup file when restore table or type changes
  useEffect(() => {
    setSelectedBackupFile('');
  }, [selectedRestoreTable, restoreType]);

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

  // Check if a backup is in progress for a specific table
  const getBackupForTable = (tableName: string) => {
    return backupsData?.backups?.find(b => b.tableName === tableName);
  };

  const selectedTableBackup = selectedBackupTable ? getBackupForTable(selectedBackupTable) : undefined;

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
      toast.success('Repair started', data.message);
      queryClient.invalidateQueries({ queryKey: ['cluster'] });
      queryClient.invalidateQueries({ queryKey: ['cluster-discovery'] });
    },
    onError: (error: any) => {
      toast.error('Repair failed', error.response?.data?.error || error.message);
    },
  });

  const backupMutation = useMutation({
    mutationFn: () => backupTable({ tableName: selectedBackupTable, type: backupType }),
    onSuccess: (data) => {
      toast.success('Backup started', data.message);
      queryClient.invalidateQueries({ queryKey: ['backups'] });
      queryClient.invalidateQueries({ queryKey: ['command-history'] });
    },
    onError: (error: any) => {
      toast.error('Backup failed', error.response?.data?.error || error.message);
    },
  });

  const restoreMutation = useMutation({
    mutationFn: () => restoreTable({ tableName: selectedRestoreTable, filename: selectedBackupFile, type: restoreType }),
    onSuccess: (data) => {
      toast.success('Restore started', data.message);
      queryClient.invalidateQueries({ queryKey: ['command-history'] });
    },
    onError: (error: any) => {
      toast.error('Restore failed', error.response?.data?.error || error.message);
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
      case 'backup':
        backupMutation.mutate();
        break;
      case 'restore':
        restoreMutation.mutate();
        break;
    }
  };

  const openConfirmModal = (action: 'import' | 'repair' | 'backup' | 'restore') => {
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
      backup: {
        title: `Backup ${backupType === 'main' ? 'Main' : 'Delta'} Table`,
        message: `Are you sure you want to backup the ${backupType} table for "${selectedBackupTable}"? This will create a physical backup and upload it to cloud storage.`,
      },
      restore: {
        title: `Restore ${restoreType === 'main' ? 'Main' : 'Delta'} Table`,
        message: `Are you sure you want to restore "${selectedRestoreTable}" ${restoreType} table from backup "${selectedBackupFile}"? This will overwrite the current ${restoreType} table data.`,
      },
    };

    setConfirmModal({
      isOpen: true,
      action,
      ...configs[action],
    });
  };

  const isAnyMutationLoading = importMutation.isPending || repairMutation.isPending || backupMutation.isPending || restoreMutation.isPending;

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
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
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
          {/* Backup Panel */}
          <Card className="p-6">
            <div className="flex items-center gap-2 mb-4">
              <CloudArrowUpIcon className="h-5 w-5 text-cpln-cyan" />
              <h3 className="text-base font-semibold text-gray-900 dark:text-white">Backup {backupType === 'main' ? 'Main' : 'Delta'}</h3>
            </div>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
              Backup a table's {backupType} data to cloud storage.
            </p>

            <div className="space-y-4">
              <div className="max-w-sm">
                {configLoading ? (
                  <LoadingSpinner size="sm" />
                ) : tableOptions.length > 0 ? (
                  <>
                    <FormSelect
                      label="Select Table"
                      value={selectedBackupTable}
                      onChange={setSelectedBackupTable}
                      options={tableOptions}
                    />
                    <FormSelect
                      label="Type"
                      value={backupType}
                      onChange={(v) => setBackupType(v as 'delta' | 'main')}
                      options={[
                        { value: 'delta', label: 'Delta' },
                        { value: 'main', label: 'Main' },
                      ]}
                    />
                  </>
                ) : (
                  <p className="text-gray-500 dark:text-gray-400">No tables configured</p>
                )}
              </div>

              {/* Backup status indicator */}
              {selectedTableBackup && (
                <div className="flex items-center gap-2 p-3 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md">
                  <ArrowPathIcon className="h-4 w-4 text-green-600 dark:text-green-400 animate-spin" />
                  <span className="text-sm text-green-700 dark:text-green-300">
                    Backup {selectedTableBackup.lifecycleStage} for "{selectedBackupTable}"
                  </span>
                  <Badge variant="success">{selectedTableBackup.lifecycleStage}</Badge>
                </div>
              )}

              <Button
                variant="secondary"
                onClick={() => openConfirmModal('backup')}
                disabled={isAnyMutationLoading || !selectedBackupTable || !!selectedTableBackup}
              >
                <CloudArrowUpIcon className="h-4 w-4 mr-2" />
                {backupMutation.isPending ? 'Starting...' : selectedTableBackup ? 'Backup in Progress' : 'Backup Table'}
              </Button>
            </div>
          </Card>

          {/* Restore Panel */}
          <Card className="p-6">
            <div className="flex items-center gap-2 mb-4">
              <CloudArrowDownIcon className="h-5 w-5 text-cpln-cyan" />
              <h3 className="text-base font-semibold text-gray-900 dark:text-white">Restore {restoreType === 'main' ? 'Main' : 'Delta'}</h3>
            </div>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
              Restore a table's {restoreType} data from a cloud backup.
            </p>

            <div className="space-y-4">
              <div className="max-w-sm">
                {configLoading ? (
                  <LoadingSpinner size="sm" />
                ) : tableOptions.length > 0 ? (
                  <>
                    <FormSelect
                      label="Select Table"
                      value={selectedRestoreTable}
                      onChange={setSelectedRestoreTable}
                      options={tableOptions}
                    />
                    <FormSelect
                      label="Type"
                      value={restoreType}
                      onChange={(v) => setRestoreType(v as 'delta' | 'main')}
                      options={[
                        { value: 'delta', label: 'Delta' },
                        { value: 'main', label: 'Main' },
                      ]}
                    />
                  </>
                ) : (
                  <p className="text-gray-500 dark:text-gray-400">No tables configured</p>
                )}
              </div>

              <div className="max-w-sm">
                {backupFilesLoading ? (
                  <div className="flex items-center gap-2">
                    <LoadingSpinner size="sm" />
                    <span className="text-sm text-gray-500">Loading backups...</span>
                  </div>
                ) : (backupFilesData?.backups?.length ?? 0) > 0 ? (
                  <FormSelect
                    label="Select Backup"
                    value={selectedBackupFile}
                    onChange={setSelectedBackupFile}
                    options={[
                      { value: '', label: 'Select a backup file...' },
                      ...(backupFilesData?.backups?.map(b => ({
                        value: b.filename,
                        label: `${b.lastModified.replace('T', ' ').replace('Z', '')} (${(b.size / 1024).toFixed(1)} KB)`,
                      })) || []),
                    ]}
                  />
                ) : selectedRestoreTable ? (
                  <p className="text-sm text-gray-500 dark:text-gray-400">No backups found for this table</p>
                ) : null}
              </div>

              <Button
                variant="secondary"
                onClick={() => openConfirmModal('restore')}
                disabled={isAnyMutationLoading || !selectedRestoreTable || !selectedBackupFile}
              >
                <CloudArrowDownIcon className="h-4 w-4 mr-2" />
                {restoreMutation.isPending ? 'Starting...' : 'Restore Table'}
              </Button>
            </div>
          </Card>
        </div>
      </div>

      {/* Command History */}
      <div className="mt-8">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Command History</h2>
        <Card className="p-6">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <ClockIcon className="h-5 w-5 text-cpln-cyan" />
              <h3 className="text-base font-semibold text-gray-900 dark:text-white">Recent Commands</h3>
            </div>
            <div className="flex items-center gap-1">
              {([
                { value: 'all', label: 'All', activeClass: 'bg-gray-700 text-white dark:bg-gray-200 dark:text-gray-900' },
                { value: 'import', label: 'Import', activeClass: 'bg-blue-600 text-white dark:bg-blue-500 dark:text-white' },
                { value: 'repair', label: 'Repair', activeClass: 'bg-yellow-500 text-white dark:bg-yellow-500 dark:text-white' },
                { value: 'backup', label: 'Backup', activeClass: 'bg-purple-600 text-white dark:bg-purple-500 dark:text-white' },
                { value: 'restore', label: 'Restore', activeClass: 'bg-cyan-600 text-white dark:bg-cyan-500 dark:text-white' },
              ] as const).map(({ value, label, activeClass }) => (
                <button
                  key={value}
                  onClick={() => setCommandFilter(value)}
                  className={`px-2.5 py-1 text-xs font-medium rounded-full transition-colors ${
                    commandFilter === value
                      ? activeClass
                      : 'bg-gray-100 dark:bg-stone-700 text-gray-500 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-stone-600'
                  }`}
                >
                  {label}
                </button>
              ))}
            </div>
          </div>

          {(() => {
            const filteredCommands = commandHistoryData?.commands
              ?.filter(cmd => commandFilter === 'all' || cmd.action === commandFilter)
              ?.slice(0, 15);

            if (!filteredCommands?.length) {
              return (
                <p className="text-sm text-gray-500 dark:text-gray-400">
                  {commandFilter === 'all' ? 'No commands have been run yet.' : `No ${commandFilter} commands found.`}
                </p>
              );
            }

            return (
              <div className="space-y-2">
                {filteredCommands.map((cmd) => (
                <div
                  key={cmd.id}
                  className="flex items-center justify-between py-2 px-3 bg-gray-50 dark:bg-stone-700 rounded"
                >
                  <div className="flex items-center gap-3">
                    <span className="text-xs text-gray-500 dark:text-gray-400 font-mono">
                      {new Date(cmd.created).toISOString().replace('T', ' ').replace(/\.\d{3}Z$/, 'Z')}
                    </span>
                    <Badge variant={
                      cmd.action === 'import' ? 'info' :
                      cmd.action === 'backup' ? 'purple' :
                      cmd.action === 'restore' ? 'cyan' : 'warning'
                    }>
                      {cmd.action}
                    </Badge>
                    <span className="font-medium text-gray-900 dark:text-white">
                      {cmd.action === 'import' || cmd.action === 'backup' || cmd.action === 'restore'
                        ? cmd.tableName
                        : `Replica ${cmd.sourceReplica ?? 'auto'}`}
                    </span>
                    <span className="text-xs text-gray-400 font-mono">{cmd.id}</span>
                  </div>
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
                </div>
              ))}
            </div>
            );
          })()}
        </Card>
      </div>

      {/* Confirm Modal */}
      <ConfirmActionModal
        isOpen={confirmModal.isOpen}
        onClose={() => setConfirmModal({ ...confirmModal, isOpen: false })}
        onConfirm={handleConfirmAction}
        title={confirmModal.title}
        message={confirmModal.message}
        confirmText={
          confirmModal.action === 'repair' ? 'Repair' :
          confirmModal.action === 'backup' ? 'Backup' :
          confirmModal.action === 'restore' ? 'Restore' : 'Confirm'
        }
        confirmButtonClass={
          confirmModal.action === 'repair' || confirmModal.action === 'restore'
            ? 'bg-red-600 text-white hover:bg-red-700'
            : 'bg-cpln-cyan text-white hover:bg-cpln-cyan-dark'
        }
        isLoading={isAnyMutationLoading}
      />
    </div>
  );
};
