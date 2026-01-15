import { useQuery } from '@tanstack/react-query';
import { PageHeader } from '../components/PageHeader';
import { Card } from '../components/Card';
import { Badge } from '../components/Badge';
import { LoadingSpinner, LoadingPage } from '../components/LoadingSpinner';
import { Button } from '../components/Button';
import { getStatus, getCluster, getConfig } from '../api/orchestrator';
import type { ReplicaStatus } from '../types/api';
import {
  HeartIcon,
  ServerIcon,
  ArrowPathIcon,
  CheckCircleIcon,
  XCircleIcon,
  ExclamationTriangleIcon,
  ServerStackIcon,
  MinusCircleIcon,
} from '@heroicons/react/24/outline';

// Get badge for cluster status
const getClusterStatusBadge = (status: string | undefined) => {
  switch (status) {
    case 'healthy':
      return (
        <Badge variant="success">
          <CheckCircleIcon className="h-4 w-4 mr-1" />
          Healthy
        </Badge>
      );
    case 'degraded':
      return (
        <Badge variant="warning">
          <ExclamationTriangleIcon className="h-4 w-4 mr-1" />
          Degraded
        </Badge>
      );
    case 'uninitialized':
      return (
        <Badge variant="info">
          <ServerIcon className="h-4 w-4 mr-1" />
          Uninitialized
        </Badge>
      );
    default:
      return <Badge variant="warning">Unknown</Badge>;
  }
};

// Get badge for replica cluster status
const getReplicaStatusBadge = (replica: ReplicaStatus) => {
  // Handle non-online statuses first
  if (replica.status === 'not_in_use') {
    return (
      <Badge variant="muted">
        <MinusCircleIcon className="h-4 w-4 mr-1" />
        Not In Use
      </Badge>
    );
  }

  if (replica.status === 'offline' || replica.status === 'error') {
    return (
      <Badge variant="error">
        <XCircleIcon className="h-4 w-4 mr-1" />
        {replica.status === 'error' ? 'Error' : 'Offline'}
      </Badge>
    );
  }

  // Online replica - show cluster status
  if (!replica.clusterStatus) {
    return (
      <Badge variant="info">
        <ServerIcon className="h-4 w-4 mr-1" />
        Not in Cluster
      </Badge>
    );
  }

  switch (replica.clusterStatus) {
    case 'primary':
      return (
        <Badge variant="success">
          <CheckCircleIcon className="h-4 w-4 mr-1" />
          Primary
        </Badge>
      );
    case 'synced':
      return (
        <Badge variant="success">
          <CheckCircleIcon className="h-4 w-4 mr-1" />
          Synced
        </Badge>
      );
    case 'donor':
      return (
        <Badge variant="info">
          <ArrowPathIcon className="h-4 w-4 mr-1" />
          Donor
        </Badge>
      );
    case 'joiner':
      return (
        <Badge variant="warning">
          <ArrowPathIcon className="h-4 w-4 mr-1" />
          Joining
        </Badge>
      );
    default:
      return (
        <Badge variant="warning">
          {replica.clusterStatus}
        </Badge>
      );
  }
};

// ReplicaCard component
const ReplicaCard = ({ replica }: { replica: ReplicaStatus }) => {
  const isNotInUse = replica.status === 'not_in_use';
  const hasError = replica.status === 'offline' || replica.status === 'error';

  // Status indicator color
  const indicatorColor = replica.status === 'online' ? 'bg-green-500'
    : isNotInUse ? 'bg-gray-400'
    : 'bg-red-500';

  return (
    <Card className="p-4">
      <div className="flex items-start justify-between mb-3">
        <div className="flex items-center gap-3">
          <div className={`p-2 rounded-lg ${hasError ? 'bg-red-100 dark:bg-red-900/30' : isNotInUse ? 'bg-gray-100 dark:bg-gray-800' : 'bg-cpln-cyan/10'}`}>
            <ServerIcon className={`h-6 w-6 ${hasError ? 'text-red-500' : isNotInUse ? 'text-gray-400' : 'text-cpln-cyan'}`} />
          </div>
          <div>
            <h4 className="font-medium text-gray-900 dark:text-white">
              Replica {replica.index}
            </h4>
            <p className="text-xs text-gray-500 dark:text-gray-400 font-mono truncate max-w-[200px]" title={replica.endpoint}>
              {replica.endpoint.replace('http://', '').split(':')[0]}
            </p>
          </div>
        </div>
        <div className={`w-3 h-3 rounded-full ${indicatorColor}`} />
      </div>

      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <span className="text-sm text-gray-500 dark:text-gray-400">Status</span>
          {getReplicaStatusBadge(replica)}
        </div>

        {replica.error && replica.status !== 'not_in_use' && (
          <div className="mt-2 p-2 bg-red-50 dark:bg-red-900/20 rounded text-xs text-red-600 dark:text-red-400">
            {replica.error}
          </div>
        )}
      </div>
    </Card>
  );
};

export const Health = () => {
  const {
    data: clusterData,
    isLoading: clusterLoading,
    error: clusterError,
    refetch: refetchCluster,
  } = useQuery({
    queryKey: ['cluster'],
    queryFn: getCluster,
    refetchInterval: 30000,
  });

  const {
    data: statusData,
    isLoading: statusLoading,
    error: statusError,
    refetch: refetchStatus,
  } = useQuery({
    queryKey: ['status'],
    queryFn: getStatus,
    refetchInterval: 10000,
  });

  const {
    data: configData,
  } = useQuery({
    queryKey: ['config'],
    queryFn: getConfig,
    refetchInterval: 60000,
  });

  const handleRefresh = () => {
    refetchCluster();
    refetchStatus();
  };

  if (clusterLoading && statusLoading) {
    return <LoadingPage message="Loading health status..." />;
  }

  const onlineCount = clusterData?.replicas?.filter(r => r.status === 'online').length || 0;
  const inUseCount = clusterData?.replicas?.filter(r => r.status !== 'not_in_use').length || 0;

  return (
    <div>
      <PageHeader
        title="Health"
        description="Cluster health and status monitoring"
        actions={
          <Button variant="secondary" onClick={handleRefresh}>
            <ArrowPathIcon className="h-4 w-4 mr-2" />
            Refresh
          </Button>
        }
      />

      {/* Status Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
        {/* Orchestrator Status */}
        <Card className="p-6">
          <div className="flex items-center gap-4 mb-4">
            <ServerIcon className="h-8 w-8 text-cpln-cyan" />
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
              Orchestrator
            </h3>
          </div>

          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-500 dark:text-gray-400">Status</span>
              {statusLoading ? (
                <LoadingSpinner size="sm" />
              ) : statusError ? (
                <Badge variant="error">
                  <XCircleIcon className="h-4 w-4 mr-1" />
                  Unavailable
                </Badge>
              ) : statusData?.status === 'ok' ? (
                <Badge variant="success">
                  <CheckCircleIcon className="h-4 w-4 mr-1" />
                  Running
                </Badge>
              ) : (
                <Badge variant="warning">Unknown</Badge>
              )}
            </div>

            {statusError && (
              <p className="text-sm text-red-600 dark:text-red-400">
                Could not connect to orchestrator service
              </p>
            )}
          </div>
        </Card>

        {/* Cluster Health */}
        <Card className="p-6">
          <div className="flex items-center gap-4 mb-4">
            <HeartIcon className="h-8 w-8 text-cpln-cyan" />
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
              Cluster Health
            </h3>
          </div>

          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-500 dark:text-gray-400">Status</span>
              {clusterLoading ? (
                <LoadingSpinner size="sm" />
              ) : clusterError ? (
                <Badge variant="error">
                  <XCircleIcon className="h-4 w-4 mr-1" />
                  Error
                </Badge>
              ) : (
                getClusterStatusBadge(clusterData?.status)
              )}
            </div>

            {clusterError && (
              <p className="text-sm text-red-600 dark:text-red-400">
                Could not retrieve cluster health status
              </p>
            )}
          </div>
        </Card>

        {/* Replicas Summary */}
        <Card className="p-6">
          <div className="flex items-center gap-4 mb-4">
            <ServerStackIcon className="h-8 w-8 text-cpln-cyan" />
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
              Replicas
            </h3>
          </div>

          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-500 dark:text-gray-400">Online</span>
              {clusterLoading ? (
                <LoadingSpinner size="sm" />
              ) : (
                <span className="text-2xl font-semibold">
                  <span className={onlineCount === inUseCount ? 'text-green-600' : 'text-yellow-600'}>
                    {onlineCount}
                  </span>
                  <span className="text-gray-400 text-lg">/{inUseCount}</span>
                </span>
              )}
            </div>
            {configData && (
              <p className="text-xs text-gray-500 dark:text-gray-400">
                Workload: {configData.workloadName}
              </p>
            )}
          </div>
        </Card>
      </div>

      {/* Replica Status */}
      <div className="mb-8">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Replica Status
        </h2>

        {clusterLoading ? (
          <div className="flex justify-center py-8">
            <LoadingSpinner size="lg" />
          </div>
        ) : clusterError ? (
          <div className="text-center py-8 text-red-600 dark:text-red-400">
            Failed to load replica status
          </div>
        ) : clusterData?.replicas?.length ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
            {clusterData.replicas.map((replica) => (
              <ReplicaCard key={replica.index} replica={replica} />
            ))}
          </div>
        ) : (
          <div className="text-center py-8 text-gray-500 dark:text-gray-400">
            No replicas found
          </div>
        )}
      </div>

      {/* Cluster Configuration */}
      {configData && (
        <div>
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
            Cluster Configuration
          </h2>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Card className="p-4">
              <h4 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-2">
                Workload Name
              </h4>
              <code className="text-sm text-gray-900 dark:text-gray-100 font-mono">
                {configData.workloadName}
              </code>
            </Card>

            <Card className="p-4">
              <h4 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-2">
                GVC
              </h4>
              <code className="text-sm text-gray-900 dark:text-gray-100 font-mono">
                {configData.gvc || 'N/A'}
              </code>
            </Card>

            <Card className="p-4">
              <h4 className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-2">
                Location
              </h4>
              <code className="text-sm text-gray-900 dark:text-gray-100 font-mono">
                {configData.location || 'N/A'}
              </code>
            </Card>
          </div>
        </div>
      )}
    </div>
  );
};
