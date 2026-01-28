import { useState, useMemo } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { PageHeader } from '../components/PageHeader';
import { Card } from '../components/Card';
import { Badge } from '../components/Badge';
import { FormSelect } from '../components/FormSelect';
import { LoadingSpinner } from '../components/LoadingSpinner';
import { DataTable } from '../components/DataTable';
import { SqlEditor } from '../components/SqlEditor';
import { useToast } from '../hooks/useToast';
import { getCluster, executeSqlQuery } from '../api/orchestrator';
import type {
  SqlQueryRequest,
  SqlQueryResponse,
  SqlBroadcastResponse,
  SqlReplicaResult,
} from '../types/api';
import type { TableColumn } from '../types/table';
import {
  ClockIcon,
  TableCellsIcon,
  ServerStackIcon,
  CheckCircleIcon,
  XCircleIcon,
} from '@heroicons/react/24/outline';

// Type guard for broadcast response
function isBroadcastResponse(
  response: SqlQueryResponse | SqlBroadcastResponse
): response is SqlBroadcastResponse {
  return 'results' in response && Array.isArray(response.results);
}

// Helper to add cluster prefix to table names in SQL queries
// Handles: FROM table, INTO table, UPDATE table, JOIN table
function addClusterPrefix(sql: string, cluster: string): string {
  if (!cluster) return sql;

  // Pattern matches table names after FROM, INTO, UPDATE, JOIN (case insensitive)
  // Avoids prefixing if already has a colon (already prefixed)
  const patterns = [
    /(\bFROM\s+)([a-zA-Z_][a-zA-Z0-9_]*)(?!:)/gi,
    /(\bINTO\s+)([a-zA-Z_][a-zA-Z0-9_]*)(?!:)/gi,
    /(\bUPDATE\s+)([a-zA-Z_][a-zA-Z0-9_]*)(?!:)/gi,
    /(\bJOIN\s+)([a-zA-Z_][a-zA-Z0-9_]*)(?!:)/gi,
  ];

  let result = sql;
  for (const pattern of patterns) {
    result = result.replace(pattern, `$1${cluster}:$2`);
  }
  return result;
}

export const Query = () => {
  const toast = useToast();
  const [query, setQuery] = useState('');
  const [targetReplica, setTargetReplica] = useState('broadcast'); // 'broadcast' = all replicas, '' = load balanced, number = specific replica
  const [activeResultTab, setActiveResultTab] = useState(0);
  const [clusterPrefix, setClusterPrefix] = useState('manticore');
  const [autoPrefix, setAutoPrefix] = useState(true);

  // Store the last successful response
  const [lastResponse, setLastResponse] = useState<
    SqlQueryResponse | SqlBroadcastResponse | null
  >(null);

  // Fetch cluster status for replica dropdown
  const { data: clusterData, isLoading: clusterLoading } = useQuery({
    queryKey: ['cluster'],
    queryFn: getCluster,
    refetchInterval: 30000,
  });

  // Build replica options for dropdown
  const replicaOptions = useMemo(() => {
    const options = [
      { value: 'broadcast', label: 'Broadcast (all replicas)' },
      { value: '', label: 'Load Balanced' },
    ];

    if (clusterData?.replicas) {
      clusterData.replicas
        .filter((r) => r.status === 'online')
        .forEach((r) => {
          options.push({
            value: String(r.index),
            label: `Replica ${r.index}${r.clusterStatus ? ` (${r.clusterStatus})` : ''}`,
          });
        });
    }

    return options;
  }, [clusterData]);

  // Execute query mutation
  const queryMutation = useMutation({
    mutationFn: (request: SqlQueryRequest) => executeSqlQuery(request),
    onSuccess: (data) => {
      setLastResponse(data);
      setActiveResultTab(0);

      if (isBroadcastResponse(data)) {
        const successCount = data.results.filter(
          (r) => r.status === 'success'
        ).length;
        const errorCount = data.results.filter(
          (r) => r.status === 'error'
        ).length;

        if (errorCount > 0) {
          toast.warning(
            'Query completed with errors',
            `${successCount} succeeded, ${errorCount} failed`
          );
        } else {
          toast.success(
            'Query executed on all replicas',
            `${successCount} replicas responded`
          );
        }
      } else {
        const rowCount = data.rowCount ?? data.rows?.length ?? 0;
        toast.success(
          'Query executed',
          `${rowCount} rows returned in ${data.executionTimeMs ?? 0}ms`
        );
      }
    },
    onError: (error: any) => {
      toast.error(
        'Query failed',
        error.response?.data?.error || error.message
      );
    },
  });

  const handleExecute = () => {
    if (!query.trim()) return;

    // Apply cluster prefix if enabled
    let finalQuery = query.trim();
    if (autoPrefix && clusterPrefix) {
      finalQuery = addClusterPrefix(finalQuery, clusterPrefix);
    }

    const request: SqlQueryRequest = {
      query: finalQuery,
    };

    if (targetReplica === 'broadcast') {
      request.broadcast = true;
    } else if (targetReplica !== '') {
      request.replicaIndex = parseInt(targetReplica, 10);
    }
    // else: empty string means load balanced (no replicaIndex, no broadcast)

    queryMutation.mutate(request);
  };

  // Generate columns from the response
  const generateColumns = (
    result: SqlQueryResponse | SqlReplicaResult
  ): TableColumn<Record<string, any>>[] => {
    if (!result.columns || result.columns.length === 0) {
      return [];
    }

    return result.columns.map((col) => ({
      key: col.name,
      label: col.name,
      sortable: true,
      render: (value: any) => {
        if (value === null) {
          return <span className="text-gray-400 italic">NULL</span>;
        }
        if (typeof value === 'boolean') {
          return value ? 'true' : 'false';
        }
        if (typeof value === 'object') {
          return JSON.stringify(value);
        }
        return String(value);
      },
    }));
  };

  // Render single result
  const renderSingleResult = (
    result: SqlQueryResponse | SqlReplicaResult,
    replicaLabel?: string
  ) => {
    const columns = generateColumns(result);
    const rawRows = result.rows ?? [];
    // Add __rowIndex for unique key generation
    const rows = rawRows.map((row, idx) => ({ ...row, __rowIndex: idx }));
    const rowCount = result.rowCount ?? rawRows.length;

    return (
      <div className="space-y-4">
        {/* Stats bar */}
        <div className="flex items-center gap-4 text-sm text-gray-600 dark:text-gray-400">
          <div className="flex items-center gap-1.5">
            <TableCellsIcon className="h-4 w-4" />
            <span>{rowCount} rows</span>
          </div>
          {result.executionTimeMs !== undefined && (
            <div className="flex items-center gap-1.5">
              <ClockIcon className="h-4 w-4" />
              <span>{result.executionTimeMs}ms</span>
            </div>
          )}
          {replicaLabel && (
            <div className="flex items-center gap-1.5">
              <ServerStackIcon className="h-4 w-4" />
              <span>{replicaLabel}</span>
            </div>
          )}
        </div>

        {/* Results table */}
        {columns.length > 0 && rows.length > 0 ? (
          <DataTable
            data={rows}
            columns={columns}
            enablePagination={true}
            enableSorting={true}
            enableFiltering={true}
            defaultPageSize={25}
            getRowKey={(row) => String(row.__rowIndex)}
          />
        ) : (
          <div className="text-center py-8 text-gray-500 dark:text-gray-400">
            {columns.length === 0
              ? 'Query executed successfully (no columns returned)'
              : 'No rows returned'}
          </div>
        )}
      </div>
    );
  };

  // Render broadcast results with tabs
  const renderBroadcastResults = (response: SqlBroadcastResponse) => {
    return (
      <div className="space-y-4">
        {/* Tab buttons */}
        <div className="flex flex-wrap gap-2 border-b border-gray-200 dark:border-stone-700 pb-2">
          {response.results.map((result, idx) => (
            <button
              key={idx}
              onClick={() => setActiveResultTab(idx)}
              className={`flex items-center gap-2 px-3 py-1.5 rounded-t-md text-sm font-medium transition-colors ${
                activeResultTab === idx
                  ? 'bg-stone-200 dark:bg-stone-700 text-gray-900 dark:text-white'
                  : 'text-gray-600 dark:text-gray-400 hover:bg-stone-100 dark:hover:bg-stone-800'
              }`}
            >
              {result.status === 'success' ? (
                <CheckCircleIcon className="h-4 w-4 text-green-500" />
              ) : (
                <XCircleIcon className="h-4 w-4 text-red-500" />
              )}
              Replica {result.replicaIndex}
              <Badge
                variant={result.status === 'success' ? 'success' : 'error'}
              >
                {result.status === 'success'
                  ? `${result.rowCount ?? result.rows?.length ?? 0} rows`
                  : 'error'}
              </Badge>
            </button>
          ))}
        </div>

        {/* Active tab content */}
        {response.results[activeResultTab] && (
          <div>
            {response.results[activeResultTab].status === 'error' ? (
              <div className="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
                <p className="text-red-700 dark:text-red-300">
                  {response.results[activeResultTab].error ||
                    'Unknown error occurred'}
                </p>
              </div>
            ) : (
              renderSingleResult(
                response.results[activeResultTab],
                `Replica ${response.results[activeResultTab].replicaIndex}`
              )
            )}
          </div>
        )}
      </div>
    );
  };

  return (
    <div>
      <PageHeader
        title="SQL Query"
        description="Execute SQL queries against the Manticore cluster"
      />

      {/* Query Input Section */}
      <Card className="p-6 mb-6">
        {/* Target Selection and Cluster Prefix */}
        <div className="flex flex-wrap items-end gap-4 mb-4">
          <div className="w-64">
            {clusterLoading ? (
              <div className="flex items-center gap-2">
                <LoadingSpinner size="sm" />
                <span className="text-sm text-gray-500">
                  Loading replicas...
                </span>
              </div>
            ) : (
              <FormSelect
                label="Target"
                value={targetReplica}
                onChange={setTargetReplica}
                options={replicaOptions}
              />
            )}
          </div>

          <div className="w-40">
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Cluster Prefix
            </label>
            <input
              type="text"
              value={clusterPrefix}
              onChange={(e) => setClusterPrefix(e.target.value)}
              className="w-full px-3 py-2 rounded-md border border-gray-300 dark:border-stone-600 bg-white dark:bg-stone-700 text-gray-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-cpln-cyan focus:border-transparent"
              placeholder="manticore"
            />
          </div>

          <div className="flex items-center gap-2 pb-2">
            <input
              type="checkbox"
              id="autoPrefix"
              checked={autoPrefix}
              onChange={(e) => setAutoPrefix(e.target.checked)}
              className="h-4 w-4 rounded border-gray-300 text-cpln-cyan focus:ring-cpln-cyan"
            />
            <label htmlFor="autoPrefix" className="text-sm text-gray-700 dark:text-gray-300">
              Auto-prefix tables
            </label>
          </div>
        </div>

        {/* SQL Editor */}
        <SqlEditor
          value={query}
          onChange={setQuery}
          onExecute={handleExecute}
          disabled={queryMutation.isPending}
        />
      </Card>

      {/* Results Section */}
      <Card className="p-6">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Results
        </h2>

        {queryMutation.isPending ? (
          <div className="flex flex-col items-center justify-center py-12">
            <LoadingSpinner size="lg" />
            <p className="mt-4 text-gray-500 dark:text-gray-400">
              Executing query...
            </p>
          </div>
        ) : lastResponse ? (
          isBroadcastResponse(lastResponse) ? (
            renderBroadcastResults(lastResponse)
          ) : (
            renderSingleResult(
              lastResponse,
              lastResponse.replicaIndex !== undefined
                ? `Replica ${lastResponse.replicaIndex}`
                : undefined
            )
          )
        ) : (
          <div className="text-center py-12 text-gray-500 dark:text-gray-400">
            <TableCellsIcon className="h-12 w-12 mx-auto mb-4 opacity-50" />
            <p>Execute a query to see results</p>
          </div>
        )}
      </Card>
    </div>
  );
};
