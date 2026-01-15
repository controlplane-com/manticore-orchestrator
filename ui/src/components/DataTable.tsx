import { useState, useMemo } from 'react';
import { ChevronUpIcon, ChevronDownIcon, MagnifyingGlassIcon } from '@heroicons/react/24/outline';
import { TableColumn, PaginationState, SortingState } from '../types/table';
import { LoadingPage } from './LoadingSpinner';
import { EmptyState } from './EmptyState';

interface DataTableProps<T> {
  data: T[];
  columns: TableColumn<T>[];
  onRowClick?: (row: T) => void;
  isLoading?: boolean;
  emptyMessage?: string;
  enablePagination?: boolean;
  enableSorting?: boolean;
  enableFiltering?: boolean;
  defaultPageSize?: number;
  getRowKey?: (row: T) => string;
}

export function DataTable<T extends Record<string, any>>({
  data,
  columns,
  onRowClick,
  isLoading = false,
  emptyMessage = 'No data available',
  enablePagination = true,
  enableSorting = true,
  enableFiltering = true,
  defaultPageSize = 10,
  getRowKey = (row) => String(row.id || Math.random()),
}: DataTableProps<T>) {
  const [pagination, setPagination] = useState<PaginationState>({
    pageIndex: 0,
    pageSize: defaultPageSize,
  });
  const [sorting, setSorting] = useState<SortingState | null>(null);
  const [filter, setFilter] = useState('');

  // Filter data
  const filteredData = useMemo(() => {
    if (!enableFiltering || !filter) return data;

    return data.filter((row) =>
      Object.values(row).some((value) =>
        String(value).toLowerCase().includes(filter.toLowerCase())
      )
    );
  }, [data, filter, enableFiltering]);

  // Sort data
  const sortedData = useMemo(() => {
    if (!enableSorting || !sorting) return filteredData;

    return [...filteredData].sort((a, b) => {
      const aVal = a[sorting.column];
      const bVal = b[sorting.column];

      if (aVal === bVal) return 0;
      const comparison = aVal > bVal ? 1 : -1;
      return sorting.direction === 'asc' ? comparison : -comparison;
    });
  }, [filteredData, sorting, enableSorting]);

  // Paginate data
  const paginatedData = useMemo(() => {
    if (!enablePagination) return sortedData;

    const start = pagination.pageIndex * pagination.pageSize;
    const end = start + pagination.pageSize;
    return sortedData.slice(start, end);
  }, [sortedData, pagination, enablePagination]);

  const totalPages = Math.ceil(sortedData.length / pagination.pageSize);

  const handleSort = (column: string) => {
    if (!enableSorting) return;

    setSorting((prev) => {
      if (prev?.column !== column) {
        return { column, direction: 'asc' };
      }
      if (prev.direction === 'asc') {
        return { column, direction: 'desc' };
      }
      return null;
    });
  };

  if (isLoading && data.length === 0) {
    return <LoadingPage message="Loading data..." />;
  }

  return (
    <div className="space-y-6">
      {/* Search Toolbar */}
      {enableFiltering && (
        <div className="flex items-center gap-4">
          <div className="flex-1 max-w-md relative">
            <MagnifyingGlassIcon className="absolute left-3 top-1/2 transform -translate-y-1/2 h-5 w-5 text-gray-400" />
            <input
              type="text"
              placeholder="Search..."
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-cpln-cyan focus:border-transparent"
            />
          </div>
        </div>
      )}

      {/* Table */}
      <div className="bg-white dark:bg-gray-800 rounded-lg overflow-hidden border border-gray-200 dark:border-gray-700">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
            <thead className="bg-gray-50 dark:bg-gray-900">
              <tr>
                {columns.map((col) => {
                  const isSortable = col.sortable !== false && enableSorting;
                  const isCurrentSort = sorting?.column === col.key;

                  return (
                    <th
                      key={String(col.key)}
                      scope="col"
                      className={`px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider ${
                        isSortable ? 'cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-800 select-none' : ''
                      }`}
                      style={{ width: col.width }}
                      onClick={() => isSortable && handleSort(String(col.key))}
                    >
                      <div className="flex items-center gap-2">
                        {col.label}
                        {isSortable && (
                          <span className="flex-shrink-0">
                            {isCurrentSort ? (
                              sorting?.direction === 'asc' ? (
                                <ChevronUpIcon className="h-4 w-4 text-cpln-cyan" />
                              ) : (
                                <ChevronDownIcon className="h-4 w-4 text-cpln-cyan" />
                              )
                            ) : (
                              <div className="h-4 w-4 opacity-30">
                                <ChevronDownIcon className="h-4 w-4" />
                              </div>
                            )}
                          </span>
                        )}
                      </div>
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
              {paginatedData.length === 0 ? (
                <tr>
                  <td colSpan={columns.length}>
                    <EmptyState title={emptyMessage} />
                  </td>
                </tr>
              ) : (
                paginatedData.map((row) => {
                  const rowKey = getRowKey(row);

                  return (
                    <tr
                      key={rowKey}
                      onClick={() => onRowClick?.(row)}
                      className={onRowClick ? 'hover:bg-gray-50 dark:hover:bg-gray-700 cursor-pointer' : 'hover:bg-gray-50 dark:hover:bg-gray-700'}
                    >
                      {columns.map((col) => (
                        <td
                          key={String(col.key)}
                          className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100"
                        >
                          {col.render
                            ? col.render(row[col.key as keyof T], row)
                            : String(row[col.key as keyof T] ?? '')}
                        </td>
                      ))}
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {enablePagination && sortedData.length > 0 && (
          <div className="bg-white dark:bg-gray-800 px-4 py-3 flex items-center justify-between border-t border-gray-200 dark:border-gray-700 sm:px-6">
            <div className="flex-1 flex justify-between sm:hidden">
              <button
                onClick={() => setPagination((p) => ({ ...p, pageIndex: Math.max(0, p.pageIndex - 1) }))}
                disabled={pagination.pageIndex === 0}
                className="relative inline-flex items-center px-4 py-2 border border-gray-300 dark:border-gray-600 text-sm font-medium rounded-md text-gray-700 dark:text-gray-200 bg-white dark:bg-gray-800 hover:bg-gray-50 dark:hover:bg-gray-700 disabled:opacity-50"
              >
                Previous
              </button>
              <button
                onClick={() => setPagination((p) => ({ ...p, pageIndex: Math.min(totalPages - 1, p.pageIndex + 1) }))}
                disabled={pagination.pageIndex >= totalPages - 1}
                className="ml-3 relative inline-flex items-center px-4 py-2 border border-gray-300 dark:border-gray-600 text-sm font-medium rounded-md text-gray-700 dark:text-gray-200 bg-white dark:bg-gray-800 hover:bg-gray-50 dark:hover:bg-gray-700 disabled:opacity-50"
              >
                Next
              </button>
            </div>
            <div className="hidden sm:flex-1 sm:flex sm:items-center sm:justify-between">
              <div>
                <p className="text-sm text-gray-700 dark:text-gray-300">
                  Showing <span className="font-medium">{pagination.pageIndex * pagination.pageSize + 1}</span> to{' '}
                  <span className="font-medium">
                    {Math.min((pagination.pageIndex + 1) * pagination.pageSize, sortedData.length)}
                  </span>{' '}
                  of <span className="font-medium">{sortedData.length}</span> results
                </p>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setPagination((p) => ({ ...p, pageIndex: Math.max(0, p.pageIndex - 1) }))}
                  disabled={pagination.pageIndex === 0}
                  className="relative inline-flex items-center px-2 py-2 border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-sm font-medium text-gray-500 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-700 disabled:opacity-50 rounded-l-md"
                >
                  Previous
                </button>
                <span className="text-sm text-gray-700 dark:text-gray-300">
                  Page {pagination.pageIndex + 1} of {totalPages}
                </span>
                <button
                  onClick={() => setPagination((p) => ({ ...p, pageIndex: Math.min(totalPages - 1, p.pageIndex + 1) }))}
                  disabled={pagination.pageIndex >= totalPages - 1}
                  className="relative inline-flex items-center px-2 py-2 border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-sm font-medium text-gray-500 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-700 disabled:opacity-50 rounded-r-md"
                >
                  Next
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
