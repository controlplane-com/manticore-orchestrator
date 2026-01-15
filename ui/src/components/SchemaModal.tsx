import { useQuery } from '@tanstack/react-query';
import { Modal } from './Modal';
import { LoadingSpinner } from './LoadingSpinner';
import { getTableSchema } from '../api/orchestrator';
import { ExclamationTriangleIcon } from '@heroicons/react/24/outline';

interface SchemaModalProps {
  isOpen: boolean;
  onClose: () => void;
  tableName: string;
}

export const SchemaModal = ({ isOpen, onClose, tableName }: SchemaModalProps) => {
  const { data, isLoading, error } = useQuery({
    queryKey: ['table-schema', tableName],
    queryFn: () => getTableSchema(tableName),
    enabled: isOpen && !!tableName,
  });

  return (
    <Modal isOpen={isOpen} onClose={onClose} title={`Schema: ${tableName}`} size="lg">
      {isLoading ? (
        <div className="flex justify-center py-8">
          <LoadingSpinner size="lg" />
        </div>
      ) : error ? (
        <div className="flex items-center gap-3 text-red-600 dark:text-red-400 py-4">
          <ExclamationTriangleIcon className="h-6 w-6" />
          <span>Failed to load table schema</span>
        </div>
      ) : (data?.columns?.length ?? 0) === 0 ? (
        <div className="text-gray-500 dark:text-gray-400 py-4">
          No columns found in table schema
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 dark:border-gray-700">
                <th className="text-left py-2 px-3 font-semibold text-gray-900 dark:text-white">Field</th>
                <th className="text-left py-2 px-3 font-semibold text-gray-900 dark:text-white">Type</th>
                <th className="text-left py-2 px-3 font-semibold text-gray-900 dark:text-white">Properties</th>
              </tr>
            </thead>
            <tbody>
              {data?.columns.map((col) => (
                <tr key={col.field} className="border-b border-gray-100 dark:border-gray-700 last:border-b-0">
                  <td className="py-2 px-3 font-mono text-gray-900 dark:text-white">{col.field}</td>
                  <td className="py-2 px-3 font-mono text-blue-600 dark:text-blue-400">{col.type}</td>
                  <td className="py-2 px-3 text-gray-500 dark:text-gray-400">{col.props || '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Modal>
  );
};
