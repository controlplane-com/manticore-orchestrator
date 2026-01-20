import axios from 'axios';
import type {
  ApiResponse,
  ImportRequest,
  RepairRequest,
  ConfigResponse,
  ClusterResponse,
  ClusterDiscoveryResponse,
  TablesStatusResponse,
  TableSchemaResponse,
  ImportsResponse,
  RepairsResponse,
  CommandHistoryResponse,
  SqlQueryRequest,
  SqlQueryResponse,
  SqlBroadcastResponse,
} from '../types/api';

const api = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add auth token from localStorage if available
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('orchestrator-token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Health check (authenticated)
export const getHealth = async (): Promise<ApiResponse> => {
  const response = await api.get('/health');
  return response.data;
};

// Status check (unauthenticated)
export const getStatus = async (): Promise<ApiResponse> => {
  const response = await api.get('/status');
  return response.data;
};

// Get cluster configuration (authenticated)
export const getConfig = async (): Promise<ConfigResponse> => {
  const response = await api.get('/config');
  return response.data;
};

// Get cluster status with replica details (authenticated)
export const getCluster = async (): Promise<ClusterResponse> => {
  const response = await api.get('/cluster');
  return response.data;
};

// Get cluster discovery info including split-brain detection (authenticated)
export const getClusterDiscovery = async (): Promise<ClusterDiscoveryResponse> => {
  const response = await api.get('/cluster/discover');
  return response.data;
};

// Get per-replica table status (authenticated)
export const getTablesStatus = async (): Promise<TablesStatusResponse> => {
  const response = await api.get('/tables/status');
  return response.data;
};

// Import data into a table (authenticated)
export const importTable = async (request: ImportRequest): Promise<ApiResponse> => {
  const response = await api.post('/import', request);
  return response.data;
};

// Repair cluster (authenticated)
export const repairCluster = async (request?: RepairRequest): Promise<ApiResponse> => {
  const response = await api.post('/repair', request || {});
  return response.data;
};

// Get table schema (authenticated)
export const getTableSchema = async (tableName: string): Promise<TableSchemaResponse> => {
  const response = await api.get(`/tables/${tableName}/schema`);
  return response.data;
};

// Get active import operations (authenticated)
export const getImports = async (): Promise<ImportsResponse> => {
  const response = await api.get('/imports');
  return response.data;
};

// Get active repair operations (authenticated)
export const getRepairs = async (): Promise<RepairsResponse> => {
  const response = await api.get('/repairs');
  return response.data;
};

// Get command history (authenticated)
export const getCommandHistory = async (): Promise<CommandHistoryResponse> => {
  const response = await api.get('/commands');
  return response.data;
};

// Execute SQL query (authenticated)
export const executeSqlQuery = async (
  request: SqlQueryRequest
): Promise<SqlQueryResponse | SqlBroadcastResponse> => {
  const response = await api.post('/query', request);
  return response.data;
};

// Export the axios instance for direct use if needed
export { api };
