import { useQuery } from "@tanstack/react-query";
import * as axios from "axios";
import type { AxiosError, AxiosRequestConfig } from "axios";

export interface WarehouseInfo {
  id: string;
  name: string;
  state: string | null;
}

export interface WarehousesOut {
  warehouses: WarehouseInfo[];
}

export interface ClusterInfo {
  id: string;
  name: string;
  state: string | null;
}

export interface ClustersOut {
  clusters: ClusterInfo[];
}

export interface CreateChecksTableOut {
  table_name: string;
  created: boolean;
}

export interface CatalogsOut {
  catalogs: string[];
}

export interface SchemasOut {
  schemas: string[];
}

export interface TablesOut {
  tables: string[];
}

export interface ColumnInfoOut {
  name: string;
  type_text: string;
  comment: string | null;
  nullable: boolean;
  position: number | null;
}

export interface TableInfoOut {
  full_name: string;
  table_type: string | null;
  data_source_format: string | null;
  owner: string | null;
  comment: string | null;
  columns: ColumnInfoOut[];
  sample_data: Record<string, unknown>[];
  sample_data_error: string | null;
}

const fetchCatalogs = (
  warehouseId?: string,
  options?: AxiosRequestConfig,
) =>
  axios.default.get<CatalogsOut>(`/api/catalogs`, {
    ...options,
    params: warehouseId ? { warehouse_id: warehouseId } : undefined,
  });

const fetchSchemas = (
  catalog: string,
  options?: AxiosRequestConfig,
) =>
  axios.default.get<SchemasOut>(`/api/schemas`, {
    ...options,
    params: { catalog },
  });

const fetchTables = (
  catalog: string,
  schema: string,
  warehouseId?: string,
  options?: AxiosRequestConfig,
) =>
  axios.default.get<TablesOut>(`/api/tables`, {
    ...options,
    params: warehouseId
      ? { catalog, schema, warehouse_id: warehouseId }
      : { catalog, schema },
  });

const fetchTableInfo = (fullName: string, options?: AxiosRequestConfig) =>
  axios.default.get<TableInfoOut>(`/api/table-info`, {
    ...options,
    params: { full_name: fullName },
  });

export function isTableInfoQueryEnabled(fullName: string | undefined): boolean {
  return !!fullName;
}

export function useCatalogs(warehouseId?: string) {
  return useQuery<CatalogsOut, AxiosError>({
    queryKey: ["/api/catalogs", warehouseId],
    queryFn: ({ signal }) =>
      fetchCatalogs(warehouseId, { signal }).then((r) => r.data),
  });
}

export function useSchemas(
  catalog: string | undefined,
) {
  return useQuery<SchemasOut, AxiosError>({
    queryKey: ["/api/schemas", catalog],
    queryFn: ({ signal }) =>
      fetchSchemas(catalog!, { signal }).then((r) => r.data),
    enabled: !!catalog,
  });
}

export function useTables(
  catalog: string | undefined,
  schema: string | undefined,
  warehouseId?: string,
) {
  return useQuery<TablesOut, AxiosError>({
    queryKey: ["/api/tables", catalog, schema, warehouseId],
    queryFn: ({ signal }) =>
      fetchTables(catalog!, schema!, warehouseId, { signal }).then((r) => r.data),
    enabled: !!catalog && !!schema,
  });
}

export function useTableInfo(fullName: string | undefined) {
  return useQuery<TableInfoOut, AxiosError>({
    queryKey: ["/api/table-info", fullName],
    queryFn: ({ signal }) => fetchTableInfo(fullName!, { signal }).then((r) => r.data),
    enabled: isTableInfoQueryEnabled(fullName),
  });
}

const fetchWarehouses = (options?: AxiosRequestConfig) =>
  axios.default.get<WarehousesOut>(`/api/warehouses`, options);

const fetchClusters = (options?: AxiosRequestConfig) =>
  axios.default.get<ClustersOut>(`/api/clusters`, options);

export function useWarehouses() {
  return useQuery<WarehousesOut, AxiosError>({
    queryKey: ["/api/warehouses"],
    queryFn: ({ signal }) =>
      fetchWarehouses({ signal }).then((r) => r.data),
  });
}

export function useClusters() {
  return useQuery<ClustersOut, AxiosError>({
    queryKey: ["/api/clusters"],
    queryFn: ({ signal }) => fetchClusters({ signal }).then((r) => r.data),
  });
}

export async function createChecksTable(
  tableName: string,
  warehouseId?: string,
): Promise<CreateChecksTableOut> {
  const resp = await axios.default.post<CreateChecksTableOut>(
    `/api/checks-table/create`,
    null,
    {
      params: warehouseId
        ? { table_name: tableName, warehouse_id: warehouseId }
        : { table_name: tableName },
    },
  );
  return resp.data;
}
