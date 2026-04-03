import { useQuery } from "@tanstack/react-query";
import * as axios from "axios";
import type { AxiosError, AxiosRequestConfig } from "axios";

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

const fetchCatalogs = (options?: AxiosRequestConfig) =>
  axios.default.get<CatalogsOut>(`/api/catalogs`, options);

const fetchSchemas = (catalog: string, options?: AxiosRequestConfig) =>
  axios.default.get<SchemasOut>(`/api/schemas`, {
    ...options,
    params: { catalog },
  });

const fetchTables = (
  catalog: string,
  schema: string,
  options?: AxiosRequestConfig,
) =>
  axios.default.get<TablesOut>(`/api/tables`, {
    ...options,
    params: { catalog, schema },
  });

const fetchTableInfo = (fullName: string, options?: AxiosRequestConfig) =>
  axios.default.get<TableInfoOut>(`/api/table-info`, {
    ...options,
    params: { full_name: fullName },
  });

export function useCatalogs() {
  return useQuery<CatalogsOut, AxiosError>({
    queryKey: ["/api/catalogs"],
    queryFn: ({ signal }) => fetchCatalogs({ signal }).then((r) => r.data),
  });
}

export function useSchemas(catalog: string | undefined) {
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
) {
  return useQuery<TablesOut, AxiosError>({
    queryKey: ["/api/tables", catalog, schema],
    queryFn: ({ signal }) =>
      fetchTables(catalog!, schema!, { signal }).then((r) => r.data),
    enabled: !!catalog && !!schema,
  });
}

export function useTableInfo(fullName: string | undefined) {
  return useQuery<TableInfoOut, AxiosError>({
    queryKey: ["/api/table-info", fullName],
    queryFn: ({ signal }) =>
      fetchTableInfo(fullName!, { signal }).then((r) => r.data),
    enabled: !!fullName,
  });
}
