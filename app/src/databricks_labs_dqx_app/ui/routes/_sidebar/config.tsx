import { createFileRoute } from "@tanstack/react-router";
import { Suspense, useState, useEffect } from "react";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { useGetSettingsSuspense, useSaveSettings } from "@/lib/api";
import {
  useCatalogs,
  useClusters,
  useSchemas,
  useTables,
  useWarehouses,
  createChecksTable,
} from "@/lib/explorer-api";
import selector from "@/lib/selector";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import {
  AlertCircle,
  Settings,
  Save,
  Loader2,
  Database,
  Server,
  Plus,
} from "lucide-react";
import { toast } from "sonner";
import { FadeIn } from "@/components/anim/FadeIn";
import { ShinyText } from "@/components/anim/ShinyText";

export const Route = createFileRoute("/_sidebar/config")({
  component: () => <ConfigPage />,
});

/* ─── App Settings (compute + data) ──────────────────────────── */

function AppSettingsCard() {
  const { data: settings } = useGetSettingsSuspense(selector());
  const { mutate: saveSettings, isPending: isSaving } = useSaveSettings();
  const queryClient = useQueryClient();

  // Local state initialised from server
  const [defaultCompute, setDefaultCompute] = useState(
    settings.use_serverless ?? true
      ? "serverless"
      : ((settings as { default_cluster_id?: string | null }).default_cluster_id ??
        "serverless"),
  );
  const [warehouseId, setWarehouseId] = useState(
    settings.default_warehouse_id ?? "",
  );
  const [catalog, setCatalog] = useState(settings.default_catalog ?? "");
  const [schema, setSchema] = useState(settings.default_schema ?? "");
  const [checksTable, setChecksTable] = useState(
    settings.default_checks_table ?? "",
  );
  const [isCreatingTable, setIsCreatingTable] = useState(false);

  // Data hooks
  const { data: clustersData, isLoading: clustersLoading } = useClusters();
  const { data: warehousesData, isLoading: warehousesLoading } =
    useWarehouses();
  const {
    data: catalogsData,
    isLoading: catalogsLoading,
    error: catalogsError,
  } = useCatalogs(warehouseId || undefined);
  const { data: schemasData, isLoading: schemasLoading } = useSchemas(
    catalog || undefined,
  );
  const { data: tablesData, isLoading: tablesLoading } = useTables(
    catalog || undefined,
    schema || undefined,
    warehouseId || undefined,
  );
  const availableChecksTables =
    tablesData?.tables.map((tableName) => `${catalog}.${schema}.${tableName}`) ??
    [];
  const ruleTableOptions = checksTable
    ? Array.from(new Set([checksTable, ...availableChecksTables]))
    : availableChecksTables;

  // Sync from server when settings change
  useEffect(() => {
    setDefaultCompute(
      settings.use_serverless ?? true
        ? "serverless"
        : ((settings as { default_cluster_id?: string | null })
            .default_cluster_id ?? "serverless"),
    );
    setWarehouseId(settings.default_warehouse_id ?? "");
    setCatalog(settings.default_catalog ?? "");
    setSchema(settings.default_schema ?? "");
    setChecksTable(settings.default_checks_table ?? "");
  }, [settings]);

  const isDirty =
    defaultCompute !==
      ((settings.use_serverless ?? true)
        ? "serverless"
        : ((settings as { default_cluster_id?: string | null })
            .default_cluster_id ?? "serverless")) ||
    warehouseId !== (settings.default_warehouse_id ?? "") ||
    catalog !== (settings.default_catalog ?? "") ||
    schema !== (settings.default_schema ?? "") ||
    checksTable !== (settings.default_checks_table ?? "");

  const handleSave = () => {
    saveSettings(
      {
        data: {
          install_folder: settings.install_folder,
          use_serverless: defaultCompute === "serverless",
          default_cluster_id:
            defaultCompute === "serverless" ? null : defaultCompute,
          default_warehouse_id: warehouseId || null,
          default_catalog: catalog || null,
          default_schema: schema || null,
          default_checks_table: checksTable || null,
        },
      },
      {
        onSuccess: async () => {
          await queryClient.invalidateQueries({ queryKey: ["/api/settings"] });
          toast.success("Settings saved");
        },
        onError: (error) => {
          const msg =
            (error as any).response?.data?.detail || error.message;
          toast.error("Failed to save settings: " + msg);
        },
      },
    );
  };

  const handleCatalogChange = (value: string) => {
    setCatalog(value);
    setSchema("");
    // Update checks table prefix when catalog changes
    if (checksTable) {
      const parts = checksTable.split(".");
      if (parts.length === 3) {
        setChecksTable(`${value}.${parts[1]}.${parts[2]}`);
      }
    }
  };

  const handleSchemaChange = (value: string) => {
    setSchema(value);
    // Update checks table prefix when schema changes
    if (checksTable) {
      const parts = checksTable.split(".");
      if (parts.length === 3) {
        setChecksTable(`${catalog}.${value}.${parts[2]}`);
      }
    }
  };

  const composedTableName =
    catalog && schema ? `${catalog}.${schema}.checks` : "";

  const handleCreateTable = async () => {
    const tableName = checksTable || composedTableName;
    if (!tableName) {
      toast.error("Please specify a table name first");
      return;
    }
    if (tableName.split(".").length !== 3) {
      toast.error("Table name must be catalog.schema.table");
      return;
    }
    setIsCreatingTable(true);
    try {
      const result = await createChecksTable(tableName, warehouseId || undefined);
      if (result.created) {
        toast.success(`Created rule table: ${result.table_name}`);
        setChecksTable(result.table_name);
      } else {
        toast.info(`Table already exists: ${result.table_name}`);
        setChecksTable(result.table_name);
      }
    } catch (error: any) {
      const msg = error?.response?.data?.detail || error.message;
      toast.error("Failed to create table: " + msg);
    } finally {
      setIsCreatingTable(false);
    }
  };

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-4">
        <CardTitle className="flex items-center gap-2">
          <Settings className="h-5 w-5" />
          App Settings
        </CardTitle>
        <Button size="sm" onClick={handleSave} disabled={!isDirty || isSaving}>
          <Save className="mr-2 h-3 w-3" />
          {isSaving ? "Saving..." : "Save Settings"}
        </Button>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="space-y-4">
          <h3 className="text-sm font-semibold text-muted-foreground">
            Configuration File
          </h3>
          <div className="space-y-1.5">
            <Label className="text-sm font-medium">Config Path</Label>
            <p className="rounded-md border bg-muted/20 px-3 py-2 font-mono text-sm">
              {settings.install_folder}/config.yml
            </p>
          </div>
        </div>

        <div className="space-y-4">
          <h3 className="text-sm font-semibold text-muted-foreground flex items-center gap-2">
            <Server className="h-4 w-4" />
            Compute Settings
          </h3>

          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-1.5">
              <Label className="text-sm font-medium">Default Compute</Label>
              <Select
                value={defaultCompute}
                onValueChange={setDefaultCompute}
                disabled={clustersLoading}
              >
                <SelectTrigger className="w-full">
                  {clustersLoading ? (
                    <span className="text-muted-foreground">Loading...</span>
                  ) : (
                    <SelectValue placeholder="Select compute" />
                  )}
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="serverless">Serverless</SelectItem>
                  {clustersData?.clusters.map((cluster) => (
                    <SelectItem key={cluster.id} value={cluster.id}>
                      <div className="flex items-center gap-2">
                        <span>{cluster.name}</span>
                        {cluster.state && (
                          <Badge
                            variant={
                              cluster.state === "RUNNING"
                                ? "default"
                                : "secondary"
                            }
                            className="text-[10px] px-1 py-0"
                          >
                            {cluster.state}
                          </Badge>
                        )}
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                Spark-backed actions use serverless or the selected classic
                cluster.
              </p>
            </div>

            <div className="space-y-1.5">
              <Label className="text-sm font-medium">Default Warehouse</Label>
              <Select
                value={warehouseId}
                onValueChange={setWarehouseId}
                disabled={warehousesLoading}
              >
                <SelectTrigger className="w-full">
                  {warehousesLoading ? (
                    <span className="text-muted-foreground">Loading...</span>
                  ) : (
                    <SelectValue placeholder="Select warehouse" />
                  )}
                </SelectTrigger>
                <SelectContent>
                  {warehousesData?.warehouses.map((w) => (
                    <SelectItem key={w.id} value={w.id}>
                      <div className="flex items-center gap-2">
                        <span>{w.name}</span>
                        {w.state && (
                          <Badge
                            variant={
                              w.state === "RUNNING" ? "default" : "secondary"
                            }
                            className="text-[10px] px-1 py-0"
                          >
                            {w.state}
                          </Badge>
                        )}
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        </div>

        {/* ── Data Settings ─────────────────────────── */}
        <div className="space-y-4">
          <h3 className="text-sm font-semibold text-muted-foreground flex items-center gap-2">
            <Database className="h-4 w-4" />
            Data Settings
          </h3>

          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-1.5">
              <Label className="text-sm font-medium">Default Catalog</Label>
              <Select
                value={catalog}
                onValueChange={handleCatalogChange}
                disabled={catalogsLoading}
              >
                <SelectTrigger className="w-full">
                  {catalogsLoading ? (
                    <span className="text-muted-foreground">Loading...</span>
                  ) : (
                    <SelectValue placeholder="Select catalog" />
                  )}
                </SelectTrigger>
                <SelectContent>
                  {catalogsData?.catalogs.map((c) => (
                    <SelectItem key={c} value={c}>
                      {c}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {catalogsError && (
                <p className="text-xs text-destructive">
                  {(catalogsError as any)?.response?.data?.detail ||
                    catalogsError.message}
                </p>
              )}
            </div>

            <div className="space-y-1.5">
              <Label className="text-sm font-medium">Default Schema</Label>
              <Select
                value={schema}
                onValueChange={handleSchemaChange}
                disabled={!catalog || schemasLoading}
              >
                <SelectTrigger className="w-full">
                  {schemasLoading ? (
                    <span className="text-muted-foreground">Loading...</span>
                  ) : (
                    <SelectValue placeholder="Select schema" />
                  )}
                </SelectTrigger>
                <SelectContent>
                  {schemasData?.schemas.map((s) => (
                    <SelectItem key={s} value={s}>
                      {s}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="space-y-1.5">
            <Label className="text-sm font-medium">Rule Table</Label>
            <div className="flex gap-2">
              <Select
                value={checksTable}
                onValueChange={setChecksTable}
                disabled={!catalog || !schema || tablesLoading}
              >
                <SelectTrigger className="w-full font-mono text-sm">
                  {tablesLoading ? (
                    <span className="text-muted-foreground">Loading...</span>
                  ) : (
                    <SelectValue
                      placeholder={composedTableName || "Select rule table"}
                    />
                  )}
                </SelectTrigger>
                <SelectContent>
                  {ruleTableOptions.map((tableName) => (
                    <SelectItem key={tableName} value={tableName}>
                      {tableName}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Button
                variant="outline"
                size="sm"
                onClick={handleCreateTable}
                disabled={
                  isCreatingTable || (!checksTable && !composedTableName)
                }
                className="shrink-0"
              >
                {isCreatingTable ? (
                  <Loader2 className="mr-2 h-3 w-3 animate-spin" />
                ) : (
                  <Plus className="mr-2 h-3 w-3" />
                )}
                Create Table
              </Button>
            </div>
            <p className="text-xs text-muted-foreground">
              Select a fully qualified table from the current catalog and schema,
              or click "Create Table" to generate a new `checks` table.
            </p>
            {tablesLoading && (
              <p className="text-xs text-muted-foreground">
                Loading available tables...
              </p>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

/* ─── Section Error fallback ─────────────────────────────────── */

function SectionError({
  error,
  resetErrorBoundary,
}: {
  error: unknown;
  resetErrorBoundary: () => void;
}) {
  const detail =
    typeof error === "object" &&
    error !== null &&
    "response" in error &&
    typeof (error as { response?: unknown }).response === "object" &&
    (error as { response?: { data?: { detail?: unknown } } }).response
      ?.data &&
    typeof (error as { response?: { data?: { detail?: unknown } } })
      .response?.data === "object"
      ? (error as { response?: { data?: { detail?: unknown } } }).response
          ?.data?.detail
      : undefined;

  const errorMessage =
    typeof detail === "string"
      ? detail
      : error instanceof Error
        ? error.message
        : "Unknown error";

  return (
    <div className="flex flex-col gap-2 items-start">
      <p className="text-sm text-destructive flex items-center gap-1">
        <AlertCircle className="h-4 w-4" /> Failed to load section
      </p>
      <p className="text-xs text-muted-foreground max-w-xl">{errorMessage}</p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary}>
        Retry
      </Button>
    </div>
  );
}

/* ─── Main Config Page ───────────────────────────────────────── */

function ConfigPage() {
  return (
    <div className="space-y-6 h-full flex flex-col">
      <div className="flex items-start justify-between shrink-0">
        <div className="space-y-2">
          <PageBreadcrumb page="Configuration" />
          <div>
            <h1 className="text-2xl font-bold tracking-tight">
              <ShinyText text="Configuration" speed={6} className="font-bold" />
            </h1>
            <p className="text-muted-foreground">
              Manage your app settings.
            </p>
          </div>
        </div>
      </div>

      <div className="flex-1 min-h-0">
        <QueryErrorResetBoundary>
          {({ reset }) => (
            <div className="space-y-6 pb-8">
              <FadeIn delay={0.1}>
                <ErrorBoundary
                  onReset={reset}
                  fallbackRender={SectionError}
                >
                  <Suspense
                    fallback={
                      <Card>
                        <CardHeader>
                          <Skeleton className="h-6 w-40" />
                        </CardHeader>
                        <CardContent>
                          <div className="grid gap-4 md:grid-cols-2">
                            <Skeleton className="h-16" />
                            <Skeleton className="h-16" />
                            <Skeleton className="h-16" />
                            <Skeleton className="h-16" />
                          </div>
                        </CardContent>
                      </Card>
                    }
                  >
                    <AppSettingsCard />
                  </Suspense>
                </ErrorBoundary>
              </FadeIn>
            </div>
          )}
        </QueryErrorResetBoundary>
      </div>
    </div>
  );
}
