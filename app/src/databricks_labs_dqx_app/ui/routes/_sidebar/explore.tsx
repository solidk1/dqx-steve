import { createFileRoute } from "@tanstack/react-router";
import { useState } from "react";
import axios from "axios";
import {
  useCatalogs,
  useSchemas,
  useTables,
  useTableInfo,
} from "@/lib/explorer-api";
import type { ColumnInfoOut } from "@/lib/explorer-api";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Separator } from "@/components/ui/separator";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Database,
  Table2,
  Columns3,
  Loader2,
  AlertCircle,
  Search,
  User,
  FileType,
  Info,
} from "lucide-react";
import { FadeIn } from "@/components/anim/FadeIn";
import { ShinyText } from "@/components/anim/ShinyText";
import { AICheckGenerator } from "@/components/AICheckGenerator";

export const Route = createFileRoute("/_sidebar/explore")({
  component: () => <ExplorePage />,
});

function CatalogSchemaTableSelector({
  onLoad,
}: {
  onLoad: (fullName: string) => void;
}) {
  const [catalog, setCatalog] = useState<string | undefined>("shao_sandbox1");
  const [schema, setSchema] = useState<string | undefined>();
  const [table, setTable] = useState<string | undefined>();

  const {
    data: catalogsData,
    isLoading: catalogsLoading,
    error: catalogsError,
  } = useCatalogs();
  const {
    data: schemasData,
    isLoading: schemasLoading,
  } = useSchemas(catalog);
  const {
    data: tablesData,
    isLoading: tablesLoading,
  } = useTables(catalog, schema);

  const handleCatalogChange = (value: string) => {
    setCatalog(value);
    setSchema(undefined);
    setTable(undefined);
  };

  const handleSchemaChange = (value: string) => {
    setSchema(value);
    setTable(undefined);
  };

  const handleLoad = () => {
    if (catalog && schema && table) {
      onLoad(`${catalog}.${schema}.${table}`);
    }
  };

  return (
    <Card>
      <CardHeader className="pb-4">
        <CardTitle className="flex items-center gap-2 text-base">
          <Search className="h-4 w-4" />
          Select Table
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex items-end gap-3 flex-wrap">
          <div className="space-y-1.5 min-w-[200px] flex-1">
            <label className="text-sm font-medium text-muted-foreground">
              Catalog
            </label>
            {catalogsError ? (
              <div className="text-sm text-destructive flex items-center gap-1">
                <AlertCircle className="h-3.5 w-3.5" />
                Failed to load catalogs
              </div>
            ) : (
              <Select
                value={catalog}
                onValueChange={handleCatalogChange}
                disabled={catalogsLoading}
              >
                <SelectTrigger className="w-full">
                  {catalogsLoading && catalog ? (
                    <span className="truncate">{catalog}</span>
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
            )}
          </div>

          <div className="space-y-1.5 min-w-[200px] flex-1">
            <label className="text-sm font-medium text-muted-foreground">
              Schema
            </label>
            <Select
              value={schema}
              onValueChange={handleSchemaChange}
              disabled={!catalog || schemasLoading}
            >
              <SelectTrigger className="w-full">
                <SelectValue
                  placeholder={
                    schemasLoading ? "Loading..." : "Select schema"
                  }
                />
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

          <div className="space-y-1.5 min-w-[200px] flex-1">
            <label className="text-sm font-medium text-muted-foreground">
              Table
            </label>
            <Select
              value={table}
              onValueChange={setTable}
              disabled={!schema || tablesLoading}
            >
              <SelectTrigger className="w-full">
                <SelectValue
                  placeholder={
                    tablesLoading ? "Loading..." : "Select table"
                  }
                />
              </SelectTrigger>
              <SelectContent>
                {tablesData?.tables.map((t) => (
                  <SelectItem key={t} value={t}>
                    {t}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <Button
            onClick={handleLoad}
            disabled={!catalog || !schema || !table}
            className="gap-2"
          >
            <Table2 className="h-4 w-4" />
            Load Table
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

function TableMetadataCard({
  fullName,
  tableType,
  format,
  owner,
  comment,
}: {
  fullName: string;
  tableType: string | null;
  format: string | null;
  owner: string | null;
  comment: string | null;
}) {
  return (
    <FadeIn delay={0.1}>
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <Info className="h-4 w-4" />
            Table Information
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
            <div className="space-y-1 min-w-0">
              <p className="text-sm font-medium text-muted-foreground flex items-center gap-1">
                <Database className="h-3 w-3" /> Full Name
              </p>
              <p className="font-mono text-sm break-all">{fullName}</p>
            </div>
            <div className="space-y-1">
              <p className="text-sm font-medium text-muted-foreground flex items-center gap-1">
                <FileType className="h-3 w-3" /> Type
              </p>
              <div className="flex items-center gap-2">
                {tableType && <Badge variant="outline">{tableType}</Badge>}
                {format && <Badge variant="secondary">{format}</Badge>}
              </div>
            </div>
            <div className="space-y-1">
              <p className="text-sm font-medium text-muted-foreground flex items-center gap-1">
                <User className="h-3 w-3" /> Owner
              </p>
              <p className="text-sm">{owner || "N/A"}</p>
            </div>
          </div>

          {comment && (
            <>
              <Separator />
              <div className="space-y-1">
                <p className="text-sm font-medium text-muted-foreground">
                  Description
                </p>
                <p className="text-sm bg-muted/50 rounded-md p-3 break-words [overflow-wrap:anywhere]">
                  {comment}
                </p>
              </div>
            </>
          )}
          {!comment && (
            <>
              <Separator />
              <div className="space-y-1">
                <p className="text-sm font-medium text-muted-foreground">
                  Description
                </p>
                <p className="text-sm text-muted-foreground italic">
                  No description set
                </p>
              </div>
            </>
          )}
        </CardContent>
      </Card>
    </FadeIn>
  );
}

function ColumnsCard({ columns }: { columns: ColumnInfoOut[] }) {
  if (columns.length === 0) return null;

  return (
    <FadeIn delay={0.2}>
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <Columns3 className="h-4 w-4" />
            Columns
            <span className="text-sm font-normal text-muted-foreground">
              ({columns.length})
            </span>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="border rounded-md overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm table-fixed min-w-[900px]">
                <thead>
                  <tr className="bg-muted/50 border-b">
                    <th className="text-left p-2 font-medium text-muted-foreground w-12">
                      #
                    </th>
                    <th className="text-left p-2 font-medium text-muted-foreground w-[18rem]">
                      Name
                    </th>
                    <th className="text-left p-2 font-medium text-muted-foreground w-[16rem]">
                      Type
                    </th>
                    <th className="text-left p-2 font-medium text-muted-foreground w-20">
                      Nullable
                    </th>
                    <th className="text-left p-2 font-medium text-muted-foreground w-[24rem]">
                      Comment
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {columns.map((col, idx) => (
                    <tr
                      key={col.name}
                      className="border-b last:border-b-0 hover:bg-muted/30 transition-colors"
                    >
                      <td className="p-2 text-muted-foreground tabular-nums">
                        {col.position ?? idx}
                      </td>
                      <td className="p-2 font-mono text-xs">
                        <div className="truncate" title={col.name}>
                          {col.name}
                        </div>
                      </td>
                      <td className="p-2">
                        <Badge
                          variant="outline"
                          className="font-mono text-xs max-w-[15rem] truncate align-middle inline-block"
                          title={col.type_text}
                        >
                          {col.type_text}
                        </Badge>
                      </td>
                      <td className="p-2">
                        <Badge
                          variant={col.nullable ? "secondary" : "default"}
                          className="text-xs"
                        >
                          {col.nullable ? "YES" : "NO"}
                        </Badge>
                      </td>
                      <td className="p-2 text-muted-foreground text-xs">
                        <div className="truncate" title={col.comment || "-"}>
                          {col.comment || "-"}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </CardContent>
      </Card>
    </FadeIn>
  );
}

function SampleDataCard({
  data,
  error,
}: {
  data: Record<string, unknown>[];
  error: string | null;
}) {
  if (error) {
    return (
      <FadeIn delay={0.3}>
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-base">
              <Table2 className="h-4 w-4" />
              Sample Data
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-2 text-sm text-destructive">
              <AlertCircle className="h-4 w-4" />
              <span>Failed to load sample data: {error}</span>
            </div>
          </CardContent>
        </Card>
      </FadeIn>
    );
  }

  if (data.length === 0) {
    return (
      <FadeIn delay={0.3}>
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-base">
              <Table2 className="h-4 w-4" />
              Sample Data
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground italic">
              No data available
            </p>
          </CardContent>
        </Card>
      </FadeIn>
    );
  }

  const columnNames = Object.keys(data[0]);

  return (
    <FadeIn delay={0.3}>
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <Table2 className="h-4 w-4" />
            Sample Data
            <span className="text-sm font-normal text-muted-foreground">
              ({data.length} rows)
            </span>
          </CardTitle>
        </CardHeader>
        <CardContent className="min-w-0">
          <div className="border rounded-md overflow-hidden max-w-full min-w-0">
            <div className="overflow-x-auto max-h-[600px] overflow-y-auto max-w-full">
              <table className="w-max min-w-full text-sm">
                <thead className="sticky top-0 z-10">
                  <tr className="bg-muted/80 backdrop-blur-sm border-b">
                    {columnNames.map((col) => (
                      <th
                        key={col}
                        className="text-left p-2 font-medium text-muted-foreground font-mono text-xs whitespace-nowrap min-w-[8rem]"
                        title={col}
                      >
                        <div className="truncate max-w-[14rem]">{col}</div>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.map((row, rowIdx) => (
                    <tr
                      key={rowIdx}
                      className="border-b last:border-b-0 hover:bg-muted/30 transition-colors"
                    >
                      {columnNames.map((col) => (
                        <td
                          key={col}
                          className="p-2 text-xs whitespace-nowrap min-w-[8rem]"
                          title={String(row[col] ?? "")}
                        >
                          <div className="max-w-[14rem] truncate">
                            {row[col] === null || row[col] === undefined ? (
                              <span className="text-muted-foreground italic">
                                null
                              </span>
                            ) : (
                              String(row[col])
                            )}
                          </div>
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </CardContent>
      </Card>
    </FadeIn>
  );
}

function TableDetails({ fullName }: { fullName: string }) {
  const { data, isLoading, error } = useTableInfo(fullName);

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-40 w-full" />
        <Skeleton className="h-60 w-full" />
        <Skeleton className="h-80 w-full" />
      </div>
    );
  }

  if (error) {
    const detail =
      (error as any)?.response?.data?.detail || error.message;
    return (
      <Card>
        <CardContent className="py-6">
          <div className="flex items-center gap-2 text-destructive">
            <AlertCircle className="h-5 w-5" />
            <span>Failed to load table info: {detail}</span>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (!data) return null;

  return (
    <div className="space-y-4 min-w-0">
      <TableMetadataCard
        fullName={data.full_name}
        tableType={data.table_type}
        format={data.data_source_format}
        owner={data.owner}
        comment={data.comment}
      />
      <ColumnsCard columns={data.columns} />
      <SampleDataCard
        data={data.sample_data}
        error={data.sample_data_error}
      />
    </div>
  );
}

function ExplorePage() {
  const [selectedTable, setSelectedTable] = useState<string | undefined>();
  const [isGenerating, setIsGenerating] = useState(false);

  const handleGenerate = async (userInput: string) => {
    setIsGenerating(true);
    try {
      const response = await axios.post<{ yaml_output: string; checks: any[] }>(
        "/api/ai-generate-checks",
        { user_input: userInput, table_name: selectedTable },
        { withCredentials: true },
      );
      return response.data;
    } finally {
      setIsGenerating(false);
    }
  };

  const handleConfirmSave = async (checks: any[]) => {
    await axios.post(
      "/api/checks-table/append",
      {
        checks,
        table_name: "shao_sandbox1.dqx.checks",
        run_config_name: selectedTable ?? null,
        mode: "upsert",
      },
      { withCredentials: true },
    );
  };

  return (
    <div className="space-y-6 min-w-0 overflow-x-hidden">
      <div className="space-y-2">
        <PageBreadcrumb page="Data Explorer" />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">
            <ShinyText text="Data Explorer" speed={6} className="font-bold" />
          </h1>
          <p className="text-muted-foreground">
            Browse Unity Catalog tables and preview data.
          </p>
        </div>
      </div>

      <FadeIn>
        <CatalogSchemaTableSelector onLoad={setSelectedTable} />
      </FadeIn>

      {selectedTable && (
        <div className="space-y-4 min-w-0">
          <Separator />
          <div className="flex items-center gap-2 min-w-0">
            <Database className="h-4 w-4 text-primary" />
            <h2 className="text-lg font-semibold font-mono truncate min-w-0 flex-1" title={selectedTable}>
              {selectedTable}
            </h2>
          </div>
          <TableDetails fullName={selectedTable} />
        </div>
      )}

      {!selectedTable && (
        <FadeIn delay={0.2}>
          <div className="flex flex-col items-center justify-center py-16 text-center">
            <Database className="h-12 w-12 text-muted-foreground/30 mb-4" />
            <p className="text-muted-foreground">
              Select a catalog, schema, and table to explore its metadata and
              preview data.
            </p>
          </div>
        </FadeIn>
      )}

      <FadeIn delay={0.25}>
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base">AI-Assisted Rules Generation</CardTitle>
          </CardHeader>
          <CardContent>
            <AICheckGenerator
              onGenerate={handleGenerate}
              onConfirmSave={handleConfirmSave}
              isGenerating={isGenerating}
            />
          </CardContent>
        </Card>
      </FadeIn>
    </div>
  );
}
