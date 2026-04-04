import { createFileRoute } from "@tanstack/react-router";
import { Button } from "@/components/ui/button";
import { RefreshCw, AlertCircle, Loader2, Play, ExternalLink } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import axios from "axios";
import { toast } from "sonner";
import { useGetSettings } from "@/lib/api";
import selector from "@/lib/selector";
import {
  DEFAULT_CHECKS_TABLE_REQUIRED_MESSAGE,
  getRequiredChecksTableName,
} from "@/lib/checks-table";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface ChecksTableOut {
  rows: Record<string, unknown>[];
  columns: string[];
}

interface DashboardOut {
  dashboard_id: string;
  embed_url: string;
}

interface CheckErrorRowsOut {
  result_table_name: string;
  rows: Record<string, unknown>[];
  columns: string[];
}

interface SelectedRule {
  key: string;
  runConfigName: string;
  checkName: string;
}

function useChecksTable(tableName: string | null) {
  return useQuery<ChecksTableOut>({
    queryKey: ["/api/checks-table", tableName],
    enabled: !!tableName,
    queryFn: ({ signal }) =>
      axios
        .get<ChecksTableOut>("/api/checks-table", {
          signal,
          params: {
            table_name: tableName,
          },
        })
        .then((r) => r.data),
  });
}

function useDashboard() {
  return useQuery<DashboardOut>({
    queryKey: ["/api/dashboard"],
    queryFn: ({ signal }) =>
      axios.get<DashboardOut>("/api/dashboard", { signal }).then((r) => r.data),
  });
}

function useCheckErrorRows(selectedRule: SelectedRule | null) {
  return useQuery<CheckErrorRowsOut>({
    queryKey: [
      "/api/checks-table/error-rows",
      selectedRule?.runConfigName,
      selectedRule?.checkName,
    ],
    enabled: !!selectedRule,
    queryFn: ({ signal }) =>
      axios
        .get<CheckErrorRowsOut>("/api/checks-table/error-rows", {
          signal,
          params: {
            run_config_name: selectedRule?.runConfigName,
            check_name: selectedRule?.checkName,
          },
        })
        .then((r) => r.data),
  });
}

export const Route = createFileRoute("/_sidebar/")({
  component: () => <Index />,
});

function Index() {
  const {
    data: settings,
    isLoading: settingsLoading,
    isError: settingsIsError,
    error: settingsError,
  } = useGetSettings(selector());
  const checksTableName = useMemo(() => {
    if (!settings) {
      return null;
    }
    try {
      return getRequiredChecksTableName(settings);
    } catch {
      return null;
    }
  }, [settings]);
  const checksTableConfigError =
    settings && !checksTableName
      ? new Error(DEFAULT_CHECKS_TABLE_REQUIRED_MESSAGE)
      : null;
  const { data, isLoading, isError, error, refetch } =
    useChecksTable(checksTableName);
  const { data: dashboardData } = useDashboard();
  const [runConfigFilter, setRunConfigFilter] = useState<string>("all");
  const [isRunningChecks, setIsRunningChecks] = useState(false);
  const [selectedRule, setSelectedRule] = useState<SelectedRule | null>(null);
  const [expandedRuleRows, setExpandedRuleRows] = useState<Record<string, boolean>>({});
  const [expandedErrorRows, setExpandedErrorRows] = useState<Record<string, boolean>>({});
  const {
    data: selectedErrorRows,
    isLoading: selectedErrorRowsLoading,
    isError: selectedErrorRowsError,
    error: selectedErrorRowsErrorDetail,
  } = useCheckErrorRows(selectedRule);
  const shouldShowSelectedErrorRows = useMemo(() => {
    if (!selectedRule) return false;
    if (selectedErrorRowsLoading || selectedErrorRowsError || !selectedErrorRows) return true;
    return selectedErrorRows.rows.length > 0 || selectedErrorRows.columns.length > 0;
  }, [
    selectedRule,
    selectedErrorRows,
    selectedErrorRowsError,
    selectedErrorRowsLoading,
  ]);

  const handleRunChecksNow = async () => {
    if (!checksTableName) {
      toast.error(DEFAULT_CHECKS_TABLE_REQUIRED_MESSAGE);
      return;
    }
    setIsRunningChecks(true);
    try {
      const response = await axios.post("/api/checks-table/run-job", undefined, {
        params: {
          table_name: checksTableName,
        },
      });
      const jobUrl = response?.data?.job_url;
      const runUrl = response?.data?.run_url;
      toast.success("DQX check job triggered", {
        description: (
          <div className="flex flex-col gap-1 mt-1">
            {jobUrl && (
              <a href={jobUrl} target="_blank" rel="noopener noreferrer" className="underline text-primary text-xs">
                View job in Databricks
              </a>
            )}
            {runUrl && (
              <a href={runUrl} target="_blank" rel="noopener noreferrer" className="underline text-primary text-xs">
                View run in Databricks
              </a>
            )}
          </div>
        ),
        duration: 10000,
      });
    } catch (error: any) {
      const detail = error?.response?.data?.detail;
      toast.error(detail ? String(detail) : "Failed to start check job");
    } finally {
      setIsRunningChecks(false);
    }
  };

  const runConfigOptions = useMemo(() => {
    if (!data) return [];
    return Array.from(
      new Set(
        data.rows
          .map((row) => row.run_config_name)
          .filter((value) => value != null && String(value).trim().length > 0)
          .map((value) => String(value)),
      ),
    ).sort();
  }, [data]);

  const filteredRows = useMemo(() => {
    if (!data) return [];
    if (runConfigFilter === "all") return data.rows;
    return data.rows.filter((row) => String(row.run_config_name ?? "") === runConfigFilter);
  }, [data, runConfigFilter]);

  const selectedRuleExistsInFilteredRows = useMemo(() => {
    if (!selectedRule) return false;
    return filteredRows.some((row) => {
      const rowRunConfig = String(row.run_config_name ?? "").trim();
      const rowCheckName = String(row.name ?? "").trim();
      const key = `${rowRunConfig}::${rowCheckName}`;
      return key === selectedRule.key;
    });
  }, [filteredRows, selectedRule]);

  useEffect(() => {
    if (runConfigFilter !== "all" && !runConfigOptions.includes(runConfigFilter)) {
      setRunConfigFilter("all");
    }
  }, [runConfigFilter, runConfigOptions]);

  useEffect(() => {
    if (selectedRule && !selectedRuleExistsInFilteredRows) {
      setSelectedRule(null);
    }
  }, [selectedRule, selectedRuleExistsInFilteredRows]);

  return (
    <div className="flex flex-1 flex-col -m-6 min-h-0 min-w-0 overflow-x-hidden">
      {/* Top: Rules Table */}
      <div className="flex-1 min-h-0 min-w-0 overflow-y-auto overflow-x-hidden border-b border-border">
        <div className="p-6 min-w-0">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h2 className="text-2xl font-bold">Data Quality Rules</h2>
              <p className="text-sm text-muted-foreground mt-1">
                Rules from{" "}
                <code className="bg-muted px-1.5 py-0.5 rounded text-xs">
                  {checksTableName ?? "not configured"}
                </code>
              </p>
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => refetch()}
                disabled={isLoading || !checksTableName}
              >
                <RefreshCw className={`h-4 w-4 mr-2 ${isLoading ? "animate-spin" : ""}`} />
                Refresh
              </Button>
              <Button
                size="sm"
                onClick={handleRunChecksNow}
                disabled={isRunningChecks}
              >
                {isRunningChecks ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <Play className="h-4 w-4 mr-2" />
                )}
                Run Check Now
              </Button>
            </div>
          </div>

          {data && data.rows.length > 0 && (
            <div className="mb-4 flex items-center gap-3">
              <span className="text-sm text-muted-foreground">Filter by table</span>
              <Select value={runConfigFilter} onValueChange={setRunConfigFilter}>
                <SelectTrigger className="w-[300px]">
                  <SelectValue placeholder="All tables" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All tables</SelectItem>
                  {runConfigOptions.map((value) => (
                    <SelectItem key={value} value={value}>
                      {value}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}

          {(settingsLoading || isLoading) && (
            <div className="flex items-center justify-center py-16 text-muted-foreground">
              <Loader2 className="h-6 w-6 animate-spin mr-3" />
              Loading rules...
            </div>
          )}

          {(settingsIsError || checksTableConfigError || isError) && (
            <div className="flex items-center gap-3 p-4 rounded-lg bg-destructive/10 text-destructive border border-destructive/20">
              <AlertCircle className="h-5 w-5 shrink-0" />
              <p className="text-sm">
                Failed to load rules:{" "}
                {(settingsError as any)?.response?.data?.detail ||
                  settingsError?.message ||
                  checksTableConfigError?.message ||
                  (error as any)?.response?.data?.detail ||
                  error?.message}
              </p>
            </div>
          )}

          {checksTableName && data && data.rows.length === 0 && (
            <div className="text-center py-16 text-muted-foreground">
              No rules found in the checks table.
            </div>
          )}

          {checksTableName && data && data.rows.length > 0 && filteredRows.length === 0 && (
            <div className="text-center py-16 text-muted-foreground">
              No rules found for selected table filter.
            </div>
          )}

          {checksTableName && data && data.rows.length > 0 && filteredRows.length > 0 && (
            <div className="rounded-lg border overflow-hidden max-w-full min-w-0">
              <div className="overflow-x-auto max-w-full">
                <table className="w-full text-sm min-w-[1000px] table-fixed">
                  <thead>
                    <tr className="bg-muted/50 border-b">
                      {data.columns.map((col) => (
                        <th
                          key={col}
                          className="px-4 py-3 text-left font-semibold text-muted-foreground whitespace-nowrap"
                        >
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {filteredRows.map((row, idx) => {
                      const runConfigName = String(row.run_config_name ?? "").trim();
                      const checkName = String(row.name ?? "").trim();
                      const rowKey = `${runConfigName}::${checkName}`;
                      const isSelectable = runConfigName.length > 0 && checkName.length > 0;
                      const isSelected = selectedRule?.key === rowKey;
                      const isExpanded = !!expandedRuleRows[rowKey];
                      return (
                        <tr
                          key={`${idx}-${rowKey}`}
                          className={[
                            "border-b transition-colors",
                            isSelected
                              ? "bg-primary/10 ring-1 ring-inset ring-primary/30"
                              : "hover:bg-muted/30",
                            isSelectable ? "cursor-pointer" : "",
                          ].join(" ")}
                          onClick={() => {
                            if (!isSelectable) return;
                            setSelectedRule({
                              key: rowKey,
                              runConfigName,
                              checkName,
                            });
                            setExpandedRuleRows((prev) => ({
                              ...prev,
                              [rowKey]: !prev[rowKey],
                            }));
                          }}
                          title={
                            isSelectable
                              ? `Select and expand check "${checkName}"`
                              : "Row must contain run_config_name and name to be selectable"
                          }
                        >
                          {data.columns.map((col) => {
                            if (row[col] == null) {
                              return (
                                <td key={col} className="px-4 py-3">
                                  <span className="text-muted-foreground/50 italic">null</span>
                                </td>
                              );
                            }

                            const value = String(row[col]);
                            return (
                              <td key={col} className="px-4 py-3 align-top">
                                <div
                                  className={
                                    isExpanded
                                      ? "w-full max-w-[28em] whitespace-pre-wrap break-words"
                                      : "w-full max-w-[28em] whitespace-nowrap overflow-hidden text-ellipsis"
                                  }
                                  title={value}
                                >
                                  {value}
                                </div>
                              </td>
                            );
                          })}
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              <div className="px-4 py-2 bg-muted/30 border-t text-xs text-muted-foreground">
                {filteredRows.length} rule{filteredRows.length !== 1 ? "s" : ""}
              </div>
            </div>
          )}

          {shouldShowSelectedErrorRows && selectedRule && (
            <div className="mt-6 space-y-3 min-w-0">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <h3 className="text-base font-semibold">Error Rows for Selected Rule</h3>
                  <p className="text-xs text-muted-foreground mt-1">
                    Check <code className="bg-muted px-1 py-0.5 rounded">{selectedRule.checkName}</code> on{" "}
                    <code className="bg-muted px-1 py-0.5 rounded">{selectedRule.runConfigName}</code>
                    {selectedErrorRows?.result_table_name && (
                      <>
                        {" "}from{" "}
                        <code className="bg-muted px-1 py-0.5 rounded">{selectedErrorRows.result_table_name}</code>
                      </>
                    )}
                  </p>
                </div>
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => setSelectedRule(null)}
                >
                  Clear
                </Button>
              </div>

              {selectedErrorRowsLoading && (
                <div className="flex items-center gap-2 text-sm text-muted-foreground py-4">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Loading error rows...
                </div>
              )}

              {selectedErrorRowsError && (
                <div className="flex items-center gap-2 p-3 rounded-md bg-destructive/10 text-destructive text-sm border border-destructive/20">
                  <AlertCircle className="h-4 w-4 shrink-0" />
                  Failed to load error rows:{" "}
                  {(selectedErrorRowsErrorDetail as any)?.response?.data?.detail ||
                    (selectedErrorRowsErrorDetail as any)?.message ||
                    "Unknown error"}
                </div>
              )}

              {!selectedErrorRowsLoading &&
                !selectedErrorRowsError &&
                selectedErrorRows &&
                selectedErrorRows.rows.length === 0 && (
                  <div className="text-sm text-muted-foreground py-2">
                    No failing rows found for this check in the result table.
                  </div>
                )}

              {!selectedErrorRowsLoading &&
                !selectedErrorRowsError &&
                selectedErrorRows &&
                selectedErrorRows.rows.length > 0 && (
                  <div className="rounded-lg border overflow-hidden max-w-full min-w-0">
                    <div className="overflow-x-auto max-w-full">
                      <table className="w-max min-w-full text-sm">
                        <thead>
                          <tr className="bg-muted/50 border-b">
                            {selectedErrorRows.columns.map((col) => (
                              <th
                                key={col}
                                className="px-4 py-3 text-left font-semibold text-muted-foreground whitespace-nowrap min-w-[8rem]"
                              >
                                <div className="truncate max-w-[14rem]" title={col}>
                                  {col}
                                </div>
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {selectedErrorRows.rows.map((row, idx) => {
                            const rowKey = `error-${idx}`;
                            const isExpanded = !!expandedErrorRows[rowKey];
                            return (
                              <tr
                                key={rowKey}
                                className="border-b hover:bg-muted/20 transition-colors cursor-pointer"
                                onClick={() =>
                                  setExpandedErrorRows((prev) => ({
                                    ...prev,
                                    [rowKey]: !prev[rowKey],
                                  }))
                                }
                                title="Expand row text"
                              >
                                {selectedErrorRows.columns.map((col) => {
                                  const value = row[col];
                                  if (value == null) {
                                    return (
                                        <td key={col} className="px-4 py-3 min-w-[8rem]">
                                        <span className="text-muted-foreground/50 italic">null</span>
                                      </td>
                                    );
                                  }
                                  return (
                                      <td key={col} className="px-4 py-3 align-top min-w-[8rem]">
                                      <div
                                        className={
                                          isExpanded
                                              ? "max-w-[14rem] whitespace-pre-wrap break-words [overflow-wrap:anywhere]"
                                              : "max-w-[14rem] whitespace-nowrap overflow-hidden text-ellipsis"
                                        }
                                        title={String(value)}
                                      >
                                        {String(value)}
                                      </div>
                                    </td>
                                  );
                                })}
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                    <div className="px-4 py-2 bg-muted/30 border-t text-xs text-muted-foreground">
                      {selectedErrorRows.rows.length} failing row
                      {selectedErrorRows.rows.length !== 1 ? "s" : ""}
                    </div>
                  </div>
                )}
            </div>
          )}
        </div>
      </div>

      {/* Bottom: Dashboard Link */}
      {dashboardData && (
        <div className="shrink-0 px-6 py-4 flex items-center justify-between bg-muted/30 border-t border-border">
          <div>
            <h3 className="font-semibold text-sm">Overall Dashboard</h3>
            <p className="text-xs text-muted-foreground mt-0.5">View data quality results in Databricks AI/BI</p>
          </div>
          <Button asChild variant="outline" size="sm">
            <a href={dashboardData.embed_url} target="_blank" rel="noreferrer" className="gap-2">
              Open Dashboard
              <ExternalLink className="h-4 w-4" />
            </a>
          </Button>
        </div>
      )}
    </div>
  );
}

