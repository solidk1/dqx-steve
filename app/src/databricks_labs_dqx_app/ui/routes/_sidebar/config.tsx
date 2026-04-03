import { createFileRoute } from "@tanstack/react-router";
import { Suspense, useState, useEffect } from "react";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import {
  useConfigSuspense,
  useGetSettingsSuspense,
  useSaveSettings,
  useSaveConfig,
  WorkspaceConfigInput,
} from "@/lib/api";
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
import { Input } from "@/components/ui/input";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { Label } from "@/components/ui/label";
import {
  AlertCircle,
  Settings,
  Eye,
  Code2,
  Save,
  RefreshCw,
  UploadCloud,
  Loader2,
} from "lucide-react";
import { toast } from "sonner";
import yaml from "js-yaml";
import { FadeIn } from "@/components/anim/FadeIn";
import { ShinyText } from "@/components/anim/ShinyText";

export const Route = createFileRoute("/_sidebar/config")({
  component: () => <ConfigPage />,
});

function ConfigLocationSettings({ onSettingsSaved }: { onSettingsSaved?: () => void }) {
  const { data: settings } = useGetSettingsSuspense(selector());
  const { mutate: save, isPending } = useSaveSettings();
  const queryClient = useQueryClient();

  const [isOpen, setIsOpen] = useState(false);
  const [path, setPath] = useState(settings.install_folder);
  const [isSaving, setIsSaving] = useState(false);

  useEffect(() => {
    if (isOpen) {
      setPath(settings.install_folder);
    }
  }, [isOpen, settings.install_folder]);

  const onSave = () => {
    setIsSaving(true);
    save(
      { data: { install_folder: path } },
      {
        onSuccess: async () => {
          setIsOpen(false);
          try {
            // Refetch settings first
            await queryClient.refetchQueries({ queryKey: ["/api/settings"] });
            
            // Then refetch config from the new location
            // This will update all active queries and trigger re-renders
            await queryClient.refetchQueries({ queryKey: ["/api/config"] });
            
            toast.success("Configuration location updated");
            
            // Force complete remount of config components
            if (onSettingsSaved) {
              onSettingsSaved();
            }
          } catch (error) {
            // If config doesn't exist yet in new location, that's okay
            // The error boundaries will handle it gracefully
            console.warn("Config refetch after settings save:", error);
            toast.success("Configuration location updated");
            
            // Still trigger remount even on error
            if (onSettingsSaved) {
              onSettingsSaved();
            }
          } finally {
            setIsSaving(false);
          }
        },
        onError: (error) => {
          setIsSaving(false);
          const msg = (error as any).response?.data?.detail || error.message;
          toast.error("Failed to save: " + msg);
        },
      },
    );
  };

  return (
    <>
      <Popover open={isOpen} onOpenChange={setIsOpen}>
        <PopoverTrigger asChild>
          <Button variant="ghost" size="icon" className="h-8 w-8 rounded-full">
            <Settings className="h-4 w-4" />
          </Button>
        </PopoverTrigger>
        <PopoverContent className="w-[500px]" align="end">
          <div className="grid gap-4">
            <div className="space-y-2">
              <h4 className="font-medium leading-none">
                DQX Installation Folder
              </h4>
              <p className="text-sm text-muted-foreground">
                Path to the config.yml file.
              </p>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="config-path">Path</Label>
              <Input
                id="config-path"
                value={path}
                onChange={(e) => setPath(e.target.value)}
                className="h-8 font-mono text-[10px]"
              />
            </div>
            <div className="flex justify-end">
              <Button size="sm" onClick={onSave} disabled={isPending}>
                {isPending ? "Saving..." : "Save"}
              </Button>
            </div>
          </div>
        </PopoverContent>
      </Popover>

      {isSaving && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-background/80 backdrop-blur-sm">
          <div className="flex flex-col items-center gap-4 p-8 rounded-lg bg-card border shadow-lg">
            <Loader2 className="h-12 w-12 animate-spin text-primary" />
            <div className="text-center space-y-2">
              <div className="text-lg font-semibold">Saving Configuration</div>
              <p className="text-sm text-muted-foreground">
                Updating install folder and refreshing config...
              </p>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

function GeneralSettingsData() {
  const {
    data: { config },
  } = useConfigSuspense(undefined, selector());

  return (
    <div className="grid gap-4 md:grid-cols-3">
      <div>
        <p className="text-sm font-medium text-muted-foreground">Log Level</p>
        <p className="text-lg font-mono">{config.log_level || "INFO"}</p>
      </div>
      <div>
        <p className="text-sm font-medium text-muted-foreground">
          Serverless Clusters
        </p>
        <div className="flex items-center gap-2">
          <Badge variant={config.serverless_clusters ? "default" : "secondary"}>
            {config.serverless_clusters ? "Enabled" : "Disabled"}
          </Badge>
        </div>
      </div>
      <div>
        <p className="text-sm font-medium text-muted-foreground">
          Upload Dependencies
        </p>
        <div className="flex items-center gap-2">
          <Badge variant={config.upload_dependencies ? "default" : "secondary"}>
            {config.upload_dependencies ? "Enabled" : "Disabled"}
          </Badge>
          {config.upload_dependencies && (
            <UploadCloud className="h-4 w-4 text-blue-500" />
          )}
        </div>
      </div>
      <div>
        <p className="text-sm font-medium text-muted-foreground">
          Profiler Parallelism
        </p>
        <p className="text-lg">
          {config.profiler_max_parallelism || "Default"}
        </p>
      </div>
      <div>
        <p className="text-sm font-medium text-muted-foreground">
          Quality Checker Parallelism
        </p>
        <p className="text-lg">
          {config.quality_checker_max_parallelism || "Default"}
        </p>
      </div>
      {config.custom_metrics && config.custom_metrics.length > 0 && (
        <div className="col-span-full">
          <p className="text-sm font-medium text-muted-foreground mb-1">
            Custom Metrics
          </p>
          <div className="flex flex-wrap gap-1">
            {config.custom_metrics.map((metric) => (
              <Badge
                key={metric}
                variant="outline"
                className="font-mono text-xs"
              >
                {metric}
              </Badge>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}


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
    (error as { response?: { data?: { detail?: unknown } } }).response?.data &&
    typeof (error as { response?: { data?: { detail?: unknown } } }).response?.data ===
      "object"
      ? (error as { response?: { data?: { detail?: unknown } } }).response?.data?.detail
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

function YamlConfigEditor() {
  const { data: configOut } = useConfigSuspense(undefined, selector());
  // Fetch settings as well to display the path
  const { data: settings } = useGetSettingsSuspense(selector());

  const { mutate: saveConfig, isPending } = useSaveConfig();
  const queryClient = useQueryClient();
  const [yamlContent, setYamlContent] = useState("");
  const [isModified, setIsModified] = useState(false);

  useEffect(() => {
    if (configOut?.config) {
      try {
        setYamlContent(yaml.dump(configOut.config));
      } catch (e) {
        console.error("Failed to convert config to YAML", e);
        toast.error("Failed to load configuration");
      }
    }
    setIsModified(false);
  }, [configOut]);

  const handleSave = () => {
    try {
      const parsed = yaml.load(yamlContent) as WorkspaceConfigInput;
      saveConfig(
        { data: { config: parsed } },
        {
          onSuccess: async () => {
            toast.success("Configuration saved successfully");
            setIsModified(false);
            // Refetch config queries to immediately update the display
            await queryClient.refetchQueries({ queryKey: ["/api/config"] });
          },
          onError: (error) => {
            console.error(error);
            const msg = (error as any).response?.data?.detail || error.message;
            toast.error("Failed to save configuration: " + msg);
          },
        },
      );
    } catch (e) {
      console.error(e);
      toast.error("Invalid YAML: " + (e as Error).message);
    }
  };

  const handleReset = () => {
    if (configOut?.config) {
      setYamlContent(yaml.dump(configOut.config));
      setIsModified(false);
    }
  };

  return (
    <FadeIn duration={0.4}>
      <Card className="flex flex-col h-[calc(100vh-12rem)] border-border/60 shadow-sm">
        <CardHeader className="py-3 px-4 border-b flex flex-row justify-between items-center space-y-0 h-14 bg-muted/20">
          <div className="flex flex-col gap-0.5">
            <div className="flex items-center gap-2">
              <Code2 className="h-4 w-4 text-muted-foreground" />
              <CardTitle className="text-base">Config Editor</CardTitle>
            </div>
            <p className="text-[10px] font-mono text-muted-foreground pl-6">
              {settings.install_folder}/config.yml
            </p>
          </div>
          {isModified && (
            <div className="flex gap-2">
              <Button
                variant="ghost"
                size="sm"
                onClick={handleReset}
                disabled={isPending}
              >
                <RefreshCw className="mr-2 h-3 w-3" /> Reset
              </Button>
              <Button size="sm" onClick={handleSave} disabled={isPending}>
                <Save className="mr-2 h-3 w-3" /> Save Changes
              </Button>
            </div>
          )}
        </CardHeader>
        <CardContent className="flex-1 p-0 relative">
          <textarea
            className="w-full h-full font-mono text-sm p-4 bg-muted/30 resize-none focus:outline-none border-0"
            value={yamlContent}
            onChange={(e) => {
              setYamlContent(e.target.value);
              setIsModified(true);
            }}
            spellCheck={false}
            disabled={isPending}
          />
        </CardContent>
      </Card>
    </FadeIn>
  );
}

function ConfigPage() {
  const [view, setView] = useState<"ui" | "yaml">("ui");
  const [refreshKey, setRefreshKey] = useState(0);

  // Force complete remount of config components after settings save
  const forceRefresh = () => {
    setRefreshKey(prev => prev + 1);
  };

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
              View your current workspace configuration.
            </p>
          </div>
        </div>

        <div className="flex items-center gap-2 pt-6">
          <div className="bg-muted rounded-lg p-1 flex items-center">
            <Button
              variant={view === "ui" ? "secondary" : "ghost"}
              size="sm"
              className="h-7 px-2 gap-2"
              onClick={() => setView("ui")}
            >
              <Eye className="h-3.5 w-3.5" />
            </Button>
            <Button
              variant={view === "yaml" ? "secondary" : "ghost"}
              size="sm"
              className="h-7 px-2 gap-2"
              onClick={() => setView("yaml")}
            >
              <Code2 className="h-3.5 w-3.5" />
            </Button>
          </div>
          <div className="w-px h-8 bg-border mx-1" />
          <QueryErrorResetBoundary>
            {({ reset }) => (
              <ErrorBoundary
                onReset={reset}
                fallback={<Settings className="h-4 w-4 text-destructive" />}
              >
                <Suspense
                  fallback={
                    <Settings className="h-4 w-4 animate-spin text-muted-foreground" />
                  }
                >
                  <ConfigLocationSettings onSettingsSaved={forceRefresh} />
                </Suspense>
              </ErrorBoundary>
            )}
          </QueryErrorResetBoundary>
        </div>
      </div>

      <div className="flex-1 min-h-0" key={refreshKey}>
        <QueryErrorResetBoundary>
          {({ reset }) =>
            view === "ui" ? (
              <div className="space-y-6 pb-8">
                {/* General Settings */}
                <FadeIn delay={0.1}>
                  <Card>
                    <CardHeader>
                      <CardTitle className="flex items-center gap-2">
                        <Settings className="h-5 w-5" />
                        General Settings
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <ErrorBoundary
                        onReset={reset}
                        fallbackRender={SectionError}
                      >
                        <Suspense
                          fallback={
                            <div className="grid gap-4 md:grid-cols-3">
                              <Skeleton className="h-16" />
                              <Skeleton className="h-16" />
                              <Skeleton className="h-16" />
                              <Skeleton className="h-16" />
                              <Skeleton className="h-16" />
                              <Skeleton className="h-16" />
                            </div>
                          }
                        >
                          <GeneralSettingsData />
                        </Suspense>
                      </ErrorBoundary>
                    </CardContent>
                  </Card>
                </FadeIn>

              </div>
            ) : (
              <ErrorBoundary onReset={reset} fallbackRender={SectionError}>
                <Suspense fallback={<Skeleton className="h-full w-full" />}>
                  <YamlConfigEditor />
                </Suspense>
              </ErrorBoundary>
            )
          }
        </QueryErrorResetBoundary>
      </div>
    </div>
  );
}
