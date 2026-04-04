import { useMemo, useState } from "react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Loader2, ArrowRight, Copy, Sparkles, Check, Database } from "lucide-react";
import { motion, AnimatePresence } from "motion/react";
import { toast } from "sonner";
import {
  buildEditableGeneratedChecks,
  getSelectedGeneratedChecks,
  isEditableThresholdArgument,
  parseGeneratedChecksYaml,
  serializeGeneratedChecksToYaml,
  type EditableGeneratedCheck,
} from "@/lib/generated-checks";

interface AICheckGeneratorProps {
  onGenerate: (userInput: string) => Promise<{ yaml_output: string; checks: any[] }>;
  onSuggestWholeTable?: () => Promise<{ yaml_output: string; checks: any[] }>;
  onConfirmSave?: (checks: any[]) => Promise<void>;
  saveTargetLabel?: string;
  isGenerating: boolean;
}

export function AICheckGenerator({
  onGenerate,
  onSuggestWholeTable,
  onConfirmSave,
  saveTargetLabel,
  isGenerating,
}: AICheckGeneratorProps) {
  const [userInput, setUserInput] = useState("");
  const [generatedYaml, setGeneratedYaml] = useState<string | null>(null);
  const [editableChecks, setEditableChecks] = useState<EditableGeneratedCheck[]>([]);
  const [copied, setCopied] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [yamlError, setYamlError] = useState<string | null>(null);

  const selectedCount = useMemo(
    () => getSelectedGeneratedChecks(editableChecks).length,
    [editableChecks],
  );
  const totalCount = editableChecks.length;

  const syncChecksToYaml = (nextChecks: EditableGeneratedCheck[]) => {
    setEditableChecks(nextChecks);
    setGeneratedYaml(serializeGeneratedChecksToYaml(nextChecks));
    setYamlError(null);
  };

  const loadGeneratedResult = (result: { yaml_output: string; checks: any[] }) => {
    const nextChecks = buildEditableGeneratedChecks(result.checks);
    setEditableChecks(nextChecks);
    setGeneratedYaml(serializeGeneratedChecksToYaml(nextChecks));
    setYamlError(null);
  };

  const handleGenerate = async () => {
    if (!userInput.trim()) {
      toast.error("Please enter a description of your data quality requirements");
      return;
    }

    try {
      const result = await onGenerate(userInput);
      loadGeneratedResult(result);
      toast.success("Checks generated successfully!");
    } catch (error) {
      console.error("Failed to generate checks:", error);
      const detail =
        (error as any)?.response?.data?.detail ||
        (error as Error)?.message ||
        "Please try again.";
      toast.error(`Failed to generate checks: ${detail}`);
    }
  };

  const handleSuggestWholeTable = async () => {
    if (!onSuggestWholeTable) return;

    try {
      const result = await onSuggestWholeTable();
      loadGeneratedResult(result);
      toast.success("Profile-based checks generated successfully!");
    } catch (error) {
      console.error("Failed to generate profile-based checks:", error);
      const detail =
        (error as any)?.response?.data?.detail ||
        (error as Error)?.message ||
        "Please try again.";
      toast.error(`Failed to generate profile-based checks: ${detail}`);
    }
  };

  const parseEditedYaml = (): any[] => {
    if (!generatedYaml?.trim()) {
      throw new Error("No generated YAML to save.");
    }

    return parseGeneratedChecksYaml(generatedYaml);
  };

  const handleYamlChange = (nextYaml: string) => {
    setGeneratedYaml(nextYaml);

    try {
      const parsedChecks = parseGeneratedChecksYaml(nextYaml);
      setEditableChecks((currentChecks) =>
        buildEditableGeneratedChecks(parsedChecks, currentChecks),
      );
      setYamlError(null);
    } catch (error) {
      setYamlError((error as Error).message || "Please fix the YAML before saving.");
    }
  };

  const updateEditableChecks = (
    updater: (currentChecks: EditableGeneratedCheck[]) => EditableGeneratedCheck[],
  ) => {
    setEditableChecks((currentChecks) => {
      const nextChecks = updater(currentChecks);
      setGeneratedYaml(serializeGeneratedChecksToYaml(nextChecks));
      setYamlError(null);
      return nextChecks;
    });
  };

  const handleToggleSelection = (id: string, selected: boolean) => {
    updateEditableChecks((currentChecks) =>
      currentChecks.map((item) =>
        item.id === id ? { ...item, selected } : item,
      ),
    );
  };

  const handleSelectAll = (selected: boolean) => {
    updateEditableChecks((currentChecks) =>
      currentChecks.map((item) => ({ ...item, selected })),
    );
  };

  const handleCheckFieldChange = (
    id: string,
    field: "name" | "description",
    value: string,
  ) => {
    updateEditableChecks((currentChecks) =>
      currentChecks.map((item) =>
        item.id === id
          ? {
              ...item,
              check: {
                ...item.check,
                [field]: value,
              },
            }
          : item,
      ),
    );
  };

  const handleThresholdArgumentChange = (
    id: string,
    argumentName: string,
    rawValue: string,
  ) => {
    updateEditableChecks((currentChecks) =>
      currentChecks.map((item) => {
        if (item.id !== id) {
          return item;
        }

        const checkObject =
          item.check.check && typeof item.check.check === "object"
            ? item.check.check
            : {};
        const argumentsObject =
          checkObject.arguments && typeof checkObject.arguments === "object"
            ? checkObject.arguments
            : {};
        const currentValue = argumentsObject[argumentName];

        let nextValue: string | number = rawValue;
        if (typeof currentValue === "number" && rawValue.trim() !== "") {
          const parsedNumber = Number(rawValue);
          nextValue = Number.isFinite(parsedNumber) ? parsedNumber : rawValue;
        }

        return {
          ...item,
          check: {
            ...item.check,
            check: {
              ...checkObject,
              arguments: {
                ...argumentsObject,
                [argumentName]: nextValue,
              },
            },
          },
        };
      }),
    );
  };

  const getChecksForSave = (): any[] => {
    const parsedChecks = parseEditedYaml();
    const parsedEditableChecks = buildEditableGeneratedChecks(
      parsedChecks,
      editableChecks,
    );
    setEditableChecks(parsedEditableChecks);
    const selectedChecks = getSelectedGeneratedChecks(parsedEditableChecks);
    if (selectedChecks.length === 0) {
      throw new Error("Select at least one rule to save.");
    }
    return selectedChecks;
  };

  const handleConfirmSave = async () => {
    if (!onConfirmSave) return;

    let selectedChecks: any[];
    try {
      selectedChecks = getChecksForSave();
    } catch (error) {
      const detail = (error as Error).message || "Please fix the YAML and try again.";
      toast.error(`Failed to parse YAML: ${detail}`);
      return;
    }

    const confirmed = window.confirm(
      saveTargetLabel
        ? `Save generated checks to ${saveTargetLabel}?`
        : "Save generated checks?",
    );
    if (!confirmed) return;

    try {
      setIsSaving(true);
      await onConfirmSave(selectedChecks);
      toast.success("Generated checks saved successfully!");
    } catch (error) {
      const detail =
        (error as any)?.response?.data?.detail ||
        (error as Error)?.message ||
        "Please try again.";
      toast.error(`Failed to save generated checks: ${detail}`);
    } finally {
      setIsSaving(false);
    }
  };

  const handleCopy = () => {
    if (generatedYaml) {
      navigator.clipboard.writeText(generatedYaml);
      setCopied(true);
      toast.success("YAML copied to clipboard");
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleGenerate();
    }
  };

  const renderArgumentValue = (value: unknown) => {
    if (Array.isArray(value)) {
      return value.join(", ");
    }
    if (value && typeof value === "object") {
      return JSON.stringify(value);
    }
    return String(value ?? "");
  };

  return (
    <div className="flex flex-col h-full bg-gradient-to-br from-primary/5 via-background to-secondary/5 p-6">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <div className="p-2 bg-primary/10 rounded-lg">
          <Sparkles className="h-6 w-6 text-primary" />
        </div>
        <div>
          <h2 className="text-2xl font-bold">AI-Assisted Rules Generation</h2>
          <p className="text-sm text-muted-foreground">
            Describe your data quality needs
          </p>
        </div>
      </div>

      {onSuggestWholeTable && (
        <div className="mb-4">
          <Button
            onClick={handleSuggestWholeTable}
            disabled={isGenerating || isSaving}
            variant="secondary"
            className="gap-2"
          >
            {isGenerating ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin" />
                Profiling Table...
              </>
            ) : (
              <>
                <Database className="h-4 w-4" />
                Profile Table + Suggest Rules
              </>
            )}
          </Button>
        </div>
      )}

      {/* Generated Rules Output */}
      <div className="flex-1 mb-4 overflow-hidden">
        <AnimatePresence mode="wait">
          {generatedYaml ? (
            <motion.div
              key="yaml-output"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              transition={{ duration: 0.3 }}
              className="h-full flex flex-col"
            >
              <div className="flex items-center justify-between mb-2 gap-3 flex-wrap">
                <div>
                  <h3 className="text-sm font-semibold text-muted-foreground">
                    Suggested Rules
                  </h3>
                  <p className="text-xs text-muted-foreground">
                    {selectedCount} of {totalCount} rules selected for save
                  </p>
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => handleSelectAll(true)}
                    disabled={isSaving || isGenerating}
                  >
                    Select All
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => handleSelectAll(false)}
                    disabled={isSaving || isGenerating}
                  >
                    Clear All
                  </Button>
                  {onConfirmSave && (
                    <Button
                      size="sm"
                      onClick={handleConfirmSave}
                      disabled={isSaving || isGenerating}
                    >
                      {isSaving ? (
                        <>
                          <Loader2 className="h-3 w-3 mr-2 animate-spin" />
                          Saving...
                        </>
                      ) : (
                        `Save Selected (${selectedCount})`
                      )}
                    </Button>
                  )}
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={handleCopy}
                    className="gap-2"
                  >
                    {copied ? (
                      <>
                        <Check className="h-3 w-3" />
                        Copied
                      </>
                    ) : (
                      <>
                        <Copy className="h-3 w-3" />
                        Copy
                      </>
                    )}
                  </Button>
                </div>
              </div>
              <div className="grid gap-4 lg:grid-cols-[minmax(0,1.25fr)_minmax(0,0.95fr)] min-h-0 flex-1">
                <Card className="flex min-h-[420px] flex-col overflow-hidden bg-card/50 backdrop-blur-sm">
                  <div className="border-b px-4 py-3">
                    <h4 className="text-sm font-medium">Rule List</h4>
                    <p className="text-xs text-muted-foreground">
                      Uncheck rules to skip them during save. Edit threshold values directly here.
                    </p>
                  </div>
                  <div className="flex-1 overflow-auto p-4">
                    <div className="space-y-3">
                      {editableChecks.map((item, index) => {
                        const checkObject =
                          item.check.check && typeof item.check.check === "object"
                            ? item.check.check
                            : {};
                        const argumentsObject =
                          checkObject.arguments &&
                          typeof checkObject.arguments === "object"
                            ? checkObject.arguments
                            : {};
                        const functionName = String(checkObject.function ?? "unknown");
                        const columnName = String(argumentsObject.column ?? "").trim();
                        const thresholdEntries = Object.entries(argumentsObject).filter(
                          ([key]) => isEditableThresholdArgument(key),
                        );
                        const readOnlyEntries = Object.entries(argumentsObject).filter(
                          ([key]) =>
                            key !== "column" && !isEditableThresholdArgument(key),
                        );

                        return (
                          <div
                            key={item.id}
                            className={`rounded-lg border p-3 transition-colors ${
                              item.selected
                                ? "border-primary/30 bg-background"
                                : "border-border/60 bg-muted/20 opacity-80"
                            }`}
                          >
                            <div className="flex items-start gap-2.5">
                              <input
                                type="checkbox"
                                checked={item.selected}
                                onChange={(e) =>
                                  handleToggleSelection(item.id, e.target.checked)
                                }
                                className="mt-1.5 h-4 w-4 rounded border-input"
                              />
                              <div className="min-w-0 flex-1 space-y-2.5">
                                <div className="flex items-center gap-2 flex-wrap">
                                  <Badge variant="outline">Rule {index + 1}</Badge>
                                  {columnName && (
                                    <Badge
                                      variant="outline"
                                      className="max-w-[16rem] truncate font-mono"
                                      title={columnName}
                                    >
                                      {columnName}
                                    </Badge>
                                  )}
                                  <Badge variant="secondary">{functionName}</Badge>
                                  {item.check.criticality && (
                                    <Badge>{String(item.check.criticality)}</Badge>
                                  )}
                                </div>

                                <div className="grid gap-2.5">
                                  <div className="space-y-1">
                                    <label className="text-xs font-medium text-muted-foreground">
                                      Name
                                    </label>
                                    <Input
                                      value={String(item.check.name ?? "")}
                                      onChange={(e) =>
                                        handleCheckFieldChange(
                                          item.id,
                                          "name",
                                          e.target.value,
                                        )
                                      }
                                      disabled={isSaving || isGenerating}
                                    />
                                  </div>

                                  <div className="space-y-1">
                                    <label className="text-xs font-medium text-muted-foreground">
                                      Description
                                    </label>
                                    <Input
                                      value={String(item.check.description ?? "")}
                                      onChange={(e) =>
                                        handleCheckFieldChange(
                                          item.id,
                                          "description",
                                          e.target.value,
                                        )
                                      }
                                      disabled={isSaving || isGenerating}
                                    />
                                  </div>

                                  {thresholdEntries.length > 0 && (
                                    <div className="grid gap-3 md:grid-cols-3">
                                      {thresholdEntries.map(([key, value]) => (
                                        <div key={key} className="space-y-1">
                                          <label className="text-xs font-medium text-muted-foreground">
                                            {key}
                                          </label>
                                          <Input
                                            value={String(value ?? "")}
                                            onChange={(e) =>
                                              handleThresholdArgumentChange(
                                                item.id,
                                                key,
                                                e.target.value,
                                              )
                                            }
                                            disabled={isSaving || isGenerating}
                                          />
                                        </div>
                                      ))}
                                    </div>
                                  )}

                                  {readOnlyEntries.length > 0 && (
                                    <div className="space-y-2">
                                      <p className="text-xs font-medium text-muted-foreground">
                                        Other Arguments
                                      </p>
                                      <div className="grid gap-2 md:grid-cols-2">
                                        {readOnlyEntries.map(([key, value]) => (
                                          <div
                                            key={key}
                                            className="rounded-md border bg-muted/30 px-3 py-2 text-xs"
                                          >
                                            <div className="font-medium text-muted-foreground">
                                              {key}
                                            </div>
                                            <div className="mt-1 break-words">
                                              {renderArgumentValue(value)}
                                            </div>
                                          </div>
                                        ))}
                                      </div>
                                    </div>
                                  )}
                                </div>
                              </div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                </Card>

                <Card className="flex min-h-[420px] flex-col overflow-hidden bg-card/50 backdrop-blur-sm">
                  <div className="border-b px-4 py-3">
                    <h4 className="text-sm font-medium">Editable YAML</h4>
                    <p className="text-xs text-muted-foreground">
                      Manual YAML edits update the rule list when the YAML is valid.
                    </p>
                  </div>
                  <div className="flex-1 p-4">
                    <Textarea
                      value={generatedYaml}
                      onChange={(e) => handleYamlChange(e.target.value)}
                      className="min-h-[420px] h-full resize-none border-0 bg-transparent p-0 text-xs font-mono shadow-none focus-visible:ring-0"
                    />
                    {yamlError && (
                      <p className="mt-3 text-xs text-destructive">
                        YAML parse error: {yamlError}
                      </p>
                    )}
                  </div>
                </Card>
              </div>
            </motion.div>
          ) : (
            <motion.div
              key="placeholder"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.3 }}
              className="h-full flex items-center justify-center"
            >
              <div className="text-center space-y-4 text-muted-foreground">
                <Sparkles className="h-16 w-16 mx-auto opacity-20" />
                <div>
                  <p className="font-medium">No rules generated yet</p>
                  <p className="text-sm">
                    Enter your requirements below to get started
                  </p>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      {/* Input Area */}
      <div className="space-y-3">
        <div className="relative">
          <Textarea
            value={userInput}
            onChange={(e) => setUserInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Example: Sales amount must be positive"
            className="min-h-[120px] resize-none pr-12 bg-card/50 backdrop-blur-sm"
            disabled={isGenerating}
          />
          <div className="absolute bottom-3 right-3">
            <Button
              size="icon"
              onClick={handleGenerate}
              disabled={isGenerating || !userInput.trim()}
              className="rounded-full"
            >
              {isGenerating ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <ArrowRight className="h-4 w-4" />
              )}
            </Button>
          </div>
        </div>
        <p className="text-xs text-muted-foreground">
          Press <kbd className="px-1.5 py-0.5 text-xs font-semibold bg-muted rounded">Enter</kbd> to generate or{" "}
          <kbd className="px-1.5 py-0.5 text-xs font-semibold bg-muted rounded">Shift+Enter</kbd> for a new line
        </p>
      </div>
    </div>
  );
}

