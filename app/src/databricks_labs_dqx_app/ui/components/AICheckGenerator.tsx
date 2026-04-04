import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Card } from "@/components/ui/card";
import { Loader2, ArrowRight, Copy, Sparkles, Check, Database } from "lucide-react";
import { motion, AnimatePresence } from "motion/react";
import { toast } from "sonner";
import { load } from "js-yaml";

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
  const [copied, setCopied] = useState(false);
  const [isSaving, setIsSaving] = useState(false);

  const handleGenerate = async () => {
    if (!userInput.trim()) {
      toast.error("Please enter a description of your data quality requirements");
      return;
    }

    try {
      const result = await onGenerate(userInput);
      setGeneratedYaml(result.yaml_output);
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
      setGeneratedYaml(result.yaml_output);
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

    const parsed = load(generatedYaml);
    if (!Array.isArray(parsed)) {
      throw new Error("Generated YAML must be a list of checks.");
    }
    return parsed as any[];
  };

  const handleConfirmSave = async () => {
    if (!onConfirmSave) return;

    let parsedChecks: any[];
    try {
      parsedChecks = parseEditedYaml();
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
      await onConfirmSave(parsedChecks);
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

      {/* Generated YAML Output */}
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
              <div className="flex items-center justify-between mb-2">
                <h3 className="text-sm font-semibold text-muted-foreground">
                  Editable YAML
                </h3>
                <div className="flex items-center gap-2">
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
                        "Confirm & Save"
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
              <Card className="flex-1 overflow-auto p-4 bg-card/50 backdrop-blur-sm">
                <Textarea
                  value={generatedYaml}
                  onChange={(e) => setGeneratedYaml(e.target.value)}
                  className="min-h-[420px] h-full resize-none border-0 bg-transparent p-0 text-xs font-mono shadow-none focus-visible:ring-0"
                />
              </Card>
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

