import { dump, load } from "js-yaml";

export type GeneratedCheck = Record<string, any>;

export type EditableGeneratedCheck = {
  id: string;
  selected: boolean;
  check: GeneratedCheck;
};

const THRESHOLD_ARGUMENT_NAMES = new Set(["min_limit", "max_limit", "limit"]);

function getCheckSelectionKey(check: GeneratedCheck, index: number): string {
  const name = String(check.name ?? "").trim();
  if (name) {
    return `name:${name}`;
  }

  const checkObj =
    check.check && typeof check.check === "object" ? check.check : {};
  const functionName = String((checkObj as any).function ?? "").trim();
  const argumentsObject =
    (checkObj as any).arguments && typeof (checkObj as any).arguments === "object"
      ? (checkObj as any).arguments
      : {};

  return `generated:${functionName}:${JSON.stringify(argumentsObject)}:${index}`;
}

export function buildEditableGeneratedChecks(
  checks: GeneratedCheck[],
  previousChecks: EditableGeneratedCheck[] = [],
): EditableGeneratedCheck[] {
  const previousSelectionByKey = new Map(
    previousChecks.map((item) => [getCheckSelectionKey(item.check, 0), item.selected]),
  );

  return checks.map((check, index) => {
    const id = getCheckSelectionKey(check, index);
    return {
      id,
      selected: previousSelectionByKey.get(id) ?? true,
      check,
    };
  });
}

export function serializeGeneratedChecksToYaml(
  editableChecks: EditableGeneratedCheck[],
): string {
  return dump(
    editableChecks.map((item) => item.check),
    {
      sortKeys: false,
      noRefs: true,
      lineWidth: -1,
    },
  );
}

export function parseGeneratedChecksYaml(yamlText: string): GeneratedCheck[] {
  const parsed = load(yamlText);
  if (!Array.isArray(parsed)) {
    throw new Error("Generated YAML must be a list of checks.");
  }
  return parsed as GeneratedCheck[];
}

export function getSelectedGeneratedChecks(
  editableChecks: EditableGeneratedCheck[],
): GeneratedCheck[] {
  return editableChecks
    .filter((item) => item.selected)
    .map((item) => item.check);
}

export function isEditableThresholdArgument(argumentName: string): boolean {
  return THRESHOLD_ARGUMENT_NAMES.has(argumentName);
}
