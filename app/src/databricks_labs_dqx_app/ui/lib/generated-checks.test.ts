import { describe, expect, it } from "vitest";

import {
  buildEditableGeneratedChecks,
  getSelectedGeneratedChecks,
  isEditableThresholdArgument,
  parseGeneratedChecksYaml,
  serializeGeneratedChecksToYaml,
} from "./generated-checks";

describe("buildEditableGeneratedChecks", () => {
  it("defaults every generated rule to selected", () => {
    const editableChecks = buildEditableGeneratedChecks([
      {
        name: "id_is_null",
        description: "检查列id不能为空，避免缺失值。",
        check: { function: "is_not_null", arguments: { column: "id" } },
        criticality: "error",
      },
    ]);

    expect(editableChecks).toHaveLength(1);
    expect(editableChecks[0]?.selected).toBe(true);
  });

  it("preserves selection state when YAML is reparsed", () => {
    const firstPass = buildEditableGeneratedChecks([
      {
        name: "amount_isnt_in_range",
        description: "检查列amount取值应在合理范围内，避免异常值。",
        check: {
          function: "is_in_range",
          arguments: { column: "amount", min_limit: 0, max_limit: 12000 },
        },
        criticality: "error",
      },
    ]);

    const secondPass = buildEditableGeneratedChecks(
      [
        {
          name: "amount_isnt_in_range",
          description: "检查列amount取值应在合理范围内，避免异常值。",
          check: {
            function: "is_in_range",
            arguments: { column: "amount", min_limit: 100, max_limit: 10000 },
          },
          criticality: "error",
        },
      ],
      [{ ...firstPass[0]!, selected: false }],
    );

    expect(secondPass[0]?.selected).toBe(false);
  });
});

describe("generated checks yaml helpers", () => {
  it("round-trips editable checks to YAML without UI metadata", () => {
    const yaml = serializeGeneratedChecksToYaml(
      buildEditableGeneratedChecks([
        {
          name: "id_is_null",
          description: "检查列id不能为空，避免缺失值。",
          check: { function: "is_not_null", arguments: { column: "id" } },
          criticality: "error",
        },
      ]),
    );

    expect(yaml).toContain("name: id_is_null");
    expect(yaml).not.toContain("selected:");
    expect(parseGeneratedChecksYaml(yaml)).toEqual([
      {
        name: "id_is_null",
        description: "检查列id不能为空，避免缺失值。",
        check: { function: "is_not_null", arguments: { column: "id" } },
        criticality: "error",
      },
    ]);
  });

  it("returns only checked rules for save", () => {
    const editableChecks = buildEditableGeneratedChecks([
      {
        name: "id_is_null",
        description: "检查列id不能为空，避免缺失值。",
        check: { function: "is_not_null", arguments: { column: "id" } },
        criticality: "error",
      },
      {
        name: "amount_isnt_in_range",
        description: "检查列amount取值应在合理范围内，避免异常值。",
        check: {
          function: "is_in_range",
          arguments: { column: "amount", min_limit: 0, max_limit: 12000 },
        },
        criticality: "error",
      },
    ]);

    editableChecks[1]!.selected = false;

    expect(getSelectedGeneratedChecks(editableChecks)).toEqual([
      {
        name: "id_is_null",
        description: "检查列id不能为空，避免缺失值。",
        check: { function: "is_not_null", arguments: { column: "id" } },
        criticality: "error",
      },
    ]);
  });
});

describe("isEditableThresholdArgument", () => {
  it("marks threshold arguments as editable", () => {
    expect(isEditableThresholdArgument("min_limit")).toBe(true);
    expect(isEditableThresholdArgument("max_limit")).toBe(true);
    expect(isEditableThresholdArgument("limit")).toBe(true);
  });

  it("keeps non-threshold arguments read only", () => {
    expect(isEditableThresholdArgument("column")).toBe(false);
    expect(isEditableThresholdArgument("trim_strings")).toBe(false);
    expect(isEditableThresholdArgument("allowed")).toBe(false);
  });
});
