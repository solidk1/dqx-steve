import { describe, expect, it } from "vitest";

import {
  DEFAULT_CHECKS_TABLE_REQUIRED_MESSAGE,
  getOptionalChecksTableName,
  getRequiredChecksTableName,
} from "./checks-table";

describe("getRequiredChecksTableName", () => {
  it("returns the configured default checks table", () => {
    expect(
      getRequiredChecksTableName({
        default_checks_table: "main.analytics.checks",
      }),
    ).toBe("main.analytics.checks");
  });

  it("throws when no default checks table is configured", () => {
    expect(() =>
      getRequiredChecksTableName({
        default_checks_table: null,
      }),
    ).toThrow(DEFAULT_CHECKS_TABLE_REQUIRED_MESSAGE);
  });
});

describe("getOptionalChecksTableName", () => {
  it("returns the configured default checks table when present", () => {
    expect(
      getOptionalChecksTableName({
        default_checks_table: "main.analytics.checks",
      }),
    ).toBe("main.analytics.checks");
  });

  it("returns undefined when no default checks table is configured", () => {
    expect(
      getOptionalChecksTableName({
        default_checks_table: null,
      }),
    ).toBeUndefined();
  });
});
