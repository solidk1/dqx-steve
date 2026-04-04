import { describe, expect, it } from "vitest";

import { isTableInfoQueryEnabled } from "./explorer-api";

describe("isTableInfoQueryEnabled", () => {
  it("enables table info queries when a table is selected", () => {
    expect(isTableInfoQueryEnabled("main.analytics.orders")).toBe(true);
  });

  it("does not require a warehouse id to enable the query", () => {
    expect(isTableInfoQueryEnabled("main.analytics.orders")).toBe(true);
  });

  it("disables table info queries when no table is selected", () => {
    expect(isTableInfoQueryEnabled(undefined)).toBe(false);
  });
});
