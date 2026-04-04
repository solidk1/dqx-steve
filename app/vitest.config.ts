import { resolve } from "path";

import { defineConfig } from "vitest/config";

const APP_UI_PATH = resolve(__dirname, "src/databricks_labs_dqx_app/ui");

export default defineConfig({
  resolve: {
    alias: {
      "@": APP_UI_PATH,
    },
  },
  test: {
    environment: "node",
    include: ["src/databricks_labs_dqx_app/ui/**/*.test.ts"],
  },
});
