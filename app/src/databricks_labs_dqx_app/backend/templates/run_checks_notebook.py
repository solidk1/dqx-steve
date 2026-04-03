# Databricks notebook source
# This notebook reads DQ rules from a checks table and applies them to each input table.
#
# Checks table schema:
#   run_config_name  — fully-qualified input table to validate (catalog.schema.table)
#   name             — check rule name
#   criticality      — "error" or "warn"
#   check            — struct { function, arguments: { column, ... } }
#   filter           — optional SQL row filter (applied before the check)
#
# Output tables are written as  <run_config_name>_dqx_result  (overwrite mode).
# Summary metrics are appended to <metrics_table> when provided.

# COMMAND ----------

dbutils.widgets.text("checks_table", "shao_sandbox1.dqx.checks", "Checks Table")
dbutils.widgets.text("metrics_table", "", "Metrics Table (leave empty to skip)")

# COMMAND ----------

checks_table = dbutils.widgets.get("checks_table")
metrics_table = dbutils.widgets.get("metrics_table").strip() or None
print(f"Checks table:  {checks_table}")
print(f"Metrics table: {metrics_table or '(not configured — metrics will not be saved)'}")

# COMMAND ----------

# MAGIC %md ## Step 1 — Rules summary

# COMMAND ----------

checks_df = spark.read.table(checks_table)
checks_df.createOrReplaceTempView("_dqx_checks")

display(
    spark.sql("""
        SELECT
            run_config_name                                                       AS input_table,
            COUNT(*)                                                              AS total_rules,
            SUM(CASE WHEN criticality = 'error' THEN 1 ELSE 0 END)               AS error_rules,
            SUM(CASE WHEN criticality = 'warn'  THEN 1 ELSE 0 END)               AS warn_rules,
            collect_set(
                concat(check.function, '(', coalesce(check.arguments['column'], '*'), ')')
            )                                                                     AS checks_applied
        FROM _dqx_checks
        GROUP BY run_config_name
        ORDER BY run_config_name
    """)
)

# COMMAND ----------

# MAGIC %md ## Step 2 — Apply checks per table

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import TableChecksStorageConfig, InputConfig, OutputConfig
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
import json
import re
import unicodedata

ws = WorkspaceClient()


def _clean_column_name(value: str) -> str:
    if not isinstance(value, str):
        return value

    cleaned = value.strip()
    for _ in range(3):
        if cleaned.startswith('"') and cleaned.endswith('"'):
            try:
                unwrapped = json.loads(cleaned)
            except json.JSONDecodeError:
                break
            if isinstance(unwrapped, str):
                cleaned = unwrapped.strip()
                continue
        break

    for quote in ('"', "'", "`"):
        if cleaned.startswith(quote) and cleaned.endswith(quote) and len(cleaned) >= 2:
            cleaned = cleaned[1:-1].strip()

    cleaned = unicodedata.normalize("NFKC", cleaned)
    cleaned = cleaned.replace("\u200b", "").replace("\ufeff", "")
    return cleaned


def _canonical(name: str) -> str:
    return unicodedata.normalize("NFKC", name).replace("\u200b", "").replace("\ufeff", "").strip().casefold()


def _dequote_once(value: str) -> str:
    if not isinstance(value, str):
        return value
    trimmed = value.strip()
    if len(trimmed) >= 2 and trimmed[0] == trimmed[-1] and trimmed[0] in ('"', "'", "`"):
        return trimmed[1:-1].strip()
    return trimmed


def _candidate_keys(value: str) -> list[str]:
    """Generate matching keys that tolerate quoted/escaped variants."""
    if not isinstance(value, str):
        return []
    keys = []
    current = value
    for _ in range(3):
        k = _canonical(current)
        if k and k not in keys:
            keys.append(k)
        deq = _dequote_once(current)
        if deq == current:
            break
        current = deq
    return keys


def _to_sql_identifier(column_name: str) -> str:
    return f"`{column_name.replace('`', '``')}`"


def _quote_identifiers_in_expression(expression: str, table_columns: list[str]) -> str:
    if not isinstance(expression, str) or not expression.strip():
        return expression

    rewritten = expression
    # Replace longer names first to avoid partial replacements.
    for actual in sorted(table_columns, key=len, reverse=True):
        quoted = _to_sql_identifier(actual)
        if quoted in rewritten:
            continue
        pattern = re.compile(rf"(?<![`\w]){re.escape(actual)}(?![`\w])")
        rewritten = pattern.sub(quoted, rewritten)

    return rewritten


def _align_check_columns(checks: list[dict], table_columns: list[str]) -> list[dict]:
    canonical_to_actual = {}
    for col in table_columns:
        for key in _candidate_keys(col):
            if key and key not in canonical_to_actual:
                canonical_to_actual[key] = col

    for check in checks:
        check_obj = check.get("check") if isinstance(check, dict) else None
        if not isinstance(check_obj, dict):
            continue
        args = check_obj.get("arguments")
        if not isinstance(args, dict):
            continue

        fn = str(check_obj.get("function", "") or "").strip().lower()
        if fn == "sql_expression":
            for expr_key in ("expression", "sql_expression", "expr"):
                expression = args.get(expr_key)
                if isinstance(expression, str):
                    args[expr_key] = _quote_identifiers_in_expression(expression, table_columns)

        def _map_column_value(raw_col: str) -> str | None:
            cleaned_col = _clean_column_name(raw_col)
            for key in _candidate_keys(cleaned_col):
                mapped_col = canonical_to_actual.get(key)
                if mapped_col:
                    return _to_sql_identifier(mapped_col)
            return None

        col = args.get("column")
        if isinstance(col, str):
            mapped_col = _map_column_value(col)
            if mapped_col:
                args["column"] = mapped_col

        cols = args.get("columns")
        if isinstance(cols, list):
            mapped_cols = []
            changed = False
            for item in cols:
                if not isinstance(item, str):
                    mapped_cols.append(item)
                    continue
                mapped_item = _map_column_value(item)
                if mapped_item:
                    mapped_cols.append(mapped_item)
                    changed = True
                else:
                    mapped_cols.append(item)
            if changed:
                args["columns"] = mapped_cols

    return checks

run_configs = sorted({
    row["run_config_name"]
    for row in checks_df.select("run_config_name").distinct().collect()
    if row["run_config_name"]
})

print(f"Found {len(run_configs)} table(s) to check: {run_configs}")

results = []

for table in run_configs:
    sep = "=" * 60
    print(f"\n{sep}\n{table}\n{sep}")

    # Load checks for this table from the checks table
    storage_cfg = TableChecksStorageConfig(location=checks_table, run_config_name=table)

    # Fresh observer per table so metrics are tagged with the input table name
    observer = DQMetricsObserver(name=table)
    engine = DQEngine(workspace_client=ws, spark=spark, observer=observer)

    checks = engine.load_checks(storage_cfg)
    table_columns = spark.read.table(table).columns
    checks = _align_check_columns(checks, table_columns)
    print(f"  {len(checks)} rule(s):")

    for c in checks:
        fn   = (c.get("check") or {}).get("function", "?")
        col  = ((c.get("check") or {}).get("arguments") or {}).get("column", "")
        crit = (c.get("criticality") or "?").upper()
        filt = c.get("filter") or ""
        tag  = f"  [filter: {filt}]" if filt else ""
        print(f"    [{crit:5}] {c.get('name', '?'):<40}  {fn}({col}){tag}")

    output_table = f"{table}_dqx_result"
    metrics_cfg = OutputConfig(location=metrics_table, mode="append") if metrics_table else None

    try:
        engine.apply_checks_by_metadata_and_save_in_table(
            checks=checks,
            input_config=InputConfig(location=table),
            output_config=OutputConfig(location=output_table, mode="overwrite"),
            metrics_config=metrics_cfg,
            checks_location=checks_table,
        )
        print(f"  -> saved to {output_table}")
        if metrics_table:
            print(f"  -> metrics appended to {metrics_table}")
        results.append({"table": table, "rules": len(checks), "output": output_table, "ok": True})
    except Exception as exc:
        import traceback
        print(f"  FAILED: {exc}")
        traceback.print_exc()
        results.append({"table": table, "rules": len(checks), "error": str(exc), "ok": False})

# COMMAND ----------

# MAGIC %md ## Step 3 — Summary

# COMMAND ----------

ok_list  = [r for r in results if r["ok"]]
err_list = [r for r in results if not r["ok"]]

sep = "=" * 60
print(f"\n{sep}\nSUMMARY: {len(ok_list)} succeeded, {len(err_list)} failed\n{sep}")
for r in ok_list:
    print(f"  OK    {r['table']}  ({r['rules']} rules)  ->  {r['output']}")
for r in err_list:
    print(f"  FAIL  {r['table']}  ({r['rules']} rules)  ->  {r.get('error', '?')}")

# COMMAND ----------

if metrics_table:
    display(spark.read.table(metrics_table).orderBy("run_time", "run_name", "metric_name"))

# COMMAND ----------

if err_list:
    raise RuntimeError(f"{len(err_list)} table(s) failed DQX checks — see output above.")
