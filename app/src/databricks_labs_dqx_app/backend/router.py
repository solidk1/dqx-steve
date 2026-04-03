import re
import json
from itertools import islice
from pathlib import Path
from typing import Annotated

import yaml
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.config import (
    InputConfig,
    InstallationChecksStorageConfig,
    TableChecksStorageConfig,
    WorkspaceConfig,
)
from databricks.labs.dqx.config_serializer import ConfigSerializer
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidCheckError, InvalidConfigError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied, ResourceDoesNotExist
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.iam import User as UserOut
from databricks.sdk.service.workspace import ImportFormat
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pyspark.sql import SparkSession

from .config import conf
from .dependencies import get_app_spark, get_app_ws, get_engine, get_generator, get_obo_ws
from .logger import logger
from .models import (
    CheckErrorRowsOut,
    CatalogsOut,
    ChecksIn,
    ChecksOut,
    ChecksTableOut,
    ColumnInfoOut,
    ConfigIn,
    ConfigOut,
    DashboardOut,
    ExecuteRunOut,
    ExecuteRunsOut,
    GenerateChecksIn,
    GenerateChecksOut,
    InstallationSettings,
    RunConfigIn,
    RunConfigOut,
    RunChecksJobOut,
    SchemasOut,
    SaveGeneratedChecksIn,
    SaveGeneratedChecksOut,
    TableInfoOut,
    TablesOut,
    VersionOut,
)
from .settings import SettingsManager

api = APIRouter(prefix=conf.api_prefix)
_UNICODE_ESCAPE_RE = re.compile(r"\\u([0-9a-fA-F]{4})")


def get_install_folder(ws: WorkspaceClient, path: str | None, user_name: str | None = None) -> str:
    folder = path
    if not folder:
        settings = SettingsManager(ws, user_name=user_name).get_settings()
        folder = settings.install_folder
        logger.info(f"Using install folder from settings: {folder}")
    else:
        logger.info(f"Using install folder from path parameter: {folder}")
    return folder.strip()


@api.get("/version", response_model=VersionOut, operation_id="version")
async def version():
    return VersionOut.from_metadata()


@api.get("/current-user", response_model=UserOut, operation_id="currentUser")
def me(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
    return obo_ws.current_user.me()


@api.get("/settings", response_model=InstallationSettings, operation_id="get_settings")
def get_settings(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
):
    user_name = obo_ws.current_user.me().user_name
    return SettingsManager(app_ws, user_name=user_name).get_settings()


@api.post("/settings", response_model=InstallationSettings, operation_id="save_settings")
def save_settings(
    settings: InstallationSettings,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
):
    user_name = obo_ws.current_user.me().user_name
    try:
        return SettingsManager(app_ws, user_name=user_name).save_settings(settings)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@api.get("/config", response_model=ConfigOut, operation_id="config")
def get_config(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> ConfigOut:
    user_name = obo_ws.current_user.me().user_name
    install_folder = get_install_folder(app_ws, path, user_name=user_name)
    logger.info(f"Loading config from install folder: {install_folder}")
    serializer = ConfigSerializer(app_ws)
    try:
        config = serializer.load_config(install_folder=install_folder)
        logger.info(f"Successfully loaded config with {len(config.run_configs)} run configs")
        return ConfigOut(config=config)
    except ResourceDoesNotExist:
        logger.info(f"No config.yml found at {install_folder}, returning empty config")
        return ConfigOut(config=WorkspaceConfig(run_configs=[]))
    except InvalidConfigError as e:
        logger.error(f"Invalid configuration at {install_folder}: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid configuration at {install_folder}: {e}")
    except PermissionDenied as e:
        logger.error(f"Permission denied while loading config at {install_folder}: {e}")
        raise HTTPException(status_code=403, detail=f"Permission denied: {e}")


@api.post("/config", response_model=ConfigOut, operation_id="save_config")
def save_config(
    body: ConfigIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> ConfigOut:
    user_name = obo_ws.current_user.me().user_name
    install_folder = get_install_folder(app_ws, path, user_name=user_name)
    serializer = ConfigSerializer(app_ws)
    serializer.save_config(body.config, install_folder=install_folder)
    return ConfigOut(config=serializer.load_config(install_folder=install_folder))


@api.get("/config/run/{name}", response_model=RunConfigOut, operation_id="get_run_config")
def get_run_config(
    name: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> RunConfigOut:
    user_name = obo_ws.current_user.me().user_name
    install_folder = get_install_folder(app_ws, path, user_name=user_name)
    serializer = ConfigSerializer(app_ws)
    try:
        return RunConfigOut(config=serializer.load_run_config(run_config_name=name, install_folder=install_folder))
    except (ResourceDoesNotExist, InvalidConfigError):
        raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")


@api.post("/config/run", response_model=RunConfigOut, operation_id="save_run_config")
def save_run_config(
    body: RunConfigIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> RunConfigOut:
    user_name = obo_ws.current_user.me().user_name
    install_folder = get_install_folder(app_ws, path, user_name=user_name)
    serializer = ConfigSerializer(app_ws)
    serializer.save_run_config(body.config, install_folder=install_folder)
    return RunConfigOut(
        config=serializer.load_run_config(run_config_name=body.config.name, install_folder=install_folder)
    )


@api.delete("/config/run/{name}", response_model=ConfigOut, operation_id="delete_run_config")
def delete_run_config(
    name: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> ConfigOut:
    user_name = obo_ws.current_user.me().user_name
    install_folder = get_install_folder(app_ws, path, user_name=user_name)
    serializer = ConfigSerializer(app_ws)

    try:
        config = serializer.load_config(install_folder=install_folder)
    except (ResourceDoesNotExist, InvalidConfigError):
        raise HTTPException(status_code=404, detail=f"Configuration not found at {install_folder}")

    original_count = len(config.run_configs)
    config.run_configs = [rc for rc in config.run_configs if rc.name != name]

    if len(config.run_configs) == original_count:
        raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")

    serializer.save_config(config, install_folder=install_folder)
    return ConfigOut(config=config)


@api.get("/config/run/{name}/checks", response_model=ChecksOut, operation_id="get_run_checks")
def get_run_checks(
    name: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    engine: Annotated[DQEngine, Depends(get_engine)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> ChecksOut:
    user_name = obo_ws.current_user.me().user_name
    install_folder = get_install_folder(app_ws, path, user_name=user_name)
    serializer = ConfigSerializer(app_ws)
    try:
        run_config = serializer.load_run_config(run_config_name=name, install_folder=install_folder)
    except (ResourceDoesNotExist, InvalidConfigError):
        raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")

    checks_config = InstallationChecksStorageConfig(run_config_name=run_config.name, install_folder=install_folder)

    try:
        checks = engine.load_checks(checks_config)
        return ChecksOut(checks=checks)
    except (NotFound, FileNotFoundError):
        return ChecksOut(checks=[])
    except InvalidCheckError as e:
        raise HTTPException(status_code=400, detail=f"Invalid checks format: {e}")
    except InvalidConfigError as e:
        raise HTTPException(status_code=400, detail=f"Invalid configuration: {e}")


@api.post("/config/run/{name}/checks", response_model=ChecksOut, operation_id="save_run_checks")
def save_run_checks(
    name: str,
    body: ChecksIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    engine: Annotated[DQEngine, Depends(get_engine)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> ChecksOut:
    user_name = obo_ws.current_user.me().user_name
    install_folder = get_install_folder(app_ws, path, user_name=user_name)
    serializer = ConfigSerializer(app_ws)
    try:
        run_config = serializer.load_run_config(run_config_name=name, install_folder=install_folder)
    except (ResourceDoesNotExist, InvalidConfigError):
        raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")

    checks_config = InstallationChecksStorageConfig(run_config_name=run_config.name, install_folder=install_folder)
    engine.save_checks(body.checks, checks_config)
    return ChecksOut(checks=body.checks)


@api.post("/config/run/{name}/execute", response_model=ExecuteRunOut, operation_id="execute_run_config")
def execute_run_config(
    name: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    spark: Annotated[SparkSession, Depends(get_app_spark)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> ExecuteRunOut:
    user_name = obo_ws.current_user.me().user_name
    install_folder = get_install_folder(app_ws, path, user_name=user_name)
    serializer = ConfigSerializer(app_ws)
    try:
        run_config = serializer.load_run_config(run_config_name=name, install_folder=install_folder)
    except (ResourceDoesNotExist, InvalidConfigError):
        raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")

    try:
        run_id, run_url = _submit_dqx_run(app_ws, install_folder, run_config_name=name)
        return ExecuteRunOut(run_name=name, status="submitted", run_id=run_id, run_url=run_url)
    except Exception as e:
        logger.error(f"Failed to submit run config '{name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to submit run '{name}': {e}")


@api.post("/config/runs/execute", response_model=ExecuteRunsOut, operation_id="execute_all_run_configs")
def execute_all_run_configs(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    spark: Annotated[SparkSession, Depends(get_app_spark)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> ExecuteRunsOut:
    user_name = obo_ws.current_user.me().user_name
    install_folder = get_install_folder(app_ws, path, user_name=user_name)
    serializer = ConfigSerializer(app_ws)
    try:
        config = serializer.load_config(install_folder=install_folder)
    except ResourceDoesNotExist:
        raise HTTPException(status_code=400, detail="No run configs found in config.yml")
    except InvalidConfigError as e:
        raise HTTPException(status_code=400, detail=f"Invalid configuration: {e}")
    except Exception as e:
        logger.error(f"Failed to load config for executing runs from '{install_folder}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to load run configs: {e}")

    run_configs = config.run_configs or []
    if not run_configs:
        raise HTTPException(status_code=400, detail="No run configs found in config.yml")

    try:
        run_id, run_url = _submit_dqx_run(app_ws, install_folder, run_config_name="")
        return ExecuteRunsOut(status="submitted", run_id=run_id, run_url=run_url)
    except Exception as e:
        logger.error(f"Failed to submit all run configs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to submit all runs: {e}")


@api.post("/ai-generate-checks", response_model=GenerateChecksOut, operation_id="ai_assisted_checks_generation")
def ai_generate_checks(
    body: GenerateChecksIn,
    generator: Annotated[DQGenerator, Depends(get_generator)],
) -> GenerateChecksOut:
    """Generate data quality checks from natural language using AI-assisted generation."""
    try:
        user_input = body.user_input
        input_config = None
        if body.table_name:
            if not _TABLE_NAME_RE.match(body.table_name):
                raise HTTPException(status_code=400, detail="table_name must be a 3-part dotted identifier (catalog.schema.table)")

            input_config = InputConfig(location=body.table_name)
            quoted_table_name = _quote_3part_table_name(body.table_name)
            sample_rows = [row.asDict() for row in generator.spark.sql(f"SELECT * FROM {quoted_table_name} LIMIT 20").collect()]
            sample_rows_json = json.dumps(sample_rows, default=str, ensure_ascii=False)
            user_input = (
                f"{body.user_input}\n\n"
                f"Selected table: {body.table_name}\n"
                f"Sample rows (JSON):\n{sample_rows_json}"
            )

        checks = generator.generate_dq_rules_ai_assisted(user_input=user_input, input_config=input_config)
        checks = _normalize_generated_checks(checks)

        # Convert checks to YAML
        yaml_output = yaml.dump(checks, default_flow_style=False, sort_keys=False, allow_unicode=True)

        return GenerateChecksOut(yaml_output=yaml_output, checks=checks)
    except Exception as e:
        logger.error(f"Failed to generate checks: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to generate checks: {str(e)}")


_TABLE_NAME_RE = re.compile(r"^[\w]+\.[\w]+\.[\w]+$")


def _quote_3part_table_name(full_name: str) -> str:
    parts = full_name.split(".")
    return ".".join(f"`{p}`" for p in parts)


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _decode_unicode_escapes(value):
    if isinstance(value, str):
        return _UNICODE_ESCAPE_RE.sub(lambda m: chr(int(m.group(1), 16)), value)
    if isinstance(value, list):
        return [_decode_unicode_escapes(item) for item in value]
    if isinstance(value, dict):
        return {k: _decode_unicode_escapes(v) for k, v in value.items()}
    return value


def _normalize_column_argument(value: str) -> str:
    normalized = value.strip()

    # Some model outputs arrive as a JSON-encoded string literal (sometimes nested),
    # e.g. "\"Rev\\u5b8c\\u6210\\u91d1\\u989d\"". Unwrap up to 3 layers.
    for _ in range(3):
        if normalized.startswith('"') and normalized.endswith('"'):
            try:
                unwrapped = json.loads(normalized)
            except json.JSONDecodeError:
                break
            if isinstance(unwrapped, str):
                normalized = unwrapped.strip()
                continue
        break

    return _decode_unicode_escapes(normalized)


def _normalize_generated_checks(checks):
    normalized = _decode_unicode_escapes(checks)
    if not isinstance(normalized, list):
        return normalized

    result = []
    for check_item in normalized:
        if not isinstance(check_item, dict):
            result.append(check_item)
            continue

        check_obj = check_item.get("check")
        if isinstance(check_obj, dict):
            arguments = check_obj.get("arguments")
            if isinstance(arguments, dict):
                column = arguments.get("column")
                if isinstance(column, str):
                    arguments["column"] = _normalize_column_argument(column)
        result.append(check_item)
    return result


def _canonical_column_name(value: str) -> str:
    normalized = _normalize_column_argument(value).strip()
    for quote in ('"', "'", "`"):
        if normalized.startswith(quote) and normalized.endswith(quote) and len(normalized) >= 2:
            normalized = normalized[1:-1].strip()
    return normalized


def _align_check_columns_with_table_schema(checks, run_config_name: str, spark: SparkSession):
    if not isinstance(checks, list) or not _TABLE_NAME_RE.match(run_config_name):
        return checks

    try:
        quoted_table_name = _quote_3part_table_name(run_config_name)
        actual_columns = spark.sql(f"SELECT * FROM {quoted_table_name} LIMIT 0").columns
    except Exception:
        # If schema lookup fails, keep original checks and let normal validation handle runtime behavior.
        return checks

    canonical_to_actual: dict[str, str] = {}
    for actual in actual_columns:
        canonical = _canonical_column_name(actual).casefold()
        if canonical and canonical not in canonical_to_actual:
            canonical_to_actual[canonical] = actual

    for check_item in checks:
        if not isinstance(check_item, dict):
            continue
        check_obj = check_item.get("check")
        if not isinstance(check_obj, dict):
            continue
        arguments = check_obj.get("arguments")
        if not isinstance(arguments, dict):
            continue
        column = arguments.get("column")
        if not isinstance(column, str):
            continue

        normalized_column = _canonical_column_name(column)
        matched = canonical_to_actual.get(normalized_column.casefold())
        if matched:
            arguments["column"] = matched
        else:
            # Keep normalized value even when no match is found to avoid persisting escaped/quoted artifacts.
            arguments["column"] = normalized_column

    return checks


def _upsert_checks_by_name(existing_checks, incoming_checks):
    existing_list = existing_checks if isinstance(existing_checks, list) else []
    incoming_list = incoming_checks if isinstance(incoming_checks, list) else []

    incoming_named: dict[str, dict] = {}
    incoming_unnamed: list[dict] = []
    for check in incoming_list:
        if not isinstance(check, dict):
            continue
        name = str(check.get("name", "")).strip() if check.get("name") is not None else ""
        if name:
            incoming_named[name] = check
        else:
            incoming_unnamed.append(check)

    merged: list[dict] = []
    for check in existing_list:
        if not isinstance(check, dict):
            continue
        existing_name = str(check.get("name", "")).strip() if check.get("name") is not None else ""
        if existing_name and existing_name in incoming_named:
            merged.append(incoming_named.pop(existing_name))
        else:
            merged.append(check)

    merged.extend(incoming_named.values())
    merged.extend(incoming_unnamed)
    return merged


def _update_missing_descriptions_with_ai(
    spark: SparkSession,
    checks_table_name: str,
    run_config_name: str,
) -> None:
    quoted_table_name = _quote_3part_table_name(checks_table_name)
    run_config_literal = _sql_string_literal(run_config_name)
    spark.sql(
        f"""
        MERGE INTO {quoted_table_name} AS target
        USING (
          SELECT
            `run_config_name`,
            `name`,
            CAST(`criticality` AS STRING) AS `criticality_text`,
            to_json(`check`) AS `check_json`,
            CAST(`filter` AS STRING) AS `filter_text`,
            ai_query(
              'databricks-claude-sonnet-4-6',
              concat(
                '你是一个数据质量专家。请根据以下数据质量检查规则',
                ', 检查参数: ', to_json(`check`),
                ', 严重级别: ', CAST(`criticality` AS STRING),
                '。只输出描述文字，不要加任何前缀或解释。输出格式：字段：{{check.arguments.column}}， 检查内容：xxxx, 错误级别：{{criticality}}'
              )
            ) AS `generated_description`
          FROM {quoted_table_name}
          WHERE `run_config_name` = {run_config_literal}
            AND (`description` IS NULL OR trim(`description`) = '')
        ) AS source
        ON target.`run_config_name` = source.`run_config_name`
          AND coalesce(target.`name`, '') = coalesce(source.`name`, '')
          AND coalesce(CAST(target.`criticality` AS STRING), '') = coalesce(source.`criticality_text`, '')
          AND coalesce(to_json(target.`check`), '') = coalesce(source.`check_json`, '')
          AND coalesce(CAST(target.`filter` AS STRING), '') = coalesce(source.`filter_text`, '')
          AND (target.`description` IS NULL OR trim(target.`description`) = '')
        WHEN MATCHED THEN
          UPDATE SET target.`description` = source.`generated_description`
        """
    )

    return None


def _format_spark_error_message(error: Exception) -> str:
    """
    Convert verbose Spark/Databricks exceptions into a concise, user-facing message.
    """
    message = str(error).split("JVM stacktrace:")[0].strip()
    message = " ".join(message.split())

    if "[INSUFFICIENT_PERMISSIONS]" in message:
        return f"Insufficient permissions to read data. {message}"
    if "[TABLE_OR_VIEW_NOT_FOUND]" in message:
        return f"Table not found. {message}"
    return message


def _submit_dqx_run(
    ws: WorkspaceClient,
    install_folder: str,
    run_config_name: str,
) -> tuple[int, str]:
    """Submit a one-time serverless DQX quality-checker run via jobs.submit."""
    config_path = f"/Workspace{install_folder.rstrip('/')}/config.yml"
    named_parameters = {
        "config": config_path,
        "run_config_name": run_config_name,
        "workflow": "quality-checker",
        "task": "apply_checks",
    }

    label = run_config_name or "all"
    response = ws.jobs.submit(
        run_name=f"DQX quality-checker ({label})",
        tasks=[
            jobs.SubmitTask(
                task_key="apply_checks",
                environment_key="default",
                python_wheel_task=jobs.PythonWheelTask(
                    package_name="databricks_labs_dqx",
                    entry_point="runtime",
                    named_parameters=named_parameters,
                ),
            )
        ],
        environments=[
            jobs.JobEnvironment(
                environment_key="default",
                spec=compute.Environment(
                    client="1",
                    dependencies=["databricks-labs-dqx"],
                ),
            )
        ],
    )

    run_id = response.run_id
    if run_id is None:
        raise RuntimeError("Job submission succeeded but run_id is missing")
    host = (ws.config.host or "").rstrip("/")
    run_url = f"{host}/#job/runs/{int(run_id)}"
    return int(run_id), run_url


def _resolve_dashboard_ids(
    ws: WorkspaceClient,
    install_folder: str,
) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()

    installation_candidates = [
        Installation(ws, "dqx", install_folder=install_folder),
    ]
    try:
        installation_candidates.append(Installation.current(ws, "dqx"))
    except Exception:
        pass
    try:
        installation_candidates.append(Installation.current(ws, "dqx", assume_user=False))
    except Exception:
        pass
    try:
        installation_candidates.append(Installation.assume_user_home(ws, "dqx"))
    except Exception:
        pass
    try:
        installation_candidates.append(Installation.assume_global(ws, "dqx"))
    except Exception:
        pass

    for installation in installation_candidates:
        try:
            install_state = InstallState.from_installation(installation)
            for dashboard_id in install_state.dashboards.values():
                if not dashboard_id:
                    continue
                dashboard_id = str(dashboard_id)
                if dashboard_id in seen:
                    continue
                # Keep only dashboards that still exist.
                ws.lakeview.get(dashboard_id)
                seen.add(dashboard_id)
                ids.append(dashboard_id)
        except Exception:
            continue

    # Fallback: discover directly from Lakeview dashboards if install state is unavailable.
    if not ids:
        preferred: list[str] = []
        any_active: list[str] = []
        try:
            for dashboard in islice(ws.lakeview.list(), 500):
                dashboard_id = str(getattr(dashboard, "dashboard_id", "") or "").strip()
                if not dashboard_id or dashboard_id in seen:
                    continue
                lifecycle_state = str(getattr(dashboard, "lifecycle_state", "") or "")
                if "TRASHED" in lifecycle_state.upper():
                    continue
                display_name = str(getattr(dashboard, "display_name", "") or "")
                lowered = display_name.lower()
                if "dqx" in lowered or "data quality" in lowered:
                    preferred.append(dashboard_id)
                any_active.append(dashboard_id)
                seen.add(dashboard_id)
        except Exception:
            pass

        ids.extend(preferred or any_active[:1])

    return ids


def _extract_workspace_id(host: str) -> str:
    """Extract workspace ID from Databricks host URL (e.g. adb-984752964297111.11.azuredatabricks.net)."""
    m = re.search(r"adb-(\d+)", host)
    if m:
        return m.group(1)
    # AWS: e.g. dbc-12345678-abcd.cloud.databricks.com — fall back to empty string
    return ""


_DASHBOARD_ID = "01f1115b385b1fb7b2a07ece35a6abb3"
_DASHBOARD_PUBLISHED_URL = (
    "https://adb-984752964297111.11.azuredatabricks.net/"
    "dashboardsv3/01f1115b385b1fb7b2a07ece35a6abb3/published/pages/702b1719?o=984752964297111"
)


@api.get("/dashboard", response_model=DashboardOut, operation_id="get_dashboard")
def get_dashboard(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    obo_token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> DashboardOut:
    obo_ws.current_user.me()  # validate token
    host = (app_ws.config.host or "").rstrip("/")
    if not host:
        raise HTTPException(status_code=500, detail="Databricks host is not configured")
    embed_url = _DASHBOARD_PUBLISHED_URL
    return DashboardOut(
        dashboard_id=_DASHBOARD_ID,
        embed_url=embed_url,
        instance_url=host,
        workspace_id=_extract_workspace_id(host),
        token=obo_token or "",
    )


@api.get("/catalogs", response_model=CatalogsOut, operation_id="list_catalogs")
def list_catalogs(app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)]) -> CatalogsOut:
    try:
        catalogs = [c.name for c in app_ws.catalogs.list() if c.name]
        return CatalogsOut(catalogs=sorted(catalogs))
    except Exception as e:
        logger.error(f"Failed to list catalogs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list catalogs: {e}")


@api.get("/schemas", response_model=SchemasOut, operation_id="list_schemas")
def list_schemas(
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    catalog: str = Query(..., description="Catalog name"),
) -> SchemasOut:
    try:
        schemas = [s.name for s in app_ws.schemas.list(catalog_name=catalog) if s.name]
        return SchemasOut(schemas=sorted(schemas))
    except Exception as e:
        logger.error(f"Failed to list schemas for catalog '{catalog}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list schemas: {e}")


@api.get("/tables", response_model=TablesOut, operation_id="list_tables")
def list_tables(
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    catalog: str = Query(..., description="Catalog name"),
    schema: str = Query(..., description="Schema name"),
) -> TablesOut:
    try:
        tables = [t.name for t in app_ws.tables.list(catalog_name=catalog, schema_name=schema) if t.name]
        return TablesOut(tables=sorted(tables))
    except Exception as e:
        logger.error(f"Failed to list tables for '{catalog}.{schema}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {e}")


@api.get("/table-info", response_model=TableInfoOut, operation_id="get_table_info")
def get_table_info(
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    spark: Annotated[SparkSession, Depends(get_app_spark)],
    full_name: str = Query(..., description="Fully qualified table name (catalog.schema.table)"),
) -> TableInfoOut:
    if not _TABLE_NAME_RE.match(full_name):
        raise HTTPException(status_code=400, detail="full_name must be a 3-part dotted identifier (catalog.schema.table)")

    try:
        table = app_ws.tables.get(full_name=full_name)
    except NotFound:
        raise HTTPException(status_code=404, detail=f"Table '{full_name}' not found")
    except Exception as e:
        logger.error(f"Failed to get table metadata for '{full_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get table metadata: {e}")

    columns = []
    if table.columns:
        for col in table.columns:
            columns.append(
                ColumnInfoOut(
                    name=col.name or "",
                    type_text=col.type_text or "",
                    comment=col.comment,
                    nullable=col.nullable if col.nullable is not None else True,
                    position=col.position,
                )
            )

    sample_data: list[dict] = []
    sample_data_error: str | None = None
    try:
        quoted_full_name = _quote_3part_table_name(full_name)
        df = spark.sql(f"SELECT * FROM {quoted_full_name} LIMIT 50")
        sample_data = [row.asDict() for row in df.collect()]
        for row in sample_data:
            for k, v in row.items():
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    row[k] = str(v)
    except Exception as e:
        logger.warning(f"Failed to fetch sample data for '{full_name}': {e}")
        sample_data_error = _format_spark_error_message(e)

    return TableInfoOut(
        full_name=full_name,
        table_type=table.table_type.value if table.table_type else None,
        data_source_format=table.data_source_format.value if table.data_source_format else None,
        owner=table.owner,
        comment=table.comment,
        columns=columns,
        sample_data=sample_data,
        sample_data_error=sample_data_error,
    )


@api.get("/checks-table", response_model=ChecksTableOut, operation_id="get_checks_table")
def get_checks_table(
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    spark: Annotated[SparkSession, Depends(get_app_spark)],
    table_name: str = Query("shao_sandbox1.dqx.checks", description="Fully qualified checks table name"),
) -> ChecksTableOut:
    if not _TABLE_NAME_RE.match(table_name):
        raise HTTPException(status_code=400, detail="table_name must be a 3-part dotted identifier (catalog.schema.table)")

    try:
        # Handle missing checks table gracefully so UI can still render.
        app_ws.tables.get(full_name=table_name)
    except NotFound:
        logger.warning(f"Checks table '{table_name}' not found; returning empty result")
        return ChecksTableOut(rows=[], columns=[])
    except Exception as e:
        logger.error(f"Failed to validate checks table '{table_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to validate checks table: {e}")

    try:
        quoted_table_name = _quote_3part_table_name(table_name)
        df = spark.sql(f"SELECT * FROM {quoted_table_name}")
        columns = df.columns
        rows = []
        for row in df.collect():
            d = row.asDict(recursive=True)
            for k, v in d.items():
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    d[k] = str(v)
            rows.append(d)
        return ChecksTableOut(rows=rows, columns=columns)
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            logger.warning(f"Checks table '{table_name}' not found during query; returning empty result")
            return ChecksTableOut(rows=[], columns=[])
        logger.error(f"Failed to read checks table '{table_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to read checks table: {e}")


@api.get("/checks-table/error-rows", response_model=CheckErrorRowsOut, operation_id="get_check_error_rows")
def get_check_error_rows(
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    spark: Annotated[SparkSession, Depends(get_app_spark)],
    run_config_name: str = Query(..., description="Fully qualified run config input table (catalog.schema.table)"),
    check_name: str = Query(..., description="Check name to filter failing rows for"),
    limit: int = Query(200, ge=1, le=5000, description="Max number of rows to return"),
) -> CheckErrorRowsOut:
    if not _TABLE_NAME_RE.match(run_config_name):
        raise HTTPException(
            status_code=400,
            detail="run_config_name must be a 3-part dotted identifier (catalog.schema.table)",
        )
    if not check_name.strip():
        raise HTTPException(status_code=400, detail="check_name must not be empty")

    result_table_name = f"{run_config_name}_dqx_result"
    check_name_literal = _sql_string_literal(check_name.strip())

    try:
        app_ws.tables.get(full_name=result_table_name)
    except NotFound:
        logger.warning(f"Result table '{result_table_name}' not found; returning empty result")
        return CheckErrorRowsOut(result_table_name=result_table_name, rows=[], columns=[])
    except Exception as e:
        logger.error(f"Failed to validate result table '{result_table_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to validate result table: {e}")

    try:
        quoted_result_table_name = _quote_3part_table_name(result_table_name)
        base_df = spark.read.table(result_table_name)
        error_col = "_errors" if "_errors" in base_df.columns else ("errors" if "errors" in base_df.columns else None)
        warning_col = (
            "_warnings" if "_warnings" in base_df.columns else ("warnings" if "warnings" in base_df.columns else None)
        )
        if not error_col and not warning_col:
            return CheckErrorRowsOut(result_table_name=result_table_name, rows=[], columns=base_df.columns)

        predicates = []
        if error_col:
            predicates.append(
                f"""(
                    {error_col} IS NOT NULL
                    AND coalesce(exists({error_col}, e -> e.name = {check_name_literal} AND e.message IS NOT NULL), false)
                )"""
            )
        if warning_col:
            predicates.append(
                f"""(
                    {warning_col} IS NOT NULL
                    AND coalesce(exists({warning_col}, w -> w.name = {check_name_literal} AND w.message IS NOT NULL), false)
                )"""
            )

        where_clause = " OR ".join(predicates)
        df = spark.sql(
            f"""
            SELECT *
            FROM {quoted_result_table_name}
            WHERE {where_clause}
            LIMIT {limit}
            """
        )
        columns = df.columns
        rows = []
        for row in df.collect():
            d = row.asDict(recursive=True)
            for k, v in d.items():
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    d[k] = str(v)
            rows.append(d)
        return CheckErrorRowsOut(result_table_name=result_table_name, rows=rows, columns=columns)
    except Exception as e:
        logger.error(f"Failed to read filtered error rows from '{result_table_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to read error rows: {e}")


@api.post("/checks-table/append", response_model=SaveGeneratedChecksOut, operation_id="append_generated_checks")
def append_generated_checks(
    body: SaveGeneratedChecksIn,
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    spark: Annotated[SparkSession, Depends(get_app_spark)],
) -> SaveGeneratedChecksOut:
    if not _TABLE_NAME_RE.match(body.table_name):
        raise HTTPException(status_code=400, detail="table_name must be a 3-part dotted identifier (catalog.schema.table)")

    try:
        app_ws.tables.get(full_name=body.table_name)
    except NotFound:
        raise HTTPException(status_code=404, detail=f"Checks table '{body.table_name}' not found")
    except Exception as e:
        logger.error(f"Failed to validate checks table '{body.table_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to validate checks table: {e}")

    try:
        normalized_checks = _normalize_generated_checks(body.checks)
        if not normalized_checks:
            return SaveGeneratedChecksOut(inserted=0)

        if body.mode not in {"overwrite", "append", "upsert"}:
            raise HTTPException(status_code=400, detail="mode must be one of: overwrite, append, upsert")

        run_config_name = body.run_config_name or "default"
        normalized_checks = _align_check_columns_with_table_schema(
            normalized_checks,
            run_config_name=run_config_name,
            spark=spark,
        )
        engine = DQEngine(workspace_client=app_ws, spark=spark)
        checks_to_save = normalized_checks
        save_mode = body.mode

        if body.mode == "upsert":
            try:
                existing_checks = engine.load_checks(
                    TableChecksStorageConfig(
                        location=body.table_name,
                        run_config_name=run_config_name,
                        mode="append",
                    )
                )
            except Exception:
                existing_checks = []
            checks_to_save = _upsert_checks_by_name(existing_checks, normalized_checks)
            save_mode = "overwrite"

        storage_config = TableChecksStorageConfig(
            location=body.table_name,
            run_config_name=run_config_name,
            mode=save_mode,
        )
        engine.save_checks(checks_to_save, config=storage_config)
        _update_missing_descriptions_with_ai(
            spark=spark,
            checks_table_name=body.table_name,
            run_config_name=run_config_name,
        )
        return SaveGeneratedChecksOut(inserted=len(normalized_checks))
    except Exception as e:
        logger.error(f"Failed to append generated checks to '{body.table_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save generated checks: {e}")


_NOTEBOOK_TEMPLATE_PATH = Path(__file__).parent / "templates" / "run_checks_notebook.py"
_DQX_CHECKS_JOB_NAME = "DQX: Run Checks from Table"


def _read_notebook_template() -> str:
    return _NOTEBOOK_TEMPLATE_PATH.read_text(encoding="utf-8")


def _upload_checks_notebook(ws: WorkspaceClient, user_name: str, content: str) -> str:
    """Upload the notebook template to the user's .dqx folder. Returns the notebook workspace path (no extension)."""
    folder = f"/Users/{user_name}/.dqx"
    # Upload with .py extension so Databricks recognises it as a Python source notebook.
    # The workspace API then creates the notebook object at the path WITHOUT the extension,
    # which is what NotebookTask must reference.
    upload_path = f"{folder}/run_checks_notebook.py"
    notebook_path = f"{folder}/run_checks_notebook"
    ws.workspace.mkdirs(folder)
    # Delete any pre-existing object at either path to avoid type-mismatch errors
    # (e.g. a stale FILE object left from a previous failed upload attempt).
    for path_to_delete in (notebook_path, upload_path):
        try:
            ws.workspace.delete(path_to_delete)
        except Exception:
            pass  # doesn't exist or already gone — fine
    ws.workspace.upload(
        upload_path,
        content.encode("utf-8"),
        format=ImportFormat.AUTO,
        overwrite=True,
    )
    return notebook_path


def _resolve_bundle_dqx_dependency(ws: WorkspaceClient, user_name: str) -> str:
    bundle_build_path = f"/Workspace/Users/{user_name}/.bundle/databricks-labs-dqx-app/dev/files/.build"
    wheel_paths: list[str] = []
    try:
        for item in ws.workspace.list(bundle_build_path):
            path = str(getattr(item, "path", "") or "")
            if not path:
                continue
            name = Path(path).name
            if name.startswith("databricks_labs_dqx-") and name.endswith(".whl"):
                wheel_paths.append(path)
    except Exception as e:
        logger.warning(f"Failed to inspect bundle build folder for local dqx wheel: {e}")

    if wheel_paths:
        wheel_paths.sort()
        dependency = wheel_paths[-1]
        logger.info(f"Using local dqx wheel dependency for checks job: {dependency}")
        return dependency

    logger.warning("Local dqx wheel not found in bundle build folder. Falling back to PyPI package dependency.")
    return "databricks-labs-dqx"


def _ensure_dqx_checks_job(
    ws: WorkspaceClient,
    notebook_path: str,
    checks_table: str,
    metrics_table: str,
    dqx_dependency: str,
) -> int:
    """Find or create the DQX checks job. Always resets settings to the latest notebook. Returns job_id."""
    task = jobs.Task(
        task_key="run_checks",
        notebook_task=jobs.NotebookTask(
            notebook_path=notebook_path,
            base_parameters={"checks_table": checks_table, "metrics_table": metrics_table},
            source=jobs.Source.WORKSPACE,
        ),
        environment_key="default",
    )
    environment = jobs.JobEnvironment(
        environment_key="default",
        spec=compute.Environment(
            client="1",
            dependencies=[dqx_dependency],
        ),
    )
    new_settings = jobs.JobSettings(
        name=_DQX_CHECKS_JOB_NAME,
        tasks=[task],
        environments=[environment],
    )

    existing_id: int | None = None
    for job in ws.jobs.list(name=_DQX_CHECKS_JOB_NAME):
        if job.settings and job.settings.name == _DQX_CHECKS_JOB_NAME:
            existing_id = job.job_id
            break

    if existing_id:
        ws.jobs.reset(existing_id, new_settings=new_settings)
        return existing_id

    result = ws.jobs.create(
        name=_DQX_CHECKS_JOB_NAME,
        tasks=[task],
        environments=[environment],
    )
    if result.job_id is None:
        raise RuntimeError("Job creation succeeded but job_id is missing")
    return int(result.job_id)


@api.post("/checks-table/run-job", response_model=RunChecksJobOut, operation_id="run_checks_job")
def run_checks_job(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    table_name: str = Query("shao_sandbox1.dqx.checks", description="Fully qualified checks table name"),
    metrics_table: str | None = Query(None, description="Fully qualified metrics table. Defaults to <catalog>.<schema>.dqx_metrics"),
) -> RunChecksJobOut:
    """Create (or update) a Databricks Job with a notebook that applies all DQ rules from the checks table,
    then trigger an immediate run."""
    if not _TABLE_NAME_RE.match(table_name):
        raise HTTPException(status_code=400, detail="table_name must be a 3-part dotted identifier (catalog.schema.table)")

    if metrics_table is None:
        parts = table_name.split(".")
        metrics_table = f"{parts[0]}.{parts[1]}.dqx_metrics" if len(parts) >= 2 else ""

    user_name = obo_ws.current_user.me().user_name

    try:
        notebook_content = _read_notebook_template()
        notebook_path = _upload_checks_notebook(app_ws, user_name, notebook_content)
        logger.info(f"Uploaded run-checks notebook to {notebook_path}")
    except Exception as e:
        logger.error(f"Failed to upload checks notebook: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to upload checks notebook: {e}")

    try:
        dqx_dependency = _resolve_bundle_dqx_dependency(app_ws, user_name)
        job_id = _ensure_dqx_checks_job(app_ws, notebook_path, table_name, metrics_table, dqx_dependency)
        logger.info(f"Using DQX checks job {job_id}")
    except Exception as e:
        logger.error(f"Failed to create/update checks job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create checks job: {e}")

    try:
        run_response = app_ws.jobs.run_now(job_id=job_id)
        run_id = run_response.run_id
        if run_id is None:
            raise RuntimeError("run_now succeeded but run_id is missing")
        run_id = int(run_id)
    except Exception as e:
        logger.error(f"Failed to trigger checks job run: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to trigger job run: {e}")

    host = (app_ws.config.host or "").rstrip("/")
    job_url = f"{host}/#job/{job_id}"
    run_url = f"{host}/#job/runs/{run_id}"
    return RunChecksJobOut(job_id=job_id, job_url=job_url, run_id=run_id, run_url=run_url)
