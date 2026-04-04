import json
import math
import os
import re
import hashlib
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import yaml
from databricks.labs.blueprint.installation import Installation
from databricks.labs.dqx.checks_serializer import ChecksNormalizer
from databricks.labs.dqx.config import WorkspaceConfig
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.config_serializer import ConfigSerializer
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidConfigError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied, ResourceDoesNotExist
from databricks.sdk.service import compute, jobs, sql as sql_service
from databricks.sdk.service.iam import User as UserOut
from databricks.sdk.service.workspace import ImportFormat
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pyspark.sql import SparkSession

from .config import conf
from .dependencies import _get_llm_model_config, _is_session_changed_error, get_app_ws, get_generator, get_obo_ws, get_spark
from .logger import logger
from .models import (
    CheckErrorRowsOut,
    CatalogsOut,
    ChecksIn,
    ChecksOut,
    ChecksTableOut,
    ClusterInfo,
    ClustersOut,
    ColumnInfoOut,
    ConfigIn,
    ConfigOut,
    CreateChecksTableOut,
    DashboardOut,
    GenerateChecksIn,
    GenerateChecksOut,
    InstallationSettings,
    ProfileGenerateChecksIn,
    RunChecksJobOut,
    SchemasOut,
    SaveGeneratedChecksIn,
    SaveGeneratedChecksOut,
    TableInfoOut,
    TablesOut,
    VersionOut,
    WarehouseInfo,
    WarehousesOut,
)
from .settings import SettingsManager

api = APIRouter(prefix=conf.api_prefix)
_UNICODE_ESCAPE_RE = re.compile(r"\\u([0-9a-fA-F]{4})")


def get_install_folder(ws: WorkspaceClient, path: str | None) -> str:
    folder = path
    if not folder:
        settings = SettingsManager(ws).get_settings()
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
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
):
    return SettingsManager(app_ws).get_settings()


@api.post("/settings", response_model=InstallationSettings, operation_id="save_settings")
def save_settings(
    settings: InstallationSettings,
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
):
    try:
        return SettingsManager(app_ws).save_settings(settings)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@api.get("/config", response_model=ConfigOut, operation_id="config")
def get_config(
    _obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> ConfigOut:
    install_folder = get_install_folder(app_ws, path)
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
    _obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> ConfigOut:
    install_folder = get_install_folder(app_ws, path)
    serializer = ConfigSerializer(app_ws)
    serializer.save_config(body.config, install_folder=install_folder)
    return ConfigOut(config=serializer.load_config(install_folder=install_folder))


@api.post("/ai-generate-checks", response_model=GenerateChecksOut, operation_id="ai_assisted_checks_generation")
def ai_generate_checks(
    body: GenerateChecksIn,
    generator: Annotated[DQGenerator, Depends(get_generator)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
) -> GenerateChecksOut:
    """Generate data quality checks from natural language using AI-assisted generation."""
    try:
        user_input = body.user_input
        if body.table_name:
            if not _TABLE_NAME_RE.match(body.table_name):
                raise HTTPException(status_code=400, detail="table_name must be a 3-part dotted identifier (catalog.schema.table)")

            user_input = f"{body.user_input}\n\n{_build_table_context_for_ai_generation(obo_ws, app_ws, body.table_name)}"

        checks = generator.generate_dq_rules_ai_assisted(user_input=user_input)
        checks = _prepare_generated_checks_for_display(_normalize_generated_checks(checks))

        # Convert checks to YAML
        yaml_output = yaml.dump(checks, default_flow_style=False, sort_keys=False, allow_unicode=True)

        return GenerateChecksOut(yaml_output=yaml_output, checks=checks)
    except Exception as e:
        logger.error(f"Failed to generate checks: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to generate checks: {_format_spark_error_message(e)}")


@api.post(
    "/profile-ai-generate-checks",
    response_model=GenerateChecksOut,
    operation_id="profile_ai_assisted_checks_generation",
)
def profile_ai_generate_checks(
    body: ProfileGenerateChecksIn,
    generator: Annotated[DQGenerator, Depends(get_generator)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
) -> GenerateChecksOut:
    """Profile a whole table with DQX and use the profile output to drive AI rule suggestions."""
    try:
        if not _TABLE_NAME_RE.match(body.table_name):
            raise HTTPException(status_code=400, detail="table_name must be a 3-part dotted identifier (catalog.schema.table)")

        table_context = _build_table_context_for_ai_generation(obo_ws, app_ws, body.table_name)
        summary_stats, profiler_checks = _profile_table_for_ai_generation(
            obo_ws=obo_ws,
            app_ws=app_ws,
            generator=generator,
            table_name=body.table_name,
        )
        ai_checks = _normalize_generated_checks(
            generator.generate_dq_rules_ai_assisted(
                user_input=_build_profile_ai_user_input(
                    table_name=body.table_name,
                    table_context=table_context,
                    profiler_checks=profiler_checks,
                    user_input=body.user_input,
                ),
                summary_stats=summary_stats,
            )
        )
        checks = _prepare_generated_checks_for_display(
            _adjust_profile_based_range_checks(
                _normalize_generated_checks(_upsert_checks_by_name(profiler_checks, ai_checks)),
                summary_stats,
            )
        )
        yaml_output = yaml.dump(checks, default_flow_style=False, sort_keys=False, allow_unicode=True)
        return GenerateChecksOut(yaml_output=yaml_output, checks=checks)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to generate profile-based checks: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate profile-based checks: {_format_spark_error_message(e)}",
        )


_TABLE_NAME_RE = re.compile(r"^[\w]+\.[\w]+\.[\w]+$")
_AI_METADATA_ENRICHMENT_BATCH_SIZE = 10


def _quote_3part_table_name(full_name: str) -> str:
    parts = full_name.split(".")
    return ".".join(f"`{p}`" for p in parts)


def _build_table_context_for_ai_generation(
    obo_ws: WorkspaceClient,
    app_ws: WorkspaceClient,
    full_name: str,
) -> str:
    table_info = obo_ws.tables.get(full_name=full_name)
    schema_json = json.dumps(
        [
            {
                "name": column.name,
                "type": getattr(column, "type_text", None) or getattr(column, "type_name", None),
                "comment": getattr(column, "comment", None),
                "nullable": getattr(column, "nullable", True),
            }
            for column in (table_info.columns or [])
            if getattr(column, "name", None)
        ],
        default=str,
        ensure_ascii=False,
    )

    context_parts = [
        f"Selected table: {full_name}",
        f"Table schema (JSON):\n{schema_json}",
    ]

    try:
        warehouse_id = _get_default_warehouse_id(app_ws, obo_ws)
        sample_rows_response = _execute_sql_statement(
            obo_ws,
            f"SELECT * FROM {_quote_3part_table_name(full_name)} LIMIT 20",
            warehouse_id,
        )
        sample_rows = _statement_rows_to_dicts(sample_rows_response)
        if sample_rows:
            context_parts.append(f"Sample rows (JSON):\n{json.dumps(sample_rows, default=str, ensure_ascii=False)}")
    except HTTPException as e:
        if e.status_code != 400:
            raise
        logger.info("Skipping sample rows for AI generation because no default warehouse is configured")
    except Exception:
        logger.warning("Failed to load sample rows for AI generation", exc_info=True)

    return "\n\n".join(context_parts)


def _build_profile_ai_user_input(
    table_name: str,
    table_context: str,
    profiler_checks: list[dict],
    user_input: str | None = None,
) -> str:
    prompt_parts = [
        f"Suggest a fuller set of data quality rules for the whole table `{table_name}`.",
        "Use the table context, profiler-suggested checks, and summary statistics to expand coverage while avoiding duplicate rules.",
        table_context,
        f"Profiler-suggested checks (JSON):\n{json.dumps(profiler_checks, default=str, ensure_ascii=False)}",
    ]
    if user_input and user_input.strip():
        prompt_parts.append(f"Additional requirements:\n{user_input.strip()}")
    return "\n\n".join(prompt_parts)


def _get_check_target_label(check_item: dict) -> str:
    check_obj = check_item.get("check") if isinstance(check_item.get("check"), dict) else {}
    arguments = check_obj.get("arguments") if isinstance(check_obj.get("arguments"), dict) else {}

    column = arguments.get("column")
    if isinstance(column, str) and column.strip():
        return column.strip()

    columns = arguments.get("columns")
    if isinstance(columns, list) and columns:
        return "、".join(str(column).strip() for column in columns if str(column).strip())

    for_each_columns = check_obj.get("for_each_column")
    if isinstance(for_each_columns, list) and for_each_columns:
        return "、".join(str(column).strip() for column in for_each_columns if str(column).strip())

    return "该字段"


def _get_check_target_values(check_item: dict) -> list[str]:
    check_obj = check_item.get("check") if isinstance(check_item.get("check"), dict) else {}
    arguments = check_obj.get("arguments") if isinstance(check_obj.get("arguments"), dict) else {}

    column = arguments.get("column")
    if isinstance(column, str) and column.strip():
        return [column.strip()]

    columns = arguments.get("columns")
    if isinstance(columns, list):
        result = [str(item).strip() for item in columns if str(item).strip()]
        if result:
            return result

    for_each_columns = check_obj.get("for_each_column")
    if isinstance(for_each_columns, list):
        result = [str(item).strip() for item in for_each_columns if str(item).strip()]
        if result:
            return result

    return []


def _slugify_rule_name_part(value: str) -> str:
    normalized = _canonical_column_name(value).strip().lower()
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "_", ascii_only).strip("_")
    if slug and slug[0].isdigit():
        slug = f"col_{slug}"
    if slug:
        return slug
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:8]
    return f"rule_{digest}"


def _truncate_rule_name(name: str) -> str:
    if len(name) <= 64:
        return name
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    return f"{name[:55].rstrip('_')}_{digest}"


def _build_generated_rule_name(check_item: dict) -> str:
    check_obj = check_item.get("check") if isinstance(check_item.get("check"), dict) else {}
    function_name = str(check_obj.get("function", "")).strip()
    target_values = _get_check_target_values(check_item)
    target_slug = "_".join(_slugify_rule_name_part(value) for value in target_values[:3])

    if function_name == "is_not_null":
        base_name = f"{target_slug}_is_null" if target_slug else "rule_is_null"
    elif function_name == "is_not_null_and_not_empty":
        base_name = f"{target_slug}_is_null_or_empty" if target_slug else "rule_is_null_or_empty"
    elif function_name == "is_not_empty":
        base_name = f"{target_slug}_is_not_empty" if target_slug else "rule_is_not_empty"
    elif function_name == "is_in_range":
        base_name = f"{target_slug}_isnt_in_range" if target_slug else "rule_isnt_in_range"
    elif function_name == "is_not_less_than":
        base_name = f"{target_slug}_not_less_than" if target_slug else "rule_not_less_than"
    elif function_name == "is_not_greater_than":
        base_name = f"{target_slug}_not_greater_than" if target_slug else "rule_not_greater_than"
    elif function_name in {"is_in_list", "is_in"}:
        base_name = f"{target_slug}_other_value" if target_slug else "rule_other_value"
    elif function_name == "is_unique":
        base_name = f"unique_{target_slug}" if target_slug else "unique_rule"
    else:
        function_slug = _slugify_rule_name_part(function_name or "rule")
        base_name = f"{target_slug}_{function_slug}" if target_slug else function_slug

    return _truncate_rule_name(base_name)


def _generate_concise_chinese_description(check_item: dict) -> str:
    check_obj = check_item.get("check") if isinstance(check_item.get("check"), dict) else {}
    arguments = check_obj.get("arguments") if isinstance(check_obj.get("arguments"), dict) else {}
    function_name = str(check_obj.get("function", "")).strip()
    target = _get_check_target_label(check_item)

    if function_name == "is_not_null":
        return f"检查列{target}不能为空，避免缺失值。"
    if function_name == "is_not_null_and_not_empty":
        return f"检查列{target}不能为空或空字符串，避免无效内容。"
    if function_name == "is_not_empty":
        return f"检查列{target}不能是空字符串，避免无效内容。"
    if function_name == "is_in_range":
        return f"检查列{target}取值应在合理范围内，避免异常值。"
    if function_name == "is_not_less_than":
        return f"检查列{target}取值不能小于{arguments.get('limit')}，避免过小异常值。"
    if function_name == "is_not_greater_than":
        return f"检查列{target}取值不能大于{arguments.get('limit')}，避免过大异常值。"
    if function_name in {"is_in_list", "is_in"}:
        allowed = arguments.get("allowed") or arguments.get("in") or []
        if isinstance(allowed, list):
            preview = "、".join(str(item) for item in allowed[:5])
            if len(allowed) > 5:
                preview += "等"
        else:
            preview = str(allowed)
        return f"检查列{target}取值应属于{preview}，避免非法枚举值。"
    if function_name == "is_unique":
        return f"检查列{target}组合应唯一，避免重复记录。"
    return f"检查列{target}满足{function_name}规则，保证数据质量。"


def _to_numeric_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    return None


def _round_profile_numeric_limit(value: float, direction: str, span: float, clamp_non_negative: bool) -> int | float:
    safe_span = max(span, 1.0)
    magnitude = int(math.floor(math.log10(safe_span))) if safe_span > 0 else 0
    step = 10 ** max(magnitude - 1, 0)

    if direction == "down":
        rounded = math.floor(value / step) * step
    else:
        rounded = math.ceil(value / step) * step

    if clamp_non_negative:
        rounded = max(0, rounded)

    if step >= 1:
        return int(rounded)
    return rounded


def _derive_robust_numeric_range(column_stats: dict[str, object]) -> tuple[int | float, int | float] | None:
    mean_value = _to_numeric_float(column_stats.get("mean"))
    stddev_value = _to_numeric_float(column_stats.get("stddev"))
    min_value = _to_numeric_float(column_stats.get("min"))
    max_value = _to_numeric_float(column_stats.get("max"))

    if None in {mean_value, stddev_value, min_value, max_value}:
        return None

    assert mean_value is not None
    assert stddev_value is not None
    assert min_value is not None
    assert max_value is not None

    if stddev_value <= 0 or min_value >= max_value:
        return None

    lower_bound = mean_value - 3 * stddev_value
    upper_bound = mean_value + 3 * stddev_value
    clamp_non_negative = min_value >= 0 and max_value >= 0

    if clamp_non_negative:
        lower_bound = max(0.0, lower_bound)

    if lower_bound >= upper_bound:
        return None

    span = max(upper_bound - lower_bound, max_value - min_value, 1.0)
    rounded_lower = _round_profile_numeric_limit(lower_bound, "down", span, clamp_non_negative)
    rounded_upper = _round_profile_numeric_limit(upper_bound, "up", span, clamp_non_negative=False)

    if rounded_lower >= rounded_upper:
        return None

    return rounded_lower, rounded_upper


def _adjust_profile_based_range_checks(checks: list[dict], summary_stats: dict[str, object]) -> list[dict]:
    adjusted_checks: list[dict] = []
    for check_item in checks:
        if not isinstance(check_item, dict):
            adjusted_checks.append(check_item)
            continue

        check_obj = check_item.get("check") if isinstance(check_item.get("check"), dict) else {}
        arguments = check_obj.get("arguments") if isinstance(check_obj.get("arguments"), dict) else {}
        if check_obj.get("function") != "is_in_range":
            adjusted_checks.append(check_item)
            continue

        column_name = arguments.get("column")
        if not isinstance(column_name, str):
            adjusted_checks.append(check_item)
            continue

        column_stats = summary_stats.get(column_name)
        if not isinstance(column_stats, dict):
            adjusted_checks.append(check_item)
            continue

        robust_range = _derive_robust_numeric_range(column_stats)
        if robust_range is None:
            adjusted_checks.append(check_item)
            continue

        min_limit, max_limit = robust_range
        adjusted_checks.append(
            {
                **check_item,
                "check": {
                    **check_obj,
                    "arguments": {
                        **arguments,
                        "min_limit": min_limit,
                        "max_limit": max_limit,
                    },
                },
            }
        )
    return adjusted_checks


def _prepare_generated_checks_for_display(checks: list[dict]) -> list[dict]:
    prepared_checks: list[dict] = []
    generated_names_seen: set[str] = set()
    for check_item in checks:
        if not isinstance(check_item, dict):
            prepared_checks.append(check_item)
            continue

        description = str(check_item.get("description", "")).strip()
        name = str(check_item.get("name", "")).strip()
        if not name:
            generated_name = _build_generated_rule_name(check_item)
            candidate_name = generated_name
            suffix = 2
            while candidate_name in generated_names_seen:
                candidate_name = _truncate_rule_name(f"{generated_name}_{suffix}")
                suffix += 1
            name = candidate_name
        generated_names_seen.add(name)
        prepared = {}
        prepared["name"] = name
        prepared["description"] = description or _generate_concise_chinese_description(check_item)
        for key in ("criticality", "check", "filter", "user_metadata"):
            if key in check_item:
                prepared[key] = check_item[key]
        for key, value in check_item.items():
            if key not in prepared:
                prepared[key] = value
        prepared_checks.append(prepared)
    return prepared_checks


def _parse_sql_value_for_profile(value: str | None, type_text: str | None) -> object | None:
    if value is None:
        return None
    if not type_text:
        return value

    normalized_type = type_text.strip().lower()
    try:
        if normalized_type.startswith(("tinyint", "smallint", "int", "bigint", "long")):
            return int(value)
        if normalized_type.startswith(("float", "double", "real")):
            return float(value)
        if normalized_type.startswith("decimal"):
            return Decimal(value)
        if normalized_type.startswith("boolean"):
            return value.strip().lower() == "true"
        if normalized_type.startswith("date"):
            return date.fromisoformat(value)
        if normalized_type.startswith("timestamp"):
            return datetime.fromisoformat(value.replace("Z", "+00:00").replace(" ", "T"))
    except (ValueError, ArithmeticError):
        return value
    return value


def _profile_table_for_ai_generation(
    obo_ws: WorkspaceClient,
    app_ws: WorkspaceClient,
    generator: DQGenerator,
    table_name: str,
) -> tuple[dict, list[dict]]:
    table_info = obo_ws.tables.get(full_name=table_name)
    warehouse_id = _get_default_warehouse_id(app_ws, obo_ws)
    profile_limit = int(DQProfiler.default_profile_options.get("limit", 1000))
    sample_response = _execute_sql_statement(
        obo_ws,
        f"SELECT * FROM {_quote_3part_table_name(table_name)} LIMIT {profile_limit}",
        warehouse_id,
    )
    sample_rows = _statement_rows_to_dicts(sample_response)
    if not sample_rows:
        return {}, []

    typed_rows: list[dict[str, object | None]] = []
    table_columns = [column for column in (table_info.columns or []) if getattr(column, "name", None)]
    for row in sample_rows:
        typed_rows.append(
            {
                column.name: _parse_sql_value_for_profile(row.get(column.name), getattr(column, "type_text", None))
                for column in table_columns
            }
        )

    df = generator.spark.createDataFrame(typed_rows)
    profiler = DQProfiler(
        workspace_client=app_ws,
        spark=generator.spark,
        llm_model_config=_get_llm_model_config(),
    )
    summary_stats, profiles = profiler.profile(
        df=df,
        options={
            **DQProfiler.default_profile_options,
            "sample_fraction": 1.0,
            "limit": len(typed_rows),
            "llm_primary_key_detection": False,
        },
    )
    profiler_checks = _normalize_generated_checks(generator.generate_dq_rules(profiles=profiles))
    return summary_stats, profiler_checks


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


def _normalize_column_reference(value):
    if isinstance(value, str):
        return _canonical_column_name(value)
    if isinstance(value, list):
        return [_normalize_column_reference(item) for item in value]
    return value


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
                    arguments["column"] = _normalize_column_reference(column)
                columns = arguments.get("columns")
                if isinstance(columns, list):
                    arguments["columns"] = _normalize_column_reference(columns)
            for_each_columns = check_obj.get("for_each_column")
            if isinstance(for_each_columns, list):
                check_obj["for_each_column"] = _normalize_column_reference(for_each_columns)
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

    return _align_check_columns_with_actual_schema(checks, actual_columns)


def _align_check_columns_with_actual_schema(checks, actual_columns: list[str]):
    if not isinstance(checks, list):
        return checks

    canonical_to_actual: dict[str, str] = {}
    for actual in actual_columns:
        canonical = _canonical_column_name(actual).casefold()
        if canonical and canonical not in canonical_to_actual:
            canonical_to_actual[canonical] = actual

    def _align_column_value(value):
        if isinstance(value, str):
            normalized_value = _canonical_column_name(value)
            return canonical_to_actual.get(normalized_value.casefold(), normalized_value)
        if isinstance(value, list):
            return [_align_column_value(item) for item in value]
        return value

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
            column = None

        if column is not None:
            arguments["column"] = _align_column_value(column)

        columns = arguments.get("columns")
        if isinstance(columns, list):
            arguments["columns"] = _align_column_value(columns)

        for_each_columns = check_obj.get("for_each_column")
        if isinstance(for_each_columns, list):
            check_obj["for_each_column"] = _align_column_value(for_each_columns)

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


def _sql_nullable_string_literal(value: object | None) -> str:
    if value is None:
        return "NULL"
    return _sql_string_literal(str(value))


def _sql_timestamp_literal(value: datetime) -> str:
    effective = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return f"TIMESTAMP {_sql_string_literal(effective.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'))}"


def _sql_string_array_expr(values: list[str]) -> str:
    if not values:
        return "CAST(array() AS ARRAY<STRING>)"
    return "array(" + ", ".join(_sql_string_literal(str(value)) for value in values) + ")"


def _sql_string_map_expr(values: dict[str, str] | None) -> str:
    if values is None:
        return "CAST(NULL AS MAP<STRING, STRING>)"
    if not values:
        return "CAST(map() AS MAP<STRING, STRING>)"
    entries: list[str] = []
    for key, value in values.items():
        entries.append(_sql_string_literal(key))
        entries.append(_sql_string_literal(value))
    return "map(" + ", ".join(entries) + ")"


def _sql_check_struct_expr(check_item: dict) -> str:
    check_obj = check_item.get("check") if isinstance(check_item.get("check"), dict) else {}
    arguments = check_obj.get("arguments") if isinstance(check_obj.get("arguments"), dict) else {}
    argument_map = {str(key): json.dumps(value, ensure_ascii=False) for key, value in arguments.items()}
    for_each_columns = check_obj.get("for_each_column")
    if not isinstance(for_each_columns, list):
        for_each_columns = []

    return (
        "named_struct("
        f"'function', {_sql_nullable_string_literal(check_obj.get('function'))}, "
        f"'for_each_column', {_sql_string_array_expr([str(value) for value in for_each_columns])}, "
        f"'arguments', {_sql_string_map_expr(argument_map)}"
        ")"
    )


def _parse_json_object_response(value: str) -> dict[str, object]:
    candidate = value.strip()
    if candidate.startswith("```"):
        candidate = re.sub(r"^```(?:json)?\s*|\s*```$", "", candidate, flags=re.DOTALL).strip()

    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", candidate, re.DOTALL)
        if not match:
            return {}
        try:
            parsed = json.loads(match.group(0))
        except json.JSONDecodeError:
            return {}

    return parsed if isinstance(parsed, dict) else {}


def _build_generated_rule_metadata_prompt(check_item: dict, run_config_name: str) -> str:
    check_json = json.dumps(check_item, ensure_ascii=False, sort_keys=True)
    return (
        "You are a data quality expert. Given a DQ rule, generate a concise stable rule name and a short description. "
        "Return JSON only with keys name and description. "
        "The name must be lowercase snake_case, start with a letter, use only letters, digits, and underscores, "
        "and be at most 64 characters. The description should be one sentence. "
        f"Run config name: {run_config_name}\n"
        f"Rule JSON: {check_json}"
    )


def _sql_ai_query_endpoint_name() -> str:
    model_name = _get_llm_model_config().model_name.strip()
    return model_name.removeprefix("databricks/") if model_name else model_name


def _chunk_sequence(items: list[tuple[int, dict]], size: int) -> list[list[tuple[int, dict]]]:
    if size <= 0:
        return [items]
    return [items[index : index + size] for index in range(0, len(items), size)]


def _enrich_generated_checks_with_ai_via_sql_warehouse(
    obo_ws: WorkspaceClient,
    warehouse_id: str,
    checks: list[dict],
    run_config_name: str,
) -> list[dict]:
    model_name = _sql_ai_query_endpoint_name()
    checks_to_enrich = [
        (index, check)
        for index, check in enumerate(checks)
        if not str(check.get("name", "")).strip() or not str(check.get("description", "")).strip()
    ]
    if not checks_to_enrich:
        return checks

    generated_by_index: dict[int, dict[str, object]] = {}
    for check_batch in _chunk_sequence(checks_to_enrich, _AI_METADATA_ENRICHMENT_BATCH_SIZE):
        values_sql = ", ".join(
            f"({index}, {_sql_string_literal(_build_generated_rule_metadata_prompt(check, run_config_name))})"
            for index, check in check_batch
        )
        response = _execute_sql_statement(
            obo_ws,
            (
                "SELECT idx, ai_query("
                f"{_sql_string_literal(model_name)}, prompt"
                ") AS generated_json "
                f"FROM VALUES {values_sql} AS prompts(idx, prompt)"
            ),
            warehouse_id,
        )
        generated_rows = _statement_rows_to_dicts(response)
        for row in generated_rows:
            row_index = _first_present_value(row, ["idx"])
            generated_json = _first_present_value(row, ["generated_json", "ai_query(prompt)", "col_1"])
            if row_index is None or generated_json is None:
                continue
            try:
                generated_by_index[int(row_index)] = _parse_json_object_response(generated_json)
            except ValueError:
                continue

    enriched_checks: list[dict] = []
    for index, check in enumerate(checks):
        enriched = dict(check)
        generated = generated_by_index.get(index, {})
        if not str(enriched.get("name", "")).strip():
            generated_name = str(generated.get("name", "")).strip()
            if generated_name:
                enriched["name"] = generated_name
        if not str(enriched.get("description", "")).strip():
            generated_description = str(generated.get("description", "")).strip()
            if generated_description:
                enriched["description"] = generated_description
        enriched_checks.append(enriched)

    return enriched_checks


def _validate_generated_rule_metadata(checks: list[dict], run_config_name: str) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()

    for check in checks:
        name = str(check.get("name", "")).strip()
        description = str(check.get("description", "")).strip()
        if not name:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate a rule name for run_config_name '{run_config_name}'.",
            )
        if not description:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate a rule description for rule '{name}' in run_config_name '{run_config_name}'.",
            )
        if name in seen:
            duplicates.add(name)
        seen.add(name)

    return sorted(duplicates)


def _existing_rule_names_for_run_config(
    obo_ws: WorkspaceClient,
    warehouse_id: str,
    table_name: str,
    run_config_name: str,
    names: list[str],
) -> list[str]:
    unique_names = sorted({name.strip() for name in names if name.strip()})
    if not unique_names:
        return []

    name_list_sql = ", ".join(_sql_string_literal(name) for name in unique_names)
    response = _execute_sql_statement(
        obo_ws,
        (
            f"SELECT name FROM {_quote_3part_table_name(table_name)} "
            f"WHERE run_config_name = {_sql_string_literal(run_config_name)} "
            f"AND name IN ({name_list_sql})"
        ),
        warehouse_id,
    )
    rows = _statement_rows_to_dicts(response)
    return sorted(
        {
            value
            for row in rows
            for value in [str(_first_present_value(row, ["name"]) or "").strip()]
            if value
        }
    )


def _serialize_generated_checks_for_sql(checks: list[dict], run_config_name: str) -> list[str]:
    normalized_for_serialization = ChecksNormalizer.normalize(checks)
    created_at = datetime.now(timezone.utc)
    rows: list[str] = []

    for check in normalized_for_serialization:
        user_metadata = check.get("user_metadata") if isinstance(check.get("user_metadata"), dict) else None
        user_metadata_map = (
            {str(key): str(value) for key, value in user_metadata.items()} if user_metadata is not None else None
        )
        rows.append(
            "("
            + ", ".join(
                [
                    _sql_nullable_string_literal(check.get("name")),
                    _sql_nullable_string_literal(check.get("description")),
                    _sql_nullable_string_literal(check.get("criticality", "error")),
                    _sql_check_struct_expr(check),
                    _sql_nullable_string_literal(check.get("filter")),
                    _sql_string_literal(run_config_name),
                    _sql_string_map_expr(user_metadata_map),
                    _sql_timestamp_literal(created_at),
                ]
            )
            + ")"
        )

    return rows


def _save_generated_checks_via_sql_warehouse(
    obo_ws: WorkspaceClient,
    warehouse_id: str,
    table_name: str,
    checks: list[dict],
    run_config_name: str,
    mode: str,
) -> None:
    quoted_table_name = _quote_3part_table_name(table_name)

    if mode == "overwrite":
        _execute_sql_statement(
            obo_ws,
            f"DELETE FROM {quoted_table_name} WHERE run_config_name = {_sql_string_literal(run_config_name)}",
            warehouse_id,
        )

    row_sql = _serialize_generated_checks_for_sql(checks, run_config_name)
    if not row_sql:
        return

    _execute_sql_statement(
        obo_ws,
        (
            f"INSERT INTO {quoted_table_name} "
            "(`name`, `description`, `criticality`, `check`, `filter`, `run_config_name`, "
            "`user_metadata`, `created_at`) VALUES "
            + ", ".join(row_sql)
        ),
        warehouse_id,
    )


def _missing_description_backfill_sql(checks_table_name: str, run_config_name: str, model_name: str) -> str:
    quoted_table_name = _quote_3part_table_name(checks_table_name)
    run_config_literal = _sql_string_literal(run_config_name)
    model_name_literal = _sql_string_literal(model_name)
    return f"""
        MERGE INTO {quoted_table_name} AS target
        USING (
          SELECT
            `run_config_name`,
            `name`,
            CAST(`criticality` AS STRING) AS `criticality_text`,
            to_json(`check`) AS `check_json`,
            CAST(`filter` AS STRING) AS `filter_text`,
            ai_query(
              {model_name_literal},
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


def _update_missing_descriptions_with_ai(
    spark: SparkSession,
    checks_table_name: str,
    run_config_name: str,
) -> None:
    model_name = _sql_ai_query_endpoint_name()
    if not model_name:
        logger.info("Skipping AI description backfill because SERVING_ENDPOINT_NAME is not configured")
        return

    spark.sql(_missing_description_backfill_sql(checks_table_name, run_config_name, model_name))


def _update_missing_descriptions_with_ai_via_sql_warehouse(
    obo_ws: WorkspaceClient,
    warehouse_id: str,
    checks_table_name: str,
    run_config_name: str,
) -> None:
    model_name = _sql_ai_query_endpoint_name()
    if not model_name:
        logger.info("Skipping AI description backfill because SERVING_ENDPOINT_NAME is not configured")
        return

    _execute_sql_statement(
        obo_ws,
        _missing_description_backfill_sql(checks_table_name, run_config_name, model_name),
        warehouse_id,
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
    if "PERMISSION_DENIED" in message and "USE CATALOG" in message:
        return f"You do not have permission to use this catalog. {message}"
    if "[TABLE_OR_VIEW_NOT_FOUND]" in message:
        return f"Table not found. {message}"
    return message


def _get_user_name(obo_ws: WorkspaceClient) -> str:
    return obo_ws.current_user.me().user_name


def _get_user_settings(app_ws: WorkspaceClient, obo_ws: WorkspaceClient) -> InstallationSettings:
    return SettingsManager(app_ws).get_settings()


def _execute_sql_statement(
    obo_ws: WorkspaceClient,
    statement: str,
    warehouse_id: str,
    *,
    catalog: str | None = None,
    schema: str | None = None,
) -> sql_service.StatementResponse:
    response = obo_ws.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        wait_timeout="15s",
        on_wait_timeout=sql_service.ExecuteStatementRequestOnWaitTimeout.CANCEL,
    )
    state = response.status.state if response.status else None
    if state == sql_service.StatementState.SUCCEEDED:
        return response

    error = response.status.error if response.status else None
    message = error.message if error and error.message else f"SQL statement failed with state {state}"
    raise RuntimeError(message)


def _statement_rows_to_dicts(response: sql_service.StatementResponse) -> list[dict[str, str | None]]:
    columns = response.manifest.schema.columns if response.manifest and response.manifest.schema and response.manifest.schema.columns else []
    headers = [column.name or f"col_{index}" for index, column in enumerate(columns)]
    rows = response.result.data_array if response.result and response.result.data_array else []

    result: list[dict[str, str | None]] = []
    for row in rows:
        padded_row = list(row) + [None] * max(0, len(headers) - len(row))
        result.append(dict(zip(headers, padded_row, strict=False)))
    return result


def _first_present_value(row: dict[str, str | None], preferred_keys: list[str]) -> str | None:
    for key in preferred_keys:
        value = row.get(key)
        if value:
            return value
    for value in row.values():
        if value:
            return value
    return None


def _quote_sql_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def _get_default_warehouse_id(
    app_ws: WorkspaceClient,
    obo_ws: WorkspaceClient,
    explicit_warehouse_id: str | None = None,
) -> str:
    if explicit_warehouse_id:
        return explicit_warehouse_id
    settings = _get_user_settings(app_ws, obo_ws)
    if settings.default_warehouse_id:
        return settings.default_warehouse_id
    raise HTTPException(
        status_code=400,
        detail="Set a default warehouse in Settings before browsing Unity Catalog metadata.",
    )


def _describe_table_via_sql_warehouse(
    obo_ws: WorkspaceClient,
    warehouse_id: str,
    full_name: str,
) -> tuple[list[ColumnInfoOut], dict[str, str]]:
    response = _execute_sql_statement(
        obo_ws,
        f"DESCRIBE TABLE EXTENDED {_quote_3part_table_name(full_name)}",
        warehouse_id,
    )
    rows = _statement_rows_to_dicts(response)

    columns: list[ColumnInfoOut] = []
    table_metadata: dict[str, str] = {}
    in_metadata_section = False

    for row in rows:
        col_name = (row.get("col_name") or "").strip()
        data_type = (row.get("data_type") or "").strip()
        comment = row.get("comment")

        if not col_name:
            continue
        if col_name.startswith("#"):
            in_metadata_section = "Detailed Table Information" in col_name
            continue

        if in_metadata_section:
            table_metadata[col_name] = data_type
            continue

        columns.append(
            ColumnInfoOut(
                name=col_name,
                type_text=data_type,
                comment=comment,
                nullable=True,
                position=len(columns),
            )
        )

    return columns, table_metadata




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
def list_catalogs(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    warehouse_id: str | None = Query(None, description="Optional SQL warehouse ID used for metadata queries"),
) -> CatalogsOut:
    try:
        catalogs = [c.name for c in obo_ws.catalogs.list() if c.name]
        return CatalogsOut(catalogs=sorted(catalogs))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list catalogs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list catalogs: {e}")


@api.get("/schemas", response_model=SchemasOut, operation_id="list_schemas")
def list_schemas(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    catalog: str = Query(..., description="Catalog name"),
) -> SchemasOut:
    try:
        schemas = [s.name for s in obo_ws.schemas.list(catalog_name=catalog) if s.name]
        return SchemasOut(schemas=sorted(schemas))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list schemas for catalog '{catalog}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list schemas: {e}")


@api.get("/tables", response_model=TablesOut, operation_id="list_tables")
def list_tables(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    catalog: str = Query(..., description="Catalog name"),
    schema: str = Query(..., description="Schema name"),
) -> TablesOut:
    try:
        tables = [
            table.name
            for table in obo_ws.tables.list(catalog_name=catalog, schema_name=schema)
            if table.name
        ]
        return TablesOut(tables=sorted(tables))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list tables for '{catalog}.{schema}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {e}")


@api.get("/table-info", response_model=TableInfoOut, operation_id="get_table_info")
def get_table_info(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
    full_name: str = Query(..., description="Fully qualified table name (catalog.schema.table)"),
    warehouse_id: str | None = Query(None, description="Optional SQL warehouse ID used for metadata queries"),
) -> TableInfoOut:
    if not _TABLE_NAME_RE.match(full_name):
        raise HTTPException(status_code=400, detail="full_name must be a 3-part dotted identifier (catalog.schema.table)")

    try:
        table_info = obo_ws.tables.get(full_name=full_name)
        columns = [
            ColumnInfoOut(
                name=column.name or "",
                type_text=column.type_text or (column.type_name.value if column.type_name else ""),
                comment=column.comment,
                nullable=column.nullable if column.nullable is not None else True,
                position=column.position,
            )
            for column in (table_info.columns or [])
            if column.name
        ]
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or isinstance(e, NotFound):
            raise HTTPException(status_code=404, detail=f"Table '{full_name}' not found")
        logger.error(f"Failed to get table metadata for '{full_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get table metadata: {e}")

    sample_data: list[dict] = []
    sample_data_error: str | None = None
    try:
        quoted_full_name = _quote_3part_table_name(full_name)
        if os.environ.get("DATABRICKS_HOST"):
            resolved_warehouse_id = _get_default_warehouse_id(app_ws, obo_ws, warehouse_id)
            sample_response = _execute_sql_statement(
                obo_ws,
                f"SELECT * FROM {quoted_full_name} LIMIT 50",
                resolved_warehouse_id,
            )
            sample_data = _statement_rows_to_dicts(sample_response)
        else:
            spark = get_spark(token)
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
        table_type=getattr(table_info.table_type, "value", table_info.table_type),
        data_source_format=getattr(table_info.data_source_format, "value", table_info.data_source_format),
        owner=table_info.owner,
        comment=table_info.comment,
        columns=columns,
        sample_data=sample_data,
        sample_data_error=sample_data_error,
    )


@api.get("/warehouses", response_model=WarehousesOut, operation_id="list_warehouses")
def list_warehouses(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]) -> WarehousesOut:
    try:
        result = []
        for w in obo_ws.warehouses.list():
            if w.id and w.name:
                result.append(WarehouseInfo(id=w.id, name=w.name, state=w.state.value if w.state else None))
        return WarehousesOut(warehouses=result)
    except Exception as e:
        logger.error(f"Failed to list warehouses: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list warehouses: {e}")


@api.get("/clusters", response_model=ClustersOut, operation_id="list_clusters")
def list_clusters(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]) -> ClustersOut:
    try:
        result = []
        for cluster in obo_ws.clusters.list():
            if cluster.cluster_id and cluster.cluster_name:
                result.append(
                    ClusterInfo(
                        id=cluster.cluster_id,
                        name=cluster.cluster_name,
                        state=cluster.state.value if cluster.state else None,
                    )
                )
        return ClustersOut(clusters=sorted(result, key=lambda cluster: cluster.name.lower()))
    except Exception as e:
        logger.error(f"Failed to list classic clusters: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list clusters: {e}")


@api.post("/checks-table/create", response_model=CreateChecksTableOut, operation_id="create_checks_table")
def create_checks_table(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
    table_name: str = Query(..., description="Fully qualified table name (catalog.schema.table)"),
    warehouse_id: str | None = Query(None, description="Optional SQL warehouse ID used for table creation"),
) -> CreateChecksTableOut:
    if not _TABLE_NAME_RE.match(table_name):
        raise HTTPException(status_code=400, detail="table_name must be a 3-part dotted identifier (catalog.schema.table)")

    # Check if table already exists
    try:
        obo_ws.tables.get(full_name=table_name)
        return CreateChecksTableOut(table_name=table_name, created=False)
    except NotFound:
        pass
    except Exception as e:
        logger.error(f"Failed to check table existence '{table_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to check table existence: {e}")

    quoted = _quote_3part_table_name(table_name)
    catalog_name, schema_name, _ = table_name.split(".")

    def _create_table(target_spark: SparkSession) -> None:
        target_spark.sql(
            f"CREATE TABLE {quoted} ("
            "name STRING NOT NULL, description STRING, criticality STRING, "
            "check STRUCT<function: STRING, for_each_column: ARRAY<STRING>, arguments: MAP<STRING, STRING>>, "
            "filter STRING, run_config_name STRING NOT NULL, user_metadata MAP<STRING, STRING>, "
            "created_at TIMESTAMP, "
            "PRIMARY KEY (run_config_name, name) NOT ENFORCED"
            ") USING DELTA"
        )

    if os.environ.get("DATABRICKS_HOST"):
        resolved_warehouse_id = _get_default_warehouse_id(app_ws, obo_ws, warehouse_id)

        create_statement = (
            f"CREATE TABLE {quoted} ("
            "name STRING NOT NULL, description STRING, criticality STRING, "
            "check STRUCT<function: STRING, for_each_column: ARRAY<STRING>, arguments: MAP<STRING, STRING>>, "
            "filter STRING, run_config_name STRING NOT NULL, user_metadata MAP<STRING, STRING>, "
            "created_at TIMESTAMP, "
            "PRIMARY KEY (run_config_name, name) NOT ENFORCED"
            ") USING DELTA"
        )
        try:
            _execute_sql_statement(
                obo_ws,
                create_statement,
                resolved_warehouse_id,
                catalog=catalog_name,
                schema=schema_name,
            )
            return CreateChecksTableOut(table_name=table_name, created=True)
        except Exception as e:
            logger.error(f"Failed to create checks table '{table_name}' via SQL warehouse: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to create checks table: {e}")

    try:
        spark = get_spark(token)
        _create_table(spark)
        return CreateChecksTableOut(table_name=table_name, created=True)
    except Exception as e:
        if _is_session_changed_error(e):
            logger.warning(f"Spark session became stale while creating checks table '{table_name}', retrying once")
            try:
                spark = get_spark(token)
                _create_table(spark)
                return CreateChecksTableOut(table_name=table_name, created=True)
            except Exception as retry_error:
                logger.error(
                    f"Failed to create checks table '{table_name}' after refreshing Spark session: {retry_error}",
                    exc_info=True,
                )
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to create checks table: {_format_spark_error_message(retry_error)}",
                )
        logger.error(f"Failed to create checks table '{table_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create checks table: {_format_spark_error_message(e)}")


@api.get("/checks-table", response_model=ChecksTableOut, operation_id="get_checks_table")
def get_checks_table(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
    table_name: str = Query("shao_sandbox1.dqx.checks", description="Fully qualified checks table name"),
) -> ChecksTableOut:
    if not _TABLE_NAME_RE.match(table_name):
        raise HTTPException(status_code=400, detail="table_name must be a 3-part dotted identifier (catalog.schema.table)")

    try:
        # Handle missing checks table gracefully so UI can still render.
        obo_ws.tables.get(full_name=table_name)
    except NotFound:
        logger.warning(f"Checks table '{table_name}' not found; returning empty result")
        return ChecksTableOut(rows=[], columns=[])
    except Exception as e:
        logger.error(f"Failed to validate checks table '{table_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to validate checks table: {e}")

    try:
        quoted_table_name = _quote_3part_table_name(table_name)

        if os.environ.get("DATABRICKS_HOST"):
            resolved_warehouse_id = _get_default_warehouse_id(app_ws, obo_ws)
            response = _execute_sql_statement(
                obo_ws,
                f"SELECT * FROM {quoted_table_name}",
                resolved_warehouse_id,
            )
            rows = _statement_rows_to_dicts(response)
            columns = list(rows[0].keys()) if rows else [
                column.name or f"col_{index}"
                for index, column in enumerate(
                    response.manifest.schema.columns
                    if response.manifest and response.manifest.schema and response.manifest.schema.columns
                    else []
                )
            ]
            return ChecksTableOut(rows=rows, columns=columns)

        spark = get_spark(token)
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
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
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
        obo_ws.tables.get(full_name=result_table_name)
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
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            logger.warning(f"Result table '{result_table_name}' not found during Spark read; returning empty result")
            return CheckErrorRowsOut(result_table_name=result_table_name, rows=[], columns=[])
        logger.error(f"Failed to read filtered error rows from '{result_table_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to read error rows: {e}")


@api.post("/checks-table/append", response_model=SaveGeneratedChecksOut, operation_id="append_generated_checks")
def append_generated_checks(
    body: SaveGeneratedChecksIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
) -> SaveGeneratedChecksOut:
    if not _TABLE_NAME_RE.match(body.table_name):
        raise HTTPException(status_code=400, detail="table_name must be a 3-part dotted identifier (catalog.schema.table)")

    try:
        obo_ws.tables.get(full_name=body.table_name)
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
        if _TABLE_NAME_RE.match(run_config_name):
            try:
                run_config_table = obo_ws.tables.get(full_name=run_config_name)
                actual_columns = [column.name for column in (run_config_table.columns or []) if column.name]
                normalized_checks = _align_check_columns_with_actual_schema(normalized_checks, actual_columns)
            except Exception:
                logger.warning(f"Failed to align generated checks to schema for '{run_config_name}'", exc_info=True)

        resolved_warehouse_id = _get_default_warehouse_id(app_ws, obo_ws)
        duplicate_names_in_batch = _validate_generated_rule_metadata(normalized_checks, run_config_name)
        if duplicate_names_in_batch:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Duplicate rule names were generated within this save request for run_config_name "
                    f"'{run_config_name}': {', '.join(duplicate_names_in_batch)}"
                ),
            )
        if body.mode != "overwrite":
            existing_duplicate_names = _existing_rule_names_for_run_config(
                obo_ws=obo_ws,
                warehouse_id=resolved_warehouse_id,
                table_name=body.table_name,
                run_config_name=run_config_name,
                names=[str(check.get("name", "")).strip() for check in normalized_checks],
            )
            if existing_duplicate_names:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Duplicate rule names already exist for run_config_name '{run_config_name}': "
                        f"{', '.join(existing_duplicate_names)}"
                    ),
                )
        _save_generated_checks_via_sql_warehouse(
            obo_ws=obo_ws,
            warehouse_id=resolved_warehouse_id,
            table_name=body.table_name,
            checks=normalized_checks,
            run_config_name=run_config_name,
            mode=body.mode,
        )
        return SaveGeneratedChecksOut(inserted=len(normalized_checks))
    except HTTPException:
        raise
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
