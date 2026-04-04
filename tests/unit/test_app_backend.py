"""Unit tests for the DQX App backend modules."""

import base64
import logging
from types import SimpleNamespace
from unittest.mock import create_autospec

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pyspark.sql import SparkSession

from databricks_labs_dqx_app.backend import dependencies as backend_dependencies
from databricks_labs_dqx_app.backend.app import app
from databricks_labs_dqx_app.backend import router as backend_router
from databricks_labs_dqx_app.backend.dependencies import get_app_ws, get_obo_ws, get_spark
from databricks_labs_dqx_app.backend.logger import CustomFormatter, setup_logger
from databricks_labs_dqx_app.backend.models import InstallationSettings
from databricks_labs_dqx_app.backend.router import get_install_folder
from databricks_labs_dqx_app.backend.settings import SettingsManager

from databricks.labs.dqx.errors import InvalidConfigError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceDoesNotExist
from databricks.sdk.service.iam import User
from databricks.sdk.service import sql as sql_service
from databricks.sdk.service.workspace import ExportFormat, ExportResponse


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient."""
    ws = create_autospec(WorkspaceClient)
    mock_user = create_autospec(User)
    mock_user.user_name = "test_user@example.com"
    ws.current_user.me.return_value = mock_user
    return ws


# ============================================================================
# Tests for router.py - get_install_folder
# ============================================================================


class TestGetInstallFolder:
    """Unit tests for the get_install_folder helper function."""

    def test_returns_provided_path(self, mock_workspace_client):
        """Should return the provided path when it's a folder."""
        result = get_install_folder(mock_workspace_client, "/Workspace/my_project/dqx")
        assert result == "/Workspace/my_project/dqx"

    def test_strips_whitespace(self, mock_workspace_client):
        """Should strip whitespace from the provided path."""
        result = get_install_folder(mock_workspace_client, "  /Workspace/my_project/dqx  ")
        assert result == "/Workspace/my_project/dqx"

    def test_handles_yml_only_filename(self, mock_workspace_client):
        """Should handle path that is just a yml filename with no folder."""
        result = get_install_folder(mock_workspace_client, "config.yml")
        assert result == "config.yml"

    def test_uses_app_scoped_settings_when_path_missing(self, mock_workspace_client):
        """Should resolve settings from the app workspace home when path is omitted."""
        encoded = base64.b64encode(b"install_folder: /Workspace/custom/dqx").decode()
        mock_workspace_client.workspace.export.return_value = ExportResponse(content=encoded)

        result = get_install_folder(mock_workspace_client, None)

        assert result == "/Workspace/custom/dqx"
        mock_workspace_client.workspace.export.assert_called_once_with(
            "/Users/test_user@example.com/.dqx/app.yml", format=ExportFormat.SOURCE
        )


class TestUnicodeCheckNormalization:
    def test_normalize_generated_checks_decodes_unicode_for_column_lists(self):
        checks = [
            {
                "check": {
                    "function": "is_not_null",
                    "arguments": {
                        "columns": ['"\\u91d1\\u989d"', "Rev\\u5b8c\\u6210\\u91d1\\u989d"],
                    },
                    "for_each_column": ['"\\u8ba2\\u5355\\u91d1\\u989d"', "`\\u5e01\\u79cd`"],
                }
            }
        ]

        normalized = backend_router._normalize_generated_checks(checks)

        assert normalized[0]["check"]["arguments"]["columns"] == ["金额", "Rev完成金额"]
        assert normalized[0]["check"]["for_each_column"] == ["订单金额", "币种"]

    def test_align_check_columns_with_actual_schema_matches_unicode_column_lists(self):
        checks = [
            {
                "check": {
                    "function": "is_not_null",
                    "arguments": {
                        "column": '"金额"',
                        "columns": ['"\\u91d1\\u989d"', "Rev\\u5b8c\\u6210\\u91d1\\u989d"],
                    },
                    "for_each_column": ['"\\u8ba2\\u5355\\u91d1\\u989d"', "`\\u5e01\\u79cd`"],
                }
            }
        ]

        aligned = backend_router._align_check_columns_with_actual_schema(
            checks,
            ["金额", "Rev完成金额", "订单金额", "币种"],
        )

        assert aligned[0]["check"]["arguments"]["column"] == "金额"
        assert aligned[0]["check"]["arguments"]["columns"] == ["金额", "Rev完成金额"]
        assert aligned[0]["check"]["for_each_column"] == ["订单金额", "币种"]

    def test_sql_check_struct_expr_preserves_unicode_argument_values(self):
        check_struct_expr = backend_router._sql_check_struct_expr(
            {
                "check": {
                    "function": "is_not_null",
                    "arguments": {"column": "金额"},
                    "for_each_column": ["订单金额"],
                }
            }
        )

        assert '"金额"' in check_struct_expr
        assert "\\u91d1\\u989d" not in check_struct_expr


class TestGetOboWs:
    """Unit tests for the get_obo_ws dependency function."""

    def test_raises_when_no_token(self):
        """Should raise HTTPException when no token is provided."""
        with pytest.raises(HTTPException) as exc_info:
            get_obo_ws(token=None)
        assert exc_info.value.status_code == 401
        assert "Authentication required" in exc_info.value.detail

    def test_raises_when_empty_token(self):
        """Should raise HTTPException when empty token is provided."""
        with pytest.raises(HTTPException) as exc_info:
            get_obo_ws(token="")
        assert exc_info.value.status_code == 401
        assert "Authentication required" in exc_info.value.detail


class _FakeSparkQuery:
    def __init__(self, error: Exception | None = None):
        self._error = error

    def collect(self):
        if self._error is not None:
            raise self._error
        return [{"ok": 1}]


class _FakeSparkSession:
    def __init__(self, healthcheck_error: Exception | None = None):
        self.healthcheck_error = healthcheck_error
        self.sql_calls: list[str] = []
        self.stopped = False

    def sql(self, query: str):
        self.sql_calls.append(query)
        error = self.healthcheck_error if query == "SELECT 1" else None
        return _FakeSparkQuery(error=error)

    def stop(self):
        self.stopped = True


class _FakeDatabricksBuilder:
    def __init__(self, sessions: list[_FakeSparkSession]):
        self._sessions = iter(sessions)
        self.serverless_calls = 0
        self.get_or_create_calls = 0
        self.cluster_id_calls: list[str] = []
        self.host_calls: list[str] = []
        self.token_calls: list[str] = []

    def host(self, value: str):
        self.host_calls.append(value)
        return self

    def token(self, value: str):
        self.token_calls.append(value)
        return self

    def clusterId(self, value: str):
        self.cluster_id_calls.append(value)
        return self

    def serverless(self):
        self.serverless_calls += 1
        return self

    def getOrCreate(self):
        self.get_or_create_calls += 1
        return next(self._sessions)


class _FakeDatabricksSession:
    def __init__(self, builder: _FakeDatabricksBuilder):
        self.builder = builder


class TestGetAppSpark:
    def test_reconnects_when_cached_session_handle_is_invalid(self, monkeypatch):
        stale_session = _FakeSparkSession(
            healthcheck_error=Exception(
                "[INVALID_HANDLE.SESSION_CHANGED] The existing Spark server driver instance has restarted."
            )
        )
        fresh_session = _FakeSparkSession()
        builder = _FakeDatabricksBuilder([stale_session, fresh_session])

        monkeypatch.setenv("DATABRICKS_HOST", "https://example.databricks.com")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "client-id")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "client-secret")
        monkeypatch.setattr(backend_dependencies, "DatabricksSession", _FakeDatabricksSession(builder))

        spark = backend_dependencies.get_app_spark()

        assert spark is fresh_session
        assert stale_session.stopped is True
        assert stale_session.sql_calls == ["SELECT 1"]
        assert fresh_session.sql_calls == ["SELECT 1"]
        assert builder.serverless_calls == 2
        assert builder.get_or_create_calls == 2

    def test_get_spark_uses_selected_classic_cluster(self, monkeypatch):
        spark_session = _FakeSparkSession()
        builder = _FakeDatabricksBuilder([spark_session])

        monkeypatch.setenv("DATABRICKS_HOST", "https://example.databricks.com")
        monkeypatch.setattr(backend_dependencies, "DatabricksSession", _FakeDatabricksSession(builder))
        monkeypatch.setattr(backend_dependencies, "_get_default_compute_settings", lambda: (False, "cluster-123"))

        spark = backend_dependencies.get_spark(token="token-abc")

        assert spark is spark_session
        assert builder.cluster_id_calls == ["cluster-123"]
        assert builder.serverless_calls == 0


class TestGetGenerator:
    def test_uses_serving_endpoint_name_from_env(self, monkeypatch, mock_workspace_client):
        fake_spark = object()
        captured: dict[str, object] = {}

        class _FakeGenerator:
            def __init__(self, workspace_client, spark, llm_model_config):
                captured["workspace_client"] = workspace_client
                captured["spark"] = spark
                captured["llm_model_config"] = llm_model_config

        monkeypatch.setenv("DATABRICKS_HOST", "https://example.databricks.com")
        monkeypatch.setenv("SERVING_ENDPOINT_NAME", "databricks-claude-sonnet-4-6")
        monkeypatch.setattr(backend_dependencies, "DQGenerator", _FakeGenerator)

        generator = backend_dependencies.get_generator(mock_workspace_client, fake_spark)

        assert generator is not None
        assert captured["workspace_client"] is mock_workspace_client
        assert captured["spark"] is fake_spark
        assert captured["llm_model_config"].model_name == "databricks/databricks-claude-sonnet-4-6"
        assert captured["llm_model_config"].api_base == "https://example.databricks.com/serving-endpoints"

    def test_uses_updated_default_model(self, monkeypatch, mock_workspace_client):
        fake_spark = object()
        captured: dict[str, object] = {}

        class _FakeGenerator:
            def __init__(self, workspace_client, spark, llm_model_config):
                captured["workspace_client"] = workspace_client
                captured["spark"] = spark
                captured["llm_model_config"] = llm_model_config

        monkeypatch.setattr(backend_dependencies, "DQGenerator", _FakeGenerator)

        generator = backend_dependencies.get_generator(mock_workspace_client, fake_spark)

        assert generator is not None
        assert captured["workspace_client"] is mock_workspace_client
        assert captured["spark"] is fake_spark
        assert captured["llm_model_config"].model_name == "databricks/databricks-claude-sonnet-4-6"


class TestUserScopedCatalogOperations:
    def test_get_check_error_rows_returns_empty_when_spark_cannot_resolve_result_table(
        self, mock_workspace_client
    ):
        mock_workspace_client.tables.get.return_value = SimpleNamespace(
            full_name="main.analytics.orders_dqx_result"
        )
        fake_spark = create_autospec(SparkSession)
        fake_spark.read.table.side_effect = Exception("[TABLE_OR_VIEW_NOT_FOUND] Table not found")

        def _override_obo_ws():
            return mock_workspace_client

        def _override_spark():
            return fake_spark

        app.dependency_overrides[get_obo_ws] = _override_obo_ws
        app.dependency_overrides[get_spark] = _override_spark

        try:
            client = TestClient(app)
            response = client.get(
                "/api/checks-table/error-rows",
                params={"run_config_name": "main.analytics.orders", "check_name": "id_not_null"},
                headers={"X-Forwarded-Access-Token": "dummy"},
            )
            assert response.status_code == 200
            assert response.json() == {
                "result_table_name": "main.analytics.orders_dqx_result",
                "rows": [],
                "columns": [],
            }
            mock_workspace_client.tables.get.assert_called_once_with(
                full_name="main.analytics.orders_dqx_result"
            )
            fake_spark.sql.assert_not_called()
        finally:
            app.dependency_overrides.clear()

    def test_ai_save_enrichment_uses_configured_endpoint_name_in_sql(self, mock_workspace_client, monkeypatch):
        monkeypatch.setenv("SERVING_ENDPOINT_NAME", "my-sonnet-endpoint")
        mock_workspace_client.statement_execution.execute_statement.return_value = sql_service.StatementResponse(
            status=sql_service.StatementStatus(state=sql_service.StatementState.SUCCEEDED),
            manifest=sql_service.ResultManifest(
                schema=sql_service.ResultSchema(
                    columns=[
                        sql_service.ColumnInfo(name="idx"),
                        sql_service.ColumnInfo(name="generated_json"),
                    ]
                )
            ),
            result=sql_service.ResultData(
                data_array=[["0", '{"name":"id_not_null","description":"ID must not be null."}']]
            ),
        )

        backend_router._enrich_generated_checks_with_ai_via_sql_warehouse(
            obo_ws=mock_workspace_client,
            warehouse_id="wh-123",
            checks=[{"check": {"function": "is_not_null", "arguments": {"column": "id"}}, "criticality": "error"}],
            run_config_name="default",
        )

        statement = mock_workspace_client.statement_execution.execute_statement.call_args.kwargs["statement"]
        assert "'my-sonnet-endpoint'" in statement
        assert "'databricks/my-sonnet-endpoint'" not in statement

    def test_ai_save_enrichment_uses_same_default_model_as_rule_generation(self, mock_workspace_client, monkeypatch):
        monkeypatch.delenv("SERVING_ENDPOINT_NAME", raising=False)
        mock_workspace_client.statement_execution.execute_statement.return_value = sql_service.StatementResponse(
            status=sql_service.StatementStatus(state=sql_service.StatementState.SUCCEEDED),
            manifest=sql_service.ResultManifest(
                schema=sql_service.ResultSchema(
                    columns=[
                        sql_service.ColumnInfo(name="idx"),
                        sql_service.ColumnInfo(name="generated_json"),
                    ]
                )
            ),
            result=sql_service.ResultData(
                data_array=[[ "0", '{"name":"id_not_null","description":"ID must not be null."}' ]]
            ),
        )

        enriched_checks = backend_router._enrich_generated_checks_with_ai_via_sql_warehouse(
            obo_ws=mock_workspace_client,
            warehouse_id="wh-123",
            checks=[{"check": {"function": "is_not_null", "arguments": {"column": "id"}}, "criticality": "error"}],
            run_config_name="default",
        )

        assert enriched_checks[0]["name"] == "id_not_null"
        assert enriched_checks[0]["description"] == "ID must not be null."
        statement = mock_workspace_client.statement_execution.execute_statement.call_args.kwargs["statement"]
        assert "'databricks-claude-sonnet-4-6'" in statement
        assert "'databricks/databricks-claude-sonnet-4-6'" not in statement

    def test_list_catalogs_uses_obo_workspace_client_rest_api(self, mock_workspace_client):
        mock_workspace_client.catalogs.list.return_value = [
            SimpleNamespace(name="main"),
            SimpleNamespace(name="sandbox"),
        ]

        def _override_obo_ws():
            return mock_workspace_client

        app.dependency_overrides[get_obo_ws] = _override_obo_ws

        try:
            client = TestClient(app)
            response = client.get(
                "/api/catalogs",
                headers={"X-Forwarded-Access-Token": "dummy"},
            )
            assert response.status_code == 200
            assert response.json() == {"catalogs": ["main", "sandbox"]}
            mock_workspace_client.catalogs.list.assert_called_once()
        finally:
            app.dependency_overrides.clear()

    def test_list_schemas_uses_obo_workspace_client_rest_api(self, mock_workspace_client):
        mock_workspace_client.schemas.list.return_value = [
            SimpleNamespace(name="default"),
            SimpleNamespace(name="analytics"),
        ]

        def _override_obo_ws():
            return mock_workspace_client

        app.dependency_overrides[get_obo_ws] = _override_obo_ws

        try:
            client = TestClient(app)
            response = client.get(
                "/api/schemas",
                params={"catalog": "main"},
                headers={"X-Forwarded-Access-Token": "dummy"},
            )
            assert response.status_code == 200
            assert response.json() == {"schemas": ["analytics", "default"]}
            mock_workspace_client.schemas.list.assert_called_once_with(catalog_name="main")
        finally:
            app.dependency_overrides.clear()

    def test_list_tables_uses_obo_workspace_client_rest_api(self, mock_workspace_client):
        mock_workspace_client.tables.list.return_value = [
            SimpleNamespace(name="orders"),
            SimpleNamespace(name="customers"),
        ]

        def _override_obo_ws():
            return mock_workspace_client

        app.dependency_overrides[get_obo_ws] = _override_obo_ws

        try:
            client = TestClient(app)
            response = client.get(
                "/api/tables",
                params={"catalog": "main", "schema": "analytics"},
                headers={"X-Forwarded-Access-Token": "dummy"},
            )
            assert response.status_code == 200
            assert response.json() == {"tables": ["customers", "orders"]}
            mock_workspace_client.tables.list.assert_called_once_with(
                catalog_name="main",
                schema_name="analytics",
            )
        finally:
            app.dependency_overrides.clear()

    def test_get_table_info_uses_obo_workspace_client_metadata(self, mock_workspace_client, monkeypatch):
        mock_workspace_client.tables.get.return_value = SimpleNamespace(
            full_name="main.analytics.orders",
            owner="analyst@databricks.com",
            comment="Orders table",
            table_type=SimpleNamespace(value="MANAGED"),
            data_source_format=SimpleNamespace(value="delta"),
            columns=[
                SimpleNamespace(
                    name="id",
                    type_text="string",
                    comment="primary key",
                    nullable=True,
                    position=0,
                    type_name=None,
                ),
                SimpleNamespace(
                    name="amount",
                    type_text="decimal(10,2)",
                    comment="order amount",
                    nullable=True,
                    position=1,
                    type_name=None,
                ),
            ],
        )
        mock_workspace_client.statement_execution.execute_statement.return_value = sql_service.StatementResponse(
            status=sql_service.StatementStatus(state=sql_service.StatementState.SUCCEEDED),
            manifest=sql_service.ResultManifest(
                schema=sql_service.ResultSchema(
                    columns=[
                        sql_service.ColumnInfo(name="id"),
                        sql_service.ColumnInfo(name="amount"),
                    ]
                )
            ),
            result=sql_service.ResultData(data_array=[["1", "10.5"]]),
        )

        def _override_obo_ws():
            return mock_workspace_client

        def _override_app_ws():
            return mock_workspace_client

        app.dependency_overrides[get_obo_ws] = _override_obo_ws
        app.dependency_overrides[get_app_ws] = _override_app_ws
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.databricks.com")
        monkeypatch.setattr(
            backend_router,
            "_get_default_warehouse_id",
            lambda _app_ws, _obo_ws, explicit_warehouse_id=None: explicit_warehouse_id or "wh-123",
        )
        monkeypatch.setattr(
            backend_router,
            "get_spark",
            lambda _token: (_ for _ in ()).throw(AssertionError("get_spark should not be called")),
        )

        try:
            client = TestClient(app)
            response = client.get(
                "/api/table-info",
                params={"full_name": "main.analytics.orders", "warehouse_id": "wh-123"},
                headers={"X-Forwarded-Access-Token": "dummy"},
            )
            assert response.status_code == 200
            assert response.json()["table_type"] == "MANAGED"
            assert response.json()["data_source_format"] == "delta"
            assert response.json()["owner"] == "analyst@databricks.com"
            assert response.json()["comment"] == "Orders table"
            assert response.json()["columns"] == [
                {
                    "name": "id",
                    "type_text": "string",
                    "comment": "primary key",
                    "nullable": True,
                    "position": 0,
                },
                {
                    "name": "amount",
                    "type_text": "decimal(10,2)",
                    "comment": "order amount",
                    "nullable": True,
                    "position": 1,
                },
            ]
            assert response.json()["sample_data"] == [{"id": "1", "amount": "10.5"}]
            assert response.json()["sample_data_error"] is None
            mock_workspace_client.tables.get.assert_called_once_with(full_name="main.analytics.orders")
            assert mock_workspace_client.statement_execution.execute_statement.call_args.kwargs["warehouse_id"] == "wh-123"
            assert "SELECT * FROM `main`.`analytics`.`orders` LIMIT 50" in mock_workspace_client.statement_execution.execute_statement.call_args.kwargs["statement"]
        finally:
            app.dependency_overrides.clear()

    def test_get_checks_table_uses_sql_warehouse_query(self, mock_workspace_client, monkeypatch):
        mock_workspace_client.tables.get.return_value = SimpleNamespace(full_name="main.analytics.checks")
        mock_workspace_client.statement_execution.execute_statement.return_value = sql_service.StatementResponse(
            status=sql_service.StatementStatus(state=sql_service.StatementState.SUCCEEDED),
            manifest=sql_service.ResultManifest(
                schema=sql_service.ResultSchema(
                    columns=[
                        sql_service.ColumnInfo(name="name"),
                        sql_service.ColumnInfo(name="description"),
                        sql_service.ColumnInfo(name="criticality"),
                    ]
                )
            ),
            result=sql_service.ResultData(
                data_array=[
                    ["rule_1", "rule description", "error"],
                    ["rule_2", None, "warn"],
                ]
            ),
        )

        def _override_obo_ws():
            return mock_workspace_client

        def _override_app_ws():
            return mock_workspace_client

        app.dependency_overrides[get_obo_ws] = _override_obo_ws
        app.dependency_overrides[get_app_ws] = _override_app_ws
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.databricks.com")
        monkeypatch.setattr(
            backend_router,
            "_get_default_warehouse_id",
            lambda _app_ws, _obo_ws, explicit_warehouse_id=None: explicit_warehouse_id or "wh-123",
        )

        try:
            client = TestClient(app)
            response = client.get(
                "/api/checks-table",
                params={"table_name": "main.analytics.checks"},
                headers={"X-Forwarded-Access-Token": "dummy"},
            )
            assert response.status_code == 200
            assert response.json() == {
                "columns": ["name", "description", "criticality"],
                "rows": [
                    {"name": "rule_1", "description": "rule description", "criticality": "error"},
                    {"name": "rule_2", "description": None, "criticality": "warn"},
                ],
            }
            assert mock_workspace_client.statement_execution.execute_statement.call_args.kwargs["warehouse_id"] == "wh-123"
            assert "SELECT * FROM `main`.`analytics`.`checks`" in mock_workspace_client.statement_execution.execute_statement.call_args.kwargs["statement"]
        finally:
            app.dependency_overrides.clear()

    def test_ai_generate_checks_uses_obo_metadata_and_sql_for_table_context(self, mock_workspace_client, monkeypatch):
        mock_workspace_client.tables.get.return_value = SimpleNamespace(
            full_name="main.analytics.orders",
            columns=[
                SimpleNamespace(name="id", type_text="string", comment="primary key", nullable=True),
                SimpleNamespace(name="amount", type_text="decimal(10,2)", comment="order amount", nullable=True),
            ],
        )
        mock_workspace_client.statement_execution.execute_statement.return_value = sql_service.StatementResponse(
            status=sql_service.StatementStatus(state=sql_service.StatementState.SUCCEEDED),
            manifest=sql_service.ResultManifest(
                schema=sql_service.ResultSchema(
                    columns=[
                        sql_service.ColumnInfo(name="id"),
                        sql_service.ColumnInfo(name="amount"),
                    ]
                )
            ),
            result=sql_service.ResultData(data_array=[["1", "10.5"]]),
        )

        captured: dict[str, object] = {}

        class _FailingSpark:
            def sql(self, _query: str):
                raise AssertionError("generator.spark.sql should not be called")

        class _FakeGenerator:
            def __init__(self):
                self.spark = _FailingSpark()

            def generate_dq_rules_ai_assisted(self, user_input="", summary_stats=None, input_config=None):
                captured["user_input"] = user_input
                captured["summary_stats"] = summary_stats
                captured["input_config"] = input_config
                return [{"check": {"function": "is_not_null", "arguments": {"column": "id"}}, "criticality": "error"}]

        def _override_obo_ws():
            return mock_workspace_client

        def _override_app_ws():
            return mock_workspace_client

        def _override_generator():
            return _FakeGenerator()

        app.dependency_overrides[get_obo_ws] = _override_obo_ws
        app.dependency_overrides[get_app_ws] = _override_app_ws
        app.dependency_overrides[backend_dependencies.get_generator] = _override_generator
        monkeypatch.setattr(
            backend_router,
            "_get_default_warehouse_id",
            lambda _app_ws, _obo_ws, explicit_warehouse_id=None: explicit_warehouse_id or "wh-123",
        )

        try:
            client = TestClient(app)
            response = client.post(
                "/api/ai-generate-checks",
                json={"user_input": "Generate checks for this table", "table_name": "main.analytics.orders"},
                headers={"X-Forwarded-Access-Token": "dummy"},
            )
            assert response.status_code == 200
            assert response.json()["checks"] == [
                {"check": {"function": "is_not_null", "arguments": {"column": "id"}}, "criticality": "error"}
            ]
            assert captured["input_config"] is None
            assert "Selected table: main.analytics.orders" in captured["user_input"]
            assert "Table schema (JSON):" in captured["user_input"]
            assert "Sample rows (JSON):" in captured["user_input"]
            mock_workspace_client.tables.get.assert_called_once_with(full_name="main.analytics.orders")
            assert mock_workspace_client.statement_execution.execute_statement.call_args.kwargs["warehouse_id"] == "wh-123"
            assert (
                "SELECT * FROM `main`.`analytics`.`orders` LIMIT 20"
                in mock_workspace_client.statement_execution.execute_statement.call_args.kwargs["statement"]
            )
        finally:
            app.dependency_overrides.clear()

    def test_profile_ai_generate_checks_uses_profiler_summary_stats_and_merges_rules(
        self, mock_workspace_client, monkeypatch
    ):
        mock_workspace_client.tables.get.return_value = SimpleNamespace(
            full_name="main.analytics.orders",
            columns=[
                SimpleNamespace(name="id", type_text="string", comment="primary key", nullable=True),
                SimpleNamespace(name="amount", type_text="decimal(10,2)", comment="order amount", nullable=True),
            ],
        )
        mock_workspace_client.statement_execution.execute_statement.return_value = sql_service.StatementResponse(
            status=sql_service.StatementStatus(state=sql_service.StatementState.SUCCEEDED),
            manifest=sql_service.ResultManifest(
                schema=sql_service.ResultSchema(
                    columns=[
                        sql_service.ColumnInfo(name="id"),
                        sql_service.ColumnInfo(name="amount"),
                    ]
                )
            ),
            result=sql_service.ResultData(data_array=[["1", "10.5"]]),
        )

        captured: dict[str, object] = {}

        class _FakeGenerator:
            def __init__(self):
                self.spark = object()

            def generate_dq_rules(self, profiles=None, criticality="error"):
                captured["profiles"] = profiles
                captured["criticality"] = criticality
                return [
                    {
                        "name": "id_is_null",
                        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
                        "criticality": "error",
                    }
                ]

            def generate_dq_rules_ai_assisted(self, user_input="", summary_stats=None, input_config=None):
                captured["user_input"] = user_input
                captured["summary_stats"] = summary_stats
                captured["input_config"] = input_config
                return [
                    {
                        "name": "amount_isnt_in_range",
                        "check": {
                            "function": "is_in_range",
                            "arguments": {"column": "amount", "min_limit": 0, "max_limit": 1000},
                        },
                        "criticality": "error",
                    }
                ]

        def _override_obo_ws():
            return mock_workspace_client

        def _override_app_ws():
            return mock_workspace_client

        def _override_generator():
            return _FakeGenerator()

        app.dependency_overrides[get_obo_ws] = _override_obo_ws
        app.dependency_overrides[get_app_ws] = _override_app_ws
        app.dependency_overrides[backend_dependencies.get_generator] = _override_generator
        monkeypatch.setattr(
            backend_router,
            "_profile_table_for_ai_generation",
            lambda obo_ws, app_ws, generator, table_name: (
                captured.update(
                    {
                        "profile_obo_ws": obo_ws,
                        "profile_app_ws": app_ws,
                        "profile_generator": generator,
                        "profile_table_name": table_name,
                    }
                )
                or (
                    {
                        "id": {"count": 100, "count_null": 0},
                        "amount": {"count": 100, "count_null": 0, "min": 0, "max": 1000},
                    },
                    [
                        {
                            "name": "id_is_null",
                            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
                            "criticality": "error",
                        }
                    ],
                )
            ),
        )
        monkeypatch.setattr(
            backend_router,
            "_get_default_warehouse_id",
            lambda _app_ws, _obo_ws, explicit_warehouse_id=None: explicit_warehouse_id or "wh-123",
        )

        try:
            client = TestClient(app)
            response = client.post(
                "/api/profile-ai-generate-checks",
                json={"table_name": "main.analytics.orders"},
                headers={"X-Forwarded-Access-Token": "dummy"},
            )
            assert response.status_code == 200
            assert response.json()["checks"] == [
                {
                    "name": "id_is_null",
                    "check": {"function": "is_not_null", "arguments": {"column": "id"}},
                    "criticality": "error",
                },
                {
                    "name": "amount_isnt_in_range",
                    "check": {
                        "function": "is_in_range",
                        "arguments": {"column": "amount", "min_limit": 0, "max_limit": 1000},
                    },
                    "criticality": "error",
                },
            ]
            assert captured["summary_stats"] == {
                "id": {"count": 100, "count_null": 0},
                "amount": {"count": 100, "count_null": 0, "min": 0, "max": 1000},
            }
            assert captured["input_config"] is None
            assert captured["profile_obo_ws"] is mock_workspace_client
            assert captured["profile_app_ws"] is mock_workspace_client
            assert captured["profile_table_name"] == "main.analytics.orders"
            assert "Selected table: main.analytics.orders" in captured["user_input"]
            assert "Profiler-suggested checks (JSON):" in captured["user_input"]
            assert "Sample rows (JSON):" in captured["user_input"]
        finally:
            app.dependency_overrides.clear()

    def test_list_clusters_uses_obo_workspace_client(self, mock_workspace_client):
        mock_workspace_client.clusters.list.return_value = [
            SimpleNamespace(cluster_id="abc-123", cluster_name="Analytics Cluster", state=SimpleNamespace(value="RUNNING")),
        ]

        def _override_obo_ws():
            return mock_workspace_client

        app.dependency_overrides[get_obo_ws] = _override_obo_ws

        try:
            client = TestClient(app)
            response = client.get("/api/clusters", headers={"X-Forwarded-Access-Token": "dummy"})
            assert response.status_code == 200
            assert response.json() == {
                "clusters": [{"id": "abc-123", "name": "Analytics Cluster", "state": "RUNNING"}]
            }
        finally:
            app.dependency_overrides.clear()

    def test_list_warehouses_uses_obo_workspace_client(self, mock_workspace_client):
        mock_workspace_client.warehouses.list.return_value = [
            SimpleNamespace(id="wh-123", name="Serverless Warehouse", state=SimpleNamespace(value="RUNNING")),
        ]

        def _override_obo_ws():
            return mock_workspace_client

        app.dependency_overrides[get_obo_ws] = _override_obo_ws

        try:
            client = TestClient(app)
            response = client.get("/api/warehouses", headers={"X-Forwarded-Access-Token": "dummy"})
            assert response.status_code == 200
            assert response.json() == {
                "warehouses": [{"id": "wh-123", "name": "Serverless Warehouse", "state": "RUNNING"}]
            }
        finally:
            app.dependency_overrides.clear()

    def test_create_checks_table_uses_explicit_warehouse_override(self, mock_workspace_client, monkeypatch):
        mock_workspace_client.tables.get.side_effect = NotFound("missing")
        mock_workspace_client.statement_execution.execute_statement.return_value = sql_service.StatementResponse(
            status=sql_service.StatementStatus(state=sql_service.StatementState.SUCCEEDED)
        )

        def _override_obo_ws():
            return mock_workspace_client

        def _override_app_ws():
            return mock_workspace_client

        app.dependency_overrides[get_obo_ws] = _override_obo_ws
        app.dependency_overrides[get_app_ws] = _override_app_ws
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.databricks.com")
        monkeypatch.setattr(
            backend_router,
            "_get_default_warehouse_id",
            lambda _app_ws, _obo_ws, explicit_warehouse_id=None: explicit_warehouse_id or "unexpected",
        )

        try:
            client = TestClient(app)
            response = client.post(
                "/api/checks-table/create",
                params={
                    "table_name": "main.analytics.checks",
                    "warehouse_id": "wh-explicit",
                },
                headers={"X-Forwarded-Access-Token": "dummy"},
            )
            assert response.status_code == 200
            assert response.json() == {
                "table_name": "main.analytics.checks",
                "created": True,
            }
            assert mock_workspace_client.statement_execution.execute_statement.call_args.kwargs["warehouse_id"] == "wh-explicit"
            assert "description STRING" in mock_workspace_client.statement_execution.execute_statement.call_args.kwargs["statement"]
            assert "rule_fingerprint" not in mock_workspace_client.statement_execution.execute_statement.call_args.kwargs["statement"]
            assert "rule_set_fingerprint" not in mock_workspace_client.statement_execution.execute_statement.call_args.kwargs["statement"]
        finally:
            app.dependency_overrides.clear()

    def test_create_checks_table_uses_user_scoped_dependencies(self, mock_workspace_client):
        mock_workspace_client.tables.get.side_effect = NotFound("missing")
        fake_spark = _FakeSparkSession()

        def _override_obo_ws():
            return mock_workspace_client

        def _override_app_ws():
            return mock_workspace_client

        app.dependency_overrides[get_obo_ws] = _override_obo_ws
        app.dependency_overrides[get_app_ws] = _override_app_ws
        original_get_spark = backend_router.get_spark
        backend_router.get_spark = lambda _token: fake_spark

        try:
            client = TestClient(app)
            response = client.post(
                "/api/checks-table/create",
                params={"table_name": "main.analytics.checks"},
                headers={"X-Forwarded-Access-Token": "dummy"},
            )
            assert response.status_code == 200
            assert response.json() == {
                "table_name": "main.analytics.checks",
                "created": True,
            }
            assert any("CREATE TABLE `main`.`analytics`.`checks`" in sql for sql in fake_spark.sql_calls)
            assert any("description STRING" in sql for sql in fake_spark.sql_calls)
            assert all("rule_fingerprint" not in sql for sql in fake_spark.sql_calls)
            assert all("rule_set_fingerprint" not in sql for sql in fake_spark.sql_calls)
        finally:
            backend_router.get_spark = original_get_spark
            app.dependency_overrides.clear()

    def test_append_generated_checks_does_not_fallback_to_spark(
        self, mock_workspace_client, monkeypatch
    ):
        mock_workspace_client.tables.get.return_value = SimpleNamespace(full_name="main.analytics.checks")
        mock_workspace_client.statement_execution.execute_statement.return_value = sql_service.StatementResponse(
            status=sql_service.StatementStatus(state=sql_service.StatementState.SUCCEEDED)
        )

        def _override_obo_ws():
            return mock_workspace_client

        def _override_app_ws():
            return mock_workspace_client

        app.dependency_overrides[get_obo_ws] = _override_obo_ws
        app.dependency_overrides[get_app_ws] = _override_app_ws
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.setattr(
            backend_router,
            "_get_default_warehouse_id",
            lambda _app_ws, _obo_ws, explicit_warehouse_id=None: explicit_warehouse_id or "wh-123",
        )
        monkeypatch.setattr(backend_router, "get_spark", lambda _token: (_ for _ in ()).throw(AssertionError("get_spark should not be called")))
        monkeypatch.setattr(
            backend_router,
            "_enrich_generated_checks_with_ai_via_sql_warehouse",
            lambda obo_ws, warehouse_id, checks, run_config_name: [
                {
                    **checks[0],
                    "name": "id_not_null",
                    "description": "ID must not be null.",
                }
            ],
        )

        try:
            client = TestClient(app)
            response = client.post(
                "/api/checks-table/append",
                json={
                    "table_name": "main.analytics.checks",
                    "mode": "append",
                    "checks": [{"check": {"function": "is_not_null", "arguments": {"column": "id"}}, "criticality": "error"}],
                },
                headers={"X-Forwarded-Access-Token": "dummy"},
            )
            assert response.status_code == 200
            assert response.json() == {"inserted": 1}
            assert all(call.kwargs["warehouse_id"] == "wh-123" for call in mock_workspace_client.statement_execution.execute_statement.call_args_list)
        finally:
            app.dependency_overrides.clear()

    def test_append_generated_checks_uses_sql_warehouse_in_app_runtime(self, mock_workspace_client, monkeypatch):
        mock_workspace_client.tables.get.side_effect = [
            SimpleNamespace(full_name="main.analytics.checks"),
            SimpleNamespace(
                full_name="main.analytics.orders",
                columns=[SimpleNamespace(name="Order ID"), SimpleNamespace(name="amount")],
            ),
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = sql_service.StatementResponse(
            status=sql_service.StatementStatus(state=sql_service.StatementState.SUCCEEDED)
        )

        def _override_obo_ws():
            return mock_workspace_client

        def _override_app_ws():
            return mock_workspace_client

        app.dependency_overrides[get_obo_ws] = _override_obo_ws
        app.dependency_overrides[get_app_ws] = _override_app_ws
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.databricks.com")
        monkeypatch.setattr(
            backend_router,
            "_get_default_warehouse_id",
            lambda _app_ws, _obo_ws, explicit_warehouse_id=None: explicit_warehouse_id or "wh-123",
        )
        monkeypatch.setattr(
            backend_router,
            "get_spark",
            lambda _token: (_ for _ in ()).throw(AssertionError("get_spark should not be called")),
        )
        monkeypatch.setattr(
            backend_router,
            "_enrich_generated_checks_with_ai_via_sql_warehouse",
            lambda obo_ws, warehouse_id, checks, run_config_name: [
                {
                    **checks[0],
                    "name": "stock_code_four_digits",
                    "description": "Stock code must be exactly four digits.",
                }
            ],
        )

        try:
            client = TestClient(app)
            response = client.post(
                "/api/checks-table/append",
                json={
                    "table_name": "main.analytics.checks",
                    "run_config_name": "main.analytics.orders",
                    "mode": "upsert",
                    "checks": [
                        {
                            "name": "order_id_required",
                            "check": {"function": "is_not_null", "arguments": {"column": "\"order id\""}},
                            "criticality": "error",
                        }
                    ],
                },
                headers={"X-Forwarded-Access-Token": "dummy"},
            )
            assert response.status_code == 200
            assert response.json() == {"inserted": 1}
            statements = [
                call.kwargs["statement"] for call in mock_workspace_client.statement_execution.execute_statement.call_args_list
            ]
            assert all(call.kwargs["warehouse_id"] == "wh-123" for call in mock_workspace_client.statement_execution.execute_statement.call_args_list)
            insert_statement = next(statement for statement in statements if "INSERT INTO `main`.`analytics`.`checks`" in statement)
            assert '"Order ID"' in insert_statement
            assert "'stock_code_four_digits'" in insert_statement
            assert "'Stock code must be exactly four digits.'" in insert_statement
            assert "rule_fingerprint" not in insert_statement
            assert "rule_set_fingerprint" not in insert_statement
        finally:
            app.dependency_overrides.clear()

    def test_append_generated_checks_rejects_duplicate_name_for_run_config(self, mock_workspace_client, monkeypatch):
        mock_workspace_client.tables.get.side_effect = [
            SimpleNamespace(full_name="main.analytics.checks"),
            SimpleNamespace(full_name="main.analytics.orders", columns=[]),
        ]
        mock_workspace_client.statement_execution.execute_statement.side_effect = [
            sql_service.StatementResponse(
                status=sql_service.StatementStatus(state=sql_service.StatementState.SUCCEEDED),
                manifest=sql_service.ResultManifest(
                    schema=sql_service.ResultSchema(columns=[sql_service.ColumnInfo(name="name")])
                ),
                result=sql_service.ResultData(data_array=[["stock_code_four_digits"]]),
            )
        ]

        def _override_obo_ws():
            return mock_workspace_client

        def _override_app_ws():
            return mock_workspace_client

        app.dependency_overrides[get_obo_ws] = _override_obo_ws
        app.dependency_overrides[get_app_ws] = _override_app_ws
        monkeypatch.setattr(
            backend_router,
            "_get_default_warehouse_id",
            lambda _app_ws, _obo_ws, explicit_warehouse_id=None: explicit_warehouse_id or "wh-123",
        )
        monkeypatch.setattr(
            backend_router,
            "_enrich_generated_checks_with_ai_via_sql_warehouse",
            lambda obo_ws, warehouse_id, checks, run_config_name: [
                {
                    **checks[0],
                    "name": "stock_code_four_digits",
                    "description": "Stock code must be exactly four digits.",
                }
            ],
        )

        try:
            client = TestClient(app)
            response = client.post(
                "/api/checks-table/append",
                json={
                    "table_name": "main.analytics.checks",
                    "run_config_name": "main.analytics.orders",
                    "mode": "append",
                    "checks": [
                        {
                            "check": {"function": "is_not_null", "arguments": {"column": "stock_code"}},
                            "criticality": "error",
                        }
                    ],
                },
                headers={"X-Forwarded-Access-Token": "dummy"},
            )
            assert response.status_code == 400
            assert "Duplicate rule names already exist for run_config_name 'main.analytics.orders'" in response.json()["detail"]
            assert "stock_code_four_digits" in response.json()["detail"]
        finally:
            app.dependency_overrides.clear()


class TestCustomFormatter:
    """Unit tests for CustomFormatter."""

    def _create_log_record(self, module: str, func_name: str, msg: str = "Test") -> logging.LogRecord:
        """Helper to create a LogRecord with specific module and function name."""
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=f"{module}.py",
            lineno=1,
            msg=msg,
            args=(),
            exc_info=None,
        )
        record.module = module
        record.funcName = func_name
        return record

    def test_format_short_location(self):
        """Should include full location when it fits within max_length."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("router", "get_config")
        result = formatter.format(record)

        assert "router.get_config" in result

    def test_format_long_location_abbreviates_module(self):
        """Should abbreviate module parts when location is too long."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("databricks_labs_dqx_app.backend.router", "get_install_folder")
        result = formatter.format(record)

        # The location should be abbreviated to fit within 20 chars
        # Original would be "databricks_labs_dqx_app.backend.router.get_install_folder" (57 chars)
        # Abbreviated should be something like "d.l.d.b.r.get_instal" (20 chars)
        # Check that the full long name is NOT in the output
        assert "databricks_labs_dqx_app.backend.router.get_install_folder" not in result

    def test_format_module_level_code(self):
        """Should handle <module> function name by showing just module."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("router", "<module>")
        result = formatter.format(record)

        assert "router" in result
        assert "<module>" not in result

    def test_format_main_module(self):
        """Should handle __main__ module by showing just function name."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("__main__", "run")
        result = formatter.format(record)

        # __main__ should be stripped, leaving just the function name
        assert "__main__" not in result or "run" in result

    def test_format_includes_message(self):
        """Should include the log message in output."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("test", "test_func", msg="Hello World")
        result = formatter.format(record)

        assert "Hello World" in result

    def test_format_includes_level(self):
        """Should include the log level in output."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("test", "test_func")
        result = formatter.format(record)

        assert "INFO" in result

    def test_format_uses_pipe_separator(self):
        """Should use pipe-separated format."""
        formatter = CustomFormatter(use_colors=False)
        record = self._create_log_record("test", "test_func")
        result = formatter.format(record)

        assert "|" in result


class TestSetupLogger:
    """Unit tests for setup_logger function."""

    def test_creates_logger_with_name(self):
        """Should create logger with specified name."""
        log = setup_logger("test_logger", level=logging.DEBUG, use_colors=False)

        assert log.name == "test_logger"
        assert log.level == logging.DEBUG
        assert len(log.handlers) == 1

    def test_creates_logger_without_name(self):
        """Should create root logger when no name provided."""
        log = setup_logger(None, level=logging.WARNING, use_colors=False)

        assert log.name == "root"
        assert log.level == logging.WARNING

    def test_clears_existing_handlers(self):
        """Should clear existing handlers to avoid duplicates."""
        log = setup_logger("dup_test", level=logging.INFO, use_colors=False)
        initial_handlers = len(log.handlers)

        # Setup again - should still have same number of handlers
        log = setup_logger("dup_test", level=logging.INFO, use_colors=False)

        assert len(log.handlers) == initial_handlers


class TestSettingsManager:
    """Unit tests for SettingsManager class."""

    def test_init_sets_paths(self, mock_workspace_client):
        """Should initialize with correct paths based on user."""
        manager = SettingsManager(mock_workspace_client)

        assert manager.user_home == "/Users/test_user@example.com"
        assert manager.default_dqx_folder == "/Users/test_user@example.com/.dqx"
        assert manager.app_settings_path == "/Users/test_user@example.com/.dqx/app.yml"

    def test_get_default_install_folder(self, mock_workspace_client):
        """Should return the default .dqx folder path."""
        manager = SettingsManager(mock_workspace_client)
        result = manager.get_default_install_folder()

        assert result == "/Users/test_user@example.com/.dqx"

    def test_get_settings_returns_default_when_file_not_found(self, mock_workspace_client):
        """Should create default settings when app.yml doesn't exist."""
        mock_workspace_client.workspace.export.side_effect = ResourceDoesNotExist("Not found")

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/Users/test_user@example.com/.dqx"
        mock_workspace_client.workspace.upload.assert_called_once()

    def test_get_settings_returns_custom_when_file_exists(self, mock_workspace_client):
        """Should return custom settings when app.yml exists."""
        yaml_content = "install_folder: /custom/path"
        encoded = base64.b64encode(yaml_content.encode()).decode()

        mock_response = ExportResponse(content=encoded)
        mock_workspace_client.workspace.export.return_value = mock_response

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/custom/path"

    def test_get_settings_returns_all_saved_compute_fields(self, mock_workspace_client):
        yaml_content = """
install_folder: /custom/path
use_serverless: false
default_cluster_id: abc-123
default_warehouse_id: wh-456
default_catalog: main
default_schema: analytics
default_checks_table: main.analytics.checks
"""
        encoded = base64.b64encode(yaml_content.encode()).decode()

        mock_response = ExportResponse(content=encoded)
        mock_workspace_client.workspace.export.return_value = mock_response

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/custom/path"
        assert result.use_serverless is False
        assert result.default_cluster_id == "abc-123"
        assert result.default_warehouse_id == "wh-456"
        assert result.default_catalog == "main"
        assert result.default_schema == "analytics"
        assert result.default_checks_table == "main.analytics.checks"

    def test_get_settings_returns_default_when_file_has_default_path(self, mock_workspace_client):
        """Should return default settings when app.yml contains default path."""
        yaml_content = "install_folder: /Users/test_user@example.com/.dqx"
        encoded = base64.b64encode(yaml_content.encode()).decode()

        mock_response = ExportResponse(content=encoded)
        mock_workspace_client.workspace.export.return_value = mock_response

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/Users/test_user@example.com/.dqx"

    def test_get_settings_returns_default_when_yaml_malformed(self, mock_workspace_client):
        """Should return default settings when app.yml has malformed YAML."""
        yaml_content = "install_folder: [unclosed bracket"
        encoded = base64.b64encode(yaml_content.encode()).decode()

        mock_response = ExportResponse(content=encoded)
        mock_workspace_client.workspace.export.return_value = mock_response

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/Users/test_user@example.com/.dqx"

    def test_get_settings_returns_default_when_missing_install_folder_key(self, mock_workspace_client):
        """Should return default settings when app.yml is missing install_folder key."""
        yaml_content = "some_other_key: value"
        encoded = base64.b64encode(yaml_content.encode()).decode()

        mock_response = ExportResponse(content=encoded)
        mock_workspace_client.workspace.export.return_value = mock_response

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/Users/test_user@example.com/.dqx"

    def test_get_settings_returns_default_when_content_empty(self, mock_workspace_client):
        """Should return default settings when app.yml has empty content."""
        mock_response = ExportResponse(content=None)
        mock_workspace_client.workspace.export.return_value = mock_response

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/Users/test_user@example.com/.dqx"

    def test_get_settings_returns_default_on_generic_exception(self, mock_workspace_client):
        """Should return default settings when export throws unexpected error."""
        mock_workspace_client.workspace.export.side_effect = Exception("Unexpected error")

        manager = SettingsManager(mock_workspace_client)
        result = manager.get_settings()

        assert result.install_folder == "/Users/test_user@example.com/.dqx"

    def test_save_settings_creates_file_for_custom_path(self, mock_workspace_client):
        """Should create app.yml when saving custom path."""
        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="/custom/path")

        result = manager.save_settings(settings)

        assert mock_workspace_client.workspace.mkdirs.call_count == 2
        mock_workspace_client.workspace.upload.assert_called_once()
        assert result.install_folder == "/custom/path"

    def test_save_settings_persists_compute_fields(self, mock_workspace_client):
        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(
            install_folder="/custom/path",
            use_serverless=False,
            default_cluster_id="abc-123",
            default_warehouse_id="wh-456",
            default_catalog="main",
            default_schema="analytics",
            default_checks_table="main.analytics.checks",
        )

        result = manager.save_settings(settings)
        uploaded = mock_workspace_client.workspace.upload.call_args.args[1].decode()

        assert result.default_cluster_id == "abc-123"
        assert "default_cluster_id: abc-123" in uploaded
        assert "default_warehouse_id: wh-456" in uploaded
        assert "use_serverless: false" in uploaded

    def test_save_settings_creates_file_for_default_path(self, mock_workspace_client):
        """Should create app.yml even when saving default path."""
        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="/Users/test_user@example.com/.dqx")

        result = manager.save_settings(settings)

        assert mock_workspace_client.workspace.mkdirs.call_count == 1
        mock_workspace_client.workspace.upload.assert_called_once()
        assert result.install_folder == "/Users/test_user@example.com/.dqx"

    def test_save_settings_strips_whitespace(self, mock_workspace_client):
        """Should strip whitespace from install folder path."""
        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="  /custom/path  ")

        result = manager.save_settings(settings)

        assert result.install_folder == "/custom/path"
        # Verify mkdirs was called with stripped path
        assert mock_workspace_client.workspace.mkdirs.call_args_list[1][0][0] == "/custom/path"

    def test_save_settings_raises_error_when_dqx_folder_creation_fails(self, mock_workspace_client):
        """Should raise ValueError when the settings folder creation fails."""
        mock_workspace_client.workspace.mkdirs.side_effect = [Exception("Permission denied"), None]

        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="/custom/path")

        with pytest.raises(ValueError, match="Could not create settings folder"):
            manager.save_settings(settings)

    def test_save_settings_raises_error_when_install_folder_creation_fails(self, mock_workspace_client):
        """Should raise ValueError when install folder creation fails."""
        mock_workspace_client.workspace.mkdirs.side_effect = [None, Exception("Permission denied")]

        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="/custom/path")

        with pytest.raises(ValueError, match="Could not create install folder"):
            manager.save_settings(settings)

    def test_save_settings_raises_error_when_upload_fails(self, mock_workspace_client):
        """Should raise ValueError when upload fails."""
        mock_workspace_client.workspace.upload.side_effect = Exception("Upload failed")

        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="/custom/path")

        with pytest.raises(ValueError, match="Could not save app settings to"):
            manager.save_settings(settings)

    def test_save_settings_with_empty_install_folder(self, mock_workspace_client):
        """Should handle empty install folder path."""
        manager = SettingsManager(mock_workspace_client)
        settings = InstallationSettings(install_folder="")

        result = manager.save_settings(settings)

        # Empty string strips to empty string
        assert result.install_folder == ""


