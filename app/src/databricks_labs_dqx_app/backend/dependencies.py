import os
from collections.abc import Callable
from contextlib import contextmanager
from typing import Annotated

from databricks.connect import DatabricksSession
from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from fastapi import Depends, Header, HTTPException, status
from pyspark.sql import SparkSession

from .logger import logger
from .settings import SettingsManager


def _is_session_changed_error(error: Exception) -> bool:
    message = str(error)
    return "INVALID_HANDLE.SESSION_CHANGED" in message or (
        "Spark server driver instance has restarted" in message and "invalid" in message.lower()
    )


def _ensure_live_spark_session(
    spark: SparkSession, session_label: str, create_session: Callable[[], SparkSession]
) -> SparkSession:
    try:
        spark.sql("SELECT 1").collect()
        return spark
    except Exception as e:
        if not _is_session_changed_error(e):
            raise

        logger.warning(f"Detected stale {session_label} Spark session, reconnecting", exc_info=True)
        try:
            spark.stop()
        except Exception:
            logger.warning(f"Failed to stop stale {session_label} Spark session before reconnect", exc_info=True)

        refreshed_spark = create_session()
        refreshed_spark.sql("SELECT 1").collect()
        return refreshed_spark


def _get_default_compute_settings() -> tuple[bool, str | None]:
    try:
        settings = SettingsManager(get_app_ws()).get_settings()
        return settings.use_serverless, settings.default_cluster_id
    except Exception:
        logger.warning("Failed to load app compute settings, falling back to serverless", exc_info=True)
        return True, None


def get_app_ws() -> WorkspaceClient:
    """
    Create a WorkspaceClient using the app's own service principal credentials.

    Unlike get_obo_ws which uses the user's token (and is limited to user_api_scopes),
    the app service principal has direct workspace access for operations like reading/writing
    config files that require the Workspace API.
    """
    return WorkspaceClient()


@contextmanager
def _without_oauth_env_vars():
    """
    Temporarily remove OAuth environment variables to avoid conflicts with obo token auth.

    Restores them after the context exits so other services can use them if needed.
    """
    oauth_vars = ["DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"]
    saved_values = {}

    # Save and temporarily remove OAuth env vars
    for var in oauth_vars:
        if var in os.environ:
            saved_values[var] = os.environ[var]
            del os.environ[var]
            logger.debug(f"Temporarily removed {var} for OBO Spark authentication")

    try:
        yield
    finally:
        # Restore OAuth env vars for other services (DSPy/litellm)
        for var, value in saved_values.items():
            os.environ[var] = value
            logger.debug(f"Restored {var} for LLM authentication")


def get_obo_ws(
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> WorkspaceClient:
    """
    Create a Databricks WorkspaceClient using On-Behalf-Of (OBO) authentication.

    When a Databricks App runs on the platform, the X-Forwarded-Access-Token header
    is automatically injected with the logged-in user's access token. This function
    extracts that token and creates a WorkspaceClient that performs all operations
    with the user's identity and permissions.

    This dependency allows FastAPI routes to:
    - Access workspace resources (notebooks, clusters, jobs, etc.) as the user
    - Respect user permissions (users only see what they have access to)
    - Generate proper audit logs attributing actions to the correct user

    Args:
        token: User's access token from the X-Forwarded-Access-Token header.
               Automatically provided by Databricks when the app runs on the platform.

    Returns:
        WorkspaceClient: Configured with the user's token for OBO operations.

    Raises:
        HTTPException: 401 Unauthorized if the X-Forwarded-Access-Token header is not present.

    Example usage:
        @router.get("/current-user")
        def get_current_user(ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
            user = ws.current_user.me()
            return {"user_name": user.user_name, "email": user.emails[0].value}
    """
    if not token:
        logger.warning("OBO token is not provided in the header X-Forwarded-Access-Token for Spark session")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Please refresh the page or contact your administrator.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return WorkspaceClient(token=token, auth_type="pat")  # set pat explicitly to avoid issues with SP client


def get_spark(
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> SparkSession:
    """
    Create a Databricks Spark Connect session with OBO authentication on serverless compute.

    This follows the Databricks Apps pattern for using OBO tokens with serverless compute.
    Works in both production (Databricks Apps) and local development environments.

    Args:
        token: User's access token from the X-Forwarded-Access-Token header.
               Automatically provided by Databricks when the app runs on the platform.

    Returns:
        SparkSession: A Databricks Spark Connect session configured with OBO token.

    Raises:
        HTTPException: 401 Unauthorized if the X-Forwarded-Access-Token header is not present.
    """
    if not token:
        logger.warning("OBO token is not provided in the header X-Forwarded-Access-Token for Spark session")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Please refresh the page or contact your administrator.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Get Databricks host from environment
    host = os.environ.get("DATABRICKS_HOST")
    use_serverless, default_cluster_id = _get_default_compute_settings()

    def create_session() -> SparkSession:
        if not host:
            logger.info("DATABRICKS_HOST not set, using default configuration for local development")
            builder = DatabricksSession.builder.token(token)
            if not use_serverless and default_cluster_id:
                logger.info(f"Creating local Spark session with classic cluster {default_cluster_id}")
                return builder.clusterId(default_cluster_id).getOrCreate()
            return builder.getOrCreate()

        # Temporarily remove OAuth env vars to avoid multi-auth conflicts
        with _without_oauth_env_vars():
            builder = DatabricksSession.builder.host(host).token(token)  # Use the forwarded OBO access token
            if not use_serverless and default_cluster_id:
                logger.info(f"Creating Spark session with OBO token on classic cluster {default_cluster_id} for host: {host}")
                return builder.clusterId(default_cluster_id).getOrCreate()

            logger.info(f"Creating Spark session with OBO token on serverless compute for host: {host}")
            return builder.serverless().getOrCreate()

    return _ensure_live_spark_session(create_session(), "OBO", create_session)


def get_app_spark() -> SparkSession:
    """
    Create a Spark Connect session using the app's service principal credentials.

    Unlike get_spark (which uses the user's OBO token and requires the 'all-apis' scope
    that can only be granted at the account level), this uses the app's service principal
    which has direct workspace access. Use this for operations where user-level
    permissions are not strictly required (e.g., reading app-managed tables).
    """
    host = os.environ.get("DATABRICKS_HOST")
    use_serverless, default_cluster_id = _get_default_compute_settings()

    def create_session() -> SparkSession:
        if not host:
            logger.info("DATABRICKS_HOST not set, using default configuration for local development")
            builder = DatabricksSession.builder
            if not use_serverless and default_cluster_id:
                logger.info(f"Creating local app Spark session with classic cluster {default_cluster_id}")
                return builder.clusterId(default_cluster_id).getOrCreate()
            return builder.getOrCreate()

        client_id = os.environ.get("DATABRICKS_CLIENT_ID")
        client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="App service principal credentials are not configured.",
            )

        builder = DatabricksSession.builder
        if not use_serverless and default_cluster_id:
            logger.info(f"Creating app Spark session on classic cluster {default_cluster_id} for host: {host}")
            return builder.clusterId(default_cluster_id).getOrCreate()

        logger.info(f"Creating Spark session with app service principal on serverless compute for host: {host}")
        # In Databricks Apps runtime, host/client credentials are already provided via env vars.
        # Let Spark Connect read them from the environment to avoid sdkConfig conflicts.
        return builder.serverless().getOrCreate()

    return _ensure_live_spark_session(create_session(), "app", create_session)


def get_engine(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)], spark: Annotated[SparkSession, Depends(get_spark)]
) -> DQEngine:
    """
    Create a DQEngine instance with OBO authentication and Spark session.

    This dependency combines:
    - WorkspaceClient with user's identity (via get_obo_ws)
    - SparkSession for Spark operations (via get_spark)

    The DQEngine can then execute data quality checks and operations
    on behalf of the logged-in user.

    Args:
        obo_ws: WorkspaceClient with OBO authentication (injected by FastAPI).
        spark: SparkSession for data operations (injected by FastAPI).

    Returns:
        DQEngine: Configured for data quality operations with user context.

    Example usage:
        @router.post("/run-quality-check")
        def run_check(engine: Annotated[DQEngine, Depends(get_engine)]):
            result = engine.run_checks(...)
            return {"status": "success", "results": result}
    """
    return DQEngine(workspace_client=obo_ws, spark=spark)


def _get_llm_model_config() -> LLMModelConfig:
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    if not host:
        logger.info("DATABRICKS_HOST not set, using default configuration for LLM")

    serving_endpoint_name = os.environ.get("SERVING_ENDPOINT_NAME", "").strip()
    if serving_endpoint_name:
        return LLMModelConfig(
            model_name=f"databricks/{serving_endpoint_name}",
            api_base=f"{host}/serving-endpoints" if host else "",
        )

    return LLMModelConfig()


def get_generator(
    app_ws: Annotated[WorkspaceClient, Depends(get_app_ws)],
    spark: Annotated[SparkSession, Depends(get_app_spark)],
) -> DQGenerator:
    """
    Create a DQGenerator instance with app authorization and Spark session.

    This dependency provides an AI-assisted data quality rules generator that
    can create checks from natural language descriptions on behalf of the
    logged-in user. The Spark session is used for data profiling and analysis.

    In Databricks Apps runtime, LLM calls use app credentials from the runtime
    environment to avoid user-token scope limitations.

    Args:
        app_ws: WorkspaceClient with app authorization (injected by FastAPI).
        spark: SparkSession for data operations (injected by FastAPI).
    Returns:
        DQGenerator: Configured for AI-assisted rules generation with user context.

    Example usage:
        @router.post("/generate-checks")
        def generate_checks(generator: Annotated[DQGenerator, Depends(get_generator)], user_input: str):
            checks = generator.generate_dq_rules_ai_assisted(user_input=user_input)
            return {"checks": checks}
    """
    return DQGenerator(workspace_client=app_ws, spark=spark, llm_model_config=_get_llm_model_config())
