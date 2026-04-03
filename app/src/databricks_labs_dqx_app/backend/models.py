from typing import Any

from databricks.labs.dqx.config import WorkspaceConfig
from pydantic import BaseModel, Field

from .. import __version__


class VersionOut(BaseModel):
    version: str

    @classmethod
    def from_metadata(cls):
        return cls(version=__version__)


class ConfigOut(BaseModel):
    config: WorkspaceConfig


class ConfigIn(BaseModel):
    config: WorkspaceConfig


class ChecksOut(BaseModel):
    checks: list[dict[str, Any]]


class ChecksIn(BaseModel):
    checks: list[dict[str, Any]]


class InstallationSettings(BaseModel):
    install_folder: str = Field(description="Path to the folder containing config.yml")


class GenerateChecksIn(BaseModel):
    user_input: str = Field(description="Natural language description of data quality requirements")
    table_name: str | None = Field(
        default=None,
        description="Optional fully qualified table name (catalog.schema.table) to provide schema/sample context",
    )


class GenerateChecksOut(BaseModel):
    yaml_output: str = Field(description="Generated checks in YAML format")
    checks: list[dict[str, Any]] = Field(description="Generated checks as a list of dictionaries")


class SaveGeneratedChecksIn(BaseModel):
    checks: list[dict[str, Any]] = Field(description="Generated checks to append to the checks table")
    table_name: str = Field(
        default="shao_sandbox1.dqx.checks",
        description="Fully qualified checks table name",
    )
    run_config_name: str | None = Field(
        default=None,
        description="Optional run config name to store with generated checks",
    )
    mode: str = Field(
        default="upsert",
        description="Write mode for table-backed checks storage: overwrite, append, or upsert",
    )


class SaveGeneratedChecksOut(BaseModel):
    inserted: int = Field(description="Number of checks inserted")


class CatalogsOut(BaseModel):
    catalogs: list[str]


class SchemasOut(BaseModel):
    schemas: list[str]


class TablesOut(BaseModel):
    tables: list[str]


class ColumnInfoOut(BaseModel):
    name: str
    type_text: str
    comment: str | None = None
    nullable: bool = True
    position: int | None = None


class TableInfoOut(BaseModel):
    full_name: str
    table_type: str | None = None
    data_source_format: str | None = None
    owner: str | None = None
    comment: str | None = None
    columns: list[ColumnInfoOut] = Field(default_factory=list)
    sample_data: list[dict[str, Any]] = Field(default_factory=list)
    sample_data_error: str | None = None


class ChecksTableOut(BaseModel):
    rows: list[dict[str, Any]] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)


class CheckErrorRowsOut(BaseModel):
    result_table_name: str = Field(description="Fully qualified DQX result table name inferred from run config")
    rows: list[dict[str, Any]] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)


class DashboardOut(BaseModel):
    dashboard_id: str = Field(description="Databricks AI/BI dashboard ID")
    embed_url: str = Field(description="Published dashboard URL for embedding")
    instance_url: str = Field(description="Databricks workspace instance URL")
    workspace_id: str = Field(description="Databricks workspace ID")
    token: str = Field(description="OBO token for embedding")


class RunChecksJobOut(BaseModel):
    job_id: int = Field(description="Created or updated Databricks job ID")
    job_url: str = Field(description="Databricks job URL")
    run_id: int = Field(description="Triggered run ID")
    run_url: str = Field(description="Databricks run URL")
