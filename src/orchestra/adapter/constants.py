"""String constants shared across the adapter and its bundler consumers.

Every adapter-side string the modifier stamps onto an IR field or that
the bundler reads back from one is defined here.  Modules in
``orchestra.adapter``, ``orchestra.bundler``, and the test suite import
from this module to avoid string-literal drift between the producer and
consumer ends of the same value.
"""

from __future__ import annotations

from typing import Final

QUESTION_COPY_ACTIVITY_PARADIGM: Final[str] = "copy_activity_paradigm"
QUESTION_NON_DATABRICKS_TASK_COMPUTE: Final[str] = "non_databricks_task_compute"
QUESTION_USE_LAKEFLOW_CONNECTORS: Final[str] = "use_lakeflow_connectors"
QUESTION_DATABRICKS_TASK_COMPUTE: Final[str] = "databricks_task_compute"
QUESTION_LAKEFLOW_CONNECTOR_TYPE: Final[str] = "lakeflow_connector_type"
QUESTION_METADATA_DRIVEN_CONSOLIDATE: Final[str] = "metadata_driven_consolidate"
QUESTION_METADATA_DRIVEN_ACCESS: Final[str] = "metadata_driven_access"
QUESTION_METADATA_DRIVEN_SIZE: Final[str] = "metadata_driven_size"
QUESTION_METADATA_DRIVEN_LOOKUP_TOOL: Final[str] = "metadata_driven_lookup_tool"

METADATA_DRIVEN_MOTIF_ID: Final[str] = "metadata_driven_bulk_copy"

PHASE_INGEST: Final[str] = "ingest"
PHASE_TRANSLATE: Final[str] = "translate"
PHASE_PREPARE: Final[str] = "prepare"

INPUT_ADF_SOURCE_PATH: Final[str] = "adf_source_path"
INPUT_ADF_RESOURCE_URL: Final[str] = "adf_resource_url"
INPUT_OUTPUT_DIR: Final[str] = "output_dir"
INPUT_INVENTORY_PATH: Final[str] = "inventory_path"
INPUT_TRANSLATION_REPORT_PATH: Final[str] = "translation_report_path"
INPUT_OUTPUT_BUNDLE_PATH: Final[str] = "output_bundle_path"
INPUT_CATALOG: Final[str] = "catalog"
INPUT_SCHEMA: Final[str] = "schema"
INPUT_BUNDLE_NAME: Final[str] = "bundle_name"
INPUT_DATABRICKS_PROFILE: Final[str] = "databricks_profile"

LAKEFLOW_CONNECTOR_TYPE_QUERY_BASED: Final[str] = "query_based"
LAKEFLOW_CONNECTOR_TYPE_CDC: Final[str] = "cdc"

COPY_SOURCE_QUERY_KEYS: Final[tuple[str, ...]] = ("query", "sqlReaderQuery", "sql_query")

COMPUTE_MODE_SERVERLESS: Final[str] = "serverless"
COMPUTE_MODE_CLASSIC_SINGLE_NODE: Final[str] = "classic_single_node"
COMPUTE_MODE_CLASSIC_MULTI_NODE: Final[str] = "classic_multi_node"
COMPUTE_MODE_INHERIT: Final[str] = "inherit"

LAKEFLOW_CONNECT_REPLACEMENT: Final[str] = "lakeflow_connect_database"

DATABASE_SOURCE_TOKENS: Final[tuple[str, ...]] = (
    "sqlserver",
    "azuresql",
    "mysql",
    "azuremysql",
    "postgre",
    "azurepostgre",
)

DELTA_SINK_TOKENS: Final[tuple[str, ...]] = ("delta", "deltalake")

LAKEFLOW_CONNECT_MOTIF_REPLACEMENTS: Final[frozenset[str]] = frozenset(
    {
        "auto_loader",
        "auto_loader_file_notification",
        "dlt_apply_changes",
        "for_each_ingestion",
        "spark_delta_write",
    }
)

DATABASE_SOURCE_TYPE_HINT: Final[str] = "database"
