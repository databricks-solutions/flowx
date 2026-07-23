"""String constants shared across the adapter and its bundler consumers.

Defining each stamped/read-back value here keeps the producer (the modifier) and consumer (the
bundler) ends from drifting on string literals.
"""

from __future__ import annotations

from typing import Final

OPTION_COPY_ACTIVITY_PARADIGM: Final[str] = "copy_activity_paradigm"
OPTION_NON_DATABRICKS_TASK_COMPUTE: Final[str] = "non_databricks_task_compute"
OPTION_USE_LAKEFLOW_CONNECTORS: Final[str] = "use_lakeflow_connectors"
OPTION_LAKEFLOW_CONNECTOR_TYPE: Final[str] = "lakeflow_connector_type"
OPTION_METADATA_DRIVEN_CONSOLIDATE: Final[str] = "metadata_driven_consolidate"
OPTION_METADATA_DRIVEN_ACCESS: Final[str] = "metadata_driven_access"
OPTION_METADATA_DRIVEN_SIZE: Final[str] = "metadata_driven_size"
OPTION_METADATA_DRIVEN_LOOKUP_TOOL: Final[str] = "metadata_driven_lookup_tool"

# Per-motif consolidation option ids suffix the motif_id (e.g. consolidate_motif:rest_api_pagination)
# so each motif gets its own option; validation strips this prefix and checks against MotifConsolidate.
MOTIF_CONSOLIDATE_OPTION_PREFIX: Final[str] = "consolidate_motif:"

METADATA_DRIVEN_MOTIF_ID: Final[str] = "metadata_driven_bulk_copy"

PHASE_DISCOVER: Final[str] = "discover"
PHASE_CONVERT: Final[str] = "convert"
PHASE_PACKAGE: Final[str] = "package"

INPUT_ADF_SOURCE_PATH: Final[str] = "adf_source_path"
INPUT_ADF_RESOURCE_URL: Final[str] = "adf_resource_url"
INPUT_OUTPUT_DIR: Final[str] = "output_dir"
INPUT_INVENTORY_PATH: Final[str] = "inventory_path"
INPUT_GLOBAL_PARAMETER_RESOLUTION: Final[str] = "global_parameter_resolution"
INPUT_TRANSLATION_REPORT_PATH: Final[str] = "translation_report_path"
INPUT_OUTPUT_BUNDLE_PATH: Final[str] = "output_bundle_path"
INPUT_CATALOG: Final[str] = "catalog"
INPUT_SCHEMA: Final[str] = "schema"
INPUT_BUNDLE_NAME: Final[str] = "bundle_name"
INPUT_DATABRICKS_PROFILE: Final[str] = "databricks_profile"
INPUT_RESULTS_TABLE: Final[str] = "results_table"
INPUT_RESULTS_WAREHOUSE: Final[str] = "results_warehouse_id"
INPUT_INSTALL_DASHBOARD: Final[str] = "install_dashboard"

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

OPTION_NOTIFY_DESTINATION: Final[str] = "notify_destination"
OPTION_NOTIFY_EVENTS: Final[str] = "notify_events"
OPTION_NOTIFY_DESTINATION_NAME: Final[str] = "notify_destination_name"
# Per-field notification follow-up option ids (e.g. notify_email_recipients,
# notify_slack_url) are defined by the _NOTIFY_FIELDS registry in operations.py.
