"""Motif definitions for common ADF pipeline patterns."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class MotifDefinition:
    """Immutable definition of a recognised ADF motif pattern.

    Attributes:
        motif_id: Unique short identifier (e.g. ``"incremental_load_watermark"``).
        display_name: Human-readable name shown in reports.
        description: Paragraph explaining the ADF pattern and its Databricks equivalent.
        expected_activity_types: ADF activity types that participate in this motif.
        databricks_replacement: Short label for the target Databricks construct
            (e.g. ``"auto_loader"``, ``"dlt_pipeline"``, ``"structured_streaming"``).
        notebook_template: Name of the Jinja2 or code-generator template used to
            produce the replacement notebook.  ``None`` when no template exists yet.
    """

    motif_id: str
    display_name: str
    description: str
    expected_activity_types: tuple[str, ...]
    databricks_replacement: str
    notebook_template: str | None = None


@dataclass(slots=True, kw_only=True)
class DetectedMotif:
    """Result of matching a motif in a specific pipeline.

    Attributes:
        definition: The motif definition that was matched.
        matched_activities: Activity names claimed by this match.
        source_type_hint: Inferred source category (``"files"``, ``"database"``,
            ``"rest_api"``) or ``None``.
        confidence_notes: Human-readable notes explaining *why* the detector
            concluded this motif matches (useful for review).
    """

    definition: MotifDefinition
    matched_activities: list[str]
    source_type_hint: str | None = None
    confidence_notes: list[str] = field(default_factory=list)


MOTIF_INCREMENTAL_LOAD_WATERMARK = MotifDefinition(
    motif_id="incremental_load_watermark",
    display_name="Incremental Load (Watermark)",
    description=(
        "Two Lookup activities fetch the old and new watermark values, a Copy "
        "activity loads the delta between them, and a StoredProcedure updates "
        "the watermark table.  Translates to Auto Loader with checkpoint-based "
        "incremental ingestion or a Spark Structured Streaming job."
    ),
    expected_activity_types=("Lookup", "Copy", "SqlServerStoredProcedure"),
    databricks_replacement="auto_loader",
    notebook_template="incremental_watermark.py",
)

MOTIF_CDC_CHANGE_TRACKING = MotifDefinition(
    motif_id="cdc_change_tracking",
    display_name="CDC (SQL Server Change Tracking)",
    description=(
        "Similar to watermark but relies on SQL Server Change Tracking "
        "(CHANGETABLE / SYS_CHANGE_VERSION).  Translates to a Spark "
        "Structured Streaming job reading the change feed or a DLT "
        "pipeline with APPLY CHANGES."
    ),
    expected_activity_types=("Lookup", "Copy", "SqlServerStoredProcedure"),
    databricks_replacement="dlt_apply_changes",
    notebook_template="cdc_change_tracking.py",
)

MOTIF_METADATA_DRIVEN_BULK_COPY = MotifDefinition(
    motif_id="metadata_driven_bulk_copy",
    display_name="Metadata-Driven Bulk Copy",
    description=(
        "A Lookup reads a control/metadata table listing source tables, then "
        "a ForEach iterates over the list and copies each table.  Translates "
        "to a parameterised Databricks job with a for_each_task or a DLT "
        "pipeline ingesting multiple sources."
    ),
    expected_activity_types=("Lookup", "ForEach", "Copy"),
    databricks_replacement="for_each_ingestion",
    notebook_template="metadata_bulk_copy.py",
)

MOTIF_FILE_LANDING_ZONE_PROCESSING = MotifDefinition(
    motif_id="file_landing_zone_processing",
    display_name="File Landing Zone Processing",
    description=(
        "GetMetadata lists files in a landing zone, an optional Filter narrows "
        "the list, ForEach iterates and copies each file, and Delete cleans up "
        "processed files.  Translates to Auto Loader with file notification "
        "triggers."
    ),
    expected_activity_types=("GetMetadata", "Filter", "ForEach", "Copy", "Delete"),
    databricks_replacement="auto_loader_file_notification",
    notebook_template="file_landing_zone.py",
)

MOTIF_REST_API_PAGINATION = MotifDefinition(
    motif_id="rest_api_pagination",
    display_name="REST API Pagination",
    description=(
        "A WebActivity fetches an authentication token, a SetVariable "
        "initialises a pagination cursor, and an Until loop fetches pages "
        "via Copy or WebActivity until exhausted.  Translates to a Python "
        "notebook with requests-based pagination."
    ),
    expected_activity_types=("WebActivity", "SetVariable", "Until", "Copy"),
    databricks_replacement="python_rest_ingestion",
    notebook_template="rest_api_pagination.py",
)

MOTIF_PARENT_CHILD_ORCHESTRATION = MotifDefinition(
    motif_id="parent_child_orchestration",
    display_name="Parent-Child Orchestration",
    description=(
        "A Lookup provides a list of work items and a ForEach iterates, "
        "calling ExecutePipeline for each.  Translates to a Databricks "
        "for_each_task with run_job_task calling the child job."
    ),
    expected_activity_types=("Lookup", "ForEach", "ExecutePipeline"),
    databricks_replacement="for_each_run_job",
    notebook_template=None,
)

MOTIF_FILE_EXISTENCE_VALIDATION = MotifDefinition(
    motif_id="file_existence_validation",
    display_name="File Existence Validation",
    description=(
        "GetMetadata checks whether a file or folder exists, and an "
        "IfCondition gates downstream logic on the result.  Translates "
        "to a condition_task checking a file-existence notebook."
    ),
    expected_activity_types=("GetMetadata", "IfCondition"),
    databricks_replacement="condition_task",
    notebook_template="file_existence_check.py",
)

MOTIF_SCD_TYPE_2 = MotifDefinition(
    motif_id="scd_type_2",
    display_name="SCD Type 2",
    description=(
        "A Copy loads data into a staging table and an ExecuteDataFlow "
        "applies SCD Type 2 merge logic (Lookup + AlterRow + Union).  "
        "Translates to a DLT pipeline with APPLY CHANGES INTO."
    ),
    expected_activity_types=("Copy", "ExecuteDataFlow"),
    databricks_replacement="dlt_apply_changes",
    notebook_template="scd_type_2.py",
)

MOTIF_STAGED_LOAD_SYNAPSE = MotifDefinition(
    motif_id="staged_load_synapse",
    display_name="Staged Load (Synapse)",
    description=(
        "A Copy activity loads data via PolyBase/COPY command staging, "
        "followed by a StoredProcedure for post-load transforms.  "
        "Translates to a direct Spark write to Delta with post-processing."
    ),
    expected_activity_types=("Copy", "SqlServerStoredProcedure"),
    databricks_replacement="spark_delta_write",
    notebook_template="staged_load.py",
)

MOTIF_COPY_AND_NOTIFY = MotifDefinition(
    motif_id="copy_and_notify",
    display_name="Copy and Notify",
    description=(
        "A Copy activity followed by WebActivity calls for success/failure "
        "notifications (Logic Apps, Slack, email).  Translates to a notebook "
        "task with built-in notification via job email/webhook settings."
    ),
    expected_activity_types=("Copy", "WebActivity"),
    databricks_replacement="notebook_with_notification",
    notebook_template="copy_and_notify.py",
)

ALL_MOTIFS: tuple[MotifDefinition, ...] = (
    MOTIF_INCREMENTAL_LOAD_WATERMARK,
    MOTIF_CDC_CHANGE_TRACKING,
    MOTIF_METADATA_DRIVEN_BULK_COPY,
    MOTIF_FILE_LANDING_ZONE_PROCESSING,
    MOTIF_REST_API_PAGINATION,
    MOTIF_PARENT_CHILD_ORCHESTRATION,
    MOTIF_FILE_EXISTENCE_VALIDATION,
    MOTIF_SCD_TYPE_2,
    MOTIF_STAGED_LOAD_SYNAPSE,
    MOTIF_COPY_AND_NOTIFY,
)
