#!/usr/bin/env python3
"""
Azure Data Factory / Synapse / Fabric Pipeline Extractor (Detailed Analysis)
=============================================================================

Extracts and analyzes all Azure Data Factories, Synapse Analytics pipelines,
and Microsoft Fabric Data Factory pipelines with DETAILED pipeline-level analysis.

FEATURES:
- Full source/sink type discovery (traces datasets → linked services → actual types)
- Activity categorization (DataMovement, Transformation, ControlFlow, etc.)
- Integration Runtime tracking per activity
- WebActivity API endpoint parsing
- Trigger-to-pipeline mapping
- Dataflow Gen1 vs Gen2 distinction
- REST API usage breakdown

PREREQUISITES
-------------
1. Install Azure CLI:
   - macOS:   brew install azure-cli
   - Windows: winget install Microsoft.AzureCLI
   - Linux:   curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
   
2. Install Python dependencies:
   pip install -r requirements.txt

3. Authenticate:
   az login

REQUIRED PERMISSIONS
--------------------
Minimum permissions needed per feature area:

Azure Data Factory (static extraction):
  - Reader on the subscription(s), OR
  - Data Factory Contributor on specific factories

Azure Data Factory (runtime profiling — pipeline runs, activity runs, SHIR):
  - All of the above, PLUS
  - Data Factory Contributor (for pipeline run queries and SHIR monitoring APIs)

Synapse Analytics (static extraction):
  - Reader on subscription(s) (to discover workspaces via ARM)
  - Synapse Artifact User OR Synapse Artifact Publisher on each workspace
    (to read pipelines, linked services, datasets, dataflows, triggers, IRs
     via the Synapse dev endpoint — https://dev.azuresynapse.net)
  NOTE: Synapse Administrator or Synapse Contributor also work but grant
  broader permissions than needed for read-only extraction.

Microsoft Fabric:
  - Fabric workspace Member, Contributor, or Admin role

VM Specs for SHIR nodes (optional — graceful fallback if denied):
  - Reader on the subscription(s)
    (to list VMs via Microsoft.Compute/virtualMachines and resolve VM sizes)

Azure Cost Management (optional — graceful fallback if denied):
  - Cost Management Reader OR Billing Reader on the subscription(s)
    (to query actual billed costs via Microsoft.CostManagement/query)

OUTPUT FILES
------------
Each run creates a timestamped folder under ./output/:

./output/YYYY-MM-DD_HHMMSS/
├── overall.csv + .md
├── ingestion/
│   ├── ingestion.csv + .md
│   └── details/
│       ├── connectors_by_activity.csv
│       ├── connectors_by_pipeline.csv
│       ├── connectors_by_factory.csv
│       ├── connectors_by_subscription.csv
│       ├── connectors_by_tenant.csv
│       ├── ingestion_technique.csv
│       ├── runtime_by_activity.csv
│       ├── runtime_by_pipeline.csv
│       ├── runtime_by_factory.csv
│       ├── runtime_by_subscription.csv
│       ├── runtime_by_tenant.csv
│       ├── copy_activity_details.csv
│       ├── database_instances.csv
│       ├── table_discovery.csv
│       ├── shir_nodes.csv
│       ├── integration_runtimes.csv
│       └── ingestion_cost_breakdown.csv
├── orchestration/
│   ├── orchestration.csv + .md
│   └── details/
│       ├── activity_types_by_activity.csv
│       ├── activity_types_by_pipeline.csv
│       ├── activity_types_by_factory.csv
│       ├── activity_types_by_subscription.csv
│       ├── activity_types_by_tenant.csv
│       ├── pipeline_runs_detail.csv
│       ├── pipeline_runs_by_pipeline.csv
│       ├── pipeline_runs_by_factory.csv
│       ├── pipeline_runs_by_subscription.csv
│       ├── pipeline_runs_by_tenant.csv
│       ├── dataflows.csv
│       ├── datasets.csv
│       ├── linked_services.csv
│       ├── triggers.csv
│       ├── metadata_driven_analysis.csv
│       ├── databricks_analysis.csv
│       ├── orchestration_cost_breakdown.csv
│       └── managed_airflow.csv
└── extraction_YYYYMMDD_HHMMSS.log
"""

import argparse
import base64
import json
import logging
import math
import os
import queue
import re
import sys
import time
import warnings
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import wraps
from threading import Lock
from typing import Dict, List, Optional, Tuple, Any, Set
from urllib.parse import urlparse

import pandas as pd
import requests
import aiohttp
import asyncio
import ssl
import certifi
from tqdm import tqdm

# Suppress Azure SDK noise BEFORE importing Azure modules
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.ERROR)
logging.getLogger("azure.identity").setLevel(logging.ERROR)
logging.getLogger("azure.mgmt").setLevel(logging.ERROR)
logging.getLogger("azure").setLevel(logging.ERROR)
logging.getLogger("msal").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

warnings.filterwarnings("ignore", message=".*Unmapped.*")
warnings.filterwarnings("ignore", message=".*Unable to deserialize.*")

from azure.core.exceptions import AzureError, HttpResponseError
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.resource import SubscriptionClient


# =============================================================================
# CONSTANTS
# =============================================================================

AZURE_MGMT_URL = "https://management.azure.com"
FABRIC_API_URL = "https://api.fabric.microsoft.com/v1"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 2
RETRY_DELAY = 1
MAX_WORKERS = min(64, (os.cpu_count() or 4) * 8)
DEFAULT_PROFILING_DAYS = 90

# Activity category mappings
ACTIVITY_CATEGORIES = {
    "DataMovement": ["Copy", "CopyActivity"],
    "Transformation": [
        "ExecuteDataFlow", "DataFlow", "DatabricksNotebook", "DatabricksSparkPython",
        "DatabricksSparkJar", "DatabricksJob", "HDInsightHive", "HDInsightPig",
        "HDInsightMapReduce", "HDInsightStreaming", "HDInsightSpark",
        "SqlServerStoredProcedure", "Script", "SynapseNotebook", "SparkJob",
        "SynapseSpark", "AzureMLBatchExecution", "AzureMLUpdateResource",
        "AzureMLExecutePipeline", "ExecuteSSISPackage", "Custom"
    ],
    "ControlFlow": [
        "ExecutePipeline", "ForEach", "IfCondition", "If", "Switch", "Until",
        "Wait", "Validation", "SetVariable", "AppendVariable", "Filter", "Fail"
    ],
    "DataAccess": [
        "Lookup", "GetMetadata", "Delete", "WebActivity", "WebHook",
        "AzureFunctionActivity", "AzureFunction"
    ],
    "Execution": [
        "AzureBatch", "AzureDataExplorerCommand", "AzureDataExplorer"
    ]
}

# Build reverse mapping for quick lookup
ACTIVITY_TYPE_TO_CATEGORY = {}
for category, types in ACTIVITY_CATEGORIES.items():
    for act_type in types:
        ACTIVITY_TYPE_TO_CATEGORY[act_type] = category

# =============================================================================
# PRICING & COST CONSTANTS
# =============================================================================

PRICING = {
    "azure_ir": {
        "orchestration_per_1000": 1.00,
        "data_movement_per_diu_hour": 0.25,
        "pipeline_activity_per_hour": 0.005,
        "external_activity_per_hour": 0.00025,
    },
    "shir": {
        "orchestration_per_1000": 1.50,
        "data_movement_per_hour": 0.10,
        "pipeline_activity_per_hour": 0.005,
        "external_activity_per_hour": 0.00025,
    },
    "managed_vnet_ir": {
        "orchestration_per_1000": 1.00,
        "data_movement_per_diu_hour": 0.25,
        "pipeline_activity_per_hour": 1.00,
        "external_activity_per_hour": 1.00,
    }
}

COPY_ACTIVITY_TYPES = ["Copy"]
PIPELINE_ACTIVITY_TYPES = [
    "Lookup", "GetMetadata", "Delete", "Validation",
    "Filter", "SetVariable", "AppendVariable",
    "IfCondition", "ForEach", "Until", "Wait", "Switch"
]
EXTERNAL_ACTIVITY_TYPES = [
    "SqlServerStoredProcedure", "DatabricksNotebook",
    "DatabricksSparkJar", "DatabricksSparkPython",
    "HDInsightHive", "HDInsightPig", "HDInsightSpark",
    "HDInsightMapReduce", "AzureMLBatchExecution",
    "AzureMLUpdateResource", "Custom",
    "AzureFunctionActivity", "WebActivity"
]
DATA_FLOW_TYPES = ["ExecuteDataFlow"]
DATAFLOW_VCORE_RATE = 0.274   # $/vCore-hour (General Purpose)
DATAFLOW_MIN_VCORES = 8

# =============================================================================
# CONNECTOR FRIENDLY NAMES (Change 3)
# =============================================================================

# The unified display name for all file-based storage connectors
_FILES_COMBINED_LABEL = "Files (ADLS Gen2, Blob Storage, File Storage, etc.)"

# Mapping from internal Azure connector type names to human-readable names.
# Used in all MD and CSV outputs for connector columns.
CONNECTOR_FRIENDLY_NAMES = {
    "AzureSqlDatabase": "Azure SQL Database",
    "AzureSqlDW": "Azure Synapse Analytics (SQL Pool)",
    "AzureSqlMI": "Azure SQL Managed Instance",
    "AzureBlobFS": _FILES_COMBINED_LABEL,
    "AzureBlobStorage": _FILES_COMBINED_LABEL,
    "AzureDataLakeStore": "Azure Data Lake Storage Gen1",
    "SqlServer": "SQL Server (on-premises)",
    "CosmosDb": "Azure Cosmos DB",
    "CosmosDbMongoDbApi": "Azure Cosmos DB (MongoDB API)",
    "AzureDatabricksDeltaLakeDataset": "Databricks Delta Lake",
    "AzureDatabricksDeltaLake": "Databricks Delta Lake",
    "Databricks": "Databricks Delta Lake",
    "HttpServer": "HTTP",
    "RestService": "REST API",
    "Sftp": "SFTP",
    "AzureFileStorage": _FILES_COMBINED_LABEL,
    "Salesforce": "Salesforce",
    "Snowflake": "Snowflake",
    "Odbc": "ODBC",
    "DataWarehouse": "Microsoft Fabric Data Warehouse",
    "FabricWarehouse": "Microsoft Fabric Data Warehouse",
    "LakehouseTable": "Microsoft Fabric Lakehouse",
    "FabricLakehouse": "Microsoft Fabric Lakehouse",
    "Oracle": "Oracle Database",
    "MySql": "MySQL",
    "AzureMySql": "Azure Database for MySQL",
    "PostgreSql": "PostgreSQL",
    "AzurePostgreSql": "Azure Database for PostgreSQL",
    "MongoDb": "MongoDB",
    "MongoDbV2": "MongoDB",
    "MongoDbAtlas": "MongoDB Atlas",
    "DynamicsCrm": "Dynamics 365",
    "Dynamics": "Dynamics 365",
    "SharePointOnlineList": "SharePoint Online",
    "AzureTableStorage": "Azure Table Storage",
    "AzureSearch": "Azure Cognitive Search",
    "AzureDataExplorer": "Azure Data Explorer (Kusto)",
    "AmazonS3": "Amazon S3",
    "AmazonS3Compatible": "Amazon S3 Compatible",
    "AmazonRedshift": "Amazon Redshift",
    "GoogleCloudStorage": "Google Cloud Storage",
    "GoogleBigQuery": "Google BigQuery",
    "FtpServer": "FTP",
    "FileServer": "File Server",
    "OData": "OData",
    "SapTable": "SAP Table",
    "SapHana": "SAP HANA",
    "SapBw": "SAP BW",
    "SapOpenHub": "SAP Open Hub",
    "SapOdp": "SAP ODP",
    "Teradata": "Teradata",
    "Netezza": "Netezza",
    "Vertica": "Vertica",
    "Greenplum": "Greenplum",
    "Hive": "Apache Hive",
    "Spark": "Apache Spark",
    "HBase": "Apache HBase",
    "Phoenix": "Apache Phoenix",
    "Impala": "Apache Impala",
    "Presto": "Presto",
    "Db2": "IBM Db2",
    "Informix": "IBM Informix",
    "Web": "Web Table",
    "CommonDataServiceForApps": "Dataverse",
    "Office365": "Office 365",
    "AzureSynapse": "Azure Synapse Analytics",
    "Cassandra": "Apache Cassandra",
    "Hdfs": "HDFS",
    "Marketo": "Marketo",
    "ServiceNow": "ServiceNow",
    "QuickBooks": "QuickBooks",
    "Concur": "Concur",
    "Paypal": "PayPal",
    "Zoho": "Zoho",
    "Xero": "Xero",
    "Square": "Square",
    "Hubspot": "HubSpot",
    "Jira": "Jira",
    "Magento": "Magento",
    "Shopify": "Shopify",
}

# File format types that should be consolidated when they appear standalone
_FILE_FORMAT_TYPES = {"DelimitedText", "Parquet", "Json", "Binary", "Excel", "Avro", "Orc", "Xml"}

# Storage prefix types that should absorb file format suffixes
_STORAGE_PREFIX_FRIENDLY = {
    "AzureBlobFS": _FILES_COMBINED_LABEL,
    "AzureBlobStorage": _FILES_COMBINED_LABEL,
    "AzureDataLakeStore": "Azure Data Lake Storage Gen1",
    "AmazonS3": "Amazon S3",
    "AmazonS3Compatible": "Amazon S3 Compatible",
    "GoogleCloudStorage": "Google Cloud Storage",
    "OracleCloudStorage": "Oracle Cloud Storage",
    "Sftp": "SFTP",
    "FtpServer": "FTP",
    "FileServer": "File Server",
    "HttpServer": "HTTP",
    "Hdfs": "HDFS",
    "AzureFileStorage": _FILES_COMBINED_LABEL,
}


def friendly_connector_name(raw_name: str) -> str:
    """Map a raw connector type to its friendly display name.

    Handles three cases:
    1. "Storage (Format)" patterns like "AzureBlobFS (DelimitedText)" -> friendly storage name
    2. Standalone file format names like "DelimitedText" -> "File (format unspecified)"
    3. Direct lookup in CONNECTOR_FRIENDLY_NAMES
    4. Fallback: return the raw name unchanged
    """
    if not raw_name:
        return raw_name

    # Case 1: "StorageType (FileFormat)" -> friendly storage name
    m = re.match(r'^(\w+)\s*\((\w+)\)$', raw_name)
    if m:
        storage, fmt = m.group(1), m.group(2)
        if fmt in _FILE_FORMAT_TYPES:
            # Use storage-prefix friendly name if known
            if storage in _STORAGE_PREFIX_FRIENDLY:
                return _STORAGE_PREFIX_FRIENDLY[storage]
            # Otherwise try CONNECTOR_FRIENDLY_NAMES for the storage part
            if storage in CONNECTOR_FRIENDLY_NAMES:
                return CONNECTOR_FRIENDLY_NAMES[storage]
        # For non-file-format parenthetical, try the full string first
        if raw_name in CONNECTOR_FRIENDLY_NAMES:
            return CONNECTOR_FRIENDLY_NAMES[raw_name]
        # Then try just the prefix
        if storage in CONNECTOR_FRIENDLY_NAMES:
            return CONNECTOR_FRIENDLY_NAMES[storage]
        return raw_name

    # Case 2: Standalone file format name -> combined Files label
    if raw_name in _FILE_FORMAT_TYPES:
        return _FILES_COMBINED_LABEL

    # Case 3: Direct lookup
    if raw_name in CONNECTOR_FRIENDLY_NAMES:
        return CONNECTOR_FRIENDLY_NAMES[raw_name]

    return raw_name


# Orchestration activity sub-categories for detailed breakdown (Change 10)
ORCH_ACTIVITY_CATEGORIES = {
    "Control Flow": {"ForEach", "IfCondition", "If", "Switch", "Until", "Wait",
                     "SetVariable", "AppendVariable", "Filter", "Validation",
                     "ExecutePipeline", "Fail"},
    "Data Movement": {"Copy", "CopyActivity"},
    "Transformation": {"ExecuteDataFlow", "DataFlow", "DatabricksNotebook",
                       "DatabricksSparkPython", "DatabricksSparkJar", "DatabricksJob",
                       "HDInsightHive", "HDInsightSpark", "SynapseNotebook",
                       "SparkJob", "HDInsightPig", "HDInsightMapReduce",
                       "HDInsightStreaming", "Script", "Custom"},
    "External": {"WebActivity", "Lookup", "GetMetadata", "SqlServerStoredProcedure",
                 "Delete", "AzureMLBatchExecution", "AzureMLUpdateResource",
                 "WebHook", "AzureFunctionActivity", "AzureFunction",
                 "AzureBatch", "AzureDataExplorerCommand", "AzureDataExplorer",
                 "AzureMLExecutePipeline", "ExecuteSSISPackage"},
}

# Build reverse mapping for orch sub-categories
_ORCH_TYPE_TO_SUBCAT = {}
for _subcat, _types in ORCH_ACTIVITY_CATEGORIES.items():
    for _t in _types:
        _ORCH_TYPE_TO_SUBCAT[_t] = _subcat


def orch_activity_subcat(activity_type: str) -> str:
    """Return the orchestration sub-category for an activity type."""
    return _ORCH_TYPE_TO_SUBCAT.get(activity_type, "Other")


# Database-type linked service identifiers
DATABASE_LINKED_SERVICE_TYPES = {
    "AzureSqlDatabase", "SqlServer", "AzureSqlDW", "AzureSqlMI",
    "Oracle", "MySql", "PostgreSql", "AmazonRdsForSqlServer",
    "AmazonRdsForOracle", "Db2", "Sybase", "Teradata", "Informix",
    "CosmosDb", "CosmosDbMongoDbApi", "MongoDb", "MongoDbV2", "MongoDbAtlas",
    "Cassandra", "AmazonRedshift", "GoogleBigQuery", "GoogleBigQueryV2",
    "Snowflake", "SnowflakeV2", "AzureMySql", "AzurePostgreSql",
    "Netezza", "Vertica", "Greenplum", "Hive", "Spark", "HBase",
    "Phoenix", "Impala", "Presto", "SapHana", "SapTable", "SapBw",
    "SapOpenHub", "SapOdp", "AzureDataExplorer",
}


# =============================================================================
# WKMIGRATE SUPPORT MAPPINGS
# =============================================================================
# Maps ADF activity types, connector types, and dataset types to their
# wkmigrate translation support status.  Used by the assessment report.

WKMIGRATE_ACTIVITY_SUPPORT = {
    # Fully supported — dedicated translators in wkmigrate
    "Copy":                     ("Supported",   "Translates to Spark DataFrame read/write task"),
    "Lookup":                   ("Supported",   "Translates to Spark read + task values publish"),
    "WebActivity":              ("Supported",   "Translates to HTTP request task"),
    "SetVariable":              ("Supported",   "Translates to task value set operation"),
    "ForEach":                  ("Supported",   "Translates to for_each_task with array items"),
    "IfCondition":              ("Supported",   "Translates to if/else branching with condition_task"),
    "DatabricksNotebook":       ("Supported",   "Translates to notebook_task (direct mapping)"),
    "DatabricksSparkPython":    ("Supported",   "Translates to spark_python_task"),
    "DatabricksSparkJar":       ("Supported",   "Translates to spark_jar_task"),
    "DatabricksJob":            ("Supported",   "Translates to run_job_task"),
    # Unsupported — creates placeholder notebook task
    "ExecutePipeline":          ("Unsupported", "Child pipeline invocation — requires manual refactoring"),
    "Switch":                   ("Unsupported", "Multi-branch conditional — refactor to nested IfCondition"),
    "Until":                    ("Unsupported", "Polling loop — no direct Databricks equivalent"),
    "Wait":                     ("Unsupported", "Delay/timer — no direct Databricks equivalent"),
    "Fail":                     ("Unsupported", "Error signaling — handle via task error propagation"),
    "Validation":               ("Unsupported", "Data validation check — implement in notebook"),
    "Filter":                   ("Unsupported", "Array filter — implement in notebook or task values"),
    "AppendVariable":           ("Unsupported", "Variable append — implement in notebook"),
    "GetMetadata":              ("Unsupported", "File/folder metadata — implement via dbutils or Spark"),
    "Delete":                   ("Unsupported", "File deletion — implement via dbutils"),
    "ExecuteDataFlow":          ("Unsupported", "Dataflow execution — rewrite as Spark transformations"),
    "SqlServerStoredProcedure": ("Unsupported", "Stored procedure call — invoke via JDBC in notebook"),
    "Script":                   ("Unsupported", "SQL script execution — run via Spark SQL or notebook"),
    "Custom":                   ("Unsupported", "Custom .NET activity — rewrite for Databricks"),
    "HDInsightHive":            ("Unsupported", "HDInsight Hive — migrate to Spark SQL"),
    "HDInsightPig":             ("Unsupported", "HDInsight Pig — migrate to Spark"),
    "HDInsightSpark":           ("Unsupported", "HDInsight Spark — migrate to Databricks Spark"),
    "HDInsightMapReduce":       ("Unsupported", "HDInsight MapReduce — migrate to Spark"),
    "HDInsightStreaming":       ("Unsupported", "HDInsight Streaming — migrate to Structured Streaming"),
    "SynapseNotebook":          ("Unsupported", "Synapse notebook — migrate to Databricks notebook"),
    "SparkJob":                 ("Unsupported", "Synapse Spark job — migrate to Databricks job"),
    "AzureMLBatchExecution":    ("Unsupported", "Azure ML batch — migrate to MLflow on Databricks"),
    "AzureMLUpdateResource":    ("Unsupported", "Azure ML resource update — migrate to MLflow"),
    "AzureMLExecutePipeline":   ("Unsupported", "Azure ML pipeline — migrate to MLflow"),
    "ExecuteSSISPackage":       ("Unsupported", "SSIS package — requires full rewrite"),
    "AzureFunctionActivity":    ("Unsupported", "Azure Function call — implement via WebActivity or notebook"),
    "AzureFunction":            ("Unsupported", "Azure Function call — implement via WebActivity or notebook"),
    "WebHook":                  ("Unsupported", "Webhook callback — implement via WebActivity"),
    "AzureBatch":               ("Unsupported", "Azure Batch job — rewrite for Databricks compute"),
    "AzureDataExplorerCommand": ("Unsupported", "Kusto command — implement via JDBC or REST"),
    "TridentNotebook":          ("Unsupported", "Fabric notebook — migrate to Databricks notebook"),
    "RefreshDataflow":          ("Unsupported", "Fabric dataflow refresh — rewrite as Spark"),
}

# Connector types supported by wkmigrate's dataset translators.
# Keys are the RAW Azure type names as they appear in source_type_actual / sink_type_actual.
WKMIGRATE_CONNECTOR_SUPPORT = {
    # File-based datasets on supported cloud storage (ABFS, S3, GCS, Azure Blob)
    "AzureBlobFS":                          ("Supported",   "ADLS Gen2 — file datasets (Avro, CSV, JSON, ORC, Parquet)"),
    "AzureBlobStorage":                     ("Supported",   "Azure Blob Storage — file datasets"),
    "AmazonS3":                             ("Supported",   "Amazon S3 — file datasets"),
    "GoogleCloudStorage":                   ("Supported",   "Google Cloud Storage — file datasets"),
    # SQL databases
    "AzureSqlDatabase":                     ("Supported",   "Azure SQL Database — JDBC translation"),
    "AzureSqlTable":                        ("Supported",   "Azure SQL table dataset"),
    "AzurePostgreSql":                      ("Supported",   "Azure PostgreSQL — JDBC translation"),
    "AzurePostgreSqlTable":                 ("Supported",   "Azure PostgreSQL table dataset"),
    "AzureMySql":                           ("Supported",   "Azure MySQL — JDBC translation"),
    "AzureMySqlTable":                      ("Supported",   "Azure MySQL table dataset"),
    "Oracle":                               ("Supported",   "Oracle Database — JDBC translation"),
    "OracleTable":                          ("Supported",   "Oracle table dataset"),
    # Delta Lake
    "AzureDatabricksDeltaLakeDataset":      ("Supported",   "Databricks Delta Lake — native support"),
    "AzureDatabricksDeltaLake":             ("Supported",   "Databricks Delta Lake — native support"),
    "Databricks":                           ("Supported",   "Databricks Delta Lake — native support"),
    # Azure Blob variants
    "Blob":                                 ("Supported",   "Azure Blob Storage — file datasets"),
    # File format types (when they appear as standalone connector names)
    "Avro":                                 ("Supported",   "Avro file format"),
    "DelimitedText":                        ("Supported",   "CSV/delimited text file format"),
    "Json":                                 ("Supported",   "JSON file format"),
    "Orc":                                  ("Supported",   "ORC file format"),
    "Parquet":                              ("Supported",   "Parquet file format"),
}

# Friendly connector names that map to supported raw types
_FRIENDLY_TO_SUPPORT = {
    "Files (ADLS Gen2, Blob Storage, File Storage, etc.)":  ("Supported",   "File-based storage — supported for Avro, CSV, JSON, ORC, Parquet"),
    "Azure SQL Database":                                    ("Supported",   "Azure SQL Database — JDBC translation"),
    "Azure Database for PostgreSQL":                         ("Supported",   "PostgreSQL — JDBC translation"),
    "Azure Database for MySQL":                              ("Supported",   "MySQL — JDBC translation"),
    "Oracle Database":                                       ("Supported",   "Oracle — JDBC translation"),
    "Databricks Delta Lake":                                 ("Supported",   "Delta Lake — native support"),
    "Amazon S3":                                             ("Supported",   "S3 — file datasets"),
    "Google Cloud Storage":                                  ("Supported",   "GCS — file datasets"),
    "Blob":                                                  ("Supported",   "Azure Blob Storage — file datasets"),
}


def wkmigrate_connector_status(raw_type: str, friendly_name: str = "") -> tuple:
    """Return (support_status, notes) for a connector type.

    Checks raw type first, then friendly name, then returns Unsupported.
    """
    if raw_type in WKMIGRATE_CONNECTOR_SUPPORT:
        return WKMIGRATE_CONNECTOR_SUPPORT[raw_type]
    if friendly_name and friendly_name in _FRIENDLY_TO_SUPPORT:
        return _FRIENDLY_TO_SUPPORT[friendly_name]
    label = friendly_name or raw_type or "Unknown"
    return ("Unsupported", f"{label} — no wkmigrate translator available")


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class Stats:
    """Thread-safe statistics collector with detailed tracking."""
    _lock: Lock = field(default_factory=Lock, repr=False)
    resource_groups: set = field(default_factory=set)
    factories: Dict[str, Dict] = field(default_factory=dict)
    
    # Raw data collections
    pipelines: List[Dict] = field(default_factory=list)
    linked_services: List[Dict] = field(default_factory=list)
    datasets: List[Dict] = field(default_factory=list)
    integration_runtimes: List[Dict] = field(default_factory=list)
    dataflows: List[Dict] = field(default_factory=list)
    triggers: List[Dict] = field(default_factory=list)
    activities: List[Dict] = field(default_factory=list)
    
    # Lookup tables (factory_name -> {name -> data})
    ls_lookup: Dict[str, Dict[str, Dict]] = field(default_factory=lambda: defaultdict(dict))
    ds_lookup: Dict[str, Dict[str, Dict]] = field(default_factory=lambda: defaultdict(dict))
    ir_lookup: Dict[str, Dict[str, Dict]] = field(default_factory=lambda: defaultdict(dict))
    trigger_lookup: Dict[str, Dict[str, List]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(list)))
    dataflow_lookup: Dict[str, Dict[str, Dict]] = field(default_factory=lambda: defaultdict(dict))
    
    # Type breakdowns
    linked_service_types: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    activity_types: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    activity_categories: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    ir_usage: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    dataflow_types: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    
    # Copy activity tracking - actual types
    copy_source_types: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    copy_sink_types: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    
    # WebActivity tracking
    web_activity_apis: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    
    # Trigger types
    trigger_types: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    
    # IR per factory type
    ir_by_factory_type: Dict[str, Dict[str, int]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(int)))
    
    # Track skipped workspaces
    skipped_synapse: List[str] = field(default_factory=list)
    
    # --- NEW: Runtime profiling data ---
    pipeline_runs: List[Dict] = field(default_factory=list)
    activity_runs: List[Dict] = field(default_factory=list)
    db_instances: List[Dict] = field(default_factory=list)
    table_discovery: List[Dict] = field(default_factory=list)
    copy_activity_details: List[Dict] = field(default_factory=list)
    shir_nodes: List[Dict] = field(default_factory=list)
    pipeline_job_stats: List[Dict] = field(default_factory=list)
    cost_breakdown: Dict[str, Any] = field(default_factory=dict)
    actual_costs: List[Dict] = field(default_factory=list)
    cost_mgmt_denied: List[str] = field(default_factory=list)
    subscription_names: Dict[str, str] = field(default_factory=dict)

    # --- NEW: Classification data ---
    ingestion_techniques: List[Dict] = field(default_factory=list)
    metadata_driven_classifications: List[Dict] = field(default_factory=list)
    databricks_classifications: List[Dict] = field(default_factory=list)
    
    def add_pipeline(self, pipeline: Dict):
        with self._lock:
            self.pipelines.append(pipeline)
    
    def add_linked_service(self, ls: Dict, ls_type: str, factory_name: str):
        with self._lock:
            self.linked_services.append(ls)
            self.linked_service_types[ls_type] += 1
            # Add to lookup
            self.ls_lookup[factory_name][ls.get("name")] = ls
    
    def add_dataset(self, ds: Dict, factory_name: str):
        with self._lock:
            self.datasets.append(ds)
            self.ds_lookup[factory_name][ds.get("name")] = ds
    
    def add_integration_runtime(self, ir: Dict, ir_type: str, factory_name: str, factory_type: str):
        with self._lock:
            self.integration_runtimes.append(ir)
            self.ir_usage[ir_type] += 1
            self.ir_by_factory_type[factory_type][ir_type] += 1
            self.ir_lookup[factory_name][ir.get("name")] = ir
    
    def add_dataflow(self, df: Dict, factory_name: str):
        with self._lock:
            self.dataflows.append(df)
            df_type = df.get("type", "Unknown")
            self.dataflow_types[df_type] += 1
            self.dataflow_lookup[factory_name][df.get("name")] = df
    
    def add_trigger(self, trigger: Dict, factory_name: str):
        with self._lock:
            self.triggers.append(trigger)
            trigger_type = trigger.get("type", "Unknown")
            self.trigger_types[trigger_type] += 1
            # Map trigger to pipelines
            for pipeline_name in trigger.get("pipelines", []):
                self.trigger_lookup[factory_name][pipeline_name].append(trigger_type)
    
    def add_activity(self, activity: Dict):
        with self._lock:
            self.activities.append(activity)
            act_type = activity.get("activity_type", "Unknown")
            self.activity_types[act_type] += 1
            category = activity.get("activity_category", "Unknown")
            self.activity_categories[category] += 1
            
            # Track copy source/sink
            if activity.get("source_type_actual"):
                self.copy_source_types[activity["source_type_actual"]] += 1
            if activity.get("sink_type_actual"):
                self.copy_sink_types[activity["sink_type_actual"]] += 1
            
            # Track web activity APIs
            if activity.get("api_type"):
                self.web_activity_apis[activity["api_type"]] += 1
    
    def add_factory(self, name: str, factory: Dict):
        with self._lock:
            self.factories[name] = factory
            if factory.get("resource_group"):
                self.resource_groups.add(factory["resource_group"])
    
    def add_skipped_synapse(self, name: str):
        with self._lock:
            if name not in self.skipped_synapse:
                self.skipped_synapse.append(name)
    
    def get_ls(self, factory_name: str, ls_name: str) -> Optional[Dict]:
        """Get linked service by name."""
        with self._lock:
            return self.ls_lookup.get(factory_name, {}).get(ls_name)
    
    def get_ds(self, factory_name: str, ds_name: str) -> Optional[Dict]:
        """Get dataset by name."""
        with self._lock:
            return self.ds_lookup.get(factory_name, {}).get(ds_name)
    
    def get_ir(self, factory_name: str, ir_name: str) -> Optional[Dict]:
        """Get integration runtime by name."""
        with self._lock:
            return self.ir_lookup.get(factory_name, {}).get(ir_name)
    
    def get_trigger_types(self, factory_name: str, pipeline_name: str) -> List[str]:
        """Get trigger types for a pipeline."""
        with self._lock:
            return self.trigger_lookup.get(factory_name, {}).get(pipeline_name, [])
    
    def get_dataflow(self, factory_name: str, df_name: str) -> Optional[Dict]:
        """Get dataflow by name."""
        with self._lock:
            return self.dataflow_lookup.get(factory_name, {}).get(df_name)
    
    def add_pipeline_run(self, run: Dict):
        with self._lock:
            self.pipeline_runs.append(run)
    
    def add_activity_run(self, run: Dict):
        with self._lock:
            self.activity_runs.append(run)
    
    def add_db_instance(self, instance: Dict):
        with self._lock:
            self.db_instances.append(instance)
    
    def add_table_entry(self, entry: Dict):
        with self._lock:
            self.table_discovery.append(entry)
    
    def add_copy_detail(self, detail: Dict):
        with self._lock:
            self.copy_activity_details.append(detail)
    
    def add_shir_node(self, node: Dict):
        with self._lock:
            self.shir_nodes.append(node)
    
    def add_pipeline_job_stat(self, stat: Dict):
        with self._lock:
            self.pipeline_job_stats.append(stat)
    
    def set_cost_breakdown(self, breakdown: Dict):
        with self._lock:
            self.cost_breakdown = breakdown
    
    def add_actual_cost(self, cost: Dict):
        with self._lock:
            self.actual_costs.append(cost)
    
    # Batch methods — acquire lock once for many items
    def batch_add_activity_runs(self, runs: List[Dict]):
        with self._lock:
            self.activity_runs.extend(runs)
    
    def batch_add_copy_details(self, details: List[Dict]):
        with self._lock:
            self.copy_activity_details.extend(details)
    
    def batch_add_pipeline_runs(self, runs: List[Dict]):
        with self._lock:
            self.pipeline_runs.extend(runs)

    def add_ingestion_technique(self, technique: Dict):
        with self._lock:
            self.ingestion_techniques.append(technique)

    def batch_add_ingestion_techniques(self, techniques: List[Dict]):
        with self._lock:
            self.ingestion_techniques.extend(techniques)

    def add_metadata_driven_classification(self, classification: Dict):
        with self._lock:
            self.metadata_driven_classifications.append(classification)

    def add_databricks_classification(self, classification: Dict):
        with self._lock:
            self.databricks_classifications.append(classification)


@dataclass
class ExtractionTask:
    """Represents a single extraction task."""
    factory: Dict
    factory_type: str
    resource_type: str
    resource_name: Optional[str] = None
    resource_id: Optional[str] = None


@dataclass
class ExtractionError:
    """Collected error for display at end."""
    factory_name: str
    resource_type: str
    error_message: str
    is_warning: bool = False


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging(output_dir: str) -> Tuple[logging.Logger, str]:
    """Configure file logging with detailed output."""
    os.makedirs(output_dir, exist_ok=True)
    log_file = os.path.join(output_dir, f"extraction_{datetime.now():%Y%m%d_%H%M%S}.log")
    
    logger = logging.getLogger("ADFExtractor")
    logger.setLevel(logging.DEBUG)
    logger.handlers = []
    
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)
    
    logger.info("=" * 60)
    logger.info("Azure Data Factory Detailed Extraction Started")
    logger.info("=" * 60)
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Working directory: {os.getcwd()}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    
    return logger, log_file


# =============================================================================
# RETRY DECORATOR
# =============================================================================

def retry(max_attempts: int = MAX_RETRIES):
    """Fast retry with minimal delay."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except (requests.RequestException, HttpResponseError, AzureError) as e:
                    if attempt < max_attempts - 1:
                        time.sleep(RETRY_DELAY)
                    else:
                        raise
            return None
        return wrapper
    return decorator


# =============================================================================
# CREDENTIAL HELPER
# =============================================================================

def get_credential(tenant_id: str = None, client_id: str = None, client_secret: str = None):
    """Get Azure credential."""
    if tenant_id and client_id and client_secret:
        return ClientSecretCredential(tenant_id, client_id, client_secret)
    return DefaultAzureCredential()


# =============================================================================
# ACTIVITY ANALYZER
# =============================================================================

class ActivityAnalyzer:
    """Analyzes activities to extract detailed information."""
    
    def __init__(self, stats: Stats, logger: logging.Logger):
        self.stats = stats
        self.logger = logger
    
    def get_activity_category(self, activity_type: str) -> str:
        """Get category for an activity type."""
        return ACTIVITY_TYPE_TO_CATEGORY.get(activity_type, "Unknown")
    
    def parse_web_activity(self, activity: Dict) -> Dict:
        """Parse WebActivity to extract API details."""
        type_props = activity.get("typeProperties", {})
        url = type_props.get("url", "")
        method = type_props.get("method", "GET")
        
        # Handle parameterized URLs
        if isinstance(url, dict):
            url = url.get("value", str(url))
        
        api_type = self._determine_api_type(url)
        
        return {
            "api_endpoint": url[:500] if url else None,  # Truncate long URLs
            "api_type": api_type,
            "http_method": method
        }
    
    def _determine_api_type(self, url: str) -> str:
        """Determine API type from URL."""
        if not url or not isinstance(url, str):
            return "Unknown"
        
        url_lower = url.lower()
        
        # Databricks
        if "databricks" in url_lower or ".azuredatabricks." in url_lower:
            if "/jobs/" in url_lower:
                return "Databricks Jobs API"
            elif "/clusters/" in url_lower:
                return "Databricks Clusters API"
            elif "/sql/" in url_lower:
                return "Databricks SQL API"
            return "Databricks API"
        
        # Azure services
        if "cosmos" in url_lower or ".documents.azure.com" in url_lower:
            return "Cosmos DB API"
        if ".database.windows.net" in url_lower or "sql.azure" in url_lower:
            return "Azure SQL API"
        if ".postgres.database.azure.com" in url_lower:
            return "Azure PostgreSQL API"
        if ".mysql.database.azure.com" in url_lower:
            return "Azure MySQL API"
        if ".blob.core.windows.net" in url_lower:
            return "Azure Blob Storage API"
        if ".dfs.core.windows.net" in url_lower:
            return "Azure Data Lake API"
        if ".vault.azure.net" in url_lower:
            return "Azure Key Vault API"
        if ".servicebus.windows.net" in url_lower:
            return "Azure Service Bus API"
        if ".eventhub.azure.net" in url_lower or "eventhub" in url_lower:
            return "Azure Event Hub API"
        if "logic.azure.com" in url_lower:
            return "Azure Logic Apps API"
        if ".azurewebsites.net" in url_lower:
            return "Azure App Service API"
        if "management.azure.com" in url_lower:
            return "Azure Management API"
        if "graph.microsoft.com" in url_lower:
            return "Microsoft Graph API"
        if "login.microsoftonline.com" in url_lower:
            return "Azure AD API"
        if ".powerbi.com" in url_lower or "api.powerbi" in url_lower:
            return "Power BI API"
        if ".fabric.microsoft.com" in url_lower:
            return "Microsoft Fabric API"
        if ".sharepoint.com" in url_lower:
            return "SharePoint API"
        if "dev.azure.com" in url_lower or "visualstudio.com" in url_lower:
            return "Azure DevOps API"
        
        # Common third-party APIs
        if "salesforce" in url_lower:
            return "Salesforce API"
        if "servicenow" in url_lower:
            return "ServiceNow API"
        if "snowflake" in url_lower:
            return "Snowflake API"
        if "slack.com" in url_lower:
            return "Slack API"
        if "teams.microsoft.com" in url_lower:
            return "Microsoft Teams API"
        
        return "Generic REST API"
    
    def extract_copy_details(self, activity: Dict, factory_name: str) -> Dict:
        """Extract detailed source/sink information from Copy activity."""
        type_props = activity.get("typeProperties", {})
        
        # Get dataset references - handle both camelCase and snake_case (SDK serialization)
        inputs = activity.get("inputs", [])
        outputs = activity.get("outputs", [])
        
        source_dataset_name = None
        sink_dataset_name = None
        
        if inputs:
            # Try both camelCase (raw JSON) and snake_case (SDK serialized)
            source_dataset_name = (inputs[0].get("referenceName") or 
                                   inputs[0].get("reference_name"))
        if outputs:
            sink_dataset_name = (outputs[0].get("referenceName") or 
                                 outputs[0].get("reference_name"))
        
        # Source/sink can be at activity root level OR inside typeProperties
        source_props = activity.get("source") or type_props.get("source", {})
        sink_props = activity.get("sink") or type_props.get("sink", {})
        
        # Trace to get actual types
        source_type = self._resolve_source_sink_type(factory_name, source_dataset_name, source_props)
        sink_type = self._resolve_source_sink_type(factory_name, sink_dataset_name, sink_props)
        
        # Get linked service names
        source_ls_name = self._get_ls_from_dataset(factory_name, source_dataset_name)
        sink_ls_name = self._get_ls_from_dataset(factory_name, sink_dataset_name)
        
        return {
            "source_dataset_name": source_dataset_name,
            "sink_dataset_name": sink_dataset_name,
            "source_type_actual": source_type,
            "sink_type_actual": sink_type,
            "source_linked_service": source_ls_name,
            "sink_linked_service": sink_ls_name
        }
    
    def _resolve_source_sink_type(self, factory_name: str, dataset_name: str, source_sink_props: Dict) -> str:
        """Resolve actual connector type from source/sink properties."""
        
        direct_type = source_sink_props.get("type", "")
        
        # For format-based types (DelimitedText, Parquet, Json, etc.), check store_settings
        # to get the actual storage type
        format_based_types = [
            "DelimitedTextSource", "DelimitedTextSink",
            "ParquetSource", "ParquetSink",
            "JsonSource", "JsonSink",
            "AvroSource", "AvroSink",
            "OrcSource", "OrcSink",
            "BinarySource", "BinarySink",
            "ExcelSource", "ExcelSink",
            "XmlSource", "XmlSink",
        ]
        
        if direct_type in format_based_types:
            # Check store_settings for actual storage type (handle both camelCase and snake_case)
            store_settings = source_sink_props.get("storeSettings") or source_sink_props.get("store_settings", {})
            store_type = store_settings.get("type", "")
            
            # Map store settings types to storage types
            store_settings_mapping = {
                "AzureBlobStorageReadSettings": "AzureBlobStorage",
                "AzureBlobStorageWriteSettings": "AzureBlobStorage",
                "AzureBlobFSReadSettings": "AzureBlobFS",
                "AzureBlobFSWriteSettings": "AzureBlobFS",
                "AzureDataLakeStoreReadSettings": "AzureDataLakeStore",
                "AzureDataLakeStoreWriteSettings": "AzureDataLakeStore",
                "AmazonS3ReadSettings": "AmazonS3",
                "AmazonS3CompatibleReadSettings": "AmazonS3Compatible",
                "GoogleCloudStorageReadSettings": "GoogleCloudStorage",
                "OracleCloudStorageReadSettings": "OracleCloudStorage",
                "SftpReadSettings": "Sftp",
                "SftpWriteSettings": "Sftp",
                "FtpReadSettings": "FtpServer",
                "FileServerReadSettings": "FileServer",
                "FileServerWriteSettings": "FileServer",
                "HttpReadSettings": "HttpServer",
                "HdfsReadSettings": "Hdfs",
                "AzureFileStorageReadSettings": "AzureFileStorage",
                "AzureFileStorageWriteSettings": "AzureFileStorage",
            }
            
            if store_type in store_settings_mapping:
                # Return combined: Storage + Format (e.g., "AzureBlobFS (DelimitedText)")
                storage = store_settings_mapping[store_type]
                fmt = direct_type.replace("Source", "").replace("Sink", "")
                return f"{storage} ({fmt})"
            
            # If no store_settings but we have a dataset, try to get storage from linked service
            if dataset_name:
                ls_type = self._get_storage_from_dataset(factory_name, dataset_name)
                if ls_type:
                    fmt = direct_type.replace("Source", "").replace("Sink", "")
                    return f"{ls_type} ({fmt})"
            
            # Fallback: just return the format type
            return direct_type.replace("Source", "").replace("Sink", "")
        
        # Check if type doesn't end with Source/Sink - return as is
        if direct_type and not direct_type.endswith("Source") and not direct_type.endswith("Sink"):
            return direct_type
        
        # Map common source/sink types to connector types
        type_mapping = {
            "AzureBlobSource": "AzureBlobStorage",
            "AzureBlobSink": "AzureBlobStorage",
            "AzureBlobFSSource": "AzureBlobFS",
            "AzureBlobFSSink": "AzureBlobFS",
            "AzureDataLakeStoreSource": "AzureDataLakeStore",
            "AzureDataLakeStoreSink": "AzureDataLakeStore",
            "AzureSqlSource": "AzureSqlDatabase",
            "AzureSqlSink": "AzureSqlDatabase",
            "SqlServerSource": "SqlServer",
            "SqlServerSink": "SqlServer",
            "SqlSource": "SqlServer",
            "SqlSink": "SqlServer",
            "OracleSource": "Oracle",
            "OracleSink": "Oracle",
            "SqlDWSource": "AzureSqlDW",
            "SqlDWSink": "AzureSqlDW",
            "SynapseSource": "AzureSynapse",
            "SynapseSink": "AzureSynapse",
            "CosmosDbSqlApiSource": "CosmosDb",
            "CosmosDbSqlApiSink": "CosmosDb",
            "CosmosDbMongoDbApiSource": "CosmosDbMongoDbApi",
            "CosmosDbMongoDbApiSink": "CosmosDbMongoDbApi",
            "DynamicsSource": "Dynamics",
            "DynamicsSink": "Dynamics",
            "SalesforceSource": "Salesforce",
            "SalesforceSink": "Salesforce",
            "RestSource": "RestService",
            "HttpSource": "HttpServer",
            "ODataSource": "OData",
            "SharePointOnlineListSource": "SharePointOnlineList",
            "SnowflakeSource": "Snowflake",
            "SnowflakeSink": "Snowflake",
            "SnowflakeV2Source": "Snowflake",
            "SnowflakeV2Sink": "Snowflake",
            "PostgreSqlSource": "PostgreSql",
            "PostgreSqlSink": "PostgreSql",
            "MySqlSource": "MySql",
            "AzureMySqlSource": "AzureMySql",
            "AzureMySqlSink": "AzureMySql",
            "AzurePostgreSqlSource": "AzurePostgreSql",
            "AzurePostgreSqlSink": "AzurePostgreSql",
            "MongoDbSource": "MongoDb",
            "MongoDbV2Source": "MongoDb",
            "MongoDbAtlasSource": "MongoDbAtlas",
            "MongoDbAtlasSink": "MongoDbAtlas",
            "AzureTableSource": "AzureTableStorage",
            "AzureTableSink": "AzureTableStorage",
            "FileSystemSource": "FileServer",
            "FileSystemSink": "FileServer",
            "FtpSource": "FtpServer",
            "SftpSource": "Sftp",
            "SftpSink": "Sftp",
            "AmazonS3Source": "AmazonS3",
            "AmazonS3CompatibleSource": "AmazonS3Compatible",
            "AmazonRedshiftSource": "AmazonRedshift",
            "GoogleCloudStorageSource": "GoogleCloudStorage",
            "GoogleBigQuerySource": "GoogleBigQuery",
            "GoogleBigQueryV2Source": "GoogleBigQuery",
            "SapTableSource": "SapTable",
            "SapHanaSource": "SapHana",
            "SapBwSource": "SapBw",
            "SapOpenHubSource": "SapOpenHub",
            "SapOdpSource": "SapOdp",
            "TeradataSource": "Teradata",
            "NetezzaSource": "Netezza",
            "VerticaSource": "Vertica",
            "GreenplumSource": "Greenplum",
            "HiveSource": "Hive",
            "SparkSource": "Spark",
            "HBaseSource": "HBase",
            "PhoenixSource": "Phoenix",
            "ImpalaSource": "Impala",
            "PrestoSource": "Presto",
            "MarketoSource": "Marketo",
            "ServiceNowSource": "ServiceNow",
            "ServiceNowV2Source": "ServiceNow",
            "QuickBooksSource": "QuickBooks",
            "ConcurSource": "Concur",
            "PaypalSource": "Paypal",
            "ZohoSource": "Zoho",
            "XeroSource": "Xero",
            "SquareSource": "Square",
            "HubspotSource": "Hubspot",
            "JiraSource": "Jira",
            "MagentoSource": "Magento",
            "ShopifySource": "Shopify",
            "WebSource": "Web",
            "CommonDataServiceForAppsSource": "CommonDataServiceForApps",
            "CommonDataServiceForAppsSink": "CommonDataServiceForApps",
            "InformixSource": "Informix",
            "InformixSink": "Informix",
            "Db2Source": "Db2",
            "OdbcSource": "Odbc",
            "OdbcSink": "Odbc",
            "Office365Source": "Office365",
            "AzureSearchIndexSink": "AzureSearch",
            "AzureDataExplorerSource": "AzureDataExplorer",
            "AzureDataExplorerSink": "AzureDataExplorer",
            "DatabricksSource": "Databricks",
            "DatabricksSink": "Databricks",
            "LakeHouseTableSource": "FabricLakehouse",
            "LakeHouseTableSink": "FabricLakehouse",
            "WarehouseSource": "FabricWarehouse",
            "WarehouseSink": "FabricWarehouse",
        }
        
        if direct_type in type_mapping:
            return type_mapping[direct_type]
        
        # Try to get from dataset → linked service
        if dataset_name:
            ls_type = self._get_storage_from_dataset(factory_name, dataset_name)
            if ls_type:
                return ls_type
        
        # Strip Source/Sink suffix as fallback
        if direct_type.endswith("Source"):
            return direct_type[:-6]
        if direct_type.endswith("Sink"):
            return direct_type[:-4]
        
        return direct_type if direct_type else "Unknown"
    
    def _get_storage_from_dataset(self, factory_name: str, dataset_name: str) -> Optional[str]:
        """Get storage type from dataset's linked service."""
        if not dataset_name:
            return None
        ds = self.stats.get_ds(factory_name, dataset_name)
        if ds:
            ls_name = ds.get("linked_service_name")
            if ls_name:
                ls = self.stats.get_ls(factory_name, ls_name)
                if ls:
                    return ls.get("type")
            # Also try dataset type
            ds_type = ds.get("type")
            if ds_type:
                return ds_type
        return None
    
    def _get_ls_from_dataset(self, factory_name: str, dataset_name: str) -> Optional[str]:
        """Get linked service name from dataset."""
        if not dataset_name:
            return None
        ds = self.stats.get_ds(factory_name, dataset_name)
        if ds:
            return ds.get("linked_service_name")
        return None
    
    def get_ir_type(self, activity: Dict, factory_name: str, pipeline_ir: str = None) -> str:
        """Determine the IR type used by an activity."""
        # Check activity-level IR reference
        type_props = activity.get("typeProperties", {})
        ir_ref = type_props.get("connectVia", {}).get("referenceName")
        
        # Check linked service's IR
        if not ir_ref:
            ls_name = activity.get("linkedServiceName", {}).get("referenceName")
            if ls_name:
                ls = self.stats.get_ls(factory_name, ls_name)
                if ls:
                    ir_ref = ls.get("connect_via")
        
        # Fall back to pipeline-level IR
        if not ir_ref and pipeline_ir:
            ir_ref = pipeline_ir
        
        # Lookup IR type
        if ir_ref:
            ir = self.stats.get_ir(factory_name, ir_ref)
            if ir:
                return ir.get("type", "Unknown")
            return ir_ref  # Return name if not found
        
        return "Azure"  # Default IR
    
    def get_activity_purpose(self, activity: Dict) -> str:
        """Generate a brief description of activity purpose."""
        act_type = activity.get("type", "Unknown")
        type_props = activity.get("typeProperties", {})
        
        purposes = {
            "Copy": "Data movement",
            "CopyActivity": "Data movement",
            "ExecuteDataFlow": "Data transformation via dataflow",
            "DataFlow": "Data transformation via dataflow",
            "DatabricksNotebook": "Execute Databricks notebook",
            "DatabricksJob": "Run Databricks job",
            "DatabricksSparkPython": "Run Python on Databricks",
            "DatabricksSparkJar": "Run JAR on Databricks",
            "SqlServerStoredProcedure": "Execute stored procedure",
            "Script": "Execute script",
            "Lookup": "Retrieve data for pipeline logic",
            "GetMetadata": "Get file/folder metadata",
            "Delete": "Delete files/folders",
            "WebActivity": "Call REST API",
            "WebHook": "Wait for external callback",
            "AzureFunctionActivity": "Execute Azure Function",
            "AzureFunction": "Execute Azure Function",
            "ExecutePipeline": "Run child pipeline",
            "ForEach": "Iterate over collection",
            "IfCondition": "Conditional branching",
            "If": "Conditional branching",
            "Switch": "Multi-branch selection",
            "Until": "Loop until condition",
            "Wait": "Pause execution",
            "SetVariable": "Set pipeline variable",
            "AppendVariable": "Append to array variable",
            "Filter": "Filter array items",
            "Fail": "Fail pipeline with error",
            "Validation": "Validate file existence",
            "SynapseNotebook": "Run Synapse notebook",
            "SparkJob": "Run Spark job",
            "HDInsightHive": "Run Hive query",
            "HDInsightPig": "Run Pig script",
            "HDInsightSpark": "Run Spark on HDInsight",
            "Custom": "Custom activity",
            "AzureBatch": "Run on Azure Batch",
        }
        
        base = purposes.get(act_type, f"Execute {act_type}")
        
        # Add specifics for certain types
        if act_type == "ExecuteDataFlow":
            # Handle both camelCase and snake_case
            df_ref = type_props.get("dataflow") or type_props.get("data_flow", {})
            df_name = df_ref.get("referenceName") or df_ref.get("reference_name", "")
            if df_name:
                base = f"Transform via dataflow: {df_name}"
        elif act_type in ["Copy", "CopyActivity"]:
            # Source/sink can be at activity root OR in typeProperties
            src_props = activity.get("source") or type_props.get("source", {})
            snk_props = activity.get("sink") or type_props.get("sink", {})
            src = src_props.get("type", "")
            snk = snk_props.get("type", "")
            if src and snk:
                base = f"Copy from {src.replace('Source', '')} to {snk.replace('Sink', '')}"
        
        return base[:100]  # Truncate


# =============================================================================
# DISCOVERY
# =============================================================================

class ResourceDiscovery:
    """Fast discovery of factories/workspaces."""
    
    def __init__(self, credential, logger: logging.Logger, include_synapse: bool = True, include_fabric: bool = True):
        self.credential = credential
        self.logger = logger
        self.include_synapse = include_synapse
        self.include_fabric = include_fabric
        self._adf_clients: Dict[str, DataFactoryManagementClient] = {}
    
    def _get_adf_client(self, sub_id: str) -> DataFactoryManagementClient:
        if sub_id not in self._adf_clients:
            self._adf_clients[sub_id] = DataFactoryManagementClient(self.credential, sub_id)
        return self._adf_clients[sub_id]
    
    def _get_headers(self, scope: str) -> Dict[str, str]:
        token = self.credential.get_token(scope).token
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    def discover_all(
        self,
        subscription_id: str = None,
        resource_group: str = None,
        factory_name: str = None
    ) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
        """Discover all factories and workspaces."""
        
        print("\n🔍 Discovering resources...")
        self.logger.info("Starting resource discovery")
        
        subscriptions = self._discover_subscriptions(subscription_id)
        if not subscriptions:
            print("   ❌ No accessible subscriptions found")
            self.logger.error("No accessible subscriptions found")
            return [], [], [], []
        
        print(f"   Found {len(subscriptions)} subscription(s)")
        self.logger.info(f"Found {len(subscriptions)} subscriptions")
        for sub in subscriptions:
            print(f"      - {sub['display_name']} ({sub['subscription_id'][:8]}...)")
        
        adf_factories = []
        synapse_workspaces = []
        fabric_workspaces = []
        
        for sub in subscriptions:
            sub_id = sub["subscription_id"]
            self.logger.info(f"Scanning subscription: {sub['display_name']} ({sub_id})")
            
            factories = self._discover_adf_factories(sub_id, resource_group, factory_name)
            adf_factories.extend(factories)
            
            if self.include_synapse:
                workspaces = self._discover_synapse_workspaces(sub_id, resource_group)
                synapse_workspaces.extend(workspaces)
        
        if self.include_fabric:
            fabric_workspaces = self._discover_fabric_workspaces()
        
        total = len(adf_factories) + len(synapse_workspaces) + len(fabric_workspaces)
        print(f"   Found {total} factories ({len(adf_factories)} ADF, {len(synapse_workspaces)} Synapse, {len(fabric_workspaces)} Fabric)")
        
        return subscriptions, adf_factories, synapse_workspaces, fabric_workspaces

    @retry()
    def _discover_subscriptions(self, subscription_id: str = None) -> List[Dict]:
        client = SubscriptionClient(self.credential)
        subs = []
        for sub in client.subscriptions.list():
            if subscription_id and sub.subscription_id != subscription_id:
                continue
            subs.append({
                "subscription_id": sub.subscription_id,
                "display_name": sub.display_name,
                "state": sub.state
            })
        
        if subscription_id and not subs:
            try:
                sub = client.subscriptions.get(subscription_id)
                subs.append({
                    "subscription_id": sub.subscription_id,
                    "display_name": sub.display_name,
                    "state": sub.state
                })
            except Exception as e:
                self.logger.error(f"Failed to get subscription {subscription_id}: {e}")
        return subs

    @retry()
    def _discover_adf_factories(self, sub_id: str, rg_name: str = None, factory_name: str = None) -> List[Dict]:
        client = self._get_adf_client(sub_id)
        factories = []
        try:
            if rg_name and factory_name:
                f = client.factories.get(rg_name, factory_name)
                factories.append(self._factory_to_dict(f, sub_id))
            elif rg_name:
                for f in client.factories.list_by_resource_group(rg_name):
                    factories.append(self._factory_to_dict(f, sub_id))
            else:
                for f in client.factories.list():
                    factories.append(self._factory_to_dict(f, sub_id))
        except Exception as e:
            self.logger.error(f"Error discovering ADF factories in {sub_id}: {e}")
        return factories

    def _factory_to_dict(self, factory, sub_id: str) -> Dict:
        parts = factory.id.split("/")
        rg_idx = parts.index("resourceGroups") + 1 if "resourceGroups" in parts else -1
        rg = parts[rg_idx] if rg_idx > 0 else "unknown"
        return {
            "id": factory.id,
            "name": factory.name,
            "type": "ADF",
            "location": factory.location,
            "resource_group": rg,
            "subscription_id": sub_id,
        }

    @retry()
    def _discover_synapse_workspaces(self, sub_id: str, rg_name: str = None) -> List[Dict]:
        headers = self._get_headers("https://management.azure.com/.default")
        url = f"{AZURE_MGMT_URL}/subscriptions/{sub_id}"
        if rg_name:
            url += f"/resourceGroups/{rg_name}"
        url += "/providers/Microsoft.Synapse/workspaces?api-version=2021-06-01"
        
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if not resp.ok:
                return []
        except Exception as e:
            self.logger.error(f"Error listing Synapse workspaces: {e}")
            return []
        
        workspaces = []
        for ws in resp.json().get("value", []):
            parts = ws["id"].split("/")
            rg_idx = parts.index("resourceGroups") + 1 if "resourceGroups" in parts else -1
            rg = parts[rg_idx] if rg_idx > 0 else "unknown"
            
            workspaces.append({
                "id": ws["id"],
                "name": ws["name"],
                "type": "Synapse",
                "location": ws.get("location"),
                "resource_group": rg,
                "subscription_id": sub_id,
                "dev_endpoint": ws.get("properties", {}).get("connectivityEndpoints", {}).get("dev")
            })
        
        return workspaces

    @retry()
    def _discover_fabric_workspaces(self) -> List[Dict]:
        try:
            headers = self._get_headers("https://api.fabric.microsoft.com/.default")
            resp = requests.get(f"{FABRIC_API_URL}/workspaces", headers=headers, timeout=REQUEST_TIMEOUT)
            if not resp.ok:
                return []
            return [
                {"id": ws["id"], "name": ws.get("displayName", ws.get("name")), "type": "Fabric"}
                for ws in resp.json().get("value", [])
            ]
        except Exception as e:
            self.logger.error(f"Error listing Fabric workspaces: {e}")
            return []


# =============================================================================
# INGESTION CLASSIFIER
# =============================================================================

# CDC patterns in source queries
CDC_PATTERNS = [
    r'cdc\.fn_cdc_get_(all|net)_changes',
    r'CHANGETABLE\s*\(\s*CHANGES',
    r'sys\.dm_tran_change_tracking',
    r'change_tracking_current_version',
]

# Watermark patterns in source queries
WATERMARK_QUERY_PATTERNS = [
    r'@\{?pipeline\(\)\.parameters\.\w*(watermark|lastmodified|lastload|lastrun|incremental)',
    r'@\{?activity\([\'"]Lookup',
    r'WHERE\s+\w+\s*[>>=]\s*@',
    r'(LastModifiedDate|ModifiedDate|UpdatedAt|LastRunTime|WatermarkValue)',
]

WATERMARK_PARAM_NAMES = ['watermark', 'lastmodified', 'lastload', 'lastrun', 'incremental', 'highwatermark', 'lowwatermark']

SPARK_ACTIVITY_TYPES = {
    'DatabricksNotebook', 'DatabricksJob', 'DatabricksSparkPython',
    'DatabricksSparkJar', 'SynapseNotebook', 'SynapseSpark', 'SparkJob',
}

METADATA_PARAM_NAMES = [
    'tablename', 'schema', 'sourcequery', 'sinkfolder', 'loadtype',
    'watermarkcolumn', 'sourcetable', 'sinktable', 'sourceconnection',
    'sinkconnection', 'filename', 'filepath', 'containerpath',
]


class IngestionClassifier:
    """Classifies ingestion techniques for Copy activities within a pipeline."""

    def __init__(self, stats: Stats, logger: logging.Logger):
        self.stats = stats
        self.logger = logger

    def classify_pipeline(self, pipeline: Dict, factory: Dict) -> List[Dict]:
        """Classify each Copy activity in a pipeline by ingestion technique."""
        activities = pipeline.get("activities", [])
        if not activities:
            return []

        factory_name = factory.get("name", pipeline.get("factory_name", ""))
        factory_type = factory.get("type", pipeline.get("factory_type", ""))
        sub_id = factory.get("subscription_id", pipeline.get("subscription_id", ""))
        pipeline_name = pipeline.get("name", "")

        # Build name->activity and dependency graph
        activities_by_name = {a.get("name", ""): a for a in activities}
        upstream_of = defaultdict(set)  # activity_name -> set of upstream names
        downstream_of = defaultdict(set)  # activity_name -> set of downstream names
        for act in activities:
            act_name = act.get("name", "")
            for dep in act.get("dependsOn", []):
                dep_name = dep.get("activity", "")
                if dep_name:
                    upstream_of[act_name].add(dep_name)
                    downstream_of[dep_name].add(act_name)

        # Pipeline-level watermark param check
        param_names = [k.lower() for k in pipeline.get("parameters", {}).keys()]
        has_watermark_params = any(
            any(wp in pn for wp in WATERMARK_PARAM_NAMES) for pn in param_names
        )

        results = []
        for act in activities:
            act_type = act.get("type", "")
            act_name = act.get("name", "")

            if act_type in ("Copy", "CopyActivity"):
                result = self._classify_copy_activity(
                    act, act_name, activities_by_name,
                    upstream_of, downstream_of, has_watermark_params,
                    pipeline_name, factory_name, factory_type, sub_id
                )
                results.append(result)
            elif act_type in ("ExecuteDataFlow", "DataFlow"):
                result = self._classify_dataflow_activity(
                    act, act_name, pipeline_name, factory_name, factory_type, sub_id
                )
                if result:
                    results.append(result)

        return results

    def _classify_copy_activity(
        self, act, act_name, activities_by_name,
        upstream_of, downstream_of, has_watermark_params,
        pipeline_name, factory_name, factory_type, sub_id
    ) -> Dict:
        type_props = act.get("typeProperties", {})

        # a. Check sink writeBehavior for upsert
        sink_props = type_props.get("sink", {})
        write_behavior = str(sink_props.get("writeBehavior", "")).lower()
        has_upsert = "upsert" in write_behavior

        # b. Check for Lookup upstream (watermark pattern)
        upstream_types = {
            activities_by_name.get(dep, {}).get("type", "")
            for dep in upstream_of.get(act_name, set())
        }
        has_watermark_lookup = "Lookup" in upstream_types

        # c. Check for StoredProcedure / Script downstream
        downstream_types = {
            activities_by_name.get(dep, {}).get("type", "")
            for dep in downstream_of.get(act_name, set())
        }
        has_downstream_sproc = bool(
            downstream_types & {"SqlServerStoredProcedure", "Script"}
        )

        # d. Check for Databricks/Synapse downstream
        has_downstream_spark = bool(downstream_types & SPARK_ACTIVITY_TYPES)

        # e. Check source query for CDC function references
        source_query = type_props.get("source", {}).get("sqlReaderQuery", "")
        if isinstance(source_query, dict):
            source_query = source_query.get("value", str(source_query))
        source_query = str(source_query)
        has_cdc = any(re.search(p, source_query, re.IGNORECASE) for p in CDC_PATTERNS)

        # f. Check source query for watermark expressions
        has_watermark_query = any(
            re.search(p, source_query, re.IGNORECASE) for p in WATERMARK_QUERY_PATTERNS
        )

        # Classification priority
        if has_cdc:
            technique = "Native CDC"
            confidence = "Medium"
        elif has_watermark_lookup and has_downstream_sproc:
            technique = "Incremental Watermark + Stored Proc Merge"
            confidence = "High"
        elif has_watermark_lookup or has_watermark_query or has_watermark_params:
            technique = "Incremental with Watermark"
            confidence = "Medium-High"
        elif has_downstream_sproc:
            technique = "Full Load + Stored Proc"
            confidence = "Medium"
        elif has_downstream_spark:
            technique = "Copy + Downstream Databricks/Synapse"
            confidence = "High"
        elif has_upsert:
            technique = "Copy Upsert (built-in)"
            confidence = "High"
        else:
            technique = "Full Load"
            confidence = "High"

        # Build evidence string
        evidence_parts = []
        if has_cdc:
            evidence_parts.append("cdc_references_found")
        if has_watermark_lookup:
            evidence_parts.append("has_watermark_lookup")
        if has_watermark_query:
            evidence_parts.append("watermark_references_found")
        if has_watermark_params:
            evidence_parts.append("watermark_params")
        if has_downstream_sproc:
            evidence_parts.append("has_downstream_sproc")
        if has_downstream_spark:
            evidence_parts.append("has_downstream_spark")
        if has_upsert:
            evidence_parts.append("copy_upsert_enabled")

        # Get connector info from stats.activities
        source_connector = ""
        sink_connector = ""
        for a in self.stats.activities:
            if (a.get("pipeline_name") == pipeline_name
                    and a.get("factory_name") == factory_name
                    and a.get("activity_name") == act_name):
                source_connector = a.get("source_type_actual", "")
                sink_connector = a.get("sink_type_actual", "")
                break

        return {
            "pipeline_name": pipeline_name,
            "factory_name": factory_name,
            "factory_type": factory_type,
            "subscription_id": sub_id,
            "activity_name": act_name,
            "activity_type": act.get("type", ""),
            "detected_technique": technique,
            "confidence": confidence,
            "evidence": ", ".join(evidence_parts) if evidence_parts else "default",
            "source_connector": source_connector,
            "sink_connector": sink_connector,
            "has_watermark_lookup": has_watermark_lookup,
            "has_downstream_sproc": has_downstream_sproc,
            "has_downstream_spark": has_downstream_spark,
            "copy_upsert_enabled": has_upsert,
            "cdc_references_found": has_cdc,
            "watermark_references_found": has_watermark_query or has_watermark_params,
        }

    def _classify_dataflow_activity(
        self, act, act_name, pipeline_name, factory_name, factory_type, sub_id
    ) -> Optional[Dict]:
        type_props = act.get("typeProperties", {})
        df_ref = type_props.get("dataflow", {}).get("referenceName")
        if not df_ref:
            return None
        df = self.stats.get_dataflow(factory_name, df_ref)
        if df and df.get("source_count", 0) > 0:
            return {
                "pipeline_name": pipeline_name,
                "factory_name": factory_name,
                "factory_type": factory_type,
                "subscription_id": sub_id,
                "activity_name": act_name,
                "activity_type": act.get("type", ""),
                "detected_technique": "Mapping Data Flow (possible CDC)",
                "confidence": "Low",
                "evidence": "dataflow_with_sources",
                "source_connector": "",
                "sink_connector": "",
                "has_watermark_lookup": False,
                "has_downstream_sproc": False,
                "has_downstream_spark": False,
                "copy_upsert_enabled": False,
                "cdc_references_found": False,
                "watermark_references_found": False,
            }
        return None


class MetadataDrivenDetector:
    """Detects metadata-driven / templated pipeline patterns."""

    def __init__(self, stats: Stats, logger: logging.Logger):
        self.stats = stats
        self.logger = logger

    def classify_pipeline(self, pipeline: Dict, factory: Dict) -> Dict:
        """Classify a single pipeline for metadata-driven patterns."""
        factory_name = factory.get("name", pipeline.get("factory_name", ""))
        factory_type = factory.get("type", pipeline.get("factory_type", ""))
        sub_id = factory.get("subscription_id", pipeline.get("subscription_id", ""))
        pipeline_name = pipeline.get("name", "")
        activities = pipeline.get("activities", [])

        # Signal 1: Lookup -> ForEach -> ExecutePipeline
        activities_by_name = {a.get("name", ""): a for a in activities}
        has_lookup_foreach_exec = False
        for act in activities:
            if act.get("type") == "ForEach":
                inner_activities = act.get("typeProperties", {}).get("activities", [])
                inner_types = {ia.get("type") for ia in inner_activities}
                if "ExecutePipeline" in inner_types:
                    deps = {d.get("activity") for d in act.get("dependsOn", [])}
                    dep_types = {activities_by_name.get(d, {}).get("type") for d in deps}
                    if "Lookup" in dep_types:
                        has_lookup_foreach_exec = True

        # Signal 2: Metadata-like parameter names
        param_names = [k.lower() for k in pipeline.get("parameters", {}).keys()]
        metadata_param_count = sum(
            1 for pn in param_names
            if any(mp in pn for mp in METADATA_PARAM_NAMES)
        )
        has_metadata_params = metadata_param_count >= 3

        # Signal 3: Parameterized datasets
        pipeline_datasets = set()
        for a in self.stats.activities:
            if a.get("pipeline_name") == pipeline_name and a.get("factory_name") == factory_name:
                if a.get("source_dataset_name"):
                    pipeline_datasets.add(a["source_dataset_name"])
                if a.get("sink_dataset_name"):
                    pipeline_datasets.add(a["sink_dataset_name"])
        has_parameterized_datasets = any(
            self.stats.get_ds(factory_name, dsn) and self.stats.get_ds(factory_name, dsn).get("parameters")
            for dsn in pipeline_datasets
        )

        # Signal 4: Template grouping hash
        activity_structure = tuple(sorted(a.get("type", "") for a in activities))
        template_hash = hash(activity_structure)

        # Classification
        signals = []
        if has_lookup_foreach_exec:
            signals.append("Lookup->ForEach->ExecutePipeline")
        if has_metadata_params:
            signals.append(f"metadata_params({metadata_param_count})")
        if has_parameterized_datasets:
            signals.append("parameterized_datasets")

        if len(signals) >= 2:
            is_md = "Yes"
            confidence = "High"
        elif len(signals) == 1:
            is_md = "Likely"
            confidence = "Medium"
        else:
            is_md = "No"
            confidence = "N/A"

        return {
            "pipeline_name": pipeline_name,
            "factory_name": factory_name,
            "factory_type": factory_type,
            "subscription_id": sub_id,
            "is_metadata_driven": is_md,
            "confidence": confidence,
            "signals_found": ", ".join(signals) if signals else "",
            "parameter_count": len(pipeline.get("parameters", {})),
            "variable_count": len(pipeline.get("variables", {})),
            "has_foreach_executepipeline": has_lookup_foreach_exec,
            "has_parameterized_datasets": has_parameterized_datasets,
            "metadata_param_count": metadata_param_count,
            "template_group_id": str(template_hash),
        }


class DatabricksAnalyzer:
    """Analyzes Databricks activities to classify compute type."""

    def __init__(self, stats: Stats, logger: logging.Logger):
        self.stats = stats
        self.logger = logger

    def classify_activity(self, act: Dict, pipeline_name: str, factory: Dict) -> Optional[Dict]:
        """Classify a Databricks activity's compute type."""
        act_type = act.get("type", "")
        if not act_type.startswith("Databricks"):
            return None

        factory_name = factory.get("name", "")
        factory_type = factory.get("type", "")
        sub_id = factory.get("subscription_id", "")
        type_props = act.get("typeProperties", {})

        # Resolve linked service name
        ls_name = act.get("linkedServiceName", {}).get("referenceName") or \
                  type_props.get("linkedServiceName", {}).get("referenceName")

        ls = self.stats.get_ls(factory_name, ls_name) if ls_name else None
        ls_details = ls.get("connection_details", {}) if ls else {}

        existing_cluster = ls_details.get("existingClusterId", ls_details.get("existing_cluster_id", ""))
        new_cluster = ls_details.get("newClusterVersion", ls_details.get("newClusterNodeType", ""))
        instance_pool = ls_details.get("instancePoolId", ls_details.get("instance_pool_id", ""))

        if existing_cluster:
            compute_type = "all_purpose"
        elif instance_pool:
            compute_type = "instance_pool"
        elif new_cluster or act_type == "DatabricksJob":
            compute_type = "jobs_cluster"
        else:
            compute_type = "unknown"

        new_cluster_node_type = ls_details.get("newClusterNodeType", "")
        new_cluster_num_workers = ls_details.get(
            "newClusterNumOfWorker", ls_details.get("numberOfWorkers", "")
        )
        new_cluster_version = ls_details.get(
            "newClusterVersion", ls_details.get("newClusterSparkVersion", "")
        )

        notebook_path = type_props.get("notebookPath", "")
        job_id = type_props.get("jobId", "")

        return {
            "pipeline_name": pipeline_name,
            "factory_name": factory_name,
            "factory_type": factory_type,
            "subscription_id": sub_id,
            "activity_name": act.get("name", ""),
            "activity_type": act_type,
            "linked_service_name": ls_name or "",
            "compute_type_inferred": compute_type,
            "notebook_path": notebook_path,
            "job_id": job_id,
            "new_cluster_node_type": new_cluster_node_type,
            "new_cluster_num_workers": str(new_cluster_num_workers),
            "new_cluster_version": new_cluster_version,
            "existing_cluster_id": existing_cluster or "",
            "instance_pool_id": instance_pool or "",
        }


# =============================================================================
# CONCURRENT EXTRACTOR
# =============================================================================

class ConcurrentExtractor:
    """High-performance concurrent resource extractor with detailed analysis."""
    
    def __init__(self, credential, stats: Stats, error_queue: queue.Queue, logger: logging.Logger):
        self.credential = credential
        self.stats = stats
        self.error_queue = error_queue
        self.logger = logger
        self.analyzer = ActivityAnalyzer(stats, logger)
        self.ingestion_classifier = IngestionClassifier(stats, logger)
        self.metadata_detector = MetadataDrivenDetector(stats, logger)
        self.databricks_analyzer = DatabricksAnalyzer(stats, logger)
        self._adf_clients: Dict[str, DataFactoryManagementClient] = {}
        self._client_lock = Lock()
        self._synapse_403_logged: set = set()
        # Pooled HTTP session for Synapse/Fabric direct REST calls
        self._http = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=40, pool_maxsize=MAX_WORKERS)
        self._http.mount("https://", adapter)
    
    def _get_adf_client(self, sub_id: str) -> DataFactoryManagementClient:
        with self._client_lock:
            if sub_id not in self._adf_clients:
                self._adf_clients[sub_id] = DataFactoryManagementClient(self.credential, sub_id)
            return self._adf_clients[sub_id]
    
    def _get_headers(self, scope: str) -> Dict[str, str]:
        token = self.credential.get_token(scope).token
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    def extract_all(
        self,
        adf_factories: List[Dict],
        synapse_workspaces: List[Dict],
        fabric_workspaces: List[Dict]
    ) -> int:
        """Extract all resources with unified progress bar."""
        
        total_sources = len(adf_factories) + len(synapse_workspaces) + len(fabric_workspaces)
        
        print(f"\n📋 Scanning {total_sources} factories for resources...")
        
        # Build tasks with progress
        tasks = []
        with tqdm(total=total_sources, unit="factory", 
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
                  ncols=70, desc="Listing") as pbar:
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                
                for factory in adf_factories:
                    futures.append(("ADF", factory, executor.submit(self._list_adf_resources, factory)))
                
                for ws in synapse_workspaces:
                    futures.append(("Synapse", ws, executor.submit(self._list_synapse_resources, ws)))
                
                for ws in fabric_workspaces:
                    futures.append(("Fabric", ws, executor.submit(self._list_fabric_resources, ws)))
                
                for source_type, source, future in futures:
                    try:
                        result = future.result()
                        if result:
                            tasks.extend(result)
                    except Exception as e:
                        self.logger.error(f"Error listing {source_type} {source.get('name')}: {e}")
                    pbar.update(1)
        
        if not tasks:
            print("   No resources found to extract.")
            return 0
        
        print(f"   Found {len(tasks)} resources")
        self.logger.info(f"Found {len(tasks)} total resources to extract")
        
        # Extract with progress
        print("\n📥 Extracting resource details...")
        
        completed = 0
        with tqdm(total=len(tasks), unit="res", 
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
                  ncols=70, desc="Extracting") as pbar:
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_task = {
                    executor.submit(self._extract_single_resource, task): task
                    for task in tasks
                }
                
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        future.result()
                    except Exception as e:
                        self.error_queue.put(ExtractionError(
                            factory_name=task.factory["name"],
                            resource_type=task.resource_type,
                            error_message=str(e),
                            is_warning=self._is_warning(str(e))
                        ))
                        self.logger.error(f"Error extracting {task.resource_type} '{task.resource_name}' from {task.factory['name']}: {e}")
                    
                    completed += 1
                    pbar.update(1)
        
        return completed
    
    def _is_warning(self, error_msg: str) -> bool:
        warning_patterns = ["Unmapped", "deserialize", "WebConnection", "DatabricksJob", "Airflow"]
        return any(p.lower() in error_msg.lower() for p in warning_patterns)
    
    def _list_adf_resources(self, factory: Dict) -> List[ExtractionTask]:
        """List all resources in an ADF factory."""
        tasks = []
        try:
            client = self._get_adf_client(factory["subscription_id"])
            rg = factory["resource_group"]
            name = factory["name"]
            
            self.stats.add_factory(name, factory)
            
            # Order matters - extract dependencies first
            for ir in client.integration_runtimes.list_by_factory(rg, name):
                tasks.append(ExtractionTask(factory, "ADF", "integration_runtimes", ir.name))
            
            for ls in client.linked_services.list_by_factory(rg, name):
                tasks.append(ExtractionTask(factory, "ADF", "linked_services", ls.name))
            
            for ds in client.datasets.list_by_factory(rg, name):
                tasks.append(ExtractionTask(factory, "ADF", "datasets", ds.name))
            
            for df in client.data_flows.list_by_factory(rg, name):
                tasks.append(ExtractionTask(factory, "ADF", "dataflows", df.name))
            
            for t in client.triggers.list_by_factory(rg, name):
                tasks.append(ExtractionTask(factory, "ADF", "triggers", t.name))
            
            # Pipelines last - they reference everything else
            for p in client.pipelines.list_by_factory(rg, name):
                tasks.append(ExtractionTask(factory, "ADF", "pipelines", p.name))
                
        except Exception as e:
            self.logger.error(f"Error listing ADF resources from {factory['name']}: {e}")
        
        return tasks
    
    def _list_synapse_resources(self, workspace: Dict) -> List[ExtractionTask]:
        """List all resources in a Synapse workspace."""
        tasks = []
        endpoint = workspace.get("dev_endpoint")
        ws_name = workspace["name"]
        
        if not endpoint:
            self.logger.warning(f"Synapse {ws_name}: No dev endpoint available")
            self.stats.add_skipped_synapse(ws_name)
            return tasks
        
        try:
            headers = self._get_headers("https://dev.azuresynapse.net/.default")
            
            # Order: IR, LS, DS, DF, Triggers, Pipelines
            endpoints = [
                ("integration_runtimes", f"{endpoint}/integrationRuntimes?api-version=2020-12-01"),
                ("linked_services", f"{endpoint}/linkedservices?api-version=2020-12-01"),
                ("datasets", f"{endpoint}/datasets?api-version=2020-12-01"),
                ("dataflows", f"{endpoint}/dataflows?api-version=2020-12-01"),
                ("triggers", f"{endpoint}/triggers?api-version=2020-12-01"),
                ("pipelines", f"{endpoint}/pipelines?api-version=2020-12-01"),
            ]
            
            has_access = False
            for resource_type, url in endpoints:
                try:
                    resp = self._http.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                    if resp.ok:
                        has_access = True
                        for item in resp.json().get("value", []):
                            tasks.append(ExtractionTask(workspace, "Synapse", resource_type, item.get("name")))
                    elif resp.status_code == 403:
                        if ws_name not in self._synapse_403_logged:
                            self._synapse_403_logged.add(ws_name)
                            self.logger.warning(f"Synapse {ws_name}: 403 Forbidden")
                            self.stats.add_skipped_synapse(ws_name)
                except Exception as e:
                    self.logger.error(f"Error listing Synapse {ws_name} {resource_type}: {e}")
            
            if has_access:
                self.stats.add_factory(ws_name, workspace)
                    
        except Exception as e:
            self.logger.error(f"Error listing Synapse resources from {ws_name}: {e}")
        
        return tasks
    
    def _list_fabric_resources(self, workspace: Dict) -> List[ExtractionTask]:
        """List all resources in a Fabric workspace."""
        tasks = []
        ws_id = workspace["id"]
        ws_name = workspace["name"]
        
        try:
            headers = self._get_headers("https://api.fabric.microsoft.com/.default")
            self.stats.add_factory(ws_name, workspace)
            
            # Data Pipelines
            try:
                resp = self._http.get(f"{FABRIC_API_URL}/workspaces/{ws_id}/items?type=DataPipeline",
                                   headers=headers, timeout=REQUEST_TIMEOUT)
                if resp.ok:
                    for item in resp.json().get("value", []):
                        tasks.append(ExtractionTask(
                            workspace, "Fabric", "pipelines", 
                            item.get("displayName", item.get("name")),
                            item.get("id")
                        ))
            except Exception as e:
                self.logger.error(f"Error listing Fabric {ws_name} pipelines: {e}")
            
            # Dataflow Gen 1
            try:
                resp = self._http.get(f"{FABRIC_API_URL}/workspaces/{ws_id}/items?type=Dataflow",
                                   headers=headers, timeout=REQUEST_TIMEOUT)
                if resp.ok:
                    for item in resp.json().get("value", []):
                        tasks.append(ExtractionTask(
                            workspace, "Fabric", "dataflows",
                            item.get("displayName", item.get("name")),
                            item.get("id")
                        ))
            except Exception as e:
                self.logger.error(f"Error listing Fabric {ws_name} dataflows: {e}")
            
            # Dataflow Gen 2
            try:
                resp = self._http.get(f"{FABRIC_API_URL}/workspaces/{ws_id}/items?type=DataflowGen2",
                                   headers=headers, timeout=REQUEST_TIMEOUT)
                if resp.ok:
                    for item in resp.json().get("value", []):
                        tasks.append(ExtractionTask(
                            workspace, "Fabric", "dataflows_gen2",
                            item.get("displayName", item.get("name")),
                            item.get("id")
                        ))
            except Exception as e:
                self.logger.error(f"Error listing Fabric {ws_name} DataflowGen2: {e}")
                
        except Exception as e:
            self.logger.error(f"Error listing Fabric resources from {ws_name}: {e}")
        
        return tasks
    
    def _extract_single_resource(self, task: ExtractionTask) -> None:
        """Extract a single resource's full details."""
        if task.factory_type == "ADF":
            self._extract_adf_resource(task)
        elif task.factory_type == "Synapse":
            self._extract_synapse_resource(task)
        elif task.factory_type == "Fabric":
            self._extract_fabric_resource(task)
    
    def _extract_adf_resource(self, task: ExtractionTask) -> None:
        """Extract a single ADF resource."""
        client = self._get_adf_client(task.factory["subscription_id"])
        rg = task.factory["resource_group"]
        factory_name = task.factory["name"]
        resource_name = task.resource_name
        
        if task.resource_type == "integration_runtimes":
            full = client.integration_runtimes.get(rg, factory_name, resource_name)
            props = full.properties.as_dict() if full.properties else {}
            ir_type = props.get("type", "Unknown")

            # Determine IR subtype
            if ir_type == "Managed":
                tp = props.get("typeProperties", props.get("type_properties", {}))
                if tp and tp.get("ssisProperties", tp.get("ssis_properties")):
                    ir_subtype = "SSIS"
                elif tp and tp.get("computeProperties", tp.get("compute_properties", {})).get(
                        "managedVirtualNetwork", tp.get("computeProperties", tp.get("compute_properties", {})).get(
                        "managed_virtual_network")):
                    ir_subtype = "Managed"
                else:
                    ir_subtype = "Azure"
            elif ir_type == "SelfHosted":
                ir_subtype = "SelfHosted"
            else:
                ir_subtype = ir_type

            self.stats.add_integration_runtime({
                "name": full.name,
                "factory_name": factory_name,
                "factory_type": "ADF",
                "resource_group": rg,
                "subscription_id": task.factory["subscription_id"],
                "type": ir_type,
                "ir_subtype": ir_subtype,
                "is_self_hosted": ir_type == "SelfHosted",
                "state": props.get("state"),
                "description": props.get("description")
            }, ir_type, factory_name, "ADF")
            
        elif task.resource_type == "linked_services":
            full = client.linked_services.get(rg, factory_name, resource_name)
            props = full.properties.as_dict() if full.properties else {}
            ls_type = props.get("type", "Unknown")
            
            self.stats.add_linked_service({
                "name": full.name,
                "factory_name": factory_name,
                "factory_type": "ADF",
                "resource_group": rg,
                "subscription_id": task.factory["subscription_id"],
                "type": ls_type,
                "description": props.get("description"),
                "connect_via": props.get("connectVia", {}).get("referenceName"),
                "connection_details": self._sanitize(props)
            }, ls_type, factory_name)
            
        elif task.resource_type == "datasets":
            full = client.datasets.get(rg, factory_name, resource_name)
            props = full.properties.as_dict() if full.properties else {}
            
            self.stats.add_dataset({
                "name": full.name,
                "factory_name": factory_name,
                "factory_type": "ADF",
                "resource_group": rg,
                "subscription_id": task.factory["subscription_id"],
                "type": props.get("type", "Unknown"),
                "linked_service_name": props.get("linkedServiceName", {}).get("referenceName"),
                "folder": props.get("folder", {}).get("name"),
                "schema_columns": len(props.get("schema", [])),
                "parameters": props.get("parameters", {})
            }, factory_name)
            
        elif task.resource_type == "dataflows":
            full = client.data_flows.get(rg, factory_name, resource_name)
            props = full.properties.as_dict() if full.properties else {}
            type_props = props.get("typeProperties", {})
            df_type = props.get("type", "MappingDataFlow")
            
            self.stats.add_dataflow({
                "name": full.name,
                "factory_name": factory_name,
                "factory_type": "ADF",
                "resource_group": rg,
                "subscription_id": task.factory["subscription_id"],
                "type": df_type,
                "folder": props.get("folder", {}).get("name"),
                "source_count": len(type_props.get("sources", [])),
                "sink_count": len(type_props.get("sinks", [])),
                "transformation_count": len(type_props.get("transformations", []))
            }, factory_name)
            
        elif task.resource_type == "triggers":
            full = client.triggers.get(rg, factory_name, resource_name)
            props = full.properties.as_dict() if full.properties else {}
            
            pipelines = [p.get("pipelineReference", {}).get("referenceName") 
                        for p in props.get("pipelines", []) if p.get("pipelineReference")]
            
            self.stats.add_trigger({
                "name": full.name,
                "factory_name": factory_name,
                "factory_type": "ADF",
                "resource_group": rg,
                "subscription_id": task.factory["subscription_id"],
                "type": props.get("type", "Unknown"),
                "runtime_state": props.get("runtimeState"),
                "pipelines": pipelines
            }, factory_name)
            
        elif task.resource_type == "pipelines":
            full = client.pipelines.get(rg, factory_name, resource_name)
            activities = [a.as_dict() if hasattr(a, "as_dict") else {} for a in (full.activities or [])]
            
            pipeline_dict = {
                "name": full.name,
                "factory_name": factory_name,
                "factory_type": "ADF",
                "resource_group": rg,
                "subscription_id": task.factory["subscription_id"],
                "description": full.description,
                "folder": full.folder.name if full.folder else None,
                "activities": activities,
                "parameters": {k: v.as_dict() if hasattr(v, "as_dict") else str(v) 
                               for k, v in (full.parameters or {}).items()},
                "variables": {k: v.as_dict() if hasattr(v, "as_dict") else str(v) 
                              for k, v in (full.variables or {}).items()},
                "concurrency": full.concurrency,
                "full_json": full.as_dict() if hasattr(full, "as_dict") else {}
            }
            self.stats.add_pipeline(pipeline_dict)
            self._analyze_pipeline_activities(pipeline_dict, task.factory)
    
    def _extract_synapse_resource(self, task: ExtractionTask) -> None:
        """Extract a single Synapse resource."""
        endpoint = task.factory.get("dev_endpoint")
        if not endpoint:
            return
        
        headers = self._get_headers("https://dev.azuresynapse.net/.default")
        ws = task.factory
        ws_name = ws["name"]
        resource_name = task.resource_name
        
        if task.resource_type == "integration_runtimes":
            resp = self._http.get(f"{endpoint}/integrationRuntimes/{resource_name}?api-version=2020-12-01",
                               headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.ok:
                ir = resp.json()
                props = ir.get("properties", {})
                ir_type = props.get("type", "Unknown")
                # Determine IR subtype
                if ir_type == "Managed":
                    tp = props.get("typeProperties", {})
                    if tp and tp.get("ssisProperties"):
                        ir_subtype = "SSIS"
                    elif tp and tp.get("computeProperties", {}).get("managedVirtualNetwork"):
                        ir_subtype = "Managed"
                    else:
                        ir_subtype = "Azure"
                elif ir_type == "SelfHosted":
                    ir_subtype = "SelfHosted"
                else:
                    ir_subtype = ir_type
                self.stats.add_integration_runtime({
                    "name": resource_name,
                    "factory_name": ws_name,
                    "factory_type": "Synapse",
                    "resource_group": ws.get("resource_group"),
                    "subscription_id": ws.get("subscription_id"),
                    "type": ir_type,
                    "ir_subtype": ir_subtype,
                    "is_self_hosted": ir_type == "SelfHosted",
                    "state": props.get("state"),
                    "description": props.get("description")
                }, ir_type, ws_name, "Synapse")
                
        elif task.resource_type == "linked_services":
            resp = self._http.get(f"{endpoint}/linkedservices/{resource_name}?api-version=2020-12-01",
                               headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.ok:
                ls = resp.json()
                props = ls.get("properties", {})
                ls_type = props.get("type", "Unknown")
                self.stats.add_linked_service({
                    "name": resource_name,
                    "factory_name": ws_name,
                    "factory_type": "Synapse",
                    "resource_group": ws.get("resource_group"),
                    "subscription_id": ws.get("subscription_id"),
                    "type": ls_type,
                    "description": props.get("description"),
                    "connect_via": props.get("connectVia", {}).get("referenceName"),
                    "connection_details": {}
                }, ls_type, ws_name)
                
        elif task.resource_type == "datasets":
            resp = self._http.get(f"{endpoint}/datasets/{resource_name}?api-version=2020-12-01",
                               headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.ok:
                ds = resp.json()
                props = ds.get("properties", {})
                self.stats.add_dataset({
                    "name": resource_name,
                    "factory_name": ws_name,
                    "factory_type": "Synapse",
                    "resource_group": ws.get("resource_group"),
                    "subscription_id": ws.get("subscription_id"),
                    "type": props.get("type", "Unknown"),
                    "linked_service_name": props.get("linkedServiceName", {}).get("referenceName"),
                    "folder": props.get("folder", {}).get("name"),
                    "schema_columns": len(props.get("schema", [])),
                    "parameters": props.get("parameters", {})
                }, ws_name)
                
        elif task.resource_type == "dataflows":
            resp = self._http.get(f"{endpoint}/dataflows/{resource_name}?api-version=2020-12-01",
                               headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.ok:
                df = resp.json()
                props = df.get("properties", {})
                df_type = props.get("type", "MappingDataFlow")
                self.stats.add_dataflow({
                    "name": resource_name,
                    "factory_name": ws_name,
                    "factory_type": "Synapse",
                    "resource_group": ws.get("resource_group"),
                    "subscription_id": ws.get("subscription_id"),
                    "type": df_type,
                    "folder": props.get("folder", {}).get("name"),
                    "source_count": 0,
                    "sink_count": 0,
                    "transformation_count": 0
                }, ws_name)
                
        elif task.resource_type == "triggers":
            resp = self._http.get(f"{endpoint}/triggers/{resource_name}?api-version=2020-12-01",
                               headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.ok:
                t = resp.json()
                props = t.get("properties", {})
                pipelines = [p.get("pipelineReference", {}).get("referenceName") 
                            for p in props.get("pipelines", []) if p.get("pipelineReference")]
                self.stats.add_trigger({
                    "name": resource_name,
                    "factory_name": ws_name,
                    "factory_type": "Synapse",
                    "resource_group": ws.get("resource_group"),
                    "subscription_id": ws.get("subscription_id"),
                    "type": props.get("type", "Unknown"),
                    "runtime_state": props.get("runtimeState"),
                    "pipelines": pipelines
                }, ws_name)
                
        elif task.resource_type == "pipelines":
            resp = self._http.get(f"{endpoint}/pipelines/{resource_name}?api-version=2020-12-01",
                               headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.ok:
                full = resp.json()
                props = full.get("properties", {})
                activities = props.get("activities", [])
                
                pipeline_dict = {
                    "name": resource_name,
                    "factory_name": ws_name,
                    "factory_type": "Synapse",
                    "resource_group": ws.get("resource_group"),
                    "subscription_id": ws.get("subscription_id"),
                    "description": props.get("description"),
                    "folder": props.get("folder", {}).get("name"),
                    "activities": activities,
                    "parameters": props.get("parameters", {}),
                    "variables": props.get("variables", {}),
                    "full_json": full
                }
                self.stats.add_pipeline(pipeline_dict)
                self._analyze_pipeline_activities(pipeline_dict, ws)
    
    def _extract_fabric_resource(self, task: ExtractionTask) -> None:
        """Extract a single Fabric resource with full definition."""
        ws = task.factory
        ws_id = ws["id"]
        ws_name = ws["name"]
        item_id = task.resource_id
        
        headers = self._get_headers("https://api.fabric.microsoft.com/.default")
        
        if task.resource_type == "pipelines":
            activities = []
            full_json = {}
            
            if item_id:
                try:
                    resp = self._http.post(
                        f"{FABRIC_API_URL}/workspaces/{ws_id}/items/{item_id}/getDefinition",
                        headers=headers,
                        timeout=REQUEST_TIMEOUT
                    )
                    if resp.ok:
                        definition = resp.json()
                        for part in definition.get("definition", {}).get("parts", []):
                            if part.get("path") == "pipeline-content.json":
                                try:
                                    content = base64.b64decode(part.get("payload", "")).decode("utf-8")
                                    pipeline_content = json.loads(content)
                                    activities = pipeline_content.get("properties", {}).get("activities", [])
                                    full_json = pipeline_content
                                except Exception:
                                    pass
                except Exception as e:
                    self.logger.debug(f"Could not get Fabric pipeline definition: {e}")
            
            pipeline_dict = {
                "name": task.resource_name,
                "factory_name": ws_name,
                "factory_type": "Fabric",
                "resource_group": None,
                "subscription_id": None,
                "description": None,
                "folder": None,
                "activities": activities,
                "parameters": full_json.get("properties", {}).get("parameters", {}),
                "variables": full_json.get("properties", {}).get("variables", {}),
                "full_json": full_json
            }
            self.stats.add_pipeline(pipeline_dict)
            
            if activities:
                self._analyze_pipeline_activities(pipeline_dict, ws)
                
        elif task.resource_type == "dataflows":
            self.stats.add_dataflow({
                "name": task.resource_name,
                "factory_name": ws_name,
                "factory_type": "Fabric",
                "resource_group": None,
                "subscription_id": None,
                "type": "Dataflow",
                "folder": None,
                "source_count": 0,
                "sink_count": 0,
                "transformation_count": 0
            }, ws_name)
            
        elif task.resource_type == "dataflows_gen2":
            self.stats.add_dataflow({
                "name": task.resource_name,
                "factory_name": ws_name,
                "factory_type": "Fabric",
                "resource_group": None,
                "subscription_id": None,
                "type": "DataflowGen2",
                "folder": None,
                "source_count": 0,
                "sink_count": 0,
                "transformation_count": 0
            }, ws_name)
    
    def _analyze_pipeline_activities(self, pipeline: Dict, factory: Dict):
        """Parse activities with detailed analysis."""
        factory_name = factory["name"]
        factory_type = factory.get("type", pipeline.get("factory_type"))
        
        for act in pipeline.get("activities", []):
            act_type = act.get("type", "Unknown")
            type_props = act.get("typeProperties", {})
            
            # Base activity info
            activity_dict = {
                "activity_name": act.get("name", "Unknown"),
                "activity_type": act_type,
                "activity_category": self.analyzer.get_activity_category(act_type),
                "pipeline_name": pipeline["name"],
                "factory_name": factory_name,
                "factory_type": factory_type,
                "resource_group": factory.get("resource_group"),
                "subscription_id": factory.get("subscription_id"),
                "depends_on": [d.get("activity") for d in act.get("dependsOn", [])],
                "activity_purpose": self.analyzer.get_activity_purpose(act),
            }
            
            # Get IR type
            activity_dict["ir_type_used"] = self.analyzer.get_ir_type(act, factory_name)
            
            # Linked service reference
            ls_name = None
            if "linkedServiceName" in act:
                ls_name = act["linkedServiceName"].get("referenceName")
            elif "linkedService" in type_props:
                ls_name = type_props["linkedService"].get("referenceName")
            activity_dict["linked_service"] = ls_name
            
            # Copy activity details
            if act_type in ["Copy", "CopyActivity"]:
                copy_details = self.analyzer.extract_copy_details(act, factory_name)
                activity_dict.update(copy_details)
            
            # WebActivity details
            if act_type == "WebActivity":
                web_details = self.analyzer.parse_web_activity(act)
                activity_dict.update(web_details)
            
            # Script activity
            if act_type == "Script":
                activity_dict["script_type"] = type_props.get("scriptType", "Unknown")
            
            # Dataflow activity
            if act_type in ["ExecuteDataFlow", "DataFlow"]:
                df_ref = type_props.get("dataflow", {}).get("referenceName")
                activity_dict["dataflow_name"] = df_ref
                if df_ref:
                    df = self.stats.get_dataflow(factory_name, df_ref)
                    if df:
                        activity_dict["dataflow_type"] = df.get("type", "Unknown")
                    else:
                        activity_dict["dataflow_type"] = "Unknown"
            
            # Databricks classification
            if act_type.startswith("Databricks"):
                db_result = self.databricks_analyzer.classify_activity(
                    act, pipeline["name"], factory
                )
                if db_result:
                    self.stats.add_databricks_classification(db_result)

            self.stats.add_activity(activity_dict)

        # --- After all activities parsed: run pipeline-level classifiers ---
        # Ingestion technique classification
        try:
            techniques = self.ingestion_classifier.classify_pipeline(pipeline, factory)
            if techniques:
                self.stats.batch_add_ingestion_techniques(techniques)
        except Exception as e:
            self.logger.debug(f"Ingestion classification error for {pipeline.get('name')}: {e}")

        # Metadata-driven detection
        try:
            md_result = self.metadata_detector.classify_pipeline(pipeline, factory)
            self.stats.add_metadata_driven_classification(md_result)
        except Exception as e:
            self.logger.debug(f"Metadata-driven detection error for {pipeline.get('name')}: {e}")

    def _sanitize(self, props: Dict) -> Dict:
        """Remove sensitive fields from properties."""
        sensitive = {"connectionstring", "password", "serviceprincipalkey", "accountkey",
                    "sastoken", "sasuri", "credential", "encryptedcredential", "accesstoken", "refreshtoken"}
        result = {}
        for k, v in props.items():
            if k.lower() in sensitive:
                result[k] = "***REDACTED***"
            elif isinstance(v, dict):
                result[k] = self._sanitize(v)
            else:
                result[k] = v
        return result


# =============================================================================
# RUNTIME PROFILER — asyncio + aiohttp for true parallel I/O
# =============================================================================

class RuntimeProfiler:
    """Profiles runtime execution using asyncio + aiohttp for true parallel HTTP.
    
    Uses a single aiohttp.ClientSession with connection pooling and an
    asyncio.Semaphore to cap concurrency. All activity-run queries across
    all factories fire concurrently with a single unified tqdm progress bar.
    """
    
    CONCURRENCY = 100   # max simultaneous HTTP requests (semaphore limit)
    CONN_LIMIT  = 100   # aiohttp TCPConnector connection pool size
    
    def __init__(self, credential, stats: Stats, logger: logging.Logger,
                 days: int = DEFAULT_PROFILING_DAYS, pricing: Dict = None):
        self.credential = credential
        self.stats = stats
        self.logger = logger
        self.days = days
        self._permissions_warnings: List[str] = []
        
        now = datetime.now(timezone.utc)
        self.time_end = now.isoformat()
        self.time_start = (now - timedelta(days=days)).isoformat()
        
        # Detect primary factory region
        self.region = "eastus"
        for f in stats.factories.values():
            if f.get("type") == "ADF" and f.get("location"):
                self.region = f["location"].lower().replace(" ", "")
                break
        
        # Try to fetch live pricing from Azure Retail Prices API (unauthenticated, fast)
        if pricing:
            self.pricing = pricing
            self.pricing_source = "custom"
        else:
            live = self._fetch_live_pricing(self.region)
            if live:
                self.pricing = live
                self.pricing_source = f"Azure Retail Prices API ({self.region})"
            else:
                self.pricing = PRICING
                self.pricing_source = "default list rates"
        
        self.logger.info(f"Pricing source: {self.pricing_source}")
        self.logger.info(f"Pricing rates (azure_ir): {self.pricing.get('azure_ir', {})}")
    
    @staticmethod
    def _fetch_live_pricing(region: str) -> Optional[Dict]:
        """Try Azure Retail Prices API (unauthenticated, public). Returns None on failure."""
        try:
            url = "https://prices.azure.com/api/retail/prices"
            params = {
                "api-version": "2023-01-01-preview",
                "$filter": f"serviceName eq 'Azure Data Factory v2' and armRegionName eq '{region}' and priceType eq 'Consumption'"
            }
            resp = requests.get(url, params=params, timeout=5)
            if resp.status_code != 200:
                return None
            
            items = resp.json().get("Items", [])
            if not items:
                return None
            
            # Parse rates — normalize to per-unit-hour or per-1000 for orchestration
            rates = {}
            for item in items:
                meter = item.get("meterName", "").lower()
                price = item.get("retailPrice", 0)
                unit = item.get("unitOfMeasure", "").lower()
                if price > 0:
                    # Orchestration meters use "1000" unit — price IS per 1000 runs
                    # Other meters use "1 Hour" — price IS per hour
                    rates[meter] = {"price": price, "unit": unit}
            
            if not rates:
                return None
            
            def find_price(keywords, fallback):
                for k, v in rates.items():
                    if all(kw in k for kw in keywords):
                        return v["price"]
                return fallback
            
            return {
                "azure_ir": {
                    "orchestration_per_1000": find_price(["cloud", "orchestration"], 1.00),
                    "data_movement_per_diu_hour": find_price(["cloud", "data movement"], 0.25),
                    "pipeline_activity_per_hour": find_price(["cloud", "pipeline"], 0.005),
                    "external_activity_per_hour": find_price(["cloud", "external"], 0.00025),
                },
                "shir": {
                    "orchestration_per_1000": find_price(["on premises", "orchestration"], 1.50),
                    "data_movement_per_hour": find_price(["on premises", "data movement"], 0.10),
                    "pipeline_activity_per_hour": find_price(["on premises", "pipeline"], 0.005),
                    "external_activity_per_hour": find_price(["on premises", "external"], 0.00025),
                },
                "managed_vnet_ir": {
                    "orchestration_per_1000": find_price(["managed vnet", "orchestration"], 1.00),
                    "data_movement_per_diu_hour": find_price(["managed vnet", "data movement"], 0.25),
                    "pipeline_activity_per_hour": find_price(["managed vnet", "pipeline"], 1.00),
                    "external_activity_per_hour": find_price(["managed vnet", "external"], 1.00),
                }
            }
        except Exception:
            return None
    
    def _get_headers(self) -> Dict[str, str]:
        token = self.credential.get_token("https://management.azure.com/.default").token
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # ------------------------------------------------------------------
    # Sync entry point — called from run_extraction
    # ------------------------------------------------------------------
    def profile_all(self):
        """Sync entry point. Spins up asyncio event loop for true parallel I/O."""
        adf_factories = [
            f for f in self.stats.factories.values() if f.get("type") == "ADF"
        ]
        if not adf_factories:
            print("\n   ⚠️  No ADF factories found for runtime profiling")
            return
        
        try:
            asyncio.run(self._run_all(adf_factories))
        except Exception as e:
            self.logger.error(f"Async profiling error: {e}")
            print(f"\n   ⚠️  Profiling error: {e}")
        
        if self._permissions_warnings:
            print(f"\n   ⚠️  Permission warnings ({len(self._permissions_warnings)}):")
            for w in self._permissions_warnings:
                print(f"      • {w}")
    
    # ------------------------------------------------------------------
    # Async core
    # ------------------------------------------------------------------
    async def _run_all(self, factories: List[Dict]):
        """Async entry: metadata → pipeline runs → activity runs (all parallel)."""
        headers = self._get_headers()
        sem = asyncio.Semaphore(self.CONCURRENCY)
        
        # Use certifi CA bundle — same as requests library — to avoid SSL failures
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(limit=self.CONN_LIMIT, limit_per_host=50, ssl=ssl_ctx)
        timeout = aiohttp.ClientTimeout(total=30)
        
        # Error tracking for console summary
        error_counts = defaultdict(int)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            
            # ── Phase 1: metadata (DBs, tables, SHIR) — all factories in parallel ──
            meta_tasks = []
            for factory in factories:
                base = self._base_url(factory)
                meta_tasks.extend([
                    self._discover_databases(session, headers, base, factory["name"]),
                    self._discover_tables(session, headers, base, factory["name"]),
                    self._profile_shir(session, headers, base, factory["name"]),
                ])
            
            print(f"\n📋 Profiling {len(factories)} factory(s) ({self.days}-day window)...")
            with tqdm(total=len(meta_tasks), unit="task",
                      bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
                      ncols=70, desc="Profiling") as pbar:
                for coro in asyncio.as_completed(meta_tasks):
                    try:
                        await coro
                    except Exception as e:
                        err_key = type(e).__name__
                        if "SSL" in str(e): err_key = "SSLCertVerificationError"
                        elif "403" in str(e): err_key = "403 Forbidden"
                        elif "401" in str(e): err_key = "401 Unauthorized"
                        error_counts[err_key] += 1
                        self.logger.warning(f"Metadata task error ({err_key}): {e}")
                    pbar.update(1)
            
            # Show metadata results + errors
            db_count = len(self.stats.db_instances)
            tbl_count = len(self.stats.table_discovery)
            shir_count = len(self.stats.shir_nodes)
            print(f"   Found {db_count} database instances, {tbl_count} table refs, {shir_count} SHIR nodes")
            if error_counts:
                total_errs = sum(error_counts.values())
                parts = [f"{v}× {k}" for k, v in sorted(error_counts.items(), key=lambda x: -x[1])]
                print(f"   ⚠️  {total_errs} metadata errors: {', '.join(parts)}")

            print()  # blank line between Profiling and Collecting bars

            # ── Collect costs, VM specs, and pipeline runs — unified bar ──
            unique_subs = {}
            for factory in factories:
                sid = factory.get("subscription_id", "")
                if sid and sid not in unique_subs:
                    unique_subs[sid] = [f["name"] for f in factories if f.get("subscription_id") == sid]

            collect_total = (1 if self.stats.shir_nodes else 0) + len(unique_subs) + len(factories)
            all_run_tasks = []   # list of (factory_dict, pipeline_run_dict)
            ir_type_maps = {}    # factory_name -> {ir_name: ir_type}

            with tqdm(total=collect_total, unit="task",
                      bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
                      ncols=70, desc="Collecting") as pbar:

                # VM specs
                if self.stats.shir_nodes:
                    vm_specs = await self._resolve_vm_specs(session, headers, factories)
                    if vm_specs:
                        for node in self.stats.shir_nodes:
                            mname = node.get("machineName") or node.get("nodeName", "")
                            spec = vm_specs.get(mname)
                            if spec:
                                node["totalMemoryMB"] = spec["totalMemoryMB"]
                                node["vmSize"] = spec["vmSize"]
                                if spec.get("numberOfCores"):
                                    node["estimatedCores"] = spec["numberOfCores"]
                    pbar.update(1)

                # Cost Management queries
                for sub_id, factory_names in unique_subs.items():
                    await self._query_subscription_costs(session, headers, sub_id, factory_names)
                    pbar.update(1)

                # Pipeline runs per factory
                for factory in factories:
                    base = self._base_url(factory)
                    fname = factory["name"]
                    runs = await self._fetch_pipeline_runs(session, headers, base, fname)

                    ir_map = {}
                    for ir in self.stats.integration_runtimes:
                        if ir.get("factory_name") == fname:
                            ir_map[ir["name"]] = ir.get("type", "Unknown")
                    ir_type_maps[fname] = ir_map

                    for run in runs:
                        self.stats.add_pipeline_run({
                            "factory_name": fname,
                            "subscription_id": factory.get("subscription_id", ""),
                            "pipelineName": run.get("pipelineName", ""),
                            "runId": run.get("runId", ""),
                            "status": run.get("status", ""),
                            "durationInMs": run.get("durationInMs", 0),
                            "runStart": run.get("runStart", ""),
                            "runEnd": run.get("runEnd", ""),
                            "message": (run.get("message") or "")[:200],
                            "invokedBy": run.get("invokedBy", {}),
                        })
                        all_run_tasks.append((factory, run))
                    pbar.update(1)

            total_runs = len(all_run_tasks)
            if total_runs == 0:
                print(f"   No pipeline runs found in the last {self.days} days")
                return

            print(f"   Found {total_runs} pipeline runs across {len(factories)} factory(s)")
            
            # ── Phase 3: ALL activity-run queries — single progress bar ──
            # Each task is a coroutine that makes one HTTP POST and processes the result
            pipeline_agg = defaultdict(lambda: {
                "totalRuns": 0, "succeededRuns": 0, "failedRuns": 0,
                "cancelledRuns": 0, "inProgressRuns": 0,
                "durations": [], "totalActivityRuns": 0,
                "totalCost": 0.0, "orchCost": 0.0, "ingestionCost": 0.0,
                "pipelineActCost": 0.0, "externalCost": 0.0,
                "dataflowCost": 0.0,
                "subscription_id": "",
            })
            # Pre-populate subscription_id from factory data
            factory_sub_map = {f["name"]: f.get("subscription_id", "") for f in factories}
            totals = {"cost": 0.0, "orch": 0.0, "ingestion": 0.0, "pipeline": 0.0, "external": 0.0}
            
            # Build all coroutines
            coros = [
                self._process_activity_runs(
                    session, sem, headers,
                    self._base_url(factory), factory["name"],
                    run, ir_type_maps[factory["name"]],
                    factory_sub_map
                )
                for factory, run in all_run_tasks
            ]
            
            print(f"\n📥 Scanning activity runs...")
            with tqdm(total=total_runs, unit="run",
                      bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
                      ncols=70, desc="Activity runs") as pbar:
                for coro in asyncio.as_completed(coros):
                    try:
                        result, factory_name, run_dict = await coro
                        if result:
                            key = (factory_name, run_dict.get("pipelineName", ""))
                            agg = pipeline_agg[key]
                            if not agg["subscription_id"]:
                                agg["subscription_id"] = factory_sub_map.get(factory_name, "")
                            agg["totalRuns"] += 1
                            status = run_dict.get("status", "").lower()
                            if status == "succeeded":   agg["succeededRuns"] += 1
                            elif status == "failed":    agg["failedRuns"] += 1
                            elif status == "cancelled": agg["cancelledRuns"] += 1
                            else:                       agg["inProgressRuns"] += 1
                            dur = run_dict.get("durationInMs", 0)
                            if dur: agg["durations"].append(dur)
                            agg["totalActivityRuns"] += result["activity_count"]
                            for k in ("totalCost", "orchCost", "ingestionCost", "pipelineActCost", "externalCost"):
                                agg[k] += result.get(k, 0.0)
                            for tk, rk in [("cost","totalCost"),("orch","orchCost"),
                                           ("ingestion","ingestionCost"),("pipeline","pipelineActCost"),
                                           ("external","externalCost")]:
                                totals[tk] += result.get(rk, 0.0)
                    except Exception as e:
                        self.logger.debug(f"Activity run error: {e}")
                    pbar.update(1)
            
            # ── Store aggregates ──
            factory_totals = defaultdict(lambda: dict(totals))  # per-factory
            for (fname, p_name), agg in pipeline_agg.items():
                durations = agg["durations"]
                self.stats.add_pipeline_job_stat({
                    "factory_name": fname,
                    "subscription_id": agg.get("subscription_id", ""),
                    "pipelineName": p_name,
                    "totalRuns": agg["totalRuns"],
                    "succeededRuns": agg["succeededRuns"],
                    "failedRuns": agg["failedRuns"],
                    "cancelledRuns": agg["cancelledRuns"],
                    "inProgressRuns": agg["inProgressRuns"],
                    "successRate": round(agg["succeededRuns"] / max(agg["totalRuns"], 1) * 100, 1),
                    "avgDurationMin": round(sum(durations) / len(durations) / 60000, 2) if durations else 0,
                    "maxDurationMin": round(max(durations) / 60000, 2) if durations else 0,
                    "minDurationMin": round(min(durations) / 60000, 2) if durations else 0,
                    "totalActivityRuns": agg["totalActivityRuns"],
                    "totalCost": round(agg["totalCost"], 4),
                    "orchCost": round(agg["orchCost"], 4),
                    "ingestionCost": round(agg["ingestionCost"], 4),
                    "pipelineActCost": round(agg["pipelineActCost"], 4),
                    "externalCost": round(agg["externalCost"], 4),
                })
            
            self.stats.set_cost_breakdown({
                "factory_name": ", ".join(f["name"] for f in factories),
                "days": self.days,
                "region": self.region,
                "pricing_source": self.pricing_source,
                "totalPipelineRuns": total_runs,
                "totalEstimatedCost": round(totals["cost"], 4),
                "orchestrationCost": round(totals["orch"], 4),
                "ingestionCost": round(totals["ingestion"], 4),
                "pipelineActivityCost": round(totals["pipeline"], 4),
                "externalActivityCost": round(totals["external"], 4),
            })
    
    # ------------------------------------------------------------------
    # Helper: base URL for a factory
    # ------------------------------------------------------------------
    def _base_url(self, factory: Dict) -> str:
        return (f"{AZURE_MGMT_URL}/subscriptions/{factory['subscription_id']}"
                f"/resourceGroups/{factory['resource_group']}"
                f"/providers/Microsoft.DataFactory/factories/{factory['name']}")
    
    # ------------------------------------------------------------------
    # Pipeline runs (paginated POST — sequential per factory, fast)
    # ------------------------------------------------------------------
    async def _fetch_pipeline_runs(self, session: aiohttp.ClientSession,
                                   headers: Dict, base: str, factory_name: str) -> List[Dict]:
        url = f"{base}/queryPipelineRuns?api-version=2018-06-01"
        body = {"lastUpdatedAfter": self.time_start, "lastUpdatedBefore": self.time_end}
        all_runs = []
        continuation = None
        
        while True:
            req_body = dict(body)
            if continuation:
                req_body["continuationToken"] = continuation
            try:
                async with session.post(url, headers=headers, json=req_body) as resp:
                    if resp.status in (401, 403):
                        self._permissions_warnings.append(
                            f"{factory_name}: Cannot query pipeline runs (need Data Factory Contributor)")
                        return all_runs
                    if resp.status != 200:
                        self.logger.error(f"[{factory_name}] Pipeline runs: HTTP {resp.status}")
                        return all_runs
                    data = await resp.json()
                    runs = data.get("value", [])
                    all_runs.extend(runs)
                    continuation = data.get("continuationToken")
                    if not continuation or not runs:
                        break
            except Exception as e:
                self.logger.error(f"[{factory_name}] Pipeline runs error: {e}")
                break
        
        return all_runs
    
    # ------------------------------------------------------------------
    # Activity runs — one async POST per pipeline run, semaphore-gated
    # ------------------------------------------------------------------
    async def _process_activity_runs(
        self, session: aiohttp.ClientSession, sem: asyncio.Semaphore,
        headers: Dict, base: str, factory_name: str,
        pipeline_run: Dict, ir_type_map: Dict,
        factory_sub_map: Dict = None
    ) -> Tuple[Optional[Dict], str, Dict]:
        """Fetch + process activity runs for one pipeline run. Returns (result, factory_name, run)."""
        if factory_sub_map is None:
            factory_sub_map = {}
        run_id = pipeline_run.get("runId", "")
        url = f"{base}/pipelineruns/{run_id}/queryActivityruns?api-version=2018-06-01"
        body = {"lastUpdatedAfter": self.time_start, "lastUpdatedBefore": self.time_end}
        
        async with sem:
            try:
                async with session.post(url, headers=headers, json=body) as resp:
                    if resp.status != 200:
                        return None, factory_name, pipeline_run
                    data = await resp.json()
                    activities = data.get("value", [])
            except Exception:
                return None, factory_name, pipeline_run
        
        # Process activities (CPU-bound, fast — no lock needed, we aggregate later)
        run_orch = 0.0
        run_ingestion = 0.0
        run_pipeline = 0.0
        run_external = 0.0
        
        for act in activities:
            act_type = act.get("activityType", "")
            output = act.get("output") or {}
            effective_ir = output.get("effectiveIntegrationRuntime", "")
            ir_key = self._classify_ir(effective_ir, ir_type_map)
            rates = self.pricing.get(ir_key, self.pricing["azure_ir"])

            # Parse billingReference for ALL activity types
            billing_ref = output.get("billingReference", {})
            billable_durations = billing_ref.get("billableDuration", [])
            if billable_durations:
                bd = billable_durations[0]
                billing_meter_type = bd.get("meterType", "")
                billable_duration = bd.get("duration", 0)
                billable_unit = bd.get("unit", "")
            else:
                billing_meter_type = ""
                billable_duration = 0
                billable_unit = ""
            
            orch = rates["orchestration_per_1000"] / 1000
            run_orch += orch
            
            duration_ms = act.get("durationInMs", 0) or 0
            duration_hours = max(1, math.ceil(duration_ms / 60000)) / 60
            exec_cost = 0.0
            
            if act_type in COPY_ACTIVITY_TYPES:
                if ir_key == "shir":
                    exec_cost = duration_hours * rates["data_movement_per_hour"]
                else:
                    dius = output.get("usedDataIntegrationUnits",
                           output.get("usedCloudDataMovementUnits", 4))
                    exec_cost = duration_hours * dius * rates["data_movement_per_diu_hour"]
                run_ingestion += exec_cost
                
                exec_details = output.get("executionDetails", [{}])
                detail = exec_details[0] if exec_details else {}
                dius_used = output.get("usedDataIntegrationUnits",
                            output.get("usedCloudDataMovementUnits", 0))
                # Build cost formula string
                if ir_key == "shir":
                    cost_formula = f"{duration_hours:.4f}h x ${rates['data_movement_per_hour']}/h = ${exec_cost:.4f}"
                else:
                    cost_formula = f"{dius_used} DIU x {duration_hours:.4f}h x ${rates['data_movement_per_diu_hour']}/DIU-h = ${exec_cost:.4f}"

                self.stats.add_copy_detail({
                    "factory_name": factory_name,
                    "subscription_id": factory_sub_map.get(factory_name, ""),
                    "activityName": act.get("activityName", ""),
                    "pipelineName": act.get("pipelineName", ""),
                    "runId": run_id, "status": act.get("status", ""),
                    "sourceType": detail.get("source", {}).get("type", ""),
                    "sinkType": detail.get("sink", {}).get("type", ""),
                    "dataReadBytes": output.get("dataRead", 0) or 0,
                    "dataWrittenBytes": output.get("dataWritten", 0) or 0,
                    "rowsRead": output.get("rowsRead", 0) or 0,
                    "rowsCopied": output.get("rowsCopied", 0) or 0,
                    "copyDurationSec": output.get("copyDuration", 0) or 0,
                    "throughputMBps": output.get("throughput", 0) or 0,
                    "diusUsed": dius_used,
                    "parallelCopies": output.get("usedParallelCopies", 0) or 0,
                    "irUsed": effective_ir,
                    "ir_type": ir_key,
                    "queueDurationSec": detail.get("detailedDurations", {}).get("queuingDuration", 0) or 0,
                    "transferDurationSec": detail.get("detailedDurations", {}).get("transferDuration", 0) or 0,
                    "estimatedCost": round(exec_cost + orch, 6),
                    "billing_meter_type": billing_meter_type,
                    "billable_duration": billable_duration,
                    "billable_unit": billable_unit,
                    "cost_formula": cost_formula,
                })
            elif act_type in PIPELINE_ACTIVITY_TYPES:
                exec_cost = duration_hours * rates["pipeline_activity_per_hour"]
                run_pipeline += exec_cost
            elif act_type in EXTERNAL_ACTIVITY_TYPES:
                exec_cost = duration_hours * rates["external_activity_per_hour"]
                run_external += exec_cost
            elif act_type in DATA_FLOW_TYPES:
                exec_cost = duration_hours * DATAFLOW_MIN_VCORES * DATAFLOW_VCORE_RATE
                run_external += exec_cost
            else:
                exec_cost = duration_hours * rates["pipeline_activity_per_hour"]
                run_pipeline += exec_cost
            
            self.stats.add_activity_run({
                "factory_name": factory_name,
                "activityName": act.get("activityName", ""),
                "activityType": act_type,
                "pipelineName": act.get("pipelineName", ""),
                "runId": run_id, "status": act.get("status", ""),
                "durationInMs": duration_ms, "irUsed": effective_ir,
                "irType": ir_key,
                "orchCost": round(orch, 6),
                "execCost": round(exec_cost, 6),
                "totalCost": round(orch + exec_cost, 6),
            })
        
        trigger_orch = self.pricing["azure_ir"]["orchestration_per_1000"] / 1000
        run_orch += trigger_orch
        
        return ({
            "activity_count": len(activities),
            "totalCost": run_orch + run_ingestion + run_pipeline + run_external,
            "orchCost": run_orch, "ingestionCost": run_ingestion,
            "pipelineActCost": run_pipeline, "externalCost": run_external,
        }, factory_name, pipeline_run)
    
    # ------------------------------------------------------------------
    # Metadata: database discovery
    # ------------------------------------------------------------------
    async def _discover_databases(self, session, headers, base, factory_name):
        url = f"{base}/linkedservices?api-version=2018-06-01"
        linked_services = await self._paginated_get(session, headers, url)
        for ls in linked_services:
            props = ls.get("properties", {})
            ls_type = props.get("type", "")
            if ls_type not in DATABASE_LINKED_SERVICE_TYPES:
                continue
            type_props = props.get("typeProperties", {})
            server, database = self._extract_server_db(ls_type, type_props)
            ir_ref = props.get("connectVia", {}).get("referenceName", "AutoResolveIntegrationRuntime")
            self.stats.add_db_instance({
                "factory_name": factory_name, "linkedServiceName": ls.get("name"),
                "type": ls_type, "server": server, "database": database,
                "integrationRuntimeRef": ir_ref,
            })
    
    # ------------------------------------------------------------------
    # Metadata: table discovery
    # ------------------------------------------------------------------
    async def _discover_tables(self, session, headers, base, factory_name):
        url = f"{base}/datasets?api-version=2018-06-01"
        datasets = await self._paginated_get(session, headers, url)
        for ds in datasets:
            props = ds.get("properties", {})
            type_props = props.get("typeProperties", {})
            ls_name = props.get("linkedServiceName", {}).get("referenceName", "")
            schema_name = type_props.get("schema", "")
            if isinstance(schema_name, list): schema_name = ""
            table_name = type_props.get("table", type_props.get("tableName", ""))
            self.stats.add_table_entry({
                "factory_name": factory_name, "datasetName": ds.get("name"),
                "linkedService": ls_name, "schema": schema_name,
                "table": table_name, "type": props.get("type", ""),
            })
    
    # ------------------------------------------------------------------
    # VM lookup: query Azure Compute API for total memory by machine name
    # ------------------------------------------------------------------
    async def _resolve_vm_specs(self, session, headers, factories):
        """Look up VM total memory for SHIR nodes using Azure Compute API."""
        # Collect unique (subscription_id, machine_name) pairs from SHIR nodes
        machine_subs = {}
        for node in self.stats.shir_nodes:
            mname = node.get("machineName") or node.get("nodeName", "")
            if not mname:
                continue
            # Find subscription for this factory
            fname = node.get("factory_name", "")
            sub_id = None
            for f in factories:
                if f["name"] == fname:
                    sub_id = f.get("subscription_id")
                    break
            if sub_id and mname not in machine_subs:
                machine_subs[mname] = sub_id

        if not machine_subs:
            return {}

        # Query VMs per subscription (list all VMs once per sub, then match)
        vm_specs = {}  # machine_name -> {"totalMemoryMB": ..., "vmSize": ...}
        queried_subs = set()
        for mname, sub_id in machine_subs.items():
            if sub_id in queried_subs:
                continue
            queried_subs.add(sub_id)
            url = (f"{AZURE_MGMT_URL}/subscriptions/{sub_id}"
                   f"/providers/Microsoft.Compute/virtualMachines"
                   f"?api-version=2024-03-01&statusOnly=false")
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        self.logger.debug(f"VM list for sub {sub_id[:8]}... returned {resp.status}")
                        continue
                    data = await resp.json()
                    for vm in data.get("value", []):
                        vm_name = vm.get("name", "")
                        props = vm.get("properties", {})
                        hw = props.get("hardwareProfile", {})
                        vm_size = hw.get("vmSize", "")
                        # computerName is the OS hostname (matches SHIR machineName)
                        os_profile = props.get("osProfile", {})
                        computer_name = os_profile.get("computerName", vm_name)
                        vm_specs[vm_name] = {"vmSize": vm_size}
                        vm_specs[computer_name] = {"vmSize": vm_size}
            except Exception as e:
                self.logger.debug(f"VM list error for sub {sub_id[:8]}...: {e}")

        if not vm_specs:
            return {}

        # Now resolve VM sizes to actual memory — query sizes per location
        # Collect unique (sub_id, location) pairs
        sub_locations = set()
        for f in factories:
            sub_id = f.get("subscription_id", "")
            location = f.get("location", "")
            if sub_id and location:
                sub_locations.add((sub_id, location))

        size_specs = {}  # vmSize -> {"memoryInMB": ..., "numberOfCores": ...}
        for sub_id, location in sub_locations:
            url = (f"{AZURE_MGMT_URL}/subscriptions/{sub_id}"
                   f"/providers/Microsoft.Compute/locations/{location}"
                   f"/vmSizes?api-version=2024-03-01")
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                    for size in data.get("value", []):
                        size_specs[size["name"]] = {
                            "memoryInMB": size.get("memoryInMB", 0),
                            "numberOfCores": size.get("numberOfCores", 0),
                        }
            except Exception:
                pass

        # Merge: machine_name -> totalMemoryMB, numberOfCores, vmSize
        result = {}
        for mname in machine_subs:
            spec = vm_specs.get(mname, {})
            vm_size = spec.get("vmSize", "")
            if vm_size and vm_size in size_specs:
                result[mname] = {
                    "totalMemoryMB": size_specs[vm_size]["memoryInMB"],
                    "numberOfCores": size_specs[vm_size]["numberOfCores"],
                    "vmSize": vm_size,
                }

        return result

    # ------------------------------------------------------------------
    # Metadata: SHIR profiling
    # ------------------------------------------------------------------
    async def _profile_shir(self, session, headers, base, factory_name):
        shir_names = [
            ir["name"] for ir in self.stats.integration_runtimes
            if ir.get("factory_name") == factory_name and ir.get("is_self_hosted")
        ]
        if not shir_names:
            irs = await self._paginated_get(session, headers,
                f"{base}/integrationRuntimes?api-version=2018-06-01")
            shir_names = [ir["name"] for ir in irs
                          if ir.get("properties", {}).get("type") == "SelfHosted"]
        for ir_name in shir_names:
            await self._profile_single_shir(session, headers, base, factory_name, ir_name)
    
    async def _profile_single_shir(self, session, headers, base, factory_name, ir_name):
        nodes_status = {}
        try:
            async with session.post(
                f"{base}/integrationRuntimes/{ir_name}/getStatus?api-version=2018-06-01",
                headers=headers
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for node in data.get("properties", {}).get("typeProperties", {}).get("nodes", []):
                        nodes_status[node.get("nodeName", "")] = {
                            "machineName": node.get("machineName", ""),
                            "status": node.get("status", ""),
                            "maxConcurrentJobs": node.get("maxConcurrentJobs", 0),
                            "version": node.get("versionStatus", node.get("version", "")),
                            "lastConnectTime": node.get("lastConnectTime", ""),
                            "lastStartTime": node.get("lastStartTime", ""),
                        }
                elif resp.status in (401, 403):
                    self._permissions_warnings.append(
                        f"{factory_name}: SHIR {ir_name} access denied")
                    return
        except Exception as e:
            self.logger.debug(f"SHIR status error {ir_name}: {e}")
        
        try:
            async with session.post(
                f"{base}/integrationRuntimes/{ir_name}/monitoringData?api-version=2018-06-01",
                headers=headers
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for node in data.get("nodes", []):
                        nn = node.get("nodeName", "")
                        info = nodes_status.get(nn, {})
                        mc = info.get("maxConcurrentJobs", node.get("concurrentJobsLimit", 0))
                        self.stats.add_shir_node({
                            "factory_name": factory_name, "irName": ir_name,
                            "nodeName": nn, "machineName": info.get("machineName", ""),
                            "status": info.get("status", ""), "maxConcurrentJobs": mc,
                            "estimatedCores": 0,
                            "availableMemoryMB": node.get("availableMemoryInMB", 0),
                            "cpuUtilizationPct": node.get("cpuUtilization", 0),
                            "concurrentJobsRunning": node.get("concurrentJobsRunning", 0),
                            "concurrentJobsLimit": node.get("concurrentJobsLimit", 0),
                            "sentBytes": node.get("sentBytes", 0),
                            "receivedBytes": node.get("receivedBytes", 0),
                            "version": info.get("version", ""),
                            "lastConnectTime": info.get("lastConnectTime", ""),
                            "lastStartTime": info.get("lastStartTime", ""),
                        })
                    return
        except Exception:
            pass
        
        for nn, info in nodes_status.items():
            mc = info.get("maxConcurrentJobs", 0)
            self.stats.add_shir_node({
                "factory_name": factory_name, "irName": ir_name,
                "nodeName": nn, "machineName": info.get("machineName", ""),
                "status": info.get("status", ""), "maxConcurrentJobs": mc,
                "estimatedCores": 0,
                "availableMemoryMB": 0, "cpuUtilizationPct": 0,
                "concurrentJobsRunning": 0, "concurrentJobsLimit": mc,
                "sentBytes": 0, "receivedBytes": 0,
                "version": info.get("version", ""),
                "lastConnectTime": info.get("lastConnectTime", ""),
                "lastStartTime": info.get("lastStartTime", ""),
            })
    
    # ------------------------------------------------------------------
    # Cost Management (optional, graceful failure)
    # ------------------------------------------------------------------
    async def _query_subscription_costs(self, session, headers, sub_id, factory_names):
        """Query Cost Management ONCE per subscription, match costs to factories by ResourceId."""
        url = (f"{AZURE_MGMT_URL}/subscriptions/{sub_id}"
               f"/providers/Microsoft.CostManagement/query?api-version=2023-11-01")
        start = (datetime.now(timezone.utc) - timedelta(days=self.days)).strftime("%Y-%m-%dT00:00:00+00:00")
        end = datetime.now(timezone.utc).strftime("%Y-%m-%dT23:59:59+00:00")
        body = {
            "type": "ActualCost", "timeframe": "Custom",
            "timePeriod": {"from": start, "to": end},
            "dataset": {
                "granularity": "None",
                "aggregation": {"totalCost": {"name": "PreTaxCost", "function": "Sum"}},
                "grouping": [
                    {"type": "Dimension", "name": "MeterSubcategory"},
                    {"type": "Dimension", "name": "ResourceId"},
                ],
                "filter": {"dimensions": {
                    "name": "ResourceType", "operator": "In",
                    "values": ["Microsoft.DataFactory/factories"]
                }}
            }
        }
        try:
            async with session.post(url, headers=headers, json=body,
                                    timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status in (401, 403):
                    self._permissions_warnings.append(
                        f"Subscription {sub_id[:8]}...: Cost Management access denied "
                        f"(need Billing Reader or Cost Management Reader on this subscription)")
                    self.stats.cost_mgmt_denied.append(sub_id)
                    return
                if resp.status != 200:
                    self.logger.warning(f"Cost Management query for sub {sub_id[:8]}... returned HTTP {resp.status}")
                    return
                data = await resp.json()
                cols = [c["name"] for c in data.get("properties", {}).get("columns", [])]
                # Build lookup of factory names (lowercase) for matching
                name_set = {n.lower() for n in factory_names}
                for row in data.get("properties", {}).get("rows", []):
                    rd = dict(zip(cols, row))
                    resource_id = rd.get("ResourceId", "")
                    # Extract factory name from resource ID (last segment)
                    rid_lower = resource_id.lower()
                    matched_name = None
                    for fname in name_set:
                        if fname in rid_lower:
                            matched_name = fname
                            break
                    if not matched_name:
                        # Try last segment of resource ID
                        parts = resource_id.rstrip("/").split("/")
                        if parts:
                            last = parts[-1].lower()
                            if last in name_set:
                                matched_name = last
                    
                    if matched_name:
                        # Find original casing
                        orig_name = next((n for n in factory_names if n.lower() == matched_name), matched_name)
                        self.stats.add_actual_cost({
                            "factory_name": orig_name,
                            "meterSubcategory": rd.get("MeterSubcategory", ""),
                            "cost": rd.get("PreTaxCost", 0),
                            "currency": rd.get("Currency", "USD"),
                        })
        except Exception as e:
            self.logger.warning(f"Cost Management query error for sub {sub_id[:8]}...: {type(e).__name__}: {e}")
    
    # ------------------------------------------------------------------
    # Async paginated GET
    # ------------------------------------------------------------------
    async def _paginated_get(self, session, headers, url) -> List[Dict]:
        results = []
        while url:
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status in (401, 403):
                        return results
                    if resp.status != 200:
                        self.logger.warning(f"GET {url.split('?')[0]} returned HTTP {resp.status}")
                        break
                    data = await resp.json()
                    results.extend(data.get("value", []))
                    url = data.get("nextLink")
            except Exception as e:
                self.logger.warning(f"Paginated GET error: {type(e).__name__}: {e}")
                break
        return results
    
    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------
    def _classify_ir(self, ir_name: str, ir_type_map: Dict) -> str:
        if not ir_name:
            return "azure_ir"
        if "autoresolve" in ir_name.lower() or "defaultintegrationruntime" in ir_name.lower():
            return "azure_ir"
        ir_type = ir_type_map.get(ir_name, "")
        if ir_type == "SelfHosted": return "shir"
        if ir_type == "Managed":    return "managed_vnet_ir"
        return "azure_ir"
    
    def _extract_server_db(self, ls_type: str, type_props: Dict) -> Tuple[str, str]:
        server = (type_props.get("server") or type_props.get("host")
                  or type_props.get("fullyQualifiedDomainName") or "")
        database = (type_props.get("database") or type_props.get("catalog")
                    or type_props.get("databaseName") or "")
        if not server:
            conn_str = type_props.get("connectionString", "")
            if isinstance(conn_str, dict): conn_str = conn_str.get("value", "")
            if isinstance(conn_str, str) and conn_str:
                server = self._parse_conn_string(conn_str, "server", "data source")
                if not database:
                    database = self._parse_conn_string(conn_str, "database", "initial catalog")
        if not server and "accountEndpoint" in type_props:
            server = type_props.get("accountEndpoint", "")
        return server, database
    
    @staticmethod
    def _parse_conn_string(conn_str: str, *keys: str) -> str:
        for part in conn_str.split(";"):
            part = part.strip()
            for key in keys:
                if part.lower().startswith(key.lower() + "="):
                    return part.split("=", 1)[1].strip()
        return ""

# =============================================================================
# DATA EXPORT
# =============================================================================

class Exporter:
    """Exports extraction results to CSV files with detailed analysis."""

    def __init__(self, output_dir: str, stats: Stats):
        self.output_dir = output_dir
        self.stats = stats
        # New directory structure: ingestion and orchestration tracks
        self.ingestion_dir = os.path.join(output_dir, "ingestion")
        self.ingestion_details = os.path.join(self.ingestion_dir, "details")
        self.orchestration_dir = os.path.join(output_dir, "orchestration")
        self.orchestration_details = os.path.join(self.orchestration_dir, "details")
        self.assessment_dir = os.path.join(output_dir, "assessment")
        self.assessment_details = os.path.join(self.assessment_dir, "details")
        os.makedirs(self.ingestion_details, exist_ok=True)
        os.makedirs(self.orchestration_details, exist_ok=True)
        os.makedirs(self.assessment_details, exist_ok=True)

    def _sub_name(self, sub_id: str) -> str:
        """Resolve subscription ID to display name."""
        if not sub_id:
            return ""
        return self.stats.subscription_names.get(sub_id, "")

    def _sub_name_for_factory(self, factory_name: str) -> str:
        """Resolve subscription name from factory name.

        For Fabric workspaces (no subscription_id), returns 'Fabric'.
        For Synapse workspaces, prefixes with 'Synapse' if needed.
        """
        f_info = self.stats.factories.get(factory_name, {})
        sid = f_info.get("subscription_id", "")
        sub = self._sub_name(sid)
        if sub:
            return sub
        # No subscription name resolved -- check factory type
        ftype = f_info.get("factory_type", "")
        if ftype == "fabric":
            return "Fabric"
        if ftype == "synapse":
            return "Synapse"
        if not sid:
            return "Fabric"
        return sid[:12] + "..."

    def _enrich_df_with_sub_name(self, df: "pd.DataFrame") -> "pd.DataFrame":
        """Add subscription_name column to a DataFrame that has subscription_id or factory_name."""
        if "subscription_id" in df.columns:
            df["subscription_name"] = df["subscription_id"].apply(lambda sid: self._sub_name(sid) if pd.notna(sid) else "")
        elif "factory_name" in df.columns:
            df["subscription_name"] = df["factory_name"].apply(lambda fn: self._sub_name_for_factory(fn) if pd.notna(fn) else "")
        return df

    @staticmethod
    def _col_label(h: str) -> str:
        """Convert snake_case column key to display label."""
        label = h.replace("_", " ").title()
        label = (label.replace("Adf", "ADF").replace("Vm", "VM")
                      .replace("Gb", "GB").replace("Shir", "SHIR")
                      .replace("Ir", "IR").replace("Id", "ID"))
        return label

    @staticmethod
    def _rows_to_md(title: str, rows: List[Dict], keys: List[str] = None) -> str:
        """Convert a list of row dicts to a markdown table string."""
        if not rows:
            return f"# {title}\n\nNo data.\n"
        if keys is None:
            keys = list(rows[0].keys())
        labels = [Exporter._col_label(k) for k in keys]
        lines = [f"# {title}\n"]
        lines.append("| " + " | ".join(labels) + " |")
        lines.append("| " + " | ".join(["---"] * len(keys)) + " |")
        for row in rows:
            vals = [str(row.get(k, "")) for k in keys]
            lines.append("| " + " | ".join(vals) + " |")
        lines.append("")
        return "\n".join(lines)

    def _export_csv(self, directory: str, name: str, rows: List[Dict]) -> str:
        """Write a list of dicts as CSV to a directory. Returns the CSV path."""
        csv_path = os.path.join(directory, f"{name}.csv")
        pd.DataFrame(rows).to_csv(csv_path, index=False)
        return csv_path

    def _export_df_csv(self, directory: str, name: str, df: "pd.DataFrame") -> str:
        """Write a DataFrame as CSV to a directory. Returns the CSV path."""
        csv_path = os.path.join(directory, f"{name}.csv")
        df.to_csv(csv_path, index=False)
        return csv_path

    def export_all(self) -> Dict[str, str]:
        files = {}

        tasks = []

        # === INGESTION TRACK ===
        tasks.extend([
            ("ingestion/details/connectors_by_activity", self._export_connectors_by_activity),
            ("ingestion/details/connectors_by_pipeline", self._export_connectors_by_pipeline),
            ("ingestion/details/connectors_by_factory", self._export_connectors_by_factory),
            ("ingestion/details/connectors_by_subscription", self._export_connectors_by_subscription),
            ("ingestion/details/connectors_by_tenant", self._export_connectors_by_tenant),
            ("ingestion/details/ingestion_technique", self._export_ingestion_technique),
            ("ingestion/details/runtime_by_activity", self._export_runtime_by_activity),
            ("ingestion/details/runtime_by_pipeline", self._export_runtime_by_pipeline),
            ("ingestion/details/runtime_by_factory", self._export_runtime_by_factory),
            ("ingestion/details/runtime_by_subscription", self._export_runtime_by_subscription),
            ("ingestion/details/runtime_by_tenant", self._export_runtime_by_tenant),
            ("ingestion/details/copy_activity_details", self._export_copy_details),
            ("ingestion/details/database_instances", self._export_db_instances),
            ("ingestion/details/table_discovery", self._export_table_discovery),
            ("ingestion/details/shir_nodes", self._export_shir_nodes),
            ("ingestion/details/integration_runtimes", self._export_integration_runtimes),
            ("ingestion/details/ingestion_cost_breakdown", self._export_ingestion_cost_breakdown),
            ("ingestion/ingestion", self._export_ingestion_rollup),
        ])

        # === ORCHESTRATION TRACK ===
        tasks.extend([
            ("orchestration/details/activity_types_by_activity", self._export_activity_types_by_activity),
            ("orchestration/details/activity_types_by_pipeline", self._export_activity_types_by_pipeline),
            ("orchestration/details/activity_types_by_factory", self._export_activity_types_by_factory),
            ("orchestration/details/activity_types_by_subscription", self._export_activity_types_by_subscription),
            ("orchestration/details/activity_types_by_tenant", self._export_activity_types_by_tenant),
            ("orchestration/details/pipeline_runs_detail", self._export_pipeline_runs_detail),
            ("orchestration/details/pipeline_runs_by_pipeline", self._export_pipeline_runs_by_pipeline),
            ("orchestration/details/pipeline_runs_by_factory", self._export_pipeline_runs_by_factory),
            ("orchestration/details/pipeline_runs_by_subscription", self._export_pipeline_runs_by_subscription),
            ("orchestration/details/pipeline_runs_by_tenant", self._export_pipeline_runs_by_tenant),
            ("orchestration/details/dataflows", self._export_dataflows),
            ("orchestration/details/datasets", self._export_datasets),
            ("orchestration/details/linked_services", self._export_linked_services),
            ("orchestration/details/triggers", self._export_triggers),
            ("orchestration/details/metadata_driven_analysis", self._export_metadata_driven_analysis),
            ("orchestration/details/databricks_analysis", self._export_databricks_analysis),
            ("orchestration/details/orchestration_cost_breakdown", self._export_orchestration_cost_breakdown),
            ("orchestration/details/managed_airflow", self._export_managed_airflow),
            ("orchestration/orchestration", self._export_orchestration_rollup),
        ])

        # === ASSESSMENT (wkmigrate migration readiness) ===
        tasks.extend([
            ("assessment/details/assessment_by_pipeline", self._export_assessment_by_pipeline),
            ("assessment/details/assessment_by_factory", self._export_assessment_by_factory),
            ("assessment/details/assessment_by_subscription", self._export_assessment_by_subscription),
            ("assessment/details/assessment_by_tenant", self._export_assessment_by_tenant),
            ("assessment/details/connectors_by_pipeline", self._export_assessment_connectors_by_pipeline),
            ("assessment/details/connectors_by_factory", self._export_assessment_connectors_by_factory),
            ("assessment/details/connectors_by_subscription", self._export_assessment_connectors_by_subscription),
            ("assessment/details/connectors_by_tenant", self._export_assessment_connectors_by_tenant),
            ("assessment/assessment", self._export_assessment_csv),
            ("assessment/assessment_md", self._export_assessment_md),
        ])

        # === OVERALL ===
        tasks.append(("overall", self._export_overall_rollup))

        total = len(tasks)

        with tqdm(total=total, unit="file",
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
                  ncols=70, desc="Saving") as pbar:
            for label, fn in tasks:
                try:
                    path = fn()
                    files[label] = path
                except Exception as e:
                    # Don't fail the whole export for one file
                    files[label] = f"ERROR: {e}"
                pbar.update(1)

        return files

    # =====================================================================
    # INGESTION TRACK EXPORTS
    # =====================================================================

    def _get_copy_activities_df(self) -> "pd.DataFrame":
        """Get DataFrame of all Copy/DataMovement activities with enrichment."""
        rows = []
        for a in self.stats.activities:
            if a.get("activity_category") != "DataMovement":
                continue
            sub_id = a.get("subscription_id", "")
            rows.append({
                "activity_name": a.get("activity_name", ""),
                "pipeline_name": a.get("pipeline_name", ""),
                "factory_name": a.get("factory_name", ""),
                "factory_type": a.get("factory_type", ""),
                "subscription_id": sub_id,
                "subscription_name": self._sub_name(sub_id),
                "source_connector": friendly_connector_name(a.get("source_type_actual", "")),
                "sink_connector": friendly_connector_name(a.get("sink_type_actual", "")),
                "source_linked_service": a.get("source_linked_service", ""),
                "sink_linked_service": a.get("sink_linked_service", ""),
                "source_dataset": a.get("source_dataset_name", ""),
                "sink_dataset": a.get("sink_dataset_name", ""),
                "ir_type_used": a.get("ir_type_used", ""),
            })
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def _export_connectors_by_activity(self) -> str:
        df = self._get_copy_activities_df()
        return self._export_df_csv(self.ingestion_details, "connectors_by_activity", df)

    def _export_connectors_by_pipeline(self) -> str:
        df = self._get_copy_activities_df()
        if df.empty:
            return self._export_df_csv(self.ingestion_details, "connectors_by_pipeline", df)
        grouped = df.groupby(["pipeline_name", "factory_name", "subscription_id", "subscription_name"]).agg(
            copy_activity_count=("activity_name", "count"),
            distinct_source_connectors=("source_connector", "nunique"),
            distinct_sink_connectors=("sink_connector", "nunique"),
        ).reset_index()
        return self._export_df_csv(self.ingestion_details, "connectors_by_pipeline", grouped)

    def _export_connectors_by_factory(self) -> str:
        df = self._get_copy_activities_df()
        if df.empty:
            return self._export_df_csv(self.ingestion_details, "connectors_by_factory", df)
        # Melt source/sink into connector_type
        src = df[["factory_name", "subscription_id", "subscription_name", "source_connector"]].rename(
            columns={"source_connector": "connector_type"})
        src["role"] = "source"
        snk = df[["factory_name", "subscription_id", "subscription_name", "sink_connector"]].rename(
            columns={"sink_connector": "connector_type"})
        snk["role"] = "sink"
        melted = pd.concat([src, snk], ignore_index=True)
        melted = melted[melted["connector_type"] != ""]
        pivot = melted.groupby(["factory_name", "subscription_id", "subscription_name", "connector_type"]).agg(
            count_as_source=("role", lambda x: (x == "source").sum()),
            count_as_sink=("role", lambda x: (x == "sink").sum()),
        ).reset_index()
        return self._export_df_csv(self.ingestion_details, "connectors_by_factory", pivot)

    def _export_connectors_by_subscription(self) -> str:
        df = self._get_copy_activities_df()
        if df.empty:
            return self._export_df_csv(self.ingestion_details, "connectors_by_subscription", df)
        src = df[["factory_name", "subscription_id", "subscription_name", "source_connector"]].rename(
            columns={"source_connector": "connector_type"})
        src["role"] = "source"
        snk = df[["factory_name", "subscription_id", "subscription_name", "sink_connector"]].rename(
            columns={"sink_connector": "connector_type"})
        snk["role"] = "sink"
        melted = pd.concat([src, snk], ignore_index=True)
        melted = melted[melted["connector_type"] != ""]
        pivot = melted.groupby(["subscription_id", "subscription_name", "connector_type"]).agg(
            count_as_source=("role", lambda x: (x == "source").sum()),
            count_as_sink=("role", lambda x: (x == "sink").sum()),
            factory_count_using_it=("factory_name", "nunique"),
        ).reset_index()
        return self._export_df_csv(self.ingestion_details, "connectors_by_subscription", pivot)

    def _export_connectors_by_tenant(self) -> str:
        df = self._get_copy_activities_df()
        if df.empty:
            return self._export_df_csv(self.ingestion_details, "connectors_by_tenant", df)
        src = df[["factory_name", "subscription_id", "source_connector"]].rename(
            columns={"source_connector": "connector_type"})
        src["role"] = "source"
        snk = df[["factory_name", "subscription_id", "sink_connector"]].rename(
            columns={"sink_connector": "connector_type"})
        snk["role"] = "sink"
        melted = pd.concat([src, snk], ignore_index=True)
        melted = melted[melted["connector_type"] != ""]
        pivot = melted.groupby("connector_type").agg(
            count_as_source=("role", lambda x: (x == "source").sum()),
            count_as_sink=("role", lambda x: (x == "sink").sum()),
            factory_count=("factory_name", "nunique"),
            subscription_count=("subscription_id", "nunique"),
        ).reset_index()
        return self._export_df_csv(self.ingestion_details, "connectors_by_tenant", pivot)

    def _export_ingestion_technique(self) -> str:
        rows = self.stats.ingestion_techniques
        if not rows:
            return self._export_csv(self.ingestion_details, "ingestion_technique", [])
        # Enrich with subscription_name
        for r in rows:
            r["subscription_name"] = self._sub_name(r.get("subscription_id", ""))
        return self._export_csv(self.ingestion_details, "ingestion_technique", rows)

    def _export_runtime_by_activity(self) -> str:
        """Enhanced copy_activity_details with billing fields."""
        if not self.stats.copy_activity_details:
            return self._export_csv(self.ingestion_details, "runtime_by_activity", [])
        df = pd.DataFrame(self.stats.copy_activity_details)
        self._enrich_df_with_sub_name(df)
        return self._export_df_csv(self.ingestion_details, "runtime_by_activity", df)

    def _export_runtime_by_pipeline(self) -> str:
        if not self.stats.copy_activity_details:
            return self._export_csv(self.ingestion_details, "runtime_by_pipeline", [])
        df = pd.DataFrame(self.stats.copy_activity_details)
        days = self.stats.cost_breakdown.get("days", 90) if self.stats.cost_breakdown else 90
        grouped = df.groupby(["pipelineName", "factory_name"]).agg(
            subscription_id=("subscription_id", "first"),
            total_copy_runs=("runId", "count"),
            avg_duration_sec=("copyDurationSec", "mean"),
            max_duration_sec=("copyDurationSec", "max"),
            total_data_read_bytes=("dataReadBytes", "sum"),
            total_data_written_bytes=("dataWrittenBytes", "sum"),
            total_rows_copied=("rowsCopied", "sum"),
            total_estimated_cost=("estimatedCost", "sum"),
            dominant_ir_type=("ir_type", lambda x: x.mode().iloc[0] if len(x) > 0 else ""),
            avg_dius_used=("diusUsed", "mean"),
        ).reset_index()
        grouped["subscription_name"] = grouped["subscription_id"].apply(lambda s: self._sub_name(s) if pd.notna(s) else "")
        grouped["runs_per_day_avg"] = (grouped["total_copy_runs"] / max(days, 1)).round(2)
        return self._export_df_csv(self.ingestion_details, "runtime_by_pipeline", grouped)

    def _export_runtime_by_factory(self) -> str:
        if not self.stats.copy_activity_details:
            return self._export_csv(self.ingestion_details, "runtime_by_factory", [])
        df = pd.DataFrame(self.stats.copy_activity_details)
        days = self.stats.cost_breakdown.get("days", 90) if self.stats.cost_breakdown else 90
        grouped = df.groupby("factory_name").agg(
            subscription_id=("subscription_id", "first"),
            total_copy_runs=("runId", "count"),
            total_data_read_bytes=("dataReadBytes", "sum"),
            total_data_written_bytes=("dataWrittenBytes", "sum"),
            total_rows_copied=("rowsCopied", "sum"),
            total_estimated_cost=("estimatedCost", "sum"),
        ).reset_index()
        grouped["subscription_name"] = grouped["subscription_id"].apply(lambda s: self._sub_name(s) if pd.notna(s) else "")
        grouped["projected_monthly_cost"] = ((grouped["total_estimated_cost"] / max(days, 1)) * 30).round(4)
        return self._export_df_csv(self.ingestion_details, "runtime_by_factory", grouped)

    def _export_runtime_by_subscription(self) -> str:
        if not self.stats.copy_activity_details:
            return self._export_csv(self.ingestion_details, "runtime_by_subscription", [])
        df = pd.DataFrame(self.stats.copy_activity_details)
        days = self.stats.cost_breakdown.get("days", 90) if self.stats.cost_breakdown else 90
        grouped = df.groupby("subscription_id").agg(
            total_copy_runs=("runId", "count"),
            total_data_read_bytes=("dataReadBytes", "sum"),
            total_data_written_bytes=("dataWrittenBytes", "sum"),
            total_estimated_cost=("estimatedCost", "sum"),
            factory_count=("factory_name", "nunique"),
        ).reset_index()
        grouped["subscription_name"] = grouped["subscription_id"].apply(lambda s: self._sub_name(s) if pd.notna(s) else "")
        grouped["projected_monthly_cost"] = ((grouped["total_estimated_cost"] / max(days, 1)) * 30).round(4)
        return self._export_df_csv(self.ingestion_details, "runtime_by_subscription", grouped)

    def _export_runtime_by_tenant(self) -> str:
        if not self.stats.copy_activity_details:
            return self._export_csv(self.ingestion_details, "runtime_by_tenant", [])
        df = pd.DataFrame(self.stats.copy_activity_details)
        days = self.stats.cost_breakdown.get("days", 90) if self.stats.cost_breakdown else 90
        row = {
            "total_copy_runs": len(df),
            "total_data_read_bytes": int(df["dataReadBytes"].sum()),
            "total_data_written_bytes": int(df["dataWrittenBytes"].sum()),
            "total_estimated_cost": round(float(df["estimatedCost"].sum()), 4),
            "factory_count": df["factory_name"].nunique(),
            "subscription_count": df["subscription_id"].nunique() if "subscription_id" in df.columns else 0,
            "projected_monthly_cost": round(float(df["estimatedCost"].sum()) / max(days, 1) * 30, 4),
        }
        return self._export_csv(self.ingestion_details, "runtime_by_tenant", [row])

    def _export_copy_details(self) -> str:
        if not self.stats.copy_activity_details:
            return self._export_csv(self.ingestion_details, "copy_activity_details", [])
        df = pd.DataFrame(self.stats.copy_activity_details)
        self._enrich_df_with_sub_name(df)
        return self._export_df_csv(self.ingestion_details, "copy_activity_details", df)

    def _export_db_instances(self) -> str:
        if not self.stats.db_instances:
            return self._export_csv(self.ingestion_details, "database_instances", [])
        df = pd.DataFrame(self.stats.db_instances)
        self._enrich_df_with_sub_name(df)
        # Add table_count per db instance
        ls_table_counts = defaultdict(int)
        for t in self.stats.table_discovery:
            key = (t.get("factory_name", ""), t.get("linkedService", ""))
            if t.get("table"):
                ls_table_counts[key] += 1
        df["table_count"] = df.apply(
            lambda r: ls_table_counts.get((r.get("factory_name", ""), r.get("linkedServiceName", "")), 0), axis=1
        )
        # Add pipeline_count_referencing
        ls_pipeline_counts = defaultdict(set)
        for a in self.stats.activities:
            for ls_field in ("source_linked_service", "sink_linked_service"):
                ls_val = a.get(ls_field)
                if ls_val:
                    ls_pipeline_counts[(a.get("factory_name", ""), ls_val)].add(a.get("pipeline_name", ""))
        df["pipeline_count_referencing"] = df.apply(
            lambda r: len(ls_pipeline_counts.get((r.get("factory_name", ""), r.get("linkedServiceName", "")), set())), axis=1
        )
        return self._export_df_csv(self.ingestion_details, "database_instances", df)

    def _export_table_discovery(self) -> str:
        if not self.stats.table_discovery:
            return self._export_csv(self.ingestion_details, "table_discovery", [])
        df = pd.DataFrame(self.stats.table_discovery)
        self._enrich_df_with_sub_name(df)
        return self._export_df_csv(self.ingestion_details, "table_discovery", df)

    def _export_shir_nodes(self) -> str:
        if not self.stats.shir_nodes:
            return self._export_csv(self.ingestion_details, "shir_nodes", [])
        df = pd.DataFrame(self.stats.shir_nodes)
        self._enrich_df_with_sub_name(df)
        return self._export_df_csv(self.ingestion_details, "shir_nodes", df)

    def _export_integration_runtimes(self) -> str:
        """Full integration runtime definitions with run-level metrics (no truncation)."""
        if not self.stats.integration_runtimes:
            return self._export_csv(self.ingestion_details, "integration_runtimes", [])
        # Build per-IR run metrics
        ir_metrics = defaultdict(lambda: {"activity_runs": 0, "diu_hours": 0.0, "cost": 0.0,
                                           "data_read_bytes": 0, "data_written_bytes": 0})
        for c in self.stats.copy_activity_details:
            key = (c.get("factory_name", ""), c.get("irUsed", ""))
            diu = c.get("diusUsed", 0) or 0
            dur_sec = c.get("copyDurationSec", 0) or 0
            ir_metrics[key]["activity_runs"] += 1
            ir_metrics[key]["diu_hours"] += diu * dur_sec / 3600.0
            ir_metrics[key]["cost"] += c.get("estimatedCost", 0) or 0
            ir_metrics[key]["data_read_bytes"] += c.get("dataReadBytes", 0) or 0
            ir_metrics[key]["data_written_bytes"] += c.get("dataWrittenBytes", 0) or 0
        shir_nodes_by_ir = defaultdict(list)
        for n in self.stats.shir_nodes:
            shir_nodes_by_ir[(n.get("factory_name", ""), n.get("irName", ""))].append(n)
        rows = []
        for ir in self.stats.integration_runtimes:
            key = (ir.get("factory_name", ""), ir.get("name", ""))
            m = ir_metrics.get(key, {})
            nodes = shir_nodes_by_ir.get(key, [])
            rows.append({
                "ir_name": ir.get("name", ""),
                "factory_name": ir.get("factory_name", ""),
                "factory_type": ir.get("factory_type", ""),
                "subscription_id": ir.get("subscription_id", ""),
                "ir_type": ir.get("type", ""),
                "ir_subtype": ir.get("ir_subtype", ""),
                "state": ir.get("state", ""),
                "node_count": len(nodes) if nodes else 0,
                "total_cores": sum(n.get("estimatedCores", 0) for n in nodes),
                "total_memory_gb": round(sum(n.get("totalMemoryMB", 0) or n.get("availableMemoryMB", 0) for n in nodes) / 1024, 1) if nodes else 0,
                "activity_runs": m.get("activity_runs", 0),
                "diu_hours": round(m.get("diu_hours", 0.0), 4),
                "data_volume_gb": round(((m.get("data_read_bytes", 0)) + (m.get("data_written_bytes", 0))) / 1_073_741_824, 4),
                "estimated_cost": round(m.get("cost", 0.0), 6),
            })
        df = pd.DataFrame(rows)
        self._enrich_df_with_sub_name(df)
        return self._export_df_csv(self.ingestion_details, "integration_runtimes", df)

    def _export_ingestion_cost_breakdown(self) -> str:
        """Ingestion cost = data movement costs."""
        rows = []
        for ps in self.stats.pipeline_job_stats:
            ing_cost = ps.get("ingestionCost", 0)
            if ing_cost > 0:
                rows.append({
                    "factory_name": ps.get("factory_name", ""),
                    "subscription_id": ps.get("subscription_id", ""),
                    "subscription_name": self._sub_name(ps.get("subscription_id", "")),
                    "pipeline_name": ps.get("pipelineName", ""),
                    "ingestion_cost": round(ing_cost, 6),
                    "total_runs": ps.get("totalRuns", 0),
                })
        return self._export_csv(self.ingestion_details, "ingestion_cost_breakdown", rows)

    def _export_ingestion_rollup(self) -> str:
        """Ingestion rollup CSV + richly structured MD."""
        rows = []
        def add(metric, value):
            rows.append({"metric": metric, "value": value})

        copy_acts = [a for a in self.stats.activities if a.get("activity_category") == "DataMovement"]
        days = self.stats.cost_breakdown.get("days", 90) if self.stats.cost_breakdown else 90
        total_read = sum(c.get("dataReadBytes", 0) or 0 for c in self.stats.copy_activity_details)
        total_written = sum(c.get("dataWrittenBytes", 0) or 0 for c in self.stats.copy_activity_details)
        total_ing_cost = sum(c.get("estimatedCost", 0) for c in self.stats.copy_activity_details)
        total_copy_runs = len(self.stats.copy_activity_details)

        add("Total Copy Activities Defined", len(copy_acts))
        add("Total Copy Activity Runs", total_copy_runs)
        add("Total Data Volume (GB)", round((total_read + total_written) / 1_073_741_824, 2))
        add("Total Ingestion Cost (estimated)", f"${total_ing_cost:.4f}")
        add("Runs Per Day Average", round(total_copy_runs / max(days, 1), 2))
        add("Database Instance Count", len(self.stats.db_instances))
        add("Table Reference Count", len(self.stats.table_discovery))

        # Write CSV
        csv_path = os.path.join(self.ingestion_dir, "ingestion.csv")
        pd.DataFrame(rows).to_csv(csv_path, index=False)

        # ---- Build rich MD ----
        md = ["# Ingestion Rollup\n"]

        # === Section 1: Ingestion Overview (tenant level) ===
        md.append("## 1. Ingestion Overview (Tenant Level)\n")
        md.append("| Metric | Value |")
        md.append("| --- | --- |")
        md.append(f"| Total Copy Activities Defined | {len(copy_acts)} |")
        md.append(f"| Total Copy Activity Runs | {total_copy_runs} |")
        md.append(f"| Total Data Read (GB) | {round(total_read / 1_073_741_824, 2)} |")
        md.append(f"| Total Data Written (GB) | {round(total_written / 1_073_741_824, 2)} |")
        md.append(f"| Total Data Volume (GB) | {round((total_read + total_written) / 1_073_741_824, 2)} |")
        md.append(f"| Total Ingestion Cost (estimated) | ${total_ing_cost:.4f} |")
        md.append(f"| Runs Per Day Average | {round(total_copy_runs / max(days, 1), 2)} |")
        md.append(f"| Profiling Window | {days} days |")
        md.append("")

        # === Section 2: Connectors - ALL source and sink types (with friendly names + consolidation) ===
        md.append("## 2. Connectors\n")
        src_counts = defaultdict(int)
        snk_counts = defaultdict(int)
        for a in copy_acts:
            src = friendly_connector_name(a.get("source_type_actual", ""))
            snk = friendly_connector_name(a.get("sink_type_actual", ""))
            if src:
                src_counts[src] += 1
            if snk:
                snk_counts[snk] += 1
        all_connectors = sorted(set(list(src_counts.keys()) + list(snk_counts.keys())))
        if all_connectors:
            conn_rows = []
            for c in all_connectors:
                s_cnt = src_counts.get(c, 0)
                k_cnt = snk_counts.get(c, 0)
                conn_rows.append((c, s_cnt, k_cnt, s_cnt + k_cnt))
            conn_rows.sort(key=lambda x: -x[3])
            md.append("| Connector | As Source | As Sink | Total |")
            md.append("| --- | --- | --- | --- |")
            t_src = t_snk = t_total = 0
            for c, s_cnt, k_cnt, total in conn_rows:
                md.append(f"| {c} | {s_cnt} | {k_cnt} | {total} |")
                t_src += s_cnt; t_snk += k_cnt; t_total += total
            md.append(f"| **Total** | **{t_src}** | **{t_snk}** | **{t_total}** |")
        else:
            md.append("No connector data available.")
        md.append("")

        # === Section 3: Ingestion Techniques ===
        md.append("## 3. Ingestion Techniques\n")
        tech_counts = defaultdict(int)
        tech_details = defaultdict(list)
        for t in self.stats.ingestion_techniques:
            technique = t.get("detected_technique", "Unknown")
            tech_counts[technique] += 1
            tech_details[technique].append(t)
        if tech_counts:
            md.append("| Technique | Count | % of Total |")
            md.append("| --- | --- | --- |")
            total_tech = max(len(self.stats.ingestion_techniques), 1)
            for tech, cnt in sorted(tech_counts.items(), key=lambda x: -x[1]):
                pct = round(cnt / total_tech * 100, 1)
                md.append(f"| {tech} | {cnt} | {pct}% |")
        else:
            md.append("No ingestion technique data available.")
        md.append("")

        md.append("### Technique Detection Methodology\n")
        md.append("Each copy activity is classified based on the pipeline structure around it:\n")
        md.append("- **Full Load**: Copy activity with no downstream transformation activity in the same pipeline.")
        md.append("- **Full Load + Stored Procedure**: Copy activity followed by a SqlServerStoredProcedure activity downstream (directly or transitively).")
        md.append("- **Full Load + Databricks / Synapse / HDInsight**: Copy activity followed by a downstream activity whose type indicates a specific vendor (e.g., DatabricksNotebook, SynapseNotebook, HDInsightHive). The vendor name is derived from the downstream activity type or linked service type.")
        md.append("- **Incremental with Watermark**: A Lookup activity upstream of the copy that uses parameter naming conventions suggestive of watermark tracking (e.g., watermark, lastModified, incrementalStart).")
        md.append("- **Copy Upsert (built-in)**: The copy activity sink has writeBehavior set to 'upsert' or uses a merge/upsert option.")
        md.append("- **Native CDC**: Detection is pattern-based on SQL query text containing CDC-related keywords (e.g., cdc.fn_cdc_get_all_changes, __$operation, CHANGE_TRACKING).")
        md.append("")
        md.append("### Assumptions and Limitations\n")
        md.append("- Stored procedure contents cannot be inspected -- we only know a stored proc runs after copy. Whether it performs MERGE, UPSERT, or other logic is unknown.")
        md.append("- Databricks notebook contents cannot be inspected -- only the notebook path is available.")
        md.append("- CDC detection is pattern-based on SQL query text found in the pipeline definition. Actual database-level CDC enablement cannot be verified.")
        md.append("- Watermark detection checks for Lookup activities upstream of copy with parameter naming conventions. False positives are possible if parameter names happen to match.")
        md.append("- Classification is heuristic and should be validated.")
        md.append("")

        # === Helper: count IRs defined for a set of factories ===
        def _count_irs_for_factories(factory_set):
            """Return (total_ir_count, azure_count, shir_count, ssis_count, managed_vnet_count) for a set of factory names.

            Unknown/Airflow IRs are excluded from all counts (they are not data-movement IRs).
            """
            total_ir = azure = shir = ssis = managed_vnet = 0
            for ir in self.stats.integration_runtimes:
                if ir.get("factory_name") in factory_set:
                    sub = ir.get("ir_subtype", "")
                    if sub == "SelfHosted":
                        shir += 1
                        total_ir += 1
                    elif sub == "SSIS":
                        ssis += 1
                        total_ir += 1
                    elif sub == "Azure":
                        azure += 1
                        total_ir += 1
                    elif sub == "Managed":
                        managed_vnet += 1
                        total_ir += 1
                    # Unknown/Airflow IRs are excluded from counts
            return total_ir, azure, shir, ssis, managed_vnet

        # === Section 4: Breakdown by Subscription ===
        md.append("## 4. Breakdown by Subscription\n")
        sub_ids = set()
        for f_info in self.stats.factories.values():
            sid = f_info.get("subscription_id", "")
            if sid:
                sub_ids.add(sid)
        if sub_ids and self.stats.copy_activity_details:
            cd_df = pd.DataFrame(self.stats.copy_activity_details)
            md.append("| Subscription | Factories | Total IRs | Azure IRs | SHIRs | SSIS IRs | Managed VNet IRs | Copy Activities | Copy Runs | Data Volume (GB) | Est. Cost |")
            md.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
            gt_fac = gt_irs = gt_azure = gt_shirs = gt_ssis = gt_mvnet = gt_acts = gt_runs_cnt = 0
            gt_vol = gt_cost = 0.0
            for sid in sorted(sub_ids):
                sub_display = self._sub_name(sid) or sid[:12] + "..."
                sub_factories = {name for name, f in self.stats.factories.items() if f.get("subscription_id") == sid}
                sub_copy_acts = [a for a in copy_acts if a.get("subscription_id") == sid]
                sub_runs = cd_df[cd_df["subscription_id"] == sid] if "subscription_id" in cd_df.columns else pd.DataFrame()
                sub_vol = (sub_runs["dataReadBytes"].sum() + sub_runs["dataWrittenBytes"].sum()) / 1_073_741_824 if not sub_runs.empty else 0
                sub_cost = sub_runs["estimatedCost"].sum() if not sub_runs.empty else 0
                s_irs, s_azure, s_shirs, s_ssis, s_mvnet = _count_irs_for_factories(sub_factories)
                md.append(f"| {sub_display} | {len(sub_factories)} | {s_irs} | {s_azure} | {s_shirs} | {s_ssis} | {s_mvnet} | {len(sub_copy_acts)} | {len(sub_runs)} | {sub_vol:.2f} | ${sub_cost:.4f} |")
                gt_fac += len(sub_factories); gt_irs += s_irs; gt_azure += s_azure; gt_shirs += s_shirs; gt_ssis += s_ssis; gt_mvnet += s_mvnet
                gt_acts += len(sub_copy_acts); gt_runs_cnt += len(sub_runs)
                gt_vol += sub_vol; gt_cost += sub_cost
            md.append(f"| **Total** | **{gt_fac}** | **{gt_irs}** | **{gt_azure}** | **{gt_shirs}** | **{gt_ssis}** | **{gt_mvnet}** | **{gt_acts}** | **{gt_runs_cnt}** | **{gt_vol:.2f}** | **${gt_cost:.4f}** |")
        else:
            md.append("No subscription-level data available.")
        md.append("")

        # === Section 5: Breakdown by Factory (top 5 + Other) ===
        md.append("## 5. Breakdown by Factory\n")
        if copy_acts or self.stats.copy_activity_details:
            factory_copy_defined = defaultdict(list)
            for a in copy_acts:
                factory_copy_defined[a.get("factory_name", "")].append(a)
            factory_copy_runs = defaultdict(list)
            for c in self.stats.copy_activity_details:
                factory_copy_runs[c.get("factory_name", "")].append(c)
            all_factory_names = set(factory_copy_defined.keys()) | set(factory_copy_runs.keys())
            factory_rows = []
            for fname in all_factory_names:
                defined_acts = factory_copy_defined.get(fname, [])
                runs = factory_copy_runs.get(fname, [])
                factory_cost = sum(c.get("estimatedCost", 0) for c in runs)
                factory_vol = sum((c.get("dataReadBytes", 0) or 0) + (c.get("dataWrittenBytes", 0) or 0) for c in runs) / 1_073_741_824
                f_irs, f_azure, f_shirs, f_ssis, f_mvnet = _count_irs_for_factories({fname})
                factory_rows.append({
                    "subscription": self._sub_name_for_factory(fname),
                    "factory": fname,
                    "irs": f_irs, "azure": f_azure, "shirs": f_shirs, "ssis": f_ssis, "mvnet": f_mvnet,
                    "copy_activities": len(defined_acts), "copy_runs": len(runs),
                    "data_volume_gb": factory_vol, "cost": factory_cost,
                })
            factory_rows.sort(key=lambda x: (-x["cost"], -x["irs"]))
            md.append("| Subscription | Factory | Total IRs | Azure IRs | SHIRs | SSIS IRs | Managed VNet IRs | Copy Activities | Copy Runs | Data Volume (GB) | Est. Cost |")
            md.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
            gt_irs = gt_azure = gt_shirs = gt_ssis = gt_mvnet = gt_acts = gt_runs_cnt = 0
            gt_vol = gt_cost = 0.0
            top_5 = factory_rows[:5]
            remainder = factory_rows[5:]
            for fr in top_5:
                md.append(f"| {fr['subscription']} | {fr['factory']} | {fr['irs']} | {fr['azure']} | {fr['shirs']} | {fr['ssis']} | {fr['mvnet']} | {fr['copy_activities']} | {fr['copy_runs']} | {fr['data_volume_gb']:.2f} | ${fr['cost']:.4f} |")
                gt_irs += fr["irs"]; gt_azure += fr["azure"]; gt_shirs += fr["shirs"]; gt_ssis += fr["ssis"]; gt_mvnet += fr["mvnet"]
                gt_acts += fr["copy_activities"]; gt_runs_cnt += fr["copy_runs"]
                gt_vol += fr["data_volume_gb"]; gt_cost += fr["cost"]
            if remainder:
                r_irs = sum(fr["irs"] for fr in remainder)
                r_azure = sum(fr["azure"] for fr in remainder)
                r_shirs = sum(fr["shirs"] for fr in remainder)
                r_ssis = sum(fr["ssis"] for fr in remainder)
                r_mvnet = sum(fr["mvnet"] for fr in remainder)
                r_acts = sum(fr["copy_activities"] for fr in remainder)
                r_runs = sum(fr["copy_runs"] for fr in remainder)
                r_vol = sum(fr["data_volume_gb"] for fr in remainder)
                r_cost = sum(fr["cost"] for fr in remainder)
                md.append(f"| | Other ({len(remainder)} factories) | {r_irs} | {r_azure} | {r_shirs} | {r_ssis} | {r_mvnet} | {r_acts} | {r_runs} | {r_vol:.2f} | ${r_cost:.4f} |")
                gt_irs += r_irs; gt_azure += r_azure; gt_shirs += r_shirs; gt_ssis += r_ssis; gt_mvnet += r_mvnet
                gt_acts += r_acts; gt_runs_cnt += r_runs
                gt_vol += r_vol; gt_cost += r_cost
            md.append(f"| **Total** | | **{gt_irs}** | **{gt_azure}** | **{gt_shirs}** | **{gt_ssis}** | **{gt_mvnet}** | **{gt_acts}** | **{gt_runs_cnt}** | **{gt_vol:.2f}** | **${gt_cost:.4f}** |")
        else:
            md.append("No factory-level ingestion data available.")
        md.append("")

        # === Section 6: Integration Runtime Breakdown ===
        md.append("## 6. Integration Runtime Breakdown\n")

        # Build run-level metrics by ir_type classification (azure_ir / shir / managed_vnet_ir)
        ir_agg = defaultdict(lambda: {"activity_runs": 0, "diu_hours": 0.0, "cost": 0.0})
        ir_agg_by_sub = defaultdict(lambda: defaultdict(lambda: {"activity_runs": 0, "diu_hours": 0.0, "cost": 0.0}))
        # Per-IR-definition metrics keyed by (factory_name, normalized_ir_name)
        ir_def_metrics = defaultdict(lambda: {"activity_runs": 0, "diu_hours": 0.0, "cost": 0.0,
                                               "data_read_bytes": 0, "data_written_bytes": 0})
        # Track runtime IR details for IRs not in definitions (auto-provisioned)
        _runtime_ir_info = {}  # (factory, normalized_name) -> {ir_type, subscription_id}

        def _normalize_ir_name(name: str) -> str:
            """Strip region suffix like ' (Switzerland North)' from effectiveIntegrationRuntime names."""
            import re
            return re.sub(r"\s*\([^)]*\)\s*$", "", name).strip() if name else ""

        for c in self.stats.copy_activity_details:
            ir_key = c.get("ir_type", "azure_ir")
            if ir_key == "shir":
                label = "Self-Hosted IR"
            elif ir_key == "managed_vnet_ir":
                label = "Managed VNet IR"
            else:
                label = "Azure IR"
            diu = c.get("diusUsed", 0) or 0
            dur_sec = c.get("copyDurationSec", 0) or 0
            diu_hours = diu * dur_sec / 3600.0
            cost = c.get("estimatedCost", 0) or 0
            ir_agg[label]["activity_runs"] += 1
            ir_agg[label]["diu_hours"] += diu_hours
            ir_agg[label]["cost"] += cost
            sub_id = c.get("subscription_id", "")
            ir_agg_by_sub[sub_id][label]["activity_runs"] += 1
            ir_agg_by_sub[sub_id][label]["diu_hours"] += diu_hours
            ir_agg_by_sub[sub_id][label]["cost"] += cost
            # Per-definition metrics — normalize IR name to match definitions
            ir_ref_raw = c.get("irUsed", "")
            ir_ref = _normalize_ir_name(ir_ref_raw)
            factory = c.get("factory_name", "")
            dm = ir_def_metrics[(factory, ir_ref)]
            dm["activity_runs"] += 1
            dm["diu_hours"] += diu_hours
            dm["cost"] += cost
            dm["data_read_bytes"] += c.get("dataReadBytes", 0) or 0
            dm["data_written_bytes"] += c.get("dataWrittenBytes", 0) or 0
            # Track runtime info for unmatched IRs
            rt_key = (factory, ir_ref)
            if rt_key not in _runtime_ir_info:
                _runtime_ir_info[rt_key] = {"ir_type": ir_key, "subscription_id": sub_id}

        # Count IR definitions by subtype (exclude Unknown/Airflow — not data-movement IRs)
        ir_def_counts = defaultdict(int)
        for ir in self.stats.integration_runtimes:
            sub = ir.get("ir_subtype", "")
            if sub == "SelfHosted":
                ir_def_counts["Self-Hosted IR"] += 1
            elif sub == "SSIS":
                ir_def_counts["SSIS IR"] += 1
            elif sub == "Managed":
                ir_def_counts["Managed VNet IR"] += 1
            elif sub == "Azure":
                ir_def_counts["Azure IR"] += 1
            # Unknown/Airflow IRs excluded from IR definition counts

        # Build SHIR node lookup: (factory_name, ir_name) -> list of node dicts
        shir_nodes_by_ir = defaultdict(list)
        for n in self.stats.shir_nodes:
            shir_nodes_by_ir[(n.get("factory_name", ""), n.get("irName", ""))].append(n)

        # 6a. Tenant-level aggregate
        if self.stats.integration_runtimes or ir_agg:
            md.append("### Tenant Level\n")
            md.append("| IR Type | Defined | Activity Runs | Total DIU-Hours | Est. Cost |")
            md.append("| --- | --- | --- | --- | --- |")
            all_ir_labels = sorted(set(list(ir_def_counts.keys()) + list(ir_agg.keys())))
            gt_defs = gt_runs = 0; gt_diu = gt_cost = 0.0
            for label in all_ir_labels:
                defs = ir_def_counts.get(label, 0)
                vals = ir_agg.get(label, {"activity_runs": 0, "diu_hours": 0.0, "cost": 0.0})
                md.append(f"| {label} | {defs} | {vals['activity_runs']} | {vals['diu_hours']:.2f} | ${vals['cost']:.4f} |")
                gt_defs += defs; gt_runs += vals["activity_runs"]
                gt_diu += vals["diu_hours"]; gt_cost += vals["cost"]
            md.append(f"| **Total** | **{gt_defs}** | **{gt_runs}** | **{gt_diu:.2f}** | **${gt_cost:.4f}** |")
            md.append("")

            # 6b. Subscription-level aggregate
            if ir_agg_by_sub:
                md.append("### By Subscription\n")
                md.append("| Subscription | Azure IRs | SHIRs | SSIS IRs | Managed VNet IRs | Activity Runs | DIU-Hours | Est. Cost |")
                md.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
                gt_azure = gt_shir = gt_ssis = gt_mvnet = gt_runs = 0; gt_diu = gt_cost = 0.0
                for sid in sorted(ir_agg_by_sub.keys()):
                    sub_display = self._sub_name(sid) or sid[:12] + "..."
                    sub_data = ir_agg_by_sub[sid]
                    sub_factories = {name for name, f in self.stats.factories.items() if f.get("subscription_id") == sid}
                    _, s_azure, s_shir, s_ssis, s_mvnet = _count_irs_for_factories(sub_factories)
                    s_runs = sum(v["activity_runs"] for v in sub_data.values())
                    s_diu = sum(v["diu_hours"] for v in sub_data.values())
                    s_cost = sum(v["cost"] for v in sub_data.values())
                    md.append(f"| {sub_display} | {s_azure} | {s_shir} | {s_ssis} | {s_mvnet} | {s_runs} | {s_diu:.2f} | ${s_cost:.4f} |")
                    gt_azure += s_azure; gt_shir += s_shir; gt_ssis += s_ssis; gt_mvnet += s_mvnet
                    gt_runs += s_runs; gt_diu += s_diu; gt_cost += s_cost
                md.append(f"| **Total** | **{gt_azure}** | **{gt_shir}** | **{gt_ssis}** | **{gt_mvnet}** | **{gt_runs}** | **{gt_diu:.2f}** | **${gt_cost:.4f}** |")
                md.append("")

            # 6c. Per-type detail tables
            _ir_subtype_to_label = {"Azure": "Azure IR", "SelfHosted": "Self-Hosted IR",
                                     "SSIS": "SSIS IR", "Managed": "Managed VNet IR"}

            # --- Azure IRs ---
            azure_irs = [ir for ir in self.stats.integration_runtimes if ir.get("ir_subtype") == "Azure"]
            # Collect keys of defined IRs so we can find runtime-only (auto-provisioned) IRs
            _defined_ir_keys = set()
            for ir in self.stats.integration_runtimes:
                _defined_ir_keys.add((ir.get("factory_name", ""), ir.get("name", "")))
            # Find runtime-only Azure IRs not in definitions
            runtime_only_azure = []
            for (factory, ir_name), info in _runtime_ir_info.items():
                if (factory, ir_name) not in _defined_ir_keys and info["ir_type"] == "azure_ir":
                    runtime_only_azure.append({"name": ir_name, "factory_name": factory,
                                                "subscription_id": info["subscription_id"]})
            all_azure_irs = azure_irs or runtime_only_azure  # ensure we enter the block
            if azure_irs or runtime_only_azure:
                md.append("### Azure IRs\n")
                md.append("| IR Name | Factory | State | Activity Runs | DIU-Hours | Data Volume (GB) | Est. Cost |")
                md.append("| --- | --- | --- | --- | --- | --- | --- |")
                azure_rows = []
                for ir in azure_irs:
                    key = (ir.get("factory_name", ""), ir.get("name", ""))
                    m = ir_def_metrics.get(key, {})
                    vol = ((m.get("data_read_bytes", 0) or 0) + (m.get("data_written_bytes", 0) or 0)) / 1_073_741_824
                    azure_rows.append({"name": ir.get("name", ""), "factory": ir.get("factory_name", ""),
                                       "state": ir.get("state", "") or "N/A",
                                       "runs": m.get("activity_runs", 0), "diu": m.get("diu_hours", 0.0),
                                       "vol": vol, "cost": m.get("cost", 0.0)})
                # Add runtime-only (auto-provisioned) Azure IRs with their metrics
                for ir in runtime_only_azure:
                    key = (ir["factory_name"], ir["name"])
                    m = ir_def_metrics.get(key, {})
                    vol = ((m.get("data_read_bytes", 0) or 0) + (m.get("data_written_bytes", 0) or 0)) / 1_073_741_824
                    azure_rows.append({"name": ir["name"] + " *", "factory": ir["factory_name"],
                                       "state": "Auto-Provisioned",
                                       "runs": m.get("activity_runs", 0), "diu": m.get("diu_hours", 0.0),
                                       "vol": vol, "cost": m.get("cost", 0.0)})
                azure_rows.sort(key=lambda x: -x["cost"])
                gt_runs = 0; gt_diu = gt_vol = gt_cost = 0.0
                top = azure_rows[:5]; rest = azure_rows[5:]
                for r in top:
                    md.append(f"| {r['name']} | {r['factory']} | {r['state']} | {r['runs']} | {r['diu']:.2f} | {r['vol']:.2f} | ${r['cost']:.4f} |")
                    gt_runs += r["runs"]; gt_diu += r["diu"]; gt_vol += r["vol"]; gt_cost += r["cost"]
                if rest:
                    o_runs = sum(r["runs"] for r in rest); o_diu = sum(r["diu"] for r in rest)
                    o_vol = sum(r["vol"] for r in rest); o_cost = sum(r["cost"] for r in rest)
                    md.append(f"| Other ({len(rest)} Azure IRs) | | | {o_runs} | {o_diu:.2f} | {o_vol:.2f} | ${o_cost:.4f} |")
                    gt_runs += o_runs; gt_diu += o_diu; gt_vol += o_vol; gt_cost += o_cost
                md.append(f"| **Total ({len(azure_rows)})** | | | **{gt_runs}** | **{gt_diu:.2f}** | **{gt_vol:.2f}** | **${gt_cost:.4f}** |")
                if runtime_only_azure:
                    md.append("")
                    md.append("*\\* Auto-provisioned IRs were not explicitly defined in the factory but were used at runtime (e.g., AutoResolveIntegrationRuntime).*")
                md.append("")

            # --- Self-Hosted IRs ---
            shir_irs = [ir for ir in self.stats.integration_runtimes if ir.get("ir_subtype") == "SelfHosted"]
            # Find runtime-only SHIRs not in definitions
            runtime_only_shir = []
            for (factory, ir_name), info in _runtime_ir_info.items():
                if (factory, ir_name) not in _defined_ir_keys and info["ir_type"] == "shir":
                    runtime_only_shir.append({"name": ir_name, "factory_name": factory,
                                               "subscription_id": info["subscription_id"]})
            if shir_irs or runtime_only_shir:
                md.append("### Self-Hosted IRs\n")
                md.append("| IR Name | Factory | Nodes | Cores | Memory (GB) | Last Connected | Activity Runs | Data Volume (GB) | Est. Cost |")
                md.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
                shir_rows = []
                for ir in shir_irs:
                    key = (ir.get("factory_name", ""), ir.get("name", ""))
                    m = ir_def_metrics.get(key, {})
                    vol = ((m.get("data_read_bytes", 0) or 0) + (m.get("data_written_bytes", 0) or 0)) / 1_073_741_824
                    nodes = shir_nodes_by_ir.get(key, [])
                    if nodes:
                        node_count = len(nodes)
                        cores = sum(n.get("estimatedCores", 0) for n in nodes)
                        cores_str = str(cores) if cores else "Unknown"
                        mem_mb = sum(n.get("totalMemoryMB", 0) or n.get("availableMemoryMB", 0) for n in nodes)
                        mem_str = f"{round(mem_mb / 1024, 1)}" if mem_mb else "Unknown"
                        last_connects = [n.get("lastConnectTime", "") for n in nodes if n.get("lastConnectTime")]
                        last_connect_str = max(last_connects)[:10] if last_connects else "N/A"
                    else:
                        node_count = "N/A"; cores_str = "N/A"; mem_str = "N/A"; last_connect_str = "N/A"
                    shir_rows.append({"name": ir.get("name", ""), "factory": ir.get("factory_name", ""),
                                      "nodes": node_count, "cores": cores_str, "mem": mem_str,
                                      "last_connected": last_connect_str,
                                      "runs": m.get("activity_runs", 0), "vol": vol, "cost": m.get("cost", 0.0)})
                # Add runtime-only SHIRs
                for ir in runtime_only_shir:
                    key = (ir["factory_name"], ir["name"])
                    m = ir_def_metrics.get(key, {})
                    vol = ((m.get("data_read_bytes", 0) or 0) + (m.get("data_written_bytes", 0) or 0)) / 1_073_741_824
                    shir_rows.append({"name": ir["name"] + " *", "factory": ir["factory_name"],
                                      "nodes": "N/A", "cores": "N/A", "mem": "N/A",
                                      "last_connected": "N/A",
                                      "runs": m.get("activity_runs", 0), "vol": vol, "cost": m.get("cost", 0.0)})
                shir_rows.sort(key=lambda x: -x["cost"])
                gt_runs = 0; gt_vol = gt_cost = 0.0
                top = shir_rows[:5]; rest = shir_rows[5:]
                for r in top:
                    md.append(f"| {r['name']} | {r['factory']} | {r['nodes']} | {r['cores']} | {r['mem']} | {r['last_connected']} | {r['runs']} | {r['vol']:.2f} | ${r['cost']:.4f} |")
                    gt_runs += r["runs"]; gt_vol += r["vol"]; gt_cost += r["cost"]
                if rest:
                    o_runs = sum(r["runs"] for r in rest); o_vol = sum(r["vol"] for r in rest); o_cost = sum(r["cost"] for r in rest)
                    md.append(f"| Other ({len(rest)} SHIRs) | | | | | | {o_runs} | {o_vol:.2f} | ${o_cost:.4f} |")
                    gt_runs += o_runs; gt_vol += o_vol; gt_cost += o_cost
                md.append(f"| **Total ({len(shir_rows)})** | | | | | | **{gt_runs}** | **{gt_vol:.2f}** | **${gt_cost:.4f}** |")
                md.append("")

                # SHIR node-level detail
                if self.stats.shir_nodes:
                    md.append("#### SHIR Node Details\n")
                    md.append("| Node Name | Factory | IR Name | Status | Cores | Memory (GB) | Last Connected |")
                    md.append("| --- | --- | --- | --- | --- | --- | --- |")
                    for n in self.stats.shir_nodes:
                        cores = n.get("estimatedCores", 0)
                        cores_str = str(cores) if cores else "Unknown"
                        mem_mb = n.get("totalMemoryMB", 0) or n.get("availableMemoryMB", 0)
                        mem_str = f"{round(mem_mb / 1024, 1)}" if mem_mb else "Unknown"
                        last_connect = n.get("lastConnectTime", "")
                        last_connect_str = last_connect[:10] if last_connect else "N/A"
                        md.append(f"| {n.get('nodeName', '')} | {n.get('factory_name', '')} | {n.get('irName', '')} | {n.get('status', '')} | {cores_str} | {mem_str} | {last_connect_str} |")
                    md.append("")
                    md.append("*Note: Est. Cost reflects only ADF data movement charges for copy activities through this SHIR. It does not include VM infrastructure costs.*")
                    md.append("")

            # --- SSIS IRs ---
            ssis_irs = [ir for ir in self.stats.integration_runtimes if ir.get("ir_subtype") == "SSIS"]
            if ssis_irs:
                md.append("### SSIS IRs\n")
                md.append("| IR Name | Factory | State | Activity Runs | Est. Cost |")
                md.append("| --- | --- | --- | --- | --- |")
                ssis_rows = []
                for ir in ssis_irs:
                    key = (ir.get("factory_name", ""), ir.get("name", ""))
                    m = ir_def_metrics.get(key, {})
                    ssis_rows.append({"name": ir.get("name", ""), "factory": ir.get("factory_name", ""),
                                      "state": ir.get("state", "") or "N/A",
                                      "runs": m.get("activity_runs", 0), "cost": m.get("cost", 0.0)})
                ssis_rows.sort(key=lambda x: -x["cost"])
                gt_runs = 0; gt_cost = 0.0
                top = ssis_rows[:5]; rest = ssis_rows[5:]
                for r in top:
                    md.append(f"| {r['name']} | {r['factory']} | {r['state']} | {r['runs']} | ${r['cost']:.4f} |")
                    gt_runs += r["runs"]; gt_cost += r["cost"]
                if rest:
                    o_runs = sum(r["runs"] for r in rest); o_cost = sum(r["cost"] for r in rest)
                    md.append(f"| Other ({len(rest)} SSIS IRs) | | | {o_runs} | ${o_cost:.4f} |")
                    gt_runs += o_runs; gt_cost += o_cost
                md.append(f"| **Total ({len(ssis_rows)})** | | | **{gt_runs}** | **${gt_cost:.4f}** |")
                md.append("")

            # --- Managed VNet IRs ---
            mvnet_irs = [ir for ir in self.stats.integration_runtimes if ir.get("ir_subtype") == "Managed"]
            if mvnet_irs:
                md.append("### Managed VNet IRs\n")
                md.append("| IR Name | Factory | State | Activity Runs | DIU-Hours | Data Volume (GB) | Est. Cost |")
                md.append("| --- | --- | --- | --- | --- | --- | --- |")
                mvnet_rows = []
                for ir in mvnet_irs:
                    key = (ir.get("factory_name", ""), ir.get("name", ""))
                    m = ir_def_metrics.get(key, {})
                    vol = ((m.get("data_read_bytes", 0) or 0) + (m.get("data_written_bytes", 0) or 0)) / 1_073_741_824
                    mvnet_rows.append({"name": ir.get("name", ""), "factory": ir.get("factory_name", ""),
                                       "state": ir.get("state", "") or "N/A",
                                       "runs": m.get("activity_runs", 0), "diu": m.get("diu_hours", 0.0),
                                       "vol": vol, "cost": m.get("cost", 0.0)})
                mvnet_rows.sort(key=lambda x: -x["cost"])
                gt_runs = 0; gt_diu = gt_vol = gt_cost = 0.0
                top = mvnet_rows[:5]; rest = mvnet_rows[5:]
                for r in top:
                    md.append(f"| {r['name']} | {r['factory']} | {r['state']} | {r['runs']} | {r['diu']:.2f} | {r['vol']:.2f} | ${r['cost']:.4f} |")
                    gt_runs += r["runs"]; gt_diu += r["diu"]; gt_vol += r["vol"]; gt_cost += r["cost"]
                if rest:
                    o_runs = sum(r["runs"] for r in rest); o_diu = sum(r["diu"] for r in rest)
                    o_vol = sum(r["vol"] for r in rest); o_cost = sum(r["cost"] for r in rest)
                    md.append(f"| Other ({len(rest)} Managed VNet IRs) | | | {o_runs} | {o_diu:.2f} | {o_vol:.2f} | ${o_cost:.4f} |")
                    gt_runs += o_runs; gt_diu += o_diu; gt_vol += o_vol; gt_cost += o_cost
                md.append(f"| **Total ({len(mvnet_rows)})** | | | **{gt_runs}** | **{gt_diu:.2f}** | **{gt_vol:.2f}** | **${gt_cost:.4f}** |")
                md.append("")

        else:
            md.append("No integration runtime data available.")
        md.append("")

        # === Section 7: Database & Table Discovery ===
        md.append("## 7. Database & Table Discovery\n")
        md.append("| Metric | Value |")
        md.append("| --- | --- |")
        md.append(f"| Database Instance Count | {len(self.stats.db_instances)} |")
        md.append(f"| Table Reference Count | {len(self.stats.table_discovery)} |")
        md.append("")
        md.append("### Limitations\n")
        md.append("- **Tables**: Only finds tables referenced in datasets linked to pipelines. Misses tables referenced in stored procedures, Databricks notebooks, dynamic SQL, parameterized queries, and data flow transformations that use inline datasets.")
        md.append("- **Database instances**: Only those with explicit linked services in ADF. Databases accessed only via Databricks, stored procedure internal connections, or connection strings embedded in code are not discovered.")
        md.append("- The counts represent a **lower bound** of actual database/table usage.")
        md.append("")

        # Write MD
        md_path = os.path.join(self.ingestion_dir, "ingestion.md")
        with open(md_path, "w") as f:
            f.write("\n".join(md))

        return csv_path

    # =====================================================================
    # ORCHESTRATION TRACK EXPORTS
    # =====================================================================

    def _get_all_activities_df(self) -> "pd.DataFrame":
        """Get DataFrame of ALL activities with enrichment."""
        rows = []
        for a in self.stats.activities:
            sub_id = a.get("subscription_id", "")
            rows.append({
                "activity_name": a.get("activity_name", ""),
                "activity_type": a.get("activity_type", ""),
                "activity_category": a.get("activity_category", ""),
                "pipeline_name": a.get("pipeline_name", ""),
                "factory_name": a.get("factory_name", ""),
                "factory_type": a.get("factory_type", ""),
                "subscription_id": sub_id,
                "subscription_name": self._sub_name(sub_id),
                "ir_type": a.get("ir_type_used", ""),
                "linked_service": a.get("linked_service", ""),
                "depends_on": json.dumps(a.get("depends_on", [])),
            })
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def _export_activity_types_by_activity(self) -> str:
        df = self._get_all_activities_df()
        return self._export_df_csv(self.orchestration_details, "activity_types_by_activity", df)

    def _export_activity_types_by_pipeline(self) -> str:
        df = self._get_all_activities_df()
        if df.empty:
            return self._export_df_csv(self.orchestration_details, "activity_types_by_pipeline", df)
        grouped = df.groupby(
            ["pipeline_name", "factory_name", "subscription_id", "subscription_name", "activity_type"]
        ).size().reset_index(name="count")
        return self._export_df_csv(self.orchestration_details, "activity_types_by_pipeline", grouped)

    def _export_activity_types_by_factory(self) -> str:
        df = self._get_all_activities_df()
        if df.empty:
            return self._export_df_csv(self.orchestration_details, "activity_types_by_factory", df)
        grouped = df.groupby(
            ["factory_name", "subscription_id", "subscription_name", "activity_type"]
        ).size().reset_index(name="count")
        return self._export_df_csv(self.orchestration_details, "activity_types_by_factory", grouped)

    def _export_activity_types_by_subscription(self) -> str:
        df = self._get_all_activities_df()
        if df.empty:
            return self._export_df_csv(self.orchestration_details, "activity_types_by_subscription", df)
        grouped = df.groupby(
            ["subscription_id", "subscription_name", "activity_type"]
        ).size().reset_index(name="count")
        return self._export_df_csv(self.orchestration_details, "activity_types_by_subscription", grouped)

    def _export_activity_types_by_tenant(self) -> str:
        df = self._get_all_activities_df()
        if df.empty:
            return self._export_df_csv(self.orchestration_details, "activity_types_by_tenant", df)
        grouped = df.groupby("activity_type").agg(
            count=("activity_name", "count"),
            factory_count=("factory_name", "nunique"),
            subscription_count=("subscription_id", "nunique"),
        ).reset_index()
        return self._export_df_csv(self.orchestration_details, "activity_types_by_tenant", grouped)

    def _export_pipeline_runs_detail(self) -> str:
        if not self.stats.pipeline_runs:
            return self._export_csv(self.orchestration_details, "pipeline_runs_detail", [])
        rows = []
        for r in self.stats.pipeline_runs:
            sub_id = r.get("subscription_id", "")
            invoked_by = r.get("invokedBy", {})
            rows.append({
                "run_id": r.get("runId", ""),
                "pipeline_name": r.get("pipelineName", ""),
                "factory_name": r.get("factory_name", ""),
                "subscription_id": sub_id,
                "subscription_name": self._sub_name(sub_id),
                "status": r.get("status", ""),
                "duration_min": round((r.get("durationInMs", 0) or 0) / 60000, 2),
                "run_start": r.get("runStart", ""),
                "run_end": r.get("runEnd", ""),
                "invoked_by": invoked_by.get("name", "") if isinstance(invoked_by, dict) else "",
                "invoked_by_type": invoked_by.get("invokedByType", "") if isinstance(invoked_by, dict) else "",
                "message": r.get("message", ""),
            })
        return self._export_csv(self.orchestration_details, "pipeline_runs_detail", rows)

    def _export_pipeline_runs_by_pipeline(self) -> str:
        if not self.stats.pipeline_job_stats:
            return self._export_csv(self.orchestration_details, "pipeline_runs_by_pipeline", [])
        days = self.stats.cost_breakdown.get("days", 90) if self.stats.cost_breakdown else 90
        rows = []
        for ps in self.stats.pipeline_job_stats:
            total_runs = ps.get("totalRuns", 0)
            rows.append({
                **ps,
                "subscription_name": self._sub_name(ps.get("subscription_id", "")),
                "runs_per_day_avg": round(total_runs / max(days, 1), 2),
                "runs_per_hour_avg": round(total_runs / max(days * 24, 1), 4),
            })
        return self._export_csv(self.orchestration_details, "pipeline_runs_by_pipeline", rows)

    def _export_pipeline_runs_by_factory(self) -> str:
        if not self.stats.pipeline_job_stats:
            return self._export_csv(self.orchestration_details, "pipeline_runs_by_factory", [])
        df = pd.DataFrame(self.stats.pipeline_job_stats)
        grouped = df.groupby("factory_name").agg(
            subscription_id=("subscription_id", "first"),
            total_runs=("totalRuns", "sum"),
            succeeded_runs=("succeededRuns", "sum"),
            failed_runs=("failedRuns", "sum"),
            total_cost=("totalCost", "sum"),
            orch_cost=("orchCost", "sum"),
            ingestion_cost=("ingestionCost", "sum"),
            pipeline_count=("pipelineName", "nunique"),
        ).reset_index()
        grouped["subscription_name"] = grouped["subscription_id"].apply(lambda s: self._sub_name(s) if pd.notna(s) else "")
        grouped["success_rate"] = (grouped["succeeded_runs"] / grouped["total_runs"].clip(lower=1) * 100).round(1)
        return self._export_df_csv(self.orchestration_details, "pipeline_runs_by_factory", grouped)

    def _export_pipeline_runs_by_subscription(self) -> str:
        if not self.stats.pipeline_job_stats:
            return self._export_csv(self.orchestration_details, "pipeline_runs_by_subscription", [])
        df = pd.DataFrame(self.stats.pipeline_job_stats)
        grouped = df.groupby("subscription_id").agg(
            total_runs=("totalRuns", "sum"),
            succeeded_runs=("succeededRuns", "sum"),
            failed_runs=("failedRuns", "sum"),
            total_cost=("totalCost", "sum"),
            factory_count=("factory_name", "nunique"),
            pipeline_count=("pipelineName", "nunique"),
        ).reset_index()
        grouped["subscription_name"] = grouped["subscription_id"].apply(lambda s: self._sub_name(s) if pd.notna(s) else "")
        grouped["success_rate"] = (grouped["succeeded_runs"] / grouped["total_runs"].clip(lower=1) * 100).round(1)
        return self._export_df_csv(self.orchestration_details, "pipeline_runs_by_subscription", grouped)

    def _export_pipeline_runs_by_tenant(self) -> str:
        if not self.stats.pipeline_job_stats:
            return self._export_csv(self.orchestration_details, "pipeline_runs_by_tenant", [])
        df = pd.DataFrame(self.stats.pipeline_job_stats)
        row = {
            "total_runs": int(df["totalRuns"].sum()),
            "succeeded_runs": int(df["succeededRuns"].sum()),
            "failed_runs": int(df["failedRuns"].sum()),
            "total_cost": round(float(df["totalCost"].sum()), 4),
            "factory_count": df["factory_name"].nunique(),
            "pipeline_count": df["pipelineName"].nunique(),
            "subscription_count": df["subscription_id"].nunique(),
        }
        total = max(row["total_runs"], 1)
        row["success_rate"] = round(row["succeeded_runs"] / total * 100, 1)
        return self._export_csv(self.orchestration_details, "pipeline_runs_by_tenant", [row])

    def _export_dataflows(self) -> str:
        # Add pipelines_using_it
        df_usage = defaultdict(set)
        for a in self.stats.activities:
            if a.get("activity_type") in ("ExecuteDataFlow", "DataFlow") and a.get("dataflow_name"):
                df_usage[(a.get("factory_name", ""), a["dataflow_name"])].add(a.get("pipeline_name", ""))
        rows = []
        for df_item in self.stats.dataflows:
            sub_id = df_item.get("subscription_id", "")
            key = (df_item.get("factory_name", ""), df_item.get("name", ""))
            rows.append({
                "name": df_item.get("name"), "type": df_item.get("type"),
                "factory_name": df_item.get("factory_name"),
                "factory_type": df_item.get("factory_type"),
                "subscription_id": sub_id,
                "subscription_name": self._sub_name(sub_id),
                "folder": df_item.get("folder"),
                "source_count": df_item.get("source_count"),
                "sink_count": df_item.get("sink_count"),
                "transformation_count": df_item.get("transformation_count"),
                "pipelines_using_it": ", ".join(sorted(df_usage.get(key, set()))),
            })
        return self._export_csv(self.orchestration_details, "dataflows", rows)

    def _export_datasets(self) -> str:
        # Build dataset usage
        ds_usage = defaultdict(set)
        for a in self.stats.activities:
            fn = a.get("factory_name", "")
            pn = a.get("pipeline_name", "")
            for field in ("source_dataset_name", "sink_dataset_name"):
                ds_name = a.get(field)
                if ds_name:
                    ds_usage[(fn, ds_name)].add(pn)
        rows = []
        for ds in self.stats.datasets:
            sub_id = ds.get("subscription_id", "")
            key = (ds.get("factory_name", ""), ds.get("name", ""))
            rows.append({
                "name": ds.get("name"), "type": ds.get("type"),
                "factory_name": ds.get("factory_name"),
                "factory_type": ds.get("factory_type"),
                "subscription_id": sub_id,
                "subscription_name": self._sub_name(sub_id),
                "linked_service_name": ds.get("linked_service_name"),
                "folder": ds.get("folder"),
                "schema_columns": ds.get("schema_columns"),
                "parameter_count": len(ds.get("parameters", {})),
                "pipelines_using_it": ", ".join(sorted(ds_usage.get(key, set()))),
            })
        return self._export_csv(self.orchestration_details, "datasets", rows)

    def _export_linked_services(self) -> str:
        # Build usage counts
        ds_per_ls = defaultdict(int)
        for ds in self.stats.datasets:
            ls_name = ds.get("linked_service_name")
            fn = ds.get("factory_name", "")
            if ls_name:
                ds_per_ls[(fn, ls_name)] += 1
        pipeline_per_ls = defaultdict(set)
        for a in self.stats.activities:
            fn = a.get("factory_name", "")
            pn = a.get("pipeline_name", "")
            for field in ("source_linked_service", "sink_linked_service", "linked_service"):
                ls_val = a.get(field)
                if ls_val:
                    pipeline_per_ls[(fn, ls_val)].add(pn)
        rows = []
        for ls in self.stats.linked_services:
            sub_id = ls.get("subscription_id", "")
            key = (ls.get("factory_name", ""), ls.get("name", ""))
            rows.append({
                "name": ls.get("name"), "type": ls.get("type"),
                "factory_name": ls.get("factory_name"),
                "factory_type": ls.get("factory_type"),
                "subscription_id": sub_id,
                "subscription_name": self._sub_name(sub_id),
                "connect_via": ls.get("connect_via"),
                "description": ls.get("description"),
                "datasets_using_it": ds_per_ls.get(key, 0),
                "pipelines_using_it": len(pipeline_per_ls.get(key, set())),
            })
        return self._export_csv(self.orchestration_details, "linked_services", rows)

    def _export_triggers(self) -> str:
        rows = []
        for t in self.stats.triggers:
            sub_id = t.get("subscription_id", "")
            rows.append({
                "name": t.get("name"), "type": t.get("type"),
                "factory_name": t.get("factory_name"),
                "factory_type": t.get("factory_type"),
                "subscription_id": sub_id,
                "subscription_name": self._sub_name(sub_id),
                "runtime_state": t.get("runtime_state"),
                "pipelines": json.dumps(t.get("pipelines", [])),
            })
        return self._export_csv(self.orchestration_details, "triggers", rows)

    def _export_metadata_driven_analysis(self) -> str:
        rows = self.stats.metadata_driven_classifications
        if not rows:
            return self._export_csv(self.orchestration_details, "metadata_driven_analysis", [])
        for r in rows:
            r["subscription_name"] = self._sub_name(r.get("subscription_id", ""))
        return self._export_csv(self.orchestration_details, "metadata_driven_analysis", rows)

    def _export_databricks_analysis(self) -> str:
        rows = self.stats.databricks_classifications
        if not rows:
            return self._export_csv(self.orchestration_details, "databricks_analysis", [])
        for r in rows:
            r["subscription_name"] = self._sub_name(r.get("subscription_id", ""))
        return self._export_csv(self.orchestration_details, "databricks_analysis", rows)

    def _export_orchestration_cost_breakdown(self) -> str:
        """Orchestration cost = orch fees + pipeline activity + external activity costs."""
        rows = []
        for ps in self.stats.pipeline_job_stats:
            orch_total = ps.get("orchCost", 0) + ps.get("pipelineActCost", 0) + ps.get("externalCost", 0)
            if orch_total > 0:
                rows.append({
                    "factory_name": ps.get("factory_name", ""),
                    "subscription_id": ps.get("subscription_id", ""),
                    "subscription_name": self._sub_name(ps.get("subscription_id", "")),
                    "pipeline_name": ps.get("pipelineName", ""),
                    "orchestration_fee": round(ps.get("orchCost", 0), 6),
                    "pipeline_activity_cost": round(ps.get("pipelineActCost", 0), 6),
                    "external_activity_cost": round(ps.get("externalCost", 0), 6),
                    "orchestration_total": round(orch_total, 6),
                    "total_runs": ps.get("totalRuns", 0),
                })
        return self._export_csv(self.orchestration_details, "orchestration_cost_breakdown", rows)

    def _export_managed_airflow(self) -> str:
        """Export managed airflow costs to orchestration/details/managed_airflow.csv."""
        rows = []
        for ac in self.stats.actual_costs:
            meter = ac.get("meterSubcategory", "") or ""
            if "airflow" in meter.lower():
                fname = ac.get("factory_name", "")
                f_info = self.stats.factories.get(fname, {})
                sub_id = f_info.get("subscription_id", "")
                rows.append({
                    "factory_name": fname,
                    "subscription_id": sub_id,
                    "subscription_name": self._sub_name(sub_id),
                    "cost": round(ac.get("cost", 0), 4),
                })
        return self._export_csv(self.orchestration_details, "managed_airflow", rows)

    def _export_orchestration_rollup(self) -> str:
        """Orchestration rollup CSV + richly structured MD."""
        rows = []
        def add(metric, value):
            rows.append({"metric": metric, "value": value})

        total_pipelines = len(self.stats.pipelines)
        total_activities = len(self.stats.activities)
        total_runs = sum(s.get("totalRuns", 0) for s in self.stats.pipeline_job_stats) if self.stats.pipeline_job_stats else 0
        total_succeeded = sum(s.get("succeededRuns", 0) for s in self.stats.pipeline_job_stats) if self.stats.pipeline_job_stats else 0
        total_failed = sum(s.get("failedRuns", 0) for s in self.stats.pipeline_job_stats) if self.stats.pipeline_job_stats else 0
        success_rate = round(total_succeeded / max(total_runs, 1) * 100, 1)
        all_durations = [s.get("avgDurationMin", 0) for s in self.stats.pipeline_job_stats if s.get("avgDurationMin")] if self.stats.pipeline_job_stats else []
        avg_dur = round(sum(all_durations) / len(all_durations), 2) if all_durations else 0

        total_orch = sum(s.get("orchCost", 0) for s in self.stats.pipeline_job_stats) if self.stats.pipeline_job_stats else 0
        total_pipeline_act = sum(s.get("pipelineActCost", 0) for s in self.stats.pipeline_job_stats) if self.stats.pipeline_job_stats else 0
        total_external = sum(s.get("externalCost", 0) for s in self.stats.pipeline_job_stats) if self.stats.pipeline_job_stats else 0
        # Dataflow cost: estimated from ExecuteDataFlow activity runs
        total_dataflow_cost = 0.0
        dataflow_activity_types = {"ExecuteDataFlow", "DataFlow"}
        for ar in self.stats.activity_runs:
            if ar.get("activityType", "") in dataflow_activity_types:
                total_dataflow_cost += ar.get("execCost", 0) or 0
        total_orch_cost = total_orch + total_pipeline_act + total_external + total_dataflow_cost

        add("Total Pipelines", total_pipelines)
        add("Total Activities", total_activities)
        add("Total Pipeline Runs", total_runs)
        add("Success Rate", f"{success_rate}%")
        add("Total Orchestration Cost", f"${total_orch_cost:.4f}")

        # Write CSV
        csv_path = os.path.join(self.orchestration_dir, "orchestration.csv")
        pd.DataFrame(rows).to_csv(csv_path, index=False)

        # ---- Build rich MD ----
        md = ["# Orchestration Rollup\n"]

        # === Section 1: Orchestration Overview (tenant level) ===
        md.append("## 1. Orchestration Overview (Tenant Level)\n")
        md.append("| Metric | Value |")
        md.append("| --- | --- |")
        md.append(f"| Total Pipelines | {total_pipelines} |")
        md.append(f"| Total Activities | {total_activities} |")
        md.append(f"| Total Pipeline Runs | {total_runs} |")
        md.append(f"| Succeeded Runs | {total_succeeded} |")
        md.append(f"| Failed Runs | {total_failed} |")
        md.append(f"| Success Rate | {success_rate}% |")
        md.append(f"| Avg Pipeline Duration (min) | {avg_dur} |")
        md.append("")
        md.append("### Cost Breakdown\n")
        md.append("| Cost Category | Amount |")
        md.append("| --- | --- |")
        md.append(f"| Orchestration fees (per activity run) | ${total_orch:.4f} |")
        md.append(f"| Pipeline activity cost (Lookup, GetMetadata, etc.) | ${total_pipeline_act:.4f} |")
        md.append(f"| External activity cost (stored procs, web activities, Databricks) | ${total_external:.4f} |")
        md.append(f"| Dataflow cost (${DATAFLOW_VCORE_RATE}/vCore-hr x {DATAFLOW_MIN_VCORES} vCores min) | ${total_dataflow_cost:.4f} |")
        md.append(f"| Total Orchestration Cost | ${total_orch_cost:.4f} |")
        md.append("")

        # === Section 2: Activity Type Distribution (Change 10: categorized, split into sub-tables) ===
        md.append("## 2. Activity Type Distribution\n")
        act_type_counts = defaultdict(int)
        for a in self.stats.activities:
            act_type_counts[a.get("activity_type", "Unknown")] += 1
        if act_type_counts:
            # Group by category
            cat_types = defaultdict(list)
            for at, cnt in act_type_counts.items():
                subcat = orch_activity_subcat(at)
                cat_types[subcat].append((at, cnt))
            # Sort categories in a logical order
            cat_order = ["Control Flow", "Data Movement", "Transformation", "External", "Other"]
            total_act_cnt = 0
            for cat in cat_order:
                if cat not in cat_types:
                    continue
                items = sorted(cat_types[cat], key=lambda x: -x[1])
                cat_total = sum(cnt for _, cnt in items)
                md.append(f"### {cat} Activities\n")
                md.append("| Activity Type | Count |")
                md.append("| --- | --- |")
                for at, cnt in items:
                    md.append(f"| {at} | {cnt} |")
                    total_act_cnt += cnt
                md.append(f"| **Subtotal** | **{cat_total}** |")
                md.append("")
            md.append(f"**Total Activities**: {total_act_cnt}")
        else:
            md.append("No activity data available.")
        md.append("")

        # === Section 3: Breakdown by Subscription (consolidated from old sections 3+6, Change 11) ===
        md.append("## 3. Breakdown by Subscription\n")
        sub_ids = set()
        for f_info in self.stats.factories.values():
            sid = f_info.get("subscription_id", "")
            if sid:
                sub_ids.add(sid)
        if sub_ids and self.stats.pipeline_job_stats:
            md.append("| Subscription | Pipelines | Activities | Runs | Success Rate | Orch | Pipeline Act | External | Dataflow | Total Cost |")
            md.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
            gt_pipe = gt_act = gt_runs2 = 0
            gt_orch2 = gt_pipe2 = gt_ext2 = gt_df2 = gt_total2 = 0.0
            for sid in sorted(sub_ids):
                sub_display = self._sub_name(sid) or sid[:12] + "..."
                sub_factories_set = {name for name, f in self.stats.factories.items() if f.get("subscription_id") == sid}
                sub_pipelines = [p for p in self.stats.pipelines if p.get("subscription_id") == sid]
                sub_activities_list = [a for a in self.stats.activities if a.get("subscription_id") == sid]
                sub_stats = [s for s in self.stats.pipeline_job_stats if s.get("factory_name") in sub_factories_set]
                s_runs = sum(s.get("totalRuns", 0) for s in sub_stats)
                s_succeeded = sum(s.get("succeededRuns", 0) for s in sub_stats)
                s_rate = round(s_succeeded / max(s_runs, 1) * 100, 1)
                s_orch = sum(s.get("orchCost", 0) for s in sub_stats)
                s_pipe = sum(s.get("pipelineActCost", 0) for s in sub_stats)
                s_ext = sum(s.get("externalCost", 0) for s in sub_stats)
                s_df = 0.0
                for ar in self.stats.activity_runs:
                    if ar.get("activityType", "") in dataflow_activity_types and ar.get("factory_name", "") in sub_factories_set:
                        s_df += ar.get("execCost", 0) or 0
                s_total = s_orch + s_pipe + s_ext + s_df
                md.append(f"| {sub_display} | {len(sub_pipelines)} | {len(sub_activities_list)} | {s_runs} | {s_rate}% | ${s_orch:.4f} | ${s_pipe:.4f} | ${s_ext:.4f} | ${s_df:.4f} | ${s_total:.4f} |")
                gt_pipe += len(sub_pipelines); gt_act += len(sub_activities_list); gt_runs2 += s_runs
                gt_orch2 += s_orch; gt_pipe2 += s_pipe; gt_ext2 += s_ext; gt_df2 += s_df; gt_total2 += s_total
            gt_rate = round(total_succeeded / max(total_runs, 1) * 100, 1) if total_runs else 0
            md.append(f"| **Total** | **{gt_pipe}** | **{gt_act}** | **{gt_runs2}** | **{gt_rate}%** | **${gt_orch2:.4f}** | **${gt_pipe2:.4f}** | **${gt_ext2:.4f}** | **${gt_df2:.4f}** | **${gt_total2:.4f}** |")
        else:
            md.append("No subscription-level data available.")
        md.append("")

        # === Section 4: Breakdown by Factory (consolidated from old sections 3-factory+7, Change 11) ===
        md.append("## 4. Breakdown by Factory\n")
        if self.stats.pipeline_job_stats:
            factory_stats = defaultdict(lambda: {"runs": 0, "succeeded": 0, "pipelines": set(),
                                                  "orch": 0.0, "pipe": 0.0, "ext": 0.0, "df": 0.0})
            for s in self.stats.pipeline_job_stats:
                fname = s.get("factory_name", "")
                factory_stats[fname]["runs"] += s.get("totalRuns", 0)
                factory_stats[fname]["succeeded"] += s.get("succeededRuns", 0)
                factory_stats[fname]["orch"] += s.get("orchCost", 0)
                factory_stats[fname]["pipe"] += s.get("pipelineActCost", 0)
                factory_stats[fname]["ext"] += s.get("externalCost", 0)
                factory_stats[fname]["pipelines"].add(s.get("pipelineName", ""))
            for ar in self.stats.activity_runs:
                if ar.get("activityType", "") in dataflow_activity_types:
                    fname = ar.get("factory_name", "")
                    factory_stats[fname]["df"] += ar.get("execCost", 0) or 0
            md.append("| Subscription | Factory | Pipelines | Runs | Success Rate | Orch | Pipeline Act | External | Dataflow | Total Cost |")
            md.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
            gt_pipe = gt_runs2 = 0
            gt_orch2 = gt_pipe2 = gt_ext2 = gt_df2 = gt_total2 = 0.0
            # Sort by total cost descending, filter empty, collect into list
            sorted_factories = sorted(factory_stats.items(),
                                      key=lambda x: -(x[1]["orch"] + x[1]["pipe"] + x[1]["ext"] + x[1]["df"]))
            active_factories = []
            for fname, fs in sorted_factories:
                if fs["runs"] == 0:
                    continue
                f_rate = round(fs["succeeded"] / max(fs["runs"], 1) * 100, 1)
                f_total = fs["orch"] + fs["pipe"] + fs["ext"] + fs["df"]
                active_factories.append((fname, fs, f_rate, f_total))
            top_5 = active_factories[:5]
            remainder = active_factories[5:]
            for fname, fs, f_rate, f_total in top_5:
                sub_display = self._sub_name_for_factory(fname)
                md.append(f"| {sub_display} | {fname} | {len(fs['pipelines'])} | {fs['runs']} | {f_rate}% | ${fs['orch']:.4f} | ${fs['pipe']:.4f} | ${fs['ext']:.4f} | ${fs['df']:.4f} | ${f_total:.4f} |")
                gt_pipe += len(fs["pipelines"]); gt_runs2 += fs["runs"]
                gt_orch2 += fs["orch"]; gt_pipe2 += fs["pipe"]; gt_ext2 += fs["ext"]; gt_df2 += fs["df"]; gt_total2 += f_total
            if remainder:
                r_pipe = sum(len(fs["pipelines"]) for _, fs, _, _ in remainder)
                r_runs = sum(fs["runs"] for _, fs, _, _ in remainder)
                r_succeeded = sum(fs["succeeded"] for _, fs, _, _ in remainder)
                r_rate = round(r_succeeded / max(r_runs, 1) * 100, 1)
                r_orch = sum(fs["orch"] for _, fs, _, _ in remainder)
                r_pipe_act = sum(fs["pipe"] for _, fs, _, _ in remainder)
                r_ext = sum(fs["ext"] for _, fs, _, _ in remainder)
                r_df = sum(fs["df"] for _, fs, _, _ in remainder)
                r_total = r_orch + r_pipe_act + r_ext + r_df
                md.append(f"| | Other ({len(remainder)} factories) | {r_pipe} | {r_runs} | {r_rate}% | ${r_orch:.4f} | ${r_pipe_act:.4f} | ${r_ext:.4f} | ${r_df:.4f} | ${r_total:.4f} |")
                gt_pipe += r_pipe; gt_runs2 += r_runs
                gt_orch2 += r_orch; gt_pipe2 += r_pipe_act; gt_ext2 += r_ext; gt_df2 += r_df; gt_total2 += r_total
            md.append(f"| **Total** | | **{gt_pipe}** | **{gt_runs2}** | | **${gt_orch2:.4f}** | **${gt_pipe2:.4f}** | **${gt_ext2:.4f}** | **${gt_df2:.4f}** | **${gt_total2:.4f}** |")
        else:
            md.append("No factory-level run data available.")
        md.append("")

        # === Section 5: Databricks Usage ===
        md.append("## 5. Databricks Usage\n")
        db_acts = self.stats.databricks_classifications
        if db_acts:
            interactive_types = {"DatabricksNotebook", "DatabricksSparkPython", "DatabricksSparkJar"}
            job_types = {"DatabricksJob"}

            md.append("### Interactive Activities\n")
            interactive_acts = [d for d in db_acts if d.get("activity_type") in interactive_types]
            if interactive_acts:
                int_counts = defaultdict(int)
                for d in interactive_acts:
                    int_counts[d.get("activity_type", "")] += 1
                md.append("| Activity Type | Count |")
                md.append("| --- | --- |")
                for atype, cnt in sorted(int_counts.items(), key=lambda x: -x[1]):
                    md.append(f"| {atype} | {cnt} |")
            else:
                md.append("No interactive Databricks activities detected.")
            md.append("")

            md.append("### Jobs Activities\n")
            job_acts = [d for d in db_acts if d.get("activity_type") in job_types]
            if job_acts:
                md.append(f"| DatabricksJob | {len(job_acts)} |")
            else:
                md.append("No Databricks Job activities detected.")
            md.append("")

            md.append(f"**Total Databricks Activities**: {len(db_acts)}")
            md.append("")

            # Linked services used
            all_ls = set()
            for d in db_acts:
                ls = d.get("linked_service_name", "")
                if ls:
                    all_ls.add(ls)
            if all_ls:
                md.append(f"**Linked Services Used**: {', '.join(sorted(all_ls))}")
                md.append("")
        else:
            md.append("No Databricks activities detected.")
            md.append("")

        # === Section 6: Metadata-Driven Pipeline Detection ===
        md.append("## 6. Metadata-Driven Pipeline Detection\n")
        md_yes = sum(1 for m in self.stats.metadata_driven_classifications if m.get("is_metadata_driven") == "Yes")
        md_likely = sum(1 for m in self.stats.metadata_driven_classifications if m.get("is_metadata_driven") == "Likely")
        md_no = sum(1 for m in self.stats.metadata_driven_classifications if m.get("is_metadata_driven") == "No")
        md.append("| Classification | Count |")
        md.append("| --- | --- |")
        md.append(f"| Yes | {md_yes} |")
        md.append(f"| Likely | {md_likely} |")
        md.append(f"| No | {md_no} |")
        md.append("")
        md.append("### Methodology\n")
        md.append("- **Signal 1**: Lookup -> ForEach -> ExecutePipeline pattern in the pipeline structure")
        md.append("- **Signal 2**: 3+ parameter names matching metadata conventions (tableName, schema, sourceQuery, sinkFolder, loadType, watermarkColumn, sourceTable, sinkTable, sourceConnection, sinkConnection, fileName, filePath, containerPath)")
        md.append("- **Signal 3**: Pipeline uses parameterized datasets (datasets with parameters)")
        md.append("")
        md.append("### Classification Rules\n")
        md.append("- **Yes** = 2 or more signals match")
        md.append("- **Likely** = 1 signal matches")
        md.append("- **No** = 0 signals match")
        md.append("")
        md.append("*Note: This is heuristic-based. Metadata-driven pipelines that use unconventional naming or different patterns may be missed.*")
        md.append("")

        # === Section 7: Platform Coverage Notes ===
        md.append("## 7. Platform Coverage Notes\n")
        md.append("- **Synapse**: Pipeline definitions extracted if permissions allow (requires Synapse Artifact User). Synapse supports the same monitoring APIs as ADF (queryPipelineRuns, queryActivityRuns) via the dev.azuresynapse.net endpoint. Currently skipped workspaces are listed with 403 Forbidden.")
        if self.stats.skipped_synapse:
            md.append(f"  - Skipped workspaces: {', '.join(self.stats.skipped_synapse)}")
        md.append("- **Fabric**: Pipeline definitions extracted from Fabric workspaces. Fabric has a separate monitoring API (api.fabric.microsoft.com) for pipeline runs but it is not yet integrated. Runtime profiling (runs, costs) is not available for Fabric pipelines.")
        md.append("- **ADF**: Full support -- definitions, runtime profiling, SHIR nodes, cost estimation, and actual cost comparison.")
        md.append("")

        # Write MD
        md_path = os.path.join(self.orchestration_dir, "orchestration.md")
        with open(md_path, "w") as f:
            f.write("\n".join(md))

        return csv_path


    # =====================================================================
    # ASSESSMENT (wkmigrate migration readiness)
    # =====================================================================

    def _get_assessment_activities_df(self) -> "pd.DataFrame":
        """Build a DataFrame of all activities enriched with wkmigrate support status."""
        rows = []
        for a in self.stats.activities:
            atype = a.get("activity_type", "Unknown")
            support, notes = WKMIGRATE_ACTIVITY_SUPPORT.get(
                atype, ("Unsupported", "Unknown activity type — manual conversion needed"))
            sub_id = a.get("subscription_id", "")
            rows.append({
                "activity_name": a.get("activity_name", ""),
                "activity_type": atype,
                "activity_category": ACTIVITY_TYPE_TO_CATEGORY.get(atype, "Unknown"),
                "wkmigrate_support": support,
                "pipeline_name": a.get("pipeline_name", ""),
                "factory_name": a.get("factory_name", ""),
                "factory_type": a.get("factory_type", ""),
                "subscription_id": sub_id,
                "subscription_name": self._sub_name(sub_id),
                "notes": notes,
            })
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def _get_assessment_connectors_df(self) -> "pd.DataFrame":
        """Build a DataFrame of all Copy activity connectors enriched with wkmigrate support."""
        rows = []
        for a in self.stats.activities:
            if a.get("activity_category") != "DataMovement":
                continue
            raw_src = a.get("source_type_actual", "")
            raw_snk = a.get("sink_type_actual", "")
            friendly_src = friendly_connector_name(raw_src) if raw_src else ""
            friendly_snk = friendly_connector_name(raw_snk) if raw_snk else ""
            src_support, src_notes = wkmigrate_connector_status(raw_src, friendly_src) if raw_src else ("", "")
            snk_support, snk_notes = wkmigrate_connector_status(raw_snk, friendly_snk) if raw_snk else ("", "")
            sub_id = a.get("subscription_id", "")
            rows.append({
                "activity_name": a.get("activity_name", ""),
                "pipeline_name": a.get("pipeline_name", ""),
                "factory_name": a.get("factory_name", ""),
                "factory_type": a.get("factory_type", ""),
                "subscription_id": sub_id,
                "subscription_name": self._sub_name(sub_id),
                "source_connector": friendly_src,
                "source_support": src_support,
                "sink_connector": friendly_snk,
                "sink_support": snk_support,
            })
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    # --- Activity assessment detail exports ---

    def _export_assessment_by_pipeline(self) -> str:
        """Per-pipeline assessment: activity counts, supported/unsupported, readiness %."""
        df = self._get_assessment_activities_df()
        if df.empty:
            return self._export_df_csv(self.assessment_details, "assessment_by_pipeline", df)
        total = df.groupby(
            ["pipeline_name", "factory_name", "factory_type", "subscription_id", "subscription_name"]
        ).size().reset_index(name="total_activities")
        supported = df[df["wkmigrate_support"] == "Supported"].groupby(
            ["pipeline_name", "factory_name"]
        ).size().reset_index(name="supported_activities")
        merged = total.merge(supported, on=["pipeline_name", "factory_name"], how="left")
        merged["supported_activities"] = merged["supported_activities"].fillna(0).astype(int)
        merged["unsupported_activities"] = merged["total_activities"] - merged["supported_activities"]
        merged["support_pct"] = (merged["supported_activities"] / merged["total_activities"] * 100).round(1)
        # Add list of unsupported activity types per pipeline
        unsup = df[df["wkmigrate_support"] != "Supported"].groupby(
            ["pipeline_name", "factory_name"]
        )["activity_type"].apply(lambda x: ", ".join(sorted(set(x)))).reset_index(name="unsupported_types")
        merged = merged.merge(unsup, on=["pipeline_name", "factory_name"], how="left")
        merged["unsupported_types"] = merged["unsupported_types"].fillna("")
        merged = merged.sort_values("support_pct", ascending=True)
        return self._export_df_csv(self.assessment_details, "assessment_by_pipeline", merged)

    def _export_assessment_by_factory(self) -> str:
        """Per-factory assessment: pipeline/activity readiness."""
        df = self._get_assessment_activities_df()
        if df.empty:
            return self._export_df_csv(self.assessment_details, "assessment_by_factory", df)
        factory_stats = []
        for (fname, ftype, sid, sname), grp in df.groupby(
            ["factory_name", "factory_type", "subscription_id", "subscription_name"]
        ):
            total_acts = len(grp)
            supported_acts = (grp["wkmigrate_support"] == "Supported").sum()
            # Pipeline readiness
            pipe_grps = grp.groupby("pipeline_name")["wkmigrate_support"].apply(
                lambda x: (x == "Supported").all())
            total_pipes = len(pipe_grps)
            fully_supported_pipes = pipe_grps.sum()
            unsup_types = sorted(grp[grp["wkmigrate_support"] != "Supported"]["activity_type"].unique())
            factory_stats.append({
                "factory_name": fname,
                "factory_type": ftype,
                "subscription_id": sid,
                "subscription_name": sname,
                "total_pipelines": total_pipes,
                "fully_supported_pipelines": int(fully_supported_pipes),
                "pipeline_support_pct": round(fully_supported_pipes / max(total_pipes, 1) * 100, 1),
                "total_activities": total_acts,
                "supported_activities": int(supported_acts),
                "unsupported_activities": total_acts - int(supported_acts),
                "activity_support_pct": round(supported_acts / max(total_acts, 1) * 100, 1),
                "top_unsupported_types": ", ".join(unsup_types[:5]),
            })
        result = pd.DataFrame(factory_stats).sort_values("activity_support_pct", ascending=True)
        return self._export_df_csv(self.assessment_details, "assessment_by_factory", result)

    def _export_assessment_by_subscription(self) -> str:
        """Per-subscription assessment."""
        df = self._get_assessment_activities_df()
        if df.empty:
            return self._export_df_csv(self.assessment_details, "assessment_by_subscription", df)
        sub_stats = []
        for (sid, sname), grp in df.groupby(["subscription_id", "subscription_name"]):
            total_acts = len(grp)
            supported_acts = (grp["wkmigrate_support"] == "Supported").sum()
            pipe_grps = grp.groupby(["factory_name", "pipeline_name"])["wkmigrate_support"].apply(
                lambda x: (x == "Supported").all())
            total_pipes = len(pipe_grps)
            fully_supported_pipes = pipe_grps.sum()
            total_factories = grp["factory_name"].nunique()
            unsup_types = sorted(grp[grp["wkmigrate_support"] != "Supported"]["activity_type"].unique())
            sub_stats.append({
                "subscription_id": sid,
                "subscription_name": sname,
                "factory_count": total_factories,
                "total_pipelines": total_pipes,
                "fully_supported_pipelines": int(fully_supported_pipes),
                "pipeline_support_pct": round(fully_supported_pipes / max(total_pipes, 1) * 100, 1),
                "total_activities": total_acts,
                "supported_activities": int(supported_acts),
                "unsupported_activities": total_acts - int(supported_acts),
                "activity_support_pct": round(supported_acts / max(total_acts, 1) * 100, 1),
                "top_unsupported_types": ", ".join(unsup_types[:5]),
            })
        result = pd.DataFrame(sub_stats).sort_values("activity_support_pct", ascending=True)
        return self._export_df_csv(self.assessment_details, "assessment_by_subscription", result)

    def _export_assessment_by_tenant(self) -> str:
        """Tenant-level assessment summary."""
        df = self._get_assessment_activities_df()
        if df.empty:
            return self._export_df_csv(self.assessment_details, "assessment_by_tenant", df)
        total_acts = len(df)
        supported_acts = (df["wkmigrate_support"] == "Supported").sum()
        pipe_grps = df.groupby(["factory_name", "pipeline_name"])["wkmigrate_support"].apply(
            lambda x: (x == "Supported").all())
        total_pipes = len(pipe_grps)
        fully_supported_pipes = pipe_grps.sum()
        unsup_types = sorted(df[df["wkmigrate_support"] != "Supported"]["activity_type"].unique())
        result = pd.DataFrame([{
            "subscription_count": df["subscription_id"].nunique(),
            "factory_count": df["factory_name"].nunique(),
            "total_pipelines": total_pipes,
            "fully_supported_pipelines": int(fully_supported_pipes),
            "pipeline_support_pct": round(fully_supported_pipes / max(total_pipes, 1) * 100, 1),
            "total_activities": total_acts,
            "supported_activities": int(supported_acts),
            "unsupported_activities": total_acts - int(supported_acts),
            "activity_support_pct": round(supported_acts / max(total_acts, 1) * 100, 1),
            "unsupported_types": ", ".join(unsup_types),
        }])
        return self._export_df_csv(self.assessment_details, "assessment_by_tenant", result)

    # --- Connector assessment detail exports ---

    def _export_assessment_connectors_by_pipeline(self) -> str:
        """Per-pipeline connector support assessment."""
        df = self._get_assessment_connectors_df()
        if df.empty:
            return self._export_df_csv(self.assessment_details, "connectors_by_pipeline", df)
        rows = []
        for (pname, fname, ftype, sid, sname), grp in df.groupby(
            ["pipeline_name", "factory_name", "factory_type", "subscription_id", "subscription_name"]
        ):
            total = len(grp)
            src_sup = (grp["source_support"] == "Supported").sum()
            snk_sup = (grp["sink_support"] == "Supported").sum()
            total_conn = total * 2  # source + sink per activity
            supported_conn = int(src_sup + snk_sup)
            unsup_src = sorted(grp[grp["source_support"] != "Supported"]["source_connector"].unique())
            unsup_snk = sorted(grp[grp["sink_support"] != "Supported"]["sink_connector"].unique())
            unsup_all = sorted(set(unsup_src + unsup_snk))
            rows.append({
                "pipeline_name": pname,
                "factory_name": fname,
                "factory_type": ftype,
                "subscription_id": sid,
                "subscription_name": sname,
                "copy_activities": total,
                "total_connector_usages": total_conn,
                "supported_connector_usages": supported_conn,
                "connector_support_pct": round(supported_conn / max(total_conn, 1) * 100, 1),
                "unsupported_connectors": ", ".join(unsup_all) if unsup_all else "",
            })
        result = pd.DataFrame(rows).sort_values("connector_support_pct", ascending=True)
        return self._export_df_csv(self.assessment_details, "connectors_by_pipeline", result)

    def _export_assessment_connectors_by_factory(self) -> str:
        """Per-factory connector support assessment."""
        df = self._get_assessment_connectors_df()
        if df.empty:
            return self._export_df_csv(self.assessment_details, "connectors_by_factory", df)
        rows = []
        for (fname, ftype, sid, sname), grp in df.groupby(
            ["factory_name", "factory_type", "subscription_id", "subscription_name"]
        ):
            total = len(grp)
            src_sup = (grp["source_support"] == "Supported").sum()
            snk_sup = (grp["sink_support"] == "Supported").sum()
            total_conn = total * 2
            supported_conn = int(src_sup + snk_sup)
            unsup_src = sorted(grp[grp["source_support"] != "Supported"]["source_connector"].unique())
            unsup_snk = sorted(grp[grp["sink_support"] != "Supported"]["sink_connector"].unique())
            unsup_all = sorted(set(unsup_src + unsup_snk))
            rows.append({
                "factory_name": fname,
                "factory_type": ftype,
                "subscription_id": sid,
                "subscription_name": sname,
                "copy_activities": total,
                "total_connector_usages": total_conn,
                "supported_connector_usages": supported_conn,
                "connector_support_pct": round(supported_conn / max(total_conn, 1) * 100, 1),
                "unsupported_connectors": ", ".join(unsup_all) if unsup_all else "",
            })
        result = pd.DataFrame(rows).sort_values("connector_support_pct", ascending=True)
        return self._export_df_csv(self.assessment_details, "connectors_by_factory", result)

    def _export_assessment_connectors_by_subscription(self) -> str:
        """Per-subscription connector support assessment."""
        df = self._get_assessment_connectors_df()
        if df.empty:
            return self._export_df_csv(self.assessment_details, "connectors_by_subscription", df)
        rows = []
        for (sid, sname), grp in df.groupby(["subscription_id", "subscription_name"]):
            total = len(grp)
            src_sup = (grp["source_support"] == "Supported").sum()
            snk_sup = (grp["sink_support"] == "Supported").sum()
            total_conn = total * 2
            supported_conn = int(src_sup + snk_sup)
            unsup_src = sorted(grp[grp["source_support"] != "Supported"]["source_connector"].unique())
            unsup_snk = sorted(grp[grp["sink_support"] != "Supported"]["sink_connector"].unique())
            unsup_all = sorted(set(unsup_src + unsup_snk))
            rows.append({
                "subscription_id": sid,
                "subscription_name": sname,
                "factory_count": grp["factory_name"].nunique(),
                "copy_activities": total,
                "total_connector_usages": total_conn,
                "supported_connector_usages": supported_conn,
                "connector_support_pct": round(supported_conn / max(total_conn, 1) * 100, 1),
                "unsupported_connectors": ", ".join(unsup_all) if unsup_all else "",
            })
        result = pd.DataFrame(rows).sort_values("connector_support_pct", ascending=True)
        return self._export_df_csv(self.assessment_details, "connectors_by_subscription", result)

    def _export_assessment_connectors_by_tenant(self) -> str:
        """Tenant-level connector support assessment."""
        df = self._get_assessment_connectors_df()
        if df.empty:
            return self._export_df_csv(self.assessment_details, "connectors_by_tenant", df)
        total = len(df)
        src_sup = (df["source_support"] == "Supported").sum()
        snk_sup = (df["sink_support"] == "Supported").sum()
        total_conn = total * 2
        supported_conn = int(src_sup + snk_sup)
        unsup_src = sorted(df[df["source_support"] != "Supported"]["source_connector"].unique())
        unsup_snk = sorted(df[df["sink_support"] != "Supported"]["sink_connector"].unique())
        unsup_all = sorted(set(unsup_src + unsup_snk))
        result = pd.DataFrame([{
            "subscription_count": df["subscription_id"].nunique(),
            "factory_count": df["factory_name"].nunique(),
            "pipeline_count": df.groupby(["factory_name", "pipeline_name"]).ngroups,
            "copy_activities": total,
            "total_connector_usages": total_conn,
            "supported_connector_usages": supported_conn,
            "connector_support_pct": round(supported_conn / max(total_conn, 1) * 100, 1),
            "unsupported_connectors": ", ".join(unsup_all) if unsup_all else "",
        }])
        return self._export_df_csv(self.assessment_details, "connectors_by_tenant", result)

    # --- Assessment rollup exports ---

    def _build_assessment_rows(self) -> List[Dict]:
        """Build the flat list of assessment rows used by both CSV and MD exports."""
        rows = []

        # --- Activity types ---
        activity_counts = defaultdict(lambda: {"occurrences": 0, "pipelines": set(), "factories": set()})
        for a in self.stats.activities:
            atype = a.get("activity_type", "Unknown")
            pname = a.get("pipeline_name", "")
            fname = a.get("factory_name", "")
            rec = activity_counts[atype]
            rec["occurrences"] += 1
            if pname:
                rec["pipelines"].add((fname, pname))
            if fname:
                rec["factories"].add(fname)

        for atype, rec in sorted(activity_counts.items(), key=lambda x: -x[1]["occurrences"]):
            support, notes = WKMIGRATE_ACTIVITY_SUPPORT.get(atype, ("Unsupported", "Unknown activity type — manual conversion needed"))
            category = ACTIVITY_TYPE_TO_CATEGORY.get(atype, "Unknown")
            rows.append({
                "category": f"Activity Type ({category})",
                "item": atype,
                "wkmigrate_support": support,
                "occurrences": rec["occurrences"],
                "pipelines_affected": len(rec["pipelines"]),
                "factories_affected": len(rec["factories"]),
                "notes": notes,
            })

        # --- Source connectors ---
        src_counts = defaultdict(lambda: {"occurrences": 0, "pipelines": set(), "factories": set(), "raw_types": set()})
        for a in self.stats.activities:
            if a.get("activity_category") != "DataMovement":
                continue
            raw_src = a.get("source_type_actual", "")
            if not raw_src:
                continue
            friendly = friendly_connector_name(raw_src)
            rec = src_counts[friendly]
            rec["occurrences"] += 1
            rec["raw_types"].add(raw_src)
            pname = a.get("pipeline_name", "")
            fname = a.get("factory_name", "")
            if pname:
                rec["pipelines"].add((fname, pname))
            if fname:
                rec["factories"].add(fname)

        for friendly, rec in sorted(src_counts.items(), key=lambda x: -x[1]["occurrences"]):
            # Check support using any of the raw types
            support, notes = "Unsupported", f"{friendly} — no wkmigrate translator available"
            for raw in rec["raw_types"]:
                s, n = wkmigrate_connector_status(raw, friendly)
                if s == "Supported":
                    support, notes = s, n
                    break
            rows.append({
                "category": "Source Connector",
                "item": friendly,
                "wkmigrate_support": support,
                "occurrences": rec["occurrences"],
                "pipelines_affected": len(rec["pipelines"]),
                "factories_affected": len(rec["factories"]),
                "notes": notes,
            })

        # --- Sink connectors ---
        sink_counts = defaultdict(lambda: {"occurrences": 0, "pipelines": set(), "factories": set(), "raw_types": set()})
        for a in self.stats.activities:
            if a.get("activity_category") != "DataMovement":
                continue
            raw_snk = a.get("sink_type_actual", "")
            if not raw_snk:
                continue
            friendly = friendly_connector_name(raw_snk)
            rec = sink_counts[friendly]
            rec["occurrences"] += 1
            rec["raw_types"].add(raw_snk)
            pname = a.get("pipeline_name", "")
            fname = a.get("factory_name", "")
            if pname:
                rec["pipelines"].add((fname, pname))
            if fname:
                rec["factories"].add(fname)

        for friendly, rec in sorted(sink_counts.items(), key=lambda x: -x[1]["occurrences"]):
            support, notes = "Unsupported", f"{friendly} — no wkmigrate translator available"
            for raw in rec["raw_types"]:
                s, n = wkmigrate_connector_status(raw, friendly)
                if s == "Supported":
                    support, notes = s, n
                    break
            rows.append({
                "category": "Sink Connector",
                "item": friendly,
                "wkmigrate_support": support,
                "occurrences": rec["occurrences"],
                "pipelines_affected": len(rec["pipelines"]),
                "factories_affected": len(rec["factories"]),
                "notes": notes,
            })

        # --- Dataflows (always unsupported) ---
        if self.stats.dataflows:
            df_by_type = defaultdict(lambda: {"occurrences": 0, "pipelines": set(), "factories": set()})
            for df in self.stats.dataflows:
                dtype = df.get("type", "Unknown")
                rec = df_by_type[dtype]
                rec["occurrences"] += 1
                fname = df.get("factory_name", "")
                if fname:
                    rec["factories"].add(fname)
            # Count ExecuteDataFlow activities to get pipeline associations
            for a in self.stats.activities:
                if a.get("activity_type") in ("ExecuteDataFlow", "DataFlow"):
                    df_by_type["MappingDataFlow"]["pipelines"].add(
                        (a.get("factory_name", ""), a.get("pipeline_name", "")))
            for dtype, rec in sorted(df_by_type.items(), key=lambda x: -x[1]["occurrences"]):
                rows.append({
                    "category": "Dataflow",
                    "item": dtype,
                    "wkmigrate_support": "Unsupported",
                    "occurrences": rec["occurrences"],
                    "pipelines_affected": len(rec["pipelines"]),
                    "factories_affected": len(rec["factories"]),
                    "notes": "Dataflows must be rewritten as Spark transformations",
                })

        return rows

    def _compute_readiness_scores(self, rows: List[Dict]) -> Dict:
        """Compute migration readiness scores from assessment rows."""
        # Activity-level readiness: what % of activity occurrences are supported?
        act_rows = [r for r in rows if r["category"].startswith("Activity Type")]
        total_acts = sum(r["occurrences"] for r in act_rows)
        supported_acts = sum(r["occurrences"] for r in act_rows if r["wkmigrate_support"] == "Supported")

        # Pipeline-level readiness: how many pipelines have ALL activities supported?
        # Build per-pipeline support status
        pipeline_support = {}  # (factory, pipeline) -> has_unsupported
        for a in self.stats.activities:
            key = (a.get("factory_name", ""), a.get("pipeline_name", ""))
            atype = a.get("activity_type", "Unknown")
            support, _ = WKMIGRATE_ACTIVITY_SUPPORT.get(atype, ("Unsupported", ""))
            if support != "Supported":
                pipeline_support[key] = True
            elif key not in pipeline_support:
                pipeline_support[key] = False

        total_pipelines = len(pipeline_support)
        fully_supported_pipelines = sum(1 for v in pipeline_support.values() if not v)
        partially_supported = sum(1 for v in pipeline_support.values() if v)

        # Connector readiness
        conn_rows = [r for r in rows if r["category"] in ("Source Connector", "Sink Connector")]
        total_conn = sum(r["occurrences"] for r in conn_rows)
        supported_conn = sum(r["occurrences"] for r in conn_rows if r["wkmigrate_support"] == "Supported")

        # Top unsupported items by pipeline impact
        unsupported = [r for r in rows if r["wkmigrate_support"] != "Supported"]
        unsupported_by_impact = sorted(unsupported, key=lambda r: -r["pipelines_affected"])[:10]

        # Pipelines with highest unsupported activity count
        pipeline_unsupported_count = defaultdict(lambda: {"unsupported": 0, "total": 0, "factory": ""})
        for a in self.stats.activities:
            key = (a.get("factory_name", ""), a.get("pipeline_name", ""))
            atype = a.get("activity_type", "Unknown")
            support, _ = WKMIGRATE_ACTIVITY_SUPPORT.get(atype, ("Unsupported", ""))
            rec = pipeline_unsupported_count[key]
            rec["total"] += 1
            rec["factory"] = a.get("factory_name", "")
            if support != "Supported":
                rec["unsupported"] += 1
        at_risk = sorted(
            [(k, v) for k, v in pipeline_unsupported_count.items() if v["unsupported"] > 0],
            key=lambda x: -x[1]["unsupported"]
        )[:15]

        return {
            "total_activities": total_acts,
            "supported_activities": supported_acts,
            "activity_pct": round(supported_acts / max(total_acts, 1) * 100, 1),
            "total_pipelines": total_pipelines,
            "fully_supported_pipelines": fully_supported_pipelines,
            "partially_supported_pipelines": partially_supported,
            "pipeline_pct": round(fully_supported_pipelines / max(total_pipelines, 1) * 100, 1),
            "total_connectors": total_conn,
            "supported_connectors": supported_conn,
            "connector_pct": round(supported_conn / max(total_conn, 1) * 100, 1),
            "top_unsupported": unsupported_by_impact,
            "at_risk_pipelines": at_risk,
        }

    def _export_assessment_csv(self) -> str:
        """Export assessment CSV with one row per item showing wkmigrate support status."""
        rows = self._build_assessment_rows()
        return self._export_csv(self.assessment_dir, "assessment", rows)

    def _export_assessment_md(self) -> str:
        """Export human-readable migration readiness report."""
        rows = self._build_assessment_rows()
        scores = self._compute_readiness_scores(rows)

        lines = []
        lines.append("# wkmigrate Migration Readiness Assessment\n")
        lines.append("This report evaluates the ADF environment against the capabilities of the")
        lines.append("[wkmigrate](https://pypi.org/project/wkmigrate/) library for automated migration")
        lines.append("to Databricks Workflows.\n")

        # --- Section 1: Readiness Score ---
        lines.append("---\n")
        lines.append("## 1. Migration Readiness Summary\n")
        lines.append("| Metric | Supported | Total | Coverage |")
        lines.append("| --- | ---: | ---: | ---: |")
        lines.append(f"| **Activity occurrences** | {scores['supported_activities']:,} | {scores['total_activities']:,} | **{scores['activity_pct']}%** |")
        lines.append(f"| **Pipelines (fully translatable)** | {scores['fully_supported_pipelines']:,} | {scores['total_pipelines']:,} | **{scores['pipeline_pct']}%** |")
        lines.append(f"| **Connector usages** | {scores['supported_connectors']:,} | {scores['total_connectors']:,} | **{scores['connector_pct']}%** |")
        lines.append("")

        # --- Section 2: Activity Type Coverage ---
        lines.append("---\n")
        lines.append("## 2. Activity Type Coverage\n")
        act_rows = [r for r in rows if r["category"].startswith("Activity Type")]
        if act_rows:
            lines.append("| Activity Type | Category | Support | Occurrences | Pipelines | Notes |")
            lines.append("| --- | --- | --- | ---: | ---: | --- |")
            for r in act_rows:
                cat_label = r["category"].replace("Activity Type (", "").rstrip(")")
                icon = "✅" if r["wkmigrate_support"] == "Supported" else "❌"
                lines.append(
                    f"| {r['item']} | {cat_label} | {icon} {r['wkmigrate_support']} "
                    f"| {r['occurrences']:,} | {r['pipelines_affected']:,} | {r['notes']} |"
                )
            # Totals
            total = sum(r["occurrences"] for r in act_rows)
            sup = sum(r["occurrences"] for r in act_rows if r["wkmigrate_support"] == "Supported")
            lines.append(f"| **Total** | | | **{total:,}** | | **{sup:,} supported ({scores['activity_pct']}%)** |")
        lines.append("")

        # --- Section 3: Source Connector Coverage ---
        lines.append("---\n")
        lines.append("## 3. Source Connector Coverage\n")
        src_rows = [r for r in rows if r["category"] == "Source Connector"]
        if src_rows:
            lines.append("| Connector | Support | Occurrences | Pipelines | Notes |")
            lines.append("| --- | --- | ---: | ---: | --- |")
            for r in src_rows:
                icon = "✅" if r["wkmigrate_support"] == "Supported" else "❌"
                lines.append(
                    f"| {r['item']} | {icon} {r['wkmigrate_support']} "
                    f"| {r['occurrences']:,} | {r['pipelines_affected']:,} | {r['notes']} |"
                )
        else:
            lines.append("No source connectors found.\n")
        lines.append("")

        # --- Section 4: Sink Connector Coverage ---
        lines.append("---\n")
        lines.append("## 4. Sink Connector Coverage\n")
        sink_rows = [r for r in rows if r["category"] == "Sink Connector"]
        if sink_rows:
            lines.append("| Connector | Support | Occurrences | Pipelines | Notes |")
            lines.append("| --- | --- | ---: | ---: | --- |")
            for r in sink_rows:
                icon = "✅" if r["wkmigrate_support"] == "Supported" else "❌"
                lines.append(
                    f"| {r['item']} | {icon} {r['wkmigrate_support']} "
                    f"| {r['occurrences']:,} | {r['pipelines_affected']:,} | {r['notes']} |"
                )
        else:
            lines.append("No sink connectors found.\n")
        lines.append("")

        # --- Section 5: Dataflows ---
        df_rows = [r for r in rows if r["category"] == "Dataflow"]
        if df_rows:
            lines.append("---\n")
            lines.append("## 5. Dataflow Coverage\n")
            lines.append("| Type | Support | Count | Factories | Notes |")
            lines.append("| --- | --- | ---: | ---: | --- |")
            for r in df_rows:
                lines.append(
                    f"| {r['item']} | ❌ {r['wkmigrate_support']} "
                    f"| {r['occurrences']:,} | {r['factories_affected']:,} | {r['notes']} |"
                )
            lines.append("")

        # --- Section 6: Top Unsupported Items by Pipeline Impact ---
        lines.append("---\n")
        section_num = 6 if df_rows else 5
        lines.append(f"## {section_num}. Top Unsupported Items by Pipeline Impact\n")
        if scores["top_unsupported"]:
            lines.append("| Category | Item | Pipelines Affected | Occurrences | Notes |")
            lines.append("| --- | --- | ---: | ---: | --- |")
            for r in scores["top_unsupported"]:
                lines.append(
                    f"| {r['category']} | {r['item']} "
                    f"| {r['pipelines_affected']:,} | {r['occurrences']:,} | {r['notes']} |"
                )
        else:
            lines.append("All items are supported — no blockers found.\n")
        lines.append("")

        # --- Section 7: At-Risk Pipelines ---
        section_num += 1
        lines.append("---\n")
        lines.append(f"## {section_num}. Pipelines with Most Unsupported Activities\n")
        if scores["at_risk_pipelines"]:
            lines.append("| Factory | Pipeline | Unsupported Activities | Total Activities | % Unsupported |")
            lines.append("| --- | --- | ---: | ---: | ---: |")
            for (factory, pipeline), rec in scores["at_risk_pipelines"]:
                pct = round(rec["unsupported"] / max(rec["total"], 1) * 100, 1)
                lines.append(f"| {factory} | {pipeline} | {rec['unsupported']} | {rec['total']} | {pct}% |")
        else:
            lines.append("All pipelines are fully translatable.\n")
        lines.append("")

        # --- Section 8: Recommendations ---
        section_num += 1
        lines.append("---\n")
        lines.append(f"## {section_num}. Recommendations\n")

        act_rows_unsup = [r for r in rows if r["category"].startswith("Activity Type") and r["wkmigrate_support"] != "Supported"]
        if scores["activity_pct"] >= 80:
            lines.append("**High readiness.** The majority of activities can be auto-translated by wkmigrate.\n")
        elif scores["activity_pct"] >= 50:
            lines.append("**Moderate readiness.** A significant portion of activities require manual migration.\n")
        else:
            lines.append("**Low readiness.** Most activities require manual migration or rewriting.\n")

        lines.append("**Suggested next steps:**\n")
        step = 1
        lines.append(f"{step}. Run `wkmigrate` on the fully-supported pipelines first for quick wins.")
        step += 1

        if any(r["item"] == "ExecutePipeline" for r in act_rows_unsup):
            lines.append(f"{step}. Flatten `ExecutePipeline` references — inline child pipelines or restructure as Databricks multi-task jobs.")
            step += 1
        if any(r["item"] == "ExecuteDataFlow" for r in act_rows_unsup):
            lines.append(f"{step}. Rewrite dataflows as Spark Declarative Pipelines (SDP) or Lakeflow Designer Pipelines.")
            step += 1
        if any(r["item"] in ("SqlServerStoredProcedure", "Script") for r in act_rows_unsup):
            lines.append(f"{step}. Convert stored procedures and SQL scripts to Spark SQL or notebook tasks.")
            step += 1
        if any(r["item"] in ("Switch", "Until") for r in act_rows_unsup):
            lines.append(f"{step}. Refactor `Switch` to nested `IfCondition` and `Until` loops to bounded `ForEach` where possible.")
            step += 1

        conn_unsup = [r for r in rows if r["category"] in ("Source Connector", "Sink Connector") and r["wkmigrate_support"] != "Supported"]
        if conn_unsup:
            names = ", ".join(sorted(set(r["item"] for r in conn_unsup)))
            lines.append(f"{step}. Unsupported connectors ({names}) — add custom Spark JDBC/REST readers in notebooks.")
            step += 1

        lines.append("")

        md_path = os.path.join(self.assessment_dir, "assessment.md")
        with open(md_path, "w") as f:
            f.write("\n".join(lines))
        return md_path

    # =====================================================================
    # OVERALL ROLLUP
    # =====================================================================

    def _export_overall_rollup(self) -> str:
        """Overall rollup CSV + MD combining both tracks with tenant/subscription/factory summaries."""
        rows = []
        def add(metric, value):
            rows.append({"metric": metric, "value": value})

        copy_acts = [a for a in self.stats.activities if a.get("activity_category") == "DataMovement"]
        total_read = sum(c.get("dataReadBytes", 0) or 0 for c in self.stats.copy_activity_details)
        total_written = sum(c.get("dataWrittenBytes", 0) or 0 for c in self.stats.copy_activity_details)
        total_ing_cost = sum(c.get("estimatedCost", 0) for c in self.stats.copy_activity_details)
        total_runs = sum(s.get("totalRuns", 0) for s in self.stats.pipeline_job_stats) if self.stats.pipeline_job_stats else 0
        total_succeeded = sum(s.get("succeededRuns", 0) for s in self.stats.pipeline_job_stats) if self.stats.pipeline_job_stats else 0
        total_failed = sum(s.get("failedRuns", 0) for s in self.stats.pipeline_job_stats) if self.stats.pipeline_job_stats else 0
        success_rate = round(total_succeeded / max(total_runs, 1) * 100, 1)
        total_orch = sum(s.get("orchCost", 0) for s in self.stats.pipeline_job_stats) if self.stats.pipeline_job_stats else 0
        total_pipeline_act = sum(s.get("pipelineActCost", 0) for s in self.stats.pipeline_job_stats) if self.stats.pipeline_job_stats else 0
        total_external = sum(s.get("externalCost", 0) for s in self.stats.pipeline_job_stats) if self.stats.pipeline_job_stats else 0
        # Dataflow cost
        total_dataflow_cost = 0.0
        for ar in self.stats.activity_runs:
            if ar.get("activityType", "") in ("ExecuteDataFlow", "DataFlow"):
                total_dataflow_cost += ar.get("execCost", 0) or 0
        total_orch_cost = total_orch + total_pipeline_act + total_external + total_dataflow_cost
        cb = self.stats.cost_breakdown

        add("Factory/Workspace Count", len(self.stats.factories))
        add("Pipeline Count", len(self.stats.pipelines))
        add("Activity Count", len(self.stats.activities))
        add("Copy Activities Defined", len(copy_acts))
        add("Copy Activity Runs", len(self.stats.copy_activity_details))
        add("Total Pipeline Runs", total_runs)
        add("Success Rate", f"{success_rate}%")

        # Write CSV
        csv_path = os.path.join(self.output_dir, "overall.csv")
        pd.DataFrame(rows).to_csv(csv_path, index=False)

        # ---- Build rich MD ----
        md = ["# Overall Rollup\n"]

        # === Tenant-Level Summary ===
        md.append("## Tenant-Level Summary\n")
        md.append("| Metric | Value |")
        md.append("| --- | --- |")
        md.append(f"| Factory/Workspace Count | {len(self.stats.factories)} |")
        md.append(f"| Pipeline Count | {len(self.stats.pipelines)} |")
        md.append(f"| Activity Count | {len(self.stats.activities)} |")
        md.append(f"| Copy Activities Defined | {len(copy_acts)} |")
        md.append(f"| Copy Activity Runs | {len(self.stats.copy_activity_details)} |")
        md.append(f"| Total Data Volume (GB) | {round((total_read + total_written) / 1_073_741_824, 2)} |")
        md.append(f"| Total Pipeline Runs | {total_runs} |")
        md.append(f"| Succeeded / Failed | {total_succeeded} / {total_failed} |")
        md.append(f"| Success Rate | {success_rate}% |")
        md.append(f"| SHIR Node Count | {len(self.stats.shir_nodes)} |")
        md.append(f"| Database Instance Count | {len(self.stats.db_instances)} |")
        md.append(f"| Table Reference Count | {len(self.stats.table_discovery)} |")
        md.append("")

        # Cost summary
        md.append("### Cost Summary\n")
        md.append("| Cost Category | Amount |")
        md.append("| --- | --- |")
        md.append(f"| Total Ingestion Cost | ${total_ing_cost:.4f} |")
        md.append(f"| Orchestration Fee | ${total_orch:.4f} |")
        md.append(f"| Pipeline Activity Cost | ${total_pipeline_act:.4f} |")
        md.append(f"| External Activity Cost | ${total_external:.4f} |")
        md.append(f"| Dataflow Cost | ${total_dataflow_cost:.4f} |")
        md.append(f"| Total Orchestration Cost | ${total_orch_cost:.4f} |")
        if cb:
            md.append(f"| Total Estimated Cost (all) | ${cb.get('totalEstimatedCost', 0):.2f} |")
        if self.stats.actual_costs:
            # Bug Fix 1: Actual compute cost = sum of costs where meterSubcategory is empty
            # (no subcategory = direct compute/orchestration costs; subcategories are infra like Airflow, Private Link)
            actual_compute_only = sum(c.get("cost", 0) for c in self.stats.actual_costs
                                      if not (c.get("meterSubcategory", "") or "").strip())
            airflow_actual = sum(c.get("cost", 0) for c in self.stats.actual_costs
                                 if "airflow" in (c.get("meterSubcategory", "") or "").lower())
            md.append(f"| Total Actual Cost (compute only, excl. Airflow) | ${actual_compute_only:.2f} |")
            if airflow_actual > 0:
                md.append(f"| Managed Airflow Actual Cost | ${airflow_actual:.2f} |")
        md.append("")

        # Ingestion highlights
        md.append("### Ingestion Highlights\n")
        tech_counts = defaultdict(int)
        for t in self.stats.ingestion_techniques:
            tech_counts[t.get("detected_technique", "Unknown")] += 1
        if tech_counts:
            md.append("| Technique | Count |")
            md.append("| --- | --- |")
            for tech, cnt in sorted(tech_counts.items(), key=lambda x: -x[1]):
                md.append(f"| {tech} | {cnt} |")
            md.append("")

        src_counts = defaultdict(int)
        snk_counts = defaultdict(int)
        for a in copy_acts:
            src = friendly_connector_name(a.get("source_type_actual", ""))
            snk = friendly_connector_name(a.get("sink_type_actual", ""))
            if src:
                src_counts[src] += 1
            if snk:
                snk_counts[snk] += 1
        if src_counts:
            md.append("**Top 5 Source Connectors**: " + ", ".join(f"{s} ({c})" for s, c in sorted(src_counts.items(), key=lambda x: -x[1])[:5]))
            md.append("")
        if snk_counts:
            md.append("**Top 5 Sink Connectors**: " + ", ".join(f"{s} ({c})" for s, c in sorted(snk_counts.items(), key=lambda x: -x[1])[:5]))
            md.append("")

        # Orchestration highlights
        md.append("### Orchestration Highlights\n")
        md.append(f"- Datasets: {len(self.stats.datasets)}, Linked Services: {len(self.stats.linked_services)}, Triggers: {len(self.stats.triggers)}")
        md.append(f"- Dataflows: Gen1={self.stats.dataflow_types.get('MappingDataFlow', 0)}, Gen2={self.stats.dataflow_types.get('DataflowGen2', 0)}")
        db_acts = self.stats.databricks_classifications
        md.append(f"- Databricks Activities: {len(db_acts)}")
        md_yes = sum(1 for m in self.stats.metadata_driven_classifications if m.get("is_metadata_driven") == "Yes")
        md_likely = sum(1 for m in self.stats.metadata_driven_classifications if m.get("is_metadata_driven") == "Likely")
        md.append(f"- Metadata-Driven Pipelines: Yes={md_yes}, Likely={md_likely}")
        md.append("")

        # === Subscription-Level Summary ===
        sub_ids = set()
        for f_info in self.stats.factories.values():
            sid = f_info.get("subscription_id", "")
            if sid:
                sub_ids.add(sid)

        md.append("## Subscription-Level Summary\n")
        if sub_ids:
            md.append("| Subscription | Factories | Pipelines | Activities | Copy Acts | Pipeline Runs | Success Rate | Est. Cost |")
            md.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
            gt_fac = gt_pipe = gt_act = gt_copy = gt_runs2 = 0
            gt_cost = 0.0
            for sid in sorted(sub_ids):
                sub_display = self._sub_name(sid) or sid[:12] + "..."
                sub_factories_set = {name for name, f in self.stats.factories.items() if f.get("subscription_id") == sid}
                sub_pipelines = [p for p in self.stats.pipelines if p.get("subscription_id") == sid]
                sub_activities_list = [a for a in self.stats.activities if a.get("subscription_id") == sid]
                sub_copy = [a for a in sub_activities_list if a.get("activity_category") == "DataMovement"]
                sub_stats = [s for s in self.stats.pipeline_job_stats if s.get("factory_name") in sub_factories_set] if self.stats.pipeline_job_stats else []
                s_runs = sum(s.get("totalRuns", 0) for s in sub_stats)
                s_succeeded = sum(s.get("succeededRuns", 0) for s in sub_stats)
                s_rate = round(s_succeeded / max(s_runs, 1) * 100, 1)
                s_cost = sum(s.get("totalCost", 0) for s in sub_stats)
                md.append(f"| {sub_display} | {len(sub_factories_set)} | {len(sub_pipelines)} | {len(sub_activities_list)} | {len(sub_copy)} | {s_runs} | {s_rate}% | ${s_cost:.4f} |")
                gt_fac += len(sub_factories_set); gt_pipe += len(sub_pipelines); gt_act += len(sub_activities_list)
                gt_copy += len(sub_copy); gt_runs2 += s_runs; gt_cost += s_cost
            gt_rate = round(total_succeeded / max(total_runs, 1) * 100, 1) if total_runs else 0
            md.append(f"| **Total** | **{gt_fac}** | **{gt_pipe}** | **{gt_act}** | **{gt_copy}** | **{gt_runs2}** | **{gt_rate}%** | **${gt_cost:.4f}** |")
        else:
            md.append("No subscription data available.")
        md.append("")

        # === Factory-Level Summary (Change 5: subscription col, Change 7: sort by cost, Change 12: filter empty) ===
        md.append("## Factory-Level Summary\n")
        if self.stats.factories:
            # Build factory-level stats
            factory_data = []
            for fname, f_info in self.stats.factories.items():
                f_pipelines = [p for p in self.stats.pipelines if p.get("factory_name") == fname]
                f_activities = [a for a in self.stats.activities if a.get("factory_name") == fname]
                f_copy = [a for a in f_activities if a.get("activity_category") == "DataMovement"]
                f_stats = [s for s in self.stats.pipeline_job_stats if s.get("factory_name") == fname] if self.stats.pipeline_job_stats else []
                f_runs = sum(s.get("totalRuns", 0) for s in f_stats)
                f_succeeded = sum(s.get("succeededRuns", 0) for s in f_stats)
                f_rate = round(f_succeeded / max(f_runs, 1) * 100, 1)
                f_cost = sum(s.get("totalCost", 0) for s in f_stats)
                # Change 12: filter empty factories
                if len(f_pipelines) == 0 and f_runs == 0 and f_cost <= 0:
                    continue
                factory_data.append({
                    "subscription": self._sub_name_for_factory(fname),
                    "name": fname,
                    "pipelines": len(f_pipelines),
                    "activities": len(f_activities),
                    "copy_acts": len(f_copy),
                    "runs": f_runs,
                    "rate": f_rate,
                    "cost": f_cost,
                })
            factory_data.sort(key=lambda x: -x["cost"])  # Change 7: sort by cost descending
            md.append("| Subscription | Factory | Pipelines | Activities | Copy Acts | Runs | Success Rate | Est. Cost |")
            md.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
            gt_pipe = gt_act = gt_copy = gt_runs2 = 0
            gt_cost = 0.0
            top_5 = factory_data[:5]
            remainder = factory_data[5:]
            for fd in top_5:
                md.append(f"| {fd['subscription']} | {fd['name']} | {fd['pipelines']} | {fd['activities']} | {fd['copy_acts']} | {fd['runs']} | {fd['rate']}% | ${fd['cost']:.4f} |")
                gt_pipe += fd["pipelines"]; gt_act += fd["activities"]; gt_copy += fd["copy_acts"]
                gt_runs2 += fd["runs"]; gt_cost += fd["cost"]
            if remainder:
                r_pipe = sum(fd["pipelines"] for fd in remainder)
                r_act = sum(fd["activities"] for fd in remainder)
                r_copy = sum(fd["copy_acts"] for fd in remainder)
                r_runs = sum(fd["runs"] for fd in remainder)
                r_cost = sum(fd["cost"] for fd in remainder)
                r_succeeded = sum(fd["runs"] * fd["rate"] / 100 for fd in remainder)
                r_rate = round(r_succeeded / max(r_runs, 1) * 100, 1)
                md.append(f"| | Other ({len(remainder)} factories) | {r_pipe} | {r_act} | {r_copy} | {r_runs} | {r_rate}% | ${r_cost:.4f} |")
                gt_pipe += r_pipe; gt_act += r_act; gt_copy += r_copy
                gt_runs2 += r_runs; gt_cost += r_cost
            md.append(f"| **Total** | | **{gt_pipe}** | **{gt_act}** | **{gt_copy}** | **{gt_runs2}** | | **${gt_cost:.4f}** |")
        else:
            md.append("No factory data available.")
        md.append("")

        # Write MD
        md_path = os.path.join(self.output_dir, "overall.md")
        with open(md_path, "w") as f:
            f.write("\n".join(md))

        return csv_path

    # =====================================================================
    # LEGACY / REMOVED — kept as stubs to prevent attribute errors
    # =====================================================================

    def _export_cost_comparison(self) -> str:
        """Export cost comparison: factory-level estimate vs actual, then pipeline-level estimates only."""
        cb = self.stats.cost_breakdown

        # Map factory -> subscription
        def _factory_sub(fname):
            f_info = self.stats.factories.get(fname, {})
            sid = f_info.get("subscription_id", "")
            return self._sub_name(sid) or sid or "Unknown"

        # --- Estimated costs per factory (rolled up from pipeline stats) ---
        factory_est = defaultdict(lambda: {"orchestration": 0.0, "data_movement": 0.0, "pipeline_activity": 0.0, "external": 0.0})
        for ps in self.stats.pipeline_job_stats:
            fname = ps.get("factory_name", "")
            factory_est[fname]["orchestration"] += ps.get("orchCost", 0)
            factory_est[fname]["data_movement"] += ps.get("ingestionCost", 0)
            factory_est[fname]["pipeline_activity"] += ps.get("pipelineActCost", 0)
            factory_est[fname]["external"] += ps.get("externalCost", 0)

        # --- Actual costs per factory (compute/orchestration only, excluding infra) ---
        factory_actual_compute = defaultdict(float)
        for ac in self.stats.actual_costs:
            fname = ac.get("factory_name", "")
            meter = ac.get("meterSubcategory", "") or ""
            cost = ac.get("cost", 0)
            # Skip infrastructure costs (Managed Airflow, Private Link, etc.)
            # so that actual vs estimated is an apples-to-apples comparison
            if not meter:
                factory_actual_compute[fname] += cost

        all_factories = sorted(set(list(factory_est.keys()) + list(factory_actual_compute.keys())))

        # --- Factory-level rows: estimate vs actual ---
        factory_rows = []
        for fname in all_factories:
            est = factory_est.get(fname, {"orchestration": 0, "data_movement": 0, "pipeline_activity": 0, "external": 0})
            est_total = sum(est.values())
            act_compute = factory_actual_compute.get(fname, 0)
            if est_total == 0 and act_compute == 0:
                continue
            factory_rows.append({
                "subscription": _factory_sub(fname),
                "factory": fname,
                "est_orchestration": est["orchestration"],
                "est_data_movement": est["data_movement"],
                "est_pipeline_activity": est["pipeline_activity"],
                "est_external": est["external"],
                "est_total": est_total,
                "actual_compute": act_compute,
                "actual_total": act_compute,
            })
        factory_rows.sort(key=lambda r: -r["actual_total"])

        # --- Pipeline-level rows: estimates only ---
        pipeline_rows = []
        if self.stats.pipeline_job_stats:
            for ps in sorted(self.stats.pipeline_job_stats, key=lambda x: x.get("totalCost", 0), reverse=True):
                total = ps.get("totalCost", 0)
                if total == 0:
                    continue
                fname = ps.get("factory_name", "")
                pipeline_rows.append({
                    "subscription": _factory_sub(fname),
                    "factory": fname,
                    "pipeline": ps.get("pipelineName", ""),
                    "runs": ps.get("totalRuns", 0),
                    "est_orchestration": ps.get("orchCost", 0),
                    "est_data_movement": ps.get("ingestionCost", 0),
                    "est_pipeline_activity": ps.get("pipelineActCost", 0),
                    "est_external": ps.get("externalCost", 0),
                    "est_total": total,
                })

        # --- Write CSV ---
        csv_path = os.path.join(self.output_dir, "cost_comparison.csv")
        with open(csv_path, "w") as f:
            # Factory-level section
            f.write("## Factory-Level Costs (Estimated vs Actual)\n")
            if factory_rows:
                keys = ["subscription", "factory",
                        "est_orchestration", "est_data_movement", "est_pipeline_activity",
                        "est_external", "est_total",
                        "actual_compute", "actual_total"]
                f.write(",".join(keys) + "\n")
                for row in factory_rows:
                    f.write(",".join(f"${row[k]:.4f}" if isinstance(row[k], float) else str(row[k]) for k in keys) + "\n")
            f.write("\n")
            # Pipeline-level section
            f.write("## Pipeline-Level Costs (Estimates Only)\n")
            if pipeline_rows:
                keys = ["subscription", "factory", "pipeline", "runs",
                        "est_orchestration", "est_data_movement", "est_pipeline_activity",
                        "est_external", "est_total"]
                f.write(",".join(keys) + "\n")
                for row in pipeline_rows:
                    f.write(",".join(f"${row[k]:.4f}" if isinstance(row[k], float) else str(row[k]) for k in keys) + "\n")

        # --- Write markdown ---
        md_path = os.path.join(self.output_dir, "cost_comparison.md")
        md_lines = ["# Cost Comparison: Estimated vs Actual\n"]

        # Profiling context
        if isinstance(cb, dict) and cb:
            md_lines.append("## Profiling Context\n")
            md_lines.append("| Setting | Value |")
            md_lines.append("| --- | --- |")
            md_lines.append(f"| Profiling Window | {cb.get('days', '')} days |")
            md_lines.append(f"| Region | {cb.get('region', '')} |")
            md_lines.append(f"| Pricing Source | {cb.get('pricing_source', 'default list rates')} |")
            md_lines.append(f"| Total Pipeline Runs | {cb.get('totalPipelineRuns', 0)} |")
            md_lines.append(f"| Note | Estimates use Azure list/retail rates before discounts, reserved capacity, or enterprise agreements. Actual per-pipeline billing requires opt-in via Factory Settings. |")
            md_lines.append("")

        # Denied subscriptions
        if self.stats.cost_mgmt_denied:
            md_lines.append("## Cost Management Access Issues\n")
            for sub_id in self.stats.cost_mgmt_denied:
                sub_display = self._sub_name(sub_id)
                label = f"{sub_display} ({sub_id})" if sub_display else sub_id
                md_lines.append(f"- **{label}**: Cost Management access denied - need Billing Reader or Cost Management Reader role")
            md_lines.append("")

        # --- Summary by Subscription ---
        sub_totals = defaultdict(lambda: {"est": 0.0, "act": 0.0})
        for row in factory_rows:
            sub = row["subscription"]
            sub_totals[sub]["est"] += row["est_total"]
            sub_totals[sub]["act"] += row["actual_total"]

        md_lines.append("## Summary by Subscription\n")
        md_lines.append("| Subscription | Estimated Total | Actual Total | Difference |")
        md_lines.append("| --- | --- | --- | --- |")
        grand_est = grand_act = 0.0
        for sub, totals in sorted(sub_totals.items(), key=lambda x: -x[1]["act"]):
            diff = totals["act"] - totals["est"]
            md_lines.append(f"| {sub} | ${totals['est']:.4f} | ${totals['act']:.4f} | ${diff:.4f} |")
            grand_est += totals["est"]
            grand_act += totals["act"]
        md_lines.append(f"| **TOTAL** | **${grand_est:.4f}** | **${grand_act:.4f}** | **${grand_act - grand_est:.4f}** |")
        md_lines.append("")

        # --- Factory-level detail: estimate vs actual ---
        md_lines.append("## Factory-Level Costs (Estimated vs Actual)\n")
        md_lines.append("| Subscription | Factory | Est. Orch | Est. Data Mvmt | Est. Pipeline Act | Est. External | Est. Total | Act. Compute | Act. Total | Difference |")
        md_lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        for row in factory_rows:
            diff = row["actual_total"] - row["est_total"]
            md_lines.append(
                f"| {row['subscription']} | {row['factory']} "
                f"| ${row['est_orchestration']:.4f} | ${row['est_data_movement']:.4f} "
                f"| ${row['est_pipeline_activity']:.4f} | ${row['est_external']:.4f} "
                f"| ${row['est_total']:.4f} | ${row['actual_compute']:.4f} "
                f"| ${row['actual_total']:.4f} "
                f"| ${diff:.4f} |"
            )
        md_lines.append("")

        # --- Pipeline-level detail: estimates only ---
        if pipeline_rows:
            md_lines.append("## Pipeline-Level Costs (Estimates Only)\n")
            md_lines.append("| Subscription | Factory | Pipeline | Runs | Orchestration | Data Movement | Pipeline Activity | External | Total |")
            md_lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
            for row in pipeline_rows:
                md_lines.append(
                    f"| {row['subscription']} | {row['factory']} "
                    f"| {row['pipeline']} | {row['runs']} "
                    f"| ${row['est_orchestration']:.4f} | ${row['est_data_movement']:.4f} "
                    f"| ${row['est_pipeline_activity']:.4f} | ${row['est_external']:.4f} "
                    f"| ${row['est_total']:.4f} |"
                )
            md_lines.append("")

        with open(md_path, "w") as f:
            f.write("\n".join(md_lines))

        return csv_path

    def _export_summary(self) -> str:
        """Export comprehensive summary report with detailed breakdowns."""
        path = os.path.join(self.output_dir, "summary_report.csv")
        rows = []
        
        # Helper to add rows
        def add(cat, metric, value):
            rows.append({"category": cat, "metric": metric, "value": value})
        
        # Count by factory type
        adf_p = [p for p in self.stats.pipelines if p.get("factory_type") == "ADF"]
        syn_p = [p for p in self.stats.pipelines if p.get("factory_type") == "Synapse"]
        fab_p = [p for p in self.stats.pipelines if p.get("factory_type") == "Fabric"]
        
        # === OVERVIEW ===
        add("Overview - Total", "Factories/Workspaces", len(self.stats.factories))
        add("Overview - Total", "Resource Groups", len(self.stats.resource_groups))
        add("Overview - Total", "Pipelines", len(self.stats.pipelines))
        add("Overview - Total", "Linked Services", len(self.stats.linked_services))
        add("Overview - Total", "Datasets", len(self.stats.datasets))
        add("Overview - Total", "Dataflows", len(self.stats.dataflows))
        add("Overview - Total", "Integration Runtimes", len(self.stats.integration_runtimes))
        add("Overview - Total", "Triggers", len(self.stats.triggers))
        add("Overview - Total", "Activities", len(self.stats.activities))
        
        # By factory type
        for ftype, plist in [("ADF", adf_p), ("Synapse", syn_p), ("Fabric", fab_p)]:
            add(f"Overview - {ftype}", "Pipelines", len(plist))
            acts = [a for a in self.stats.activities if a.get("factory_type") == ftype]
            add(f"Overview - {ftype}", "Activities", len(acts))
        
        if self.stats.skipped_synapse:
            add("Overview - Synapse", "Skipped (403 Forbidden)", len(self.stats.skipped_synapse))

        # === OVERVIEW BY SUBSCRIPTION ===
        sub_factories = defaultdict(set)
        sub_pipelines = defaultdict(list)
        sub_activities = defaultdict(list)
        sub_linked_services = defaultdict(list)
        sub_datasets = defaultdict(list)
        for f_name, f_info in self.stats.factories.items():
            sid = f_info.get("subscription_id") or "Unknown"
            sub_factories[sid].add(f_name)
        for p in self.stats.pipelines:
            sub_pipelines[p.get("subscription_id") or "Unknown"].append(p)
        for a in self.stats.activities:
            sub_activities[a.get("subscription_id") or "Unknown"].append(a)
        for ls in self.stats.linked_services:
            sub_linked_services[ls.get("subscription_id") or "Unknown"].append(ls)
        for ds in self.stats.datasets:
            sub_datasets[ds.get("subscription_id") or "Unknown"].append(ds)
        for sid in sorted(sub_factories.keys()):
            label = f"Subscription {sid[:12]}..."
            add(label, "Factories/Workspaces", len(sub_factories[sid]))
            add(label, "Pipelines", len(sub_pipelines.get(sid, [])))
            add(label, "Activities", len(sub_activities.get(sid, [])))
            add(label, "Linked Services", len(sub_linked_services.get(sid, [])))
            add(label, "Datasets", len(sub_datasets.get(sid, [])))
            # Activity type breakdown per subscription
            sub_act_types = defaultdict(int)
            for a in sub_activities.get(sid, []):
                sub_act_types[a.get("activity_type", "Unknown")] += 1
            for act_type, count in sorted(sub_act_types.items(), key=lambda x: -x[1])[:5]:
                add(label, f"Top Activity: {act_type}", count)

        # === PIPELINES WITH COPY ACTIVITIES ===
        pipelines_with_copy = set()
        for a in self.stats.activities:
            if a.get("activity_category") == "DataMovement":
                pipelines_with_copy.add((a.get("factory_name"), a.get("pipeline_name")))
        add("Pipeline Analysis", "Pipelines with Copy Activities", len(pipelines_with_copy))
        
        # === PIPELINES WITH WEB ACTIVITIES ===
        pipelines_with_web = set()
        for a in self.stats.activities:
            if a.get("activity_type") == "WebActivity":
                pipelines_with_web.add((a.get("factory_name"), a.get("pipeline_name")))
        add("Pipeline Analysis", "Pipelines with WebActivities", len(pipelines_with_web))
        
        # === COPY SOURCE TYPES (Actual) ===
        for src_type, count in sorted(self.stats.copy_source_types.items(), key=lambda x: -x[1]):
            add("Copy Source Types (Actual)", src_type, count)
        
        # === COPY SINK TYPES (Actual) ===
        for sink_type, count in sorted(self.stats.copy_sink_types.items(), key=lambda x: -x[1]):
            add("Copy Sink Types (Actual)", sink_type, count)
        
        # === DATAFLOW TYPES ===
        gen1_count = self.stats.dataflow_types.get("MappingDataFlow", 0)
        gen2_count = self.stats.dataflow_types.get("DataflowGen2", 0)
        add("Dataflow Types", "Dataflow Gen 1 (MappingDataFlow)", gen1_count)
        add("Dataflow Types", "Dataflow Gen 2 (DataflowGen2)", gen2_count)
        for df_type, count in sorted(self.stats.dataflow_types.items(), key=lambda x: -x[1]):
            if df_type not in ["MappingDataFlow", "DataflowGen2"]:
                add("Dataflow Types", df_type, count)
        
        # === INTEGRATION RUNTIME TYPES ===
        for ir_type, count in sorted(self.stats.ir_usage.items(), key=lambda x: -x[1]):
            add("Integration Runtime Types", ir_type, count)
        
        # IR by factory type
        for ftype in ["ADF", "Synapse"]:
            if ftype in self.stats.ir_by_factory_type:
                for ir_type, count in sorted(self.stats.ir_by_factory_type[ftype].items(), key=lambda x: -x[1]):
                    add(f"IR Types - {ftype}", ir_type, count)
        
        # === WEB ACTIVITY API TYPES ===
        for api_type, count in sorted(self.stats.web_activity_apis.items(), key=lambda x: -x[1]):
            add("WebActivity API Types", api_type, count)
        
        # === ACTIVITY TYPES ===
        for act_type, count in sorted(self.stats.activity_types.items(), key=lambda x: -x[1]):
            add("Activity Types", act_type, count)
        
        # === ACTIVITY CATEGORIES ===
        for cat, count in sorted(self.stats.activity_categories.items(), key=lambda x: -x[1]):
            add("Activity Categories", cat, count)
        
        # === TRIGGER TYPES ===
        for trig_type, count in sorted(self.stats.trigger_types.items(), key=lambda x: -x[1]):
            add("Trigger Types", trig_type, count)
        
        # === LINKED SERVICE TYPES ===
        for ls_type, count in sorted(self.stats.linked_service_types.items(), key=lambda x: -x[1]):
            add("Linked Service Types", ls_type, count)
        
        # === TRANSFORMATION ACTIVITIES ===
        transform_types = ["DatabricksNotebook", "DatabricksJob", "DatabricksSparkPython", 
                          "DatabricksSparkJar", "SynapseNotebook", "SparkJob", "Script",
                          "HDInsightHive", "HDInsightPig", "HDInsightSpark", 
                          "SqlServerStoredProcedure", "ExecuteDataFlow"]
        for act_type in transform_types:
            count = self.stats.activity_types.get(act_type, 0)
            if count > 0:
                add("Transformation Activity Types", act_type, count)
        
        # === NEW: DATABASE INSTANCES ===
        if self.stats.db_instances:
            # Unique server+database combos
            unique_dbs = set()
            for db in self.stats.db_instances:
                key = f"{db.get('server', '')}|{db.get('database', '')}|{db.get('type', '')}"
                unique_dbs.add(key)
            add("Database Instances", "Total DB Linked Services", len(self.stats.db_instances))
            add("Database Instances", "Unique Server+DB Combinations", len(unique_dbs))
            # Count by type
            db_type_counts = defaultdict(int)
            for db in self.stats.db_instances:
                db_type_counts[db.get("type", "Unknown")] += 1
            for db_type, cnt in sorted(db_type_counts.items(), key=lambda x: -x[1]):
                add("Database Instances by Type", db_type, cnt)
        
        # === NEW: TABLE DISCOVERY ===
        if self.stats.table_discovery:
            add("Table Discovery", "Total Datasets with Table Refs", len(self.stats.table_discovery))
            tables_with_names = [t for t in self.stats.table_discovery if t.get("table")]
            add("Table Discovery", "Datasets with Explicit Table Names", len(tables_with_names))
            # Count per linked service
            tables_per_ls = defaultdict(int)
            for t in self.stats.table_discovery:
                ls = t.get("linkedService", "Unknown")
                if t.get("table"):
                    tables_per_ls[ls] += 1
            for ls, cnt in sorted(tables_per_ls.items(), key=lambda x: -x[1])[:15]:
                add("Tables per Linked Service", ls, cnt)
        
        # === NEW: SHIR INFRASTRUCTURE ===
        if self.stats.shir_nodes:
            add("SHIR Infrastructure", "Total Nodes", len(self.stats.shir_nodes))
            online = sum(1 for n in self.stats.shir_nodes if n.get("status", "").lower() in ("online", "running"))
            add("SHIR Infrastructure", "Nodes Online", online)
            known_cores = [n.get("estimatedCores", 0) for n in self.stats.shir_nodes if n.get("estimatedCores", 0)]
            unknown_core_nodes = [n for n in self.stats.shir_nodes if not n.get("estimatedCores", 0)]
            total_cores = sum(known_cores)
            total_mem_mb = sum(n.get("totalMemoryMB", 0) for n in self.stats.shir_nodes)
            if unknown_core_nodes:
                unknown_names = ", ".join(n.get("nodeName", "?") for n in unknown_core_nodes)
                cores_label = f"Total Cores (known nodes only — {len(unknown_core_nodes)} node(s) unknown)"
                add("SHIR Infrastructure", cores_label, total_cores)
                add("SHIR Infrastructure", "⚠ Unknown Core Nodes",
                    f"{unknown_names} — VM not found in any accessible subscription")
            else:
                add("SHIR Infrastructure", "Total Cores", total_cores)
            if total_mem_mb:
                add("SHIR Infrastructure", "Total Memory (GB)", round(total_mem_mb / 1024))
            else:
                total_avail = sum(n.get("availableMemoryMB", 0) for n in self.stats.shir_nodes)
                add("SHIR Infrastructure", "Total Available Memory (GB)", round(total_avail / 1024))
        
        # === NEW: JOB EXECUTION SUMMARY ===
        if self.stats.pipeline_job_stats:
            total_runs = sum(s.get("totalRuns", 0) for s in self.stats.pipeline_job_stats)
            total_succeeded = sum(s.get("succeededRuns", 0) for s in self.stats.pipeline_job_stats)
            total_failed = sum(s.get("failedRuns", 0) for s in self.stats.pipeline_job_stats)
            add("Job Execution", "Total Pipeline Runs", total_runs)
            add("Job Execution", "Succeeded", total_succeeded)
            add("Job Execution", "Failed", total_failed)
            add("Job Execution", "Overall Success Rate",
                f"{round(total_succeeded / max(total_runs, 1) * 100, 1)}%")
            add("Job Execution", "Unique Pipelines Executed", len(self.stats.pipeline_job_stats))
            # Job execution by subscription
            sub_job_stats = defaultdict(lambda: {"runs": 0, "succeeded": 0, "failed": 0, "cost": 0.0})
            for s in self.stats.pipeline_job_stats:
                sid = s.get("subscription_id") or "Unknown"
                sub_job_stats[sid]["runs"] += s.get("totalRuns", 0)
                sub_job_stats[sid]["succeeded"] += s.get("succeededRuns", 0)
                sub_job_stats[sid]["failed"] += s.get("failedRuns", 0)
                sub_job_stats[sid]["cost"] += s.get("totalCost", 0)
            for sid, sj in sorted(sub_job_stats.items(), key=lambda x: -x[1]["runs"]):
                sub_display = self._sub_name(sid)
                label = f"Job Execution - {sub_display}" if sub_display else f"Job Execution - {sid[:12]}..."
                add(label, "Pipeline Runs", sj["runs"])
                add(label, "Succeeded", sj["succeeded"])
                add(label, "Failed", sj["failed"])
                rate = round(sj["succeeded"] / max(sj["runs"], 1) * 100, 1)
                add(label, "Success Rate", f"{rate}%")
                add(label, "Estimated Cost", f"${sj['cost']:.4f}")

        # === NEW: COST BREAKDOWN ===
        cb = self.stats.cost_breakdown
        if cb:
            add("Cost Estimate (List Rate)", "Profiling Window (days)", cb.get("days", 0))
            add("Cost Estimate (List Rate)", "Orchestration Cost", f"${cb.get('orchestrationCost', 0):.2f}")
            add("Cost Estimate (List Rate)", "Data Movement Cost", f"${cb.get('ingestionCost', 0):.2f}")
            add("Cost Estimate (List Rate)", "Pipeline Activity Cost", f"${cb.get('pipelineActivityCost', 0):.2f}")
            add("Cost Estimate (List Rate)", "External Activity Cost", f"${cb.get('externalActivityCost', 0):.2f}")
            add("Cost Estimate (List Rate)", "TOTAL ESTIMATED", f"${cb.get('totalEstimatedCost', 0):.2f}")
        
        # === NEW: ACTUAL COSTS (compute/orchestration only, excluding infra) ===
        if self.stats.actual_costs:
            actual_total = sum(c.get("cost", 0) for c in self.stats.actual_costs if not c.get("meterSubcategory", ""))
            add("Actual Billed Cost (compute only)", "Total", f"${actual_total:.2f}")
            factory_costs = defaultdict(float)
            for ac in self.stats.actual_costs:
                if ac.get("meterSubcategory", ""):
                    continue  # skip infra costs
                factory_costs[ac.get("factory_name", "")] += ac.get("cost", 0)
            for fname, cost in sorted(factory_costs.items(), key=lambda x: -x[1])[:10]:
                add("Actual Billed Cost by Factory", fname, f"${cost:.4f}")
            # Actual costs grouped by subscription (compute only, excluding infra)
            sub_actual = defaultdict(float)
            for ac in self.stats.actual_costs:
                if ac.get("meterSubcategory", ""):
                    continue  # skip infra costs
                fname = ac.get("factory_name", "")
                for sid, fnames in sub_factories.items():
                    if fname in fnames:
                        sub_actual[sid] += ac.get("cost", 0)
                        break
            for sid, cost in sorted(sub_actual.items(), key=lambda x: -x[1]):
                sub_display = self._sub_name(sid)
                label = f"{sub_display} ({sid[:12]}...)" if sub_display else f"{sid[:12]}..."
                add("Actual Billed Cost by Subscription", label, f"${cost:.4f}")
        if self.stats.cost_mgmt_denied:
            for sub_id in self.stats.cost_mgmt_denied:
                sub_display = self._sub_name(sub_id)
                label = f"Subscription {sub_display} ({sub_id})" if sub_display else f"Subscription {sub_id}"
                add("Actual Billed Cost", label, "Cost Management access denied")

        # === BREAKDOWN BY SUBSCRIPTION ===
        sub_ids = set()
        for f_info in self.stats.factories.values():
            sid = f_info.get("subscription_id", "")
            if sid:
                sub_ids.add(sid)
        for p in self.stats.pipelines:
            sid = p.get("subscription_id", "")
            if sid:
                sub_ids.add(sid)

        if len(sub_ids) > 1:
            for sid in sorted(sub_ids):
                display = self.stats.subscription_names.get(sid, "")
                sub_label = f"Subscription: {display}" if display else f"Subscription: {sid[:8]}..."

                sub_factories = [name for name, f in self.stats.factories.items()
                                 if f.get("subscription_id") == sid]
                add(sub_label, "Subscription ID", sid)
                add(sub_label, "Factories/Workspaces", len(sub_factories))

                sub_pipelines = [p for p in self.stats.pipelines if p.get("subscription_id") == sid]
                add(sub_label, "Pipelines", len(sub_pipelines))

                sub_activities = [a for a in self.stats.activities if a.get("subscription_id") == sid]
                add(sub_label, "Activities", len(sub_activities))

                sub_ls = [ls for ls in self.stats.linked_services if ls.get("subscription_id") == sid]
                add(sub_label, "Linked Services", len(sub_ls))

                sub_ds = [ds for ds in self.stats.datasets if ds.get("subscription_id") == sid]
                add(sub_label, "Datasets", len(sub_ds))

                sub_ir = [ir for ir in self.stats.integration_runtimes if ir.get("subscription_id") == sid]
                add(sub_label, "Integration Runtimes", len(sub_ir))

                sub_df = [df for df in self.stats.dataflows if df.get("subscription_id") == sid]
                add(sub_label, "Dataflows", len(sub_df))

                sub_triggers = [t for t in self.stats.triggers if t.get("subscription_id") == sid]
                add(sub_label, "Triggers", len(sub_triggers))

                # Activity type breakdown per subscription
                sub_act_types = defaultdict(int)
                for a in sub_activities:
                    sub_act_types[a.get("activity_type", "Unknown")] += 1
                for act_type, cnt in sorted(sub_act_types.items(), key=lambda x: -x[1])[:10]:
                    add(f"{sub_label} - Activity Types", act_type, cnt)

                # Activity category breakdown per subscription
                sub_act_cats = defaultdict(int)
                for a in sub_activities:
                    sub_act_cats[a.get("activity_category", "Unknown")] += 1
                for cat, cnt in sorted(sub_act_cats.items(), key=lambda x: -x[1]):
                    add(f"{sub_label} - Activity Categories", cat, cnt)

                # Copy source/sink types per subscription
                sub_src = defaultdict(int)
                sub_snk = defaultdict(int)
                for a in sub_activities:
                    if a.get("source_type_actual"):
                        sub_src[a["source_type_actual"]] += 1
                    if a.get("sink_type_actual"):
                        sub_snk[a["sink_type_actual"]] += 1
                if sub_src:
                    for src, cnt in sorted(sub_src.items(), key=lambda x: -x[1]):
                        add(f"{sub_label} - Copy Sources", src, cnt)
                if sub_snk:
                    for snk, cnt in sorted(sub_snk.items(), key=lambda x: -x[1]):
                        add(f"{sub_label} - Copy Sinks", snk, cnt)

                # Job execution per subscription
                sub_job_stats = [s for s in self.stats.pipeline_job_stats
                                 if s.get("factory_name") in sub_factories]
                if sub_job_stats:
                    sub_runs = sum(s.get("totalRuns", 0) for s in sub_job_stats)
                    sub_succeeded = sum(s.get("succeededRuns", 0) for s in sub_job_stats)
                    sub_failed = sum(s.get("failedRuns", 0) for s in sub_job_stats)
                    add(f"{sub_label} - Job Execution", "Total Runs", sub_runs)
                    add(f"{sub_label} - Job Execution", "Succeeded", sub_succeeded)
                    add(f"{sub_label} - Job Execution", "Failed", sub_failed)
                    rate = round(sub_succeeded / max(sub_runs, 1) * 100, 1)
                    add(f"{sub_label} - Job Execution", "Success Rate", f"{rate}%")

                # Actual costs per subscription
                sub_actual = [c for c in self.stats.actual_costs
                              if c.get("factory_name") in sub_factories]
                if sub_actual:
                    sub_cost = sum(c.get("cost", 0) for c in sub_actual)
                    add(f"{sub_label} - Actual Cost", "Total", f"${sub_cost:.4f}")
                    sub_factory_costs = defaultdict(float)
                    for ac in sub_actual:
                        sub_factory_costs[ac.get("factory_name", "")] += ac.get("cost", 0)
                    for fname, cost in sorted(sub_factory_costs.items(), key=lambda x: -x[1])[:10]:
                        add(f"{sub_label} - Actual Cost by Factory", fname, f"${cost:.4f}")

                # Estimated costs per subscription
                sub_est_stats = [s for s in self.stats.pipeline_job_stats
                                 if s.get("factory_name") in sub_factories]
                if sub_est_stats:
                    sub_est = sum(s.get("totalCost", 0) for s in sub_est_stats)
                    add(f"{sub_label} - Estimated Cost", "Total (List Rate)", f"${sub_est:.4f}")

                # Database instances per subscription
                sub_dbs = [db for db in self.stats.db_instances
                           if db.get("factory_name") in sub_factories]
                if sub_dbs:
                    add(f"{sub_label} - Databases", "DB Linked Services", len(sub_dbs))

                # SHIR nodes per subscription
                sub_shir = [n for n in self.stats.shir_nodes
                            if n.get("factory_name") in sub_factories]
                if sub_shir:
                    add(f"{sub_label} - SHIR", "Nodes", len(sub_shir))
                    online = sum(1 for n in sub_shir if n.get("status", "").lower() in ("online", "running"))
                    add(f"{sub_label} - SHIR", "Nodes Online", online)

        pd.DataFrame(rows).to_csv(path, index=False)

        # Write markdown — group by category
        md_path = os.path.join(self.output_dir, "summary_report.md")
        md_lines = ["# ADF Profiler Summary Report\n"]
        current_cat = None
        for row in rows:
            cat = str(row.get("category", ""))
            if cat != current_cat:
                current_cat = cat
                md_lines.append(f"\n## {cat}\n")
                md_lines.append("| Metric | Value |")
                md_lines.append("| --- | --- |")
            md_lines.append(f"| {row.get('metric', '')} | {row.get('value', '')} |")
        md_lines.append("")
        with open(md_path, "w") as f:
            f.write("\n".join(md_lines))

        return path


# =============================================================================
# ERROR DISPLAY
# =============================================================================

def display_error_summary(error_queue: queue.Queue, stats: Stats):
    """Display collected errors/warnings at the end."""
    errors = []
    warnings_by_type = defaultdict(int)
    
    while not error_queue.empty():
        try:
            err = error_queue.get_nowait()
            if err.is_warning:
                if "WebConnection" in err.error_message:
                    warnings_by_type["WebConnection mapping"] += 1
                elif "DatabricksJob" in err.error_message:
                    warnings_by_type["DatabricksJob mapping"] += 1
                elif "Airflow" in err.error_message:
                    warnings_by_type["Airflow mapping"] += 1
                elif "Unmapped" in err.error_message or "deserialize" in err.error_message:
                    warnings_by_type["SDK type mapping"] += 1
                else:
                    warnings_by_type["Other"] += 1
            else:
                errors.append(err)
        except queue.Empty:
            break
    
    if stats.skipped_synapse:
        print(f"\n⚠️  Synapse Access Issues ({len(stats.skipped_synapse)} workspaces):")
        print("   Need Synapse Administrator/Contributor role on each workspace.")
        for ws in stats.skipped_synapse[:5]:
            print(f"   • {ws}")
        if len(stats.skipped_synapse) > 5:
            print(f"   • ... and {len(stats.skipped_synapse) - 5} more")
    
    if warnings_by_type or errors:
        print("\n⚠️  Extraction Warnings/Errors:")
        for warning_type, count in sorted(warnings_by_type.items(), key=lambda x: -x[1]):
            print(f"   • {count} {warning_type} warning(s)")
        for err in errors[:5]:
            print(f"   ❌ {err.factory_name}/{err.resource_type}: {err.error_message[:60]}")
        if len(errors) > 5:
            print(f"   ... and {len(errors) - 5} more errors")


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

def run_extraction(
    credential,
    subscription_id: str = None,
    resource_group: str = None,
    factory_name: str = None,
    output_dir: str = "./output",
    include_synapse: bool = True,
    include_fabric: bool = True,
    profiling_days: int = DEFAULT_PROFILING_DAYS,
) -> Dict[str, str]:
    """Run full extraction + runtime profiling."""

    # Create timestamped run folder under output_dir
    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    output_dir = os.path.join(output_dir, run_timestamp)

    logger, log_file = setup_logging(output_dir)
    stats = Stats()
    error_queue = queue.Queue()
    
    print("\n" + "=" * 65)
    print("  Azure Data Factory / Synapse / Fabric Profiler")
    print("=" * 65)
    
    start_time = datetime.now()
    
    # Phase 1: Discovery
    discovery_start = datetime.now()
    discovery = ResourceDiscovery(credential, logger, include_synapse, include_fabric)
    subscriptions, adf_factories, synapse_workspaces, fabric_workspaces = discovery.discover_all(
        subscription_id, resource_group, factory_name
    )
    discovery_elapsed = datetime.now() - discovery_start
    
    # Store subscription display names for reports
    for sub in subscriptions:
        stats.subscription_names[sub["subscription_id"]] = sub["display_name"]

        total_factories = len(adf_factories) + len(synapse_workspaces) + len(fabric_workspaces)
    if total_factories == 0:
        print("\n❌ No factories or workspaces found.")
        return {}
    
    # Phase 2: Concurrent extraction
    extraction_start = datetime.now()
    extractor = ConcurrentExtractor(credential, stats, error_queue, logger)
    total_extracted = extractor.extract_all(adf_factories, synapse_workspaces, fabric_workspaces)
    extraction_elapsed = datetime.now() - extraction_start
    
    # Phase 3: Runtime profiling (always on)
    profiling_start = datetime.now()
    profiler = RuntimeProfiler(credential, stats, logger, days=profiling_days)
    profiler.profile_all()
    profiling_elapsed = datetime.now() - profiling_start
    
    # Phase 4: Export
    print("\n💾 Saving output files...", flush=True)
    sys.stdout.flush()
    exporter = Exporter(output_dir, stats)
    output_files = exporter.export_all()
    
    # Display error summary
    display_error_summary(error_queue, stats)
    
    # Final summary
    total_elapsed = datetime.now() - start_time
    
    adf_p = len([p for p in stats.pipelines if p.get("factory_type") == "ADF"])
    syn_p = len([p for p in stats.pipelines if p.get("factory_type") == "Synapse"])
    fab_p = len([p for p in stats.pipelines if p.get("factory_type") == "Fabric"])
    df_gen1 = stats.dataflow_types.get("MappingDataFlow", 0)
    df_gen2 = stats.dataflow_types.get("DataflowGen2", 0)
    
    total_runs = sum(s.get("totalRuns", 0) for s in stats.pipeline_job_stats)
    cb = stats.cost_breakdown
    
    print("\n" + "=" * 65)
    print("  ✅ Profiling Complete")
    print("=" * 65)
    print(f"""
  ⏱️  Timing:
      Discovery:   {discovery_elapsed}
      Extraction:  {extraction_elapsed}
      Profiling:   {profiling_elapsed}
      Total:       {total_elapsed}
  
  📦 Resources:
      Pipelines:           {len(stats.pipelines):>5} (ADF: {adf_p}, Synapse: {syn_p}, Fabric: {fab_p})
      Linked Services:     {len(stats.linked_services):>5}
      Datasets:            {len(stats.datasets):>5}
      Dataflows:           {len(stats.dataflows):>5} (Gen1: {df_gen1}, Gen2: {df_gen2})
      Integration Runtimes:{len(stats.integration_runtimes):>5}
      Triggers:            {len(stats.triggers):>5}
      Activities:          {len(stats.activities):>5}
  
  🔬 Runtime ({profiling_days}-day window):
      Database Instances:  {len(stats.db_instances):>5}
      Table References:    {len(stats.table_discovery):>5}
      SHIR Nodes:          {len(stats.shir_nodes):>5}
      Pipeline Runs:       {total_runs:>5}
      Copy Activity Runs:  {len(stats.copy_activity_details):>5}
      Est. Compute + Orch:  ${cb.get('orchestrationCost', 0) + cb.get('ingestionCost', 0) + cb.get('pipelineActivityCost', 0) + cb.get('externalActivityCost', 0):>9.2f}""")

    if stats.actual_costs:
        actual_compute = sum(c.get("cost", 0) for c in stats.actual_costs
                             if not c.get("meterSubcategory", ""))
        airflow_cost = sum(c.get("cost", 0) for c in stats.actual_costs
                           if "airflow" in (c.get("meterSubcategory", "") or "").lower())
        print(f"      Act. Compute + Orch: ${actual_compute:>9.2f}")
        if airflow_cost > 0:
            print(f"      Managed Airflow:     ${airflow_cost:>9.2f}")
    if stats.cost_mgmt_denied:
        print(f"      ⚠️  Cost data missing for {len(stats.cost_mgmt_denied)} subscription(s) (access denied)")
    
    print(f"""
  📁 Output: {output_dir}/
  📝 Log: {log_file}
""")
    
    logger.info("=" * 60)
    logger.info("Extraction Complete")
    logger.info(f"Total time: {total_elapsed}")
    logger.info(f"Pipelines: {len(stats.pipelines)}")
    logger.info(f"Activities: {len(stats.activities)}")
    logger.info(f"Copy source types: {dict(stats.copy_source_types)}")
    logger.info(f"Copy sink types: {dict(stats.copy_sink_types)}")
    logger.info(f"WebActivity APIs: {dict(stats.web_activity_apis)}")
    
    return output_files


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="ADF Profiler - extracts and profiles Azure Data Factory pipelines.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
QUICK START:
  1. az login
  2. pip install -r requirements.txt
  3. python extract_pipelines.py

EXAMPLES:
  python extract_pipelines.py                              # Profile everything
  python extract_pipelines.py --subscription-id <SUB_ID>   # Specific subscription
  python extract_pipelines.py --factory-name myFactory      # Specific factory
  python extract_pipelines.py --days 14                     # 14-day lookback
  python extract_pipelines.py --no-synapse --no-fabric      # ADF only
"""
    )

    parser.add_argument("--subscription-id", help="Filter to specific subscription")
    parser.add_argument("--resource-group", help="Filter to specific resource group")
    parser.add_argument("--factory-name", help="Filter to specific data factory")
    parser.add_argument("--output-dir", default="./output", help="Base output directory; each run creates a timestamped subfolder (default: ./output)")
    parser.add_argument("--days", type=int, default=DEFAULT_PROFILING_DAYS,
                        help=f"Lookback window in days (default: {DEFAULT_PROFILING_DAYS})")
    parser.add_argument("--no-synapse", action="store_true", help="Skip Synapse workspaces")
    parser.add_argument("--no-fabric", action="store_true", help="Skip Fabric workspaces")
    parser.add_argument("--tenant-id", help="Azure tenant ID (for service principal auth)")
    parser.add_argument("--client-id", help="Service principal client ID")
    parser.add_argument("--client-secret", help="Service principal client secret")

    args = parser.parse_args()

    try:
        credential = get_credential(args.tenant_id, args.client_id, args.client_secret)

        run_extraction(
            credential=credential,
            subscription_id=args.subscription_id,
            resource_group=args.resource_group,
            factory_name=args.factory_name,
            output_dir=args.output_dir,
            include_synapse=not args.no_synapse,
            include_fabric=not args.no_fabric,
            profiling_days=args.days,
        )
        return 0

    except KeyboardInterrupt:
        print("\n\nCancelled by user")
        return 1
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
