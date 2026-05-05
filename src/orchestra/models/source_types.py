"""Canonical source-type taxonomy used across the translator and preparer."""

from __future__ import annotations

# Database-style sources reachable via JDBC.  Every entry here implies the
# generated notebook will read with ``spark.read.format("jdbc")`` and
# require ``jdbc-url`` / ``jdbc-password`` (and optionally ``jdbc-user``)
# secrets.
JDBC_SOURCE_TYPES: frozenset[str] = frozenset(
    {
        "AzureSqlSource",
        "AzureSqlDatabaseSource",
        "SqlServerSource",
        "OracleSource",
        "PostgreSqlSource",
        "MySqlSource",
        "SqlSource",
        "CosmosDbSqlApiSource",
        "SqlDWSource",
    }
)


# File-based sources that resolve to an object store location.  These
# trigger UC volume / external-location provisioning and use Auto Loader
# (``cloudFiles``) for ingestion.
FILE_SOURCE_TYPES: frozenset[str] = frozenset(
    {
        "BlobSource",
        "AzureBlobFSSource",
        "AzureBlobStorageSource",
        "AzureDataLakeStoreSource",
        "AmazonS3Source",
        "FileSystemSource",
        "SftpSource",
        "HttpSource",
        "DelimitedTextSource",
        "JsonSource",
        "ParquetSource",
        "AvroSource",
        "OrcSource",
    }
)


# Paginated REST API sources -- handled by a generic ``requests``-based
# pagination loop in the generated copy notebook.  ADF ``HttpSource``
# is *not* in this set: it downloads a single file (CSV / JSON /
# Parquet) over HTTP and is handled as a FILE source via Auto Loader.
REST_SOURCE_TYPES: frozenset[str] = frozenset({"RestSource"})
