"""Canonical source-type taxonomy used across the translator and preparer."""

from __future__ import annotations

# Database sources read via spark.read.format("jdbc") (need jdbc-url/jdbc-password/jdbc-user secrets).
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


# Object-store file sources: trigger UC volume / external-location provisioning and Auto Loader ingestion.
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


# Paginated REST API sources -- a generic requests-based pagination loop in the copy notebook. ADF
# HttpSource is NOT here: it downloads a single file over HTTP and is treated as a FILE source.
REST_SOURCE_TYPES: frozenset[str] = frozenset({"RestSource"})
