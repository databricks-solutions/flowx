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


# SQL Server family sources (SQL Server, Azure SQL Database, Azure SQL Managed Instance, and the older SqlSource).
# Metadata-driven consolidation builds its Lakeflow Connect pipeline on a SQL Server connection, so only these
# sources may be consolidated; the Copy preparer maps them to the SQLSERVER connection.
SQL_SERVER_SOURCE_TYPES: frozenset[str] = frozenset(
    {
        "SqlServerSource",
        "AzureSqlSource",
        "AzureSqlDatabaseSource",
        "SqlMISource",
        "SqlSource",
        "MicrosoftSqlServerSource",
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
