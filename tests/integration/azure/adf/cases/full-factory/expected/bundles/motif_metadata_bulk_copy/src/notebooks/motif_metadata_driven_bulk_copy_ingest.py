# Databricks notebook source
import json

# The for_each_task passes the current control-table row as the "item" widget via {{input}}.
item_raw = dbutils.widgets.get("item")
item = json.loads(item_raw) if item_raw else {}
schema_name = item.get("schema_name", "dbo")
table_name = item.get("table_name") or item.get("name") or "UNKNOWN_TABLE"
target = 'raw.{schema_name}_{table_name}'.format(schema_name=schema_name, table_name=table_name)
query = f"SELECT * FROM {schema_name}.{table_name}"

jdbc_url = dbutils.secrets.get(scope="motif_metadata_driven_bulk_copy", key="jdbc-url")
jdbc_user = dbutils.secrets.get(scope="motif_metadata_driven_bulk_copy", key="jdbc-user")
jdbc_password = dbutils.secrets.get(scope="motif_metadata_driven_bulk_copy", key="jdbc-password")

(
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("query", query)
    .load()
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target)
)
