# Databricks notebook source
jdbc_url = dbutils.secrets.get(scope="lookup-metadata", key="jdbc-url")
jdbc_user = dbutils.secrets.get(scope="lookup-metadata", key="jdbc-user")
jdbc_password = dbutils.secrets.get(scope="lookup-metadata", key="jdbc-password")

control_query = 'SELECT schema_name, table_name FROM dbo.copy_config'
control_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("query", control_query)
    .load()
)
items = [row.asDict() for row in control_df.collect()]
dbutils.jobs.taskValues.set(key="items", value=items)
