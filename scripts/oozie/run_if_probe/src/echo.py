# Databricks notebook source
dbutils.widgets.text("value", "")  # noqa: F821
dbutils.notebook.exit(dbutils.widgets.get("value"))  # noqa: F821
