# Databricks notebook source
# MAGIC %md
# MAGIC # Placeholder: mixed-script
# MAGIC
# MAGIC Original source activity type: **Script**
# MAGIC
# MAGIC Requires agentic (LLM-assisted) translation from the ADF/ARM JSON
# MAGIC
# MAGIC An agent should translate this activity from the source definition below,
# MAGIC then replace the `raise NotImplementedError` cell with the generated code.
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "dependsOn": [
# MAGIC     {
# MAGIC       "activity": "launch-child",
# MAGIC       "dependencyConditions": [
# MAGIC         "Succeeded"
# MAGIC       ]
# MAGIC     }
# MAGIC   ],
# MAGIC   "linkedServiceName": {
# MAGIC     "referenceName": "azure-sql",
# MAGIC     "type": "LinkedServiceReference"
# MAGIC   },
# MAGIC   "name": "mixed-script",
# MAGIC   "type": "Script",
# MAGIC   "typeProperties": {
# MAGIC     "scripts": [
# MAGIC       {
# MAGIC         "text": "SELECT 1",
# MAGIC         "type": "Query"
# MAGIC       }
# MAGIC     ]
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

raise NotImplementedError("Activity 'mixed-script' (Script) needs agentic translation.")
