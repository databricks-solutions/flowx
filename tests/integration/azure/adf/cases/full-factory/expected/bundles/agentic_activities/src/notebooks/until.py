# Databricks notebook source
# MAGIC %md
# MAGIC # Placeholder: until
# MAGIC
# MAGIC Original source activity type: **Until**
# MAGIC
# MAGIC Requires agentic (LLM-assisted) translation from the ADF/ARM JSON
# MAGIC
# MAGIC An agent should translate this activity from the source definition below,
# MAGIC then replace the `raise NotImplementedError` cell with the generated code.
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "until",
# MAGIC   "type": "Until",
# MAGIC   "typeProperties": {
# MAGIC     "activities": [
# MAGIC       {
# MAGIC         "name": "until-wait",
# MAGIC         "type": "Wait",
# MAGIC         "typeProperties": {
# MAGIC           "waitTimeInSeconds": 1
# MAGIC         }
# MAGIC       }
# MAGIC     ],
# MAGIC     "expression": {
# MAGIC       "type": "Expression",
# MAGIC       "value": "@equals(1,1)"
# MAGIC     },
# MAGIC     "timeout": "0.00:01:00"
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

raise NotImplementedError("Activity 'until' (Until) needs agentic translation.")
