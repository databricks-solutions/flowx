# Hadoop/HDFS to Databricks Migration Guide

Reference for converting on-prem Airflow DAGs that orchestrate Spark jobs on Hadoop/YARN clusters to Databricks Declarative Automation Bundles (formerly Databricks Asset Bundles; DABs). Covers HDFS path conversion, YARN Spark config cleanup, Hive metastore migration, data ingestion alternatives, and detection of `spark-submit` commands embedded in BashOperator/SSHOperator tasks.

---

## HDFS Path Conversion

All `hdfs://` paths in Spark job code, operator parameters, and configs must be converted to Databricks-compatible storage paths.

### Path Mapping Table

| On-Prem Pattern | Databricks Equivalent | Notes |
|---|---|---|
| `hdfs://namenode:8020/data/...` | `s3://bucket/data/...` or `abfss://container@account.dfs.core.windows.net/data/...` | Cloud storage mounted or accessed directly |
| `hdfs:///user/hive/warehouse/db.db/table` | Unity Catalog managed table: `catalog.schema.table` | No path needed -- use `spark.read.table()` |
| `/user/data/landing/` (implicit HDFS) | `/Volumes/catalog/schema/volume/landing/` | Unity Catalog volumes for file-based access |
| `hdfs://namenode/tmp/staging/` | `/tmp/` or a UC volume for staging | Ephemeral staging paths |
| `dbfs:/mnt/...` (legacy DBFS mount) | `/Volumes/catalog/schema/volume/...` | Migrate mounts to UC volumes |

### Conversion Rules

1. **Identify all HDFS paths** in Spark job source files (`.py`, `.jar` configs, `.sql`). Search for:
   - `hdfs://` prefixed paths
   - Bare absolute paths used with `spark.read`/`spark.write` (often implicit HDFS)
   - Paths in `--files`, `--jars`, `--py-files` arguments to `spark-submit`

2. **Map to cloud storage or Unity Catalog:**
   - **Tables** (Hive warehouse paths): convert to UC table references (`catalog.schema.table`)
   - **Landing/raw files**: convert to UC external locations or volumes
   - **Intermediate/staging**: convert to UC volumes or temp paths
   - **JARs/wheels/dependencies**: upload to UC volumes (`/Volumes/catalog/schema/libs/`)

3. **In generated notebooks**, replace HDFS reads/writes:

   ```python
   # Before (HDFS)
   df = spark.read.parquet("hdfs://namenode:8020/data/raw/events/")
   df.write.parquet("hdfs://namenode:8020/data/silver/events/")

   # After (Unity Catalog / cloud storage)
   df = spark.read.parquet("s3://datalake-bucket/data/raw/events/")
   df.write.format("delta").saveAsTable("catalog.silver.events")
   ```

4. **Flag in MIGRATION_NOTES.md**: list every HDFS path found with its proposed Databricks equivalent. This requires input from the customer to confirm cloud storage bucket names, Unity Catalog catalog/schema structure, and volume locations.

---

## YARN/Hadoop Spark Config Translation

SparkSubmitOperator `conf` and `spark-submit` `--conf` flags include YARN/Hadoop-specific settings that must be cleaned up for Databricks.

### Configs to Remove (not applicable on Databricks)

| Spark Config | Reason |
|---|---|
| `spark.yarn.queue` | No YARN queues. Databricks uses cluster policies for governance. |
| `spark.yarn.executor.memoryOverhead` | Use `spark.executor.memoryOverhead` instead (same effect, YARN prefix removed). |
| `spark.yarn.driver.memoryOverhead` | Use `spark.driver.memoryOverhead` instead. |
| `spark.yarn.am.memory` | Not applicable. |
| `spark.yarn.am.cores` | Not applicable. |
| `spark.yarn.submit.waitAppCompletion` | Not applicable. |
| `spark.yarn.maxAppAttempts` | Use DABs `max_retries` on the task instead. |
| `spark.hadoop.fs.defaultFS` | Not needed -- Databricks configures storage access via UC or instance profiles. |
| `spark.hadoop.dfs.*` | HDFS namenode configs not needed. |
| `spark.hadoop.mapreduce.*` | MapReduce configs not applicable. |
| `spark.hadoop.yarn.*` | All YARN-specific Hadoop configs. |
| `spark.eventLog.dir` (HDFS path) | Databricks manages Spark event logs automatically. |
| `spark.history.fs.logDirectory` | Managed by Databricks. |

### Configs to Translate

| On-Prem Config | Databricks Equivalent | Notes |
|---|---|---|
| `spark.executor.instances` | `num_workers` on `new_cluster` | Fixed cluster size. Or use `autoscale.min_workers`/`max_workers`. |
| `spark.executor.memory` | `spark.executor.memory` in `spark_conf` | Still valid, but Databricks auto-tunes. Often removable. |
| `spark.executor.cores` | `spark.executor.cores` in `spark_conf` | Still valid. Databricks optimizes by default. |
| `spark.driver.memory` | `spark.driver.memory` in `spark_conf` | Still valid. |
| `spark.sql.shuffle.partitions` | `spark.sql.shuffle.partitions` in `spark_conf` | Still valid. Databricks AQE auto-tunes this. Consider removing. |
| `spark.dynamicAllocation.enabled` | Databricks autoscaling | Use `autoscale` on `new_cluster` instead. Remove the Spark config. |
| `spark.dynamicAllocation.minExecutors` | `autoscale.min_workers` | Map directly. |
| `spark.dynamicAllocation.maxExecutors` | `autoscale.max_workers` | Map directly. |
| `spark.sql.warehouse.dir` | Not needed | UC manages warehouse location. |
| `spark.hive.metastore.uris` | Not needed if using UC | UC is the metastore. For external HMS, use `spark.hadoop.hive.metastore.uris`. |
| `--master yarn` | Remove | Databricks manages the Spark master. |
| `--deploy-mode cluster\|client` | Remove | Databricks always runs in cluster mode. |
| `--keytab` / `--principal` | Remove | Kerberos not needed. Use UC/instance profiles for auth. |

### DABs Cluster Config Example (translated from YARN)

**Before (spark-submit on YARN):**

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --queue etl_queue \
  --num-executors 10 \
  --executor-memory 8g \
  --executor-cores 4 \
  --driver-memory 4g \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=5 \
  --conf spark.dynamicAllocation.maxExecutors=20 \
  --conf spark.yarn.executor.memoryOverhead=2g \
  --conf spark.sql.shuffle.partitions=400 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 \
  /opt/spark/jobs/etl_pipeline.py --date 2024-01-15
```

**After (DABs job cluster):**

```yaml
job_clusters:
  - job_cluster_key: etl-cluster
    new_cluster:
      spark_version: "15.4.x-scala2.12"
      node_type_id: ${var.node_type_id}
      autoscale:
        min_workers: 5
        max_workers: 20
      spark_conf:
        spark.executor.memory: "8g"
        spark.executor.cores: "4"
        spark.driver.memory: "4g"
        spark.executor.memoryOverhead: "2g"
        # spark.sql.shuffle.partitions removed -- AQE handles this
      data_security_mode: SINGLE_USER
```

---

## Hive Metastore to Unity Catalog

On-prem Hadoop clusters use a Hive metastore. Tables referenced as `database.table` need to become `catalog.schema.table` in Unity Catalog.

### Table Reference Conversion

| Hive Pattern | Unity Catalog Equivalent |
|---|---|
| `database_name.table_name` | `catalog.schema.table_name` |
| `default.table_name` | `catalog.default.table_name` |
| `spark.sql("SELECT * FROM db.table")` | `spark.sql("SELECT * FROM catalog.schema.table")` |
| `spark.read.table("db.table")` | `spark.read.table("catalog.schema.table")` |
| `spark.write.saveAsTable("db.table")` | `spark.write.saveAsTable("catalog.schema.table")` |
| `CREATE TABLE db.table ...` | `CREATE TABLE catalog.schema.table ...` |
| `INSERT INTO db.table ...` | `INSERT INTO catalog.schema.table ...` |

### Conversion Rules

1. **Define a catalog/schema mapping** as a DABs variable:

   ```yaml
   variables:
     catalog:
       description: Unity Catalog name
       default: "main"
     schema_prefix:
       description: Schema prefix mapping from Hive databases
       default: ""
   ```

2. **In generated notebooks**, add a `USE CATALOG` / `USE SCHEMA` at the top:

   ```python
   # Databricks notebook source
   spark.sql(f"USE CATALOG {dbutils.widgets.get('catalog')}")
   spark.sql(f"USE SCHEMA {dbutils.widgets.get('schema')}")
   ```

3. **Flag in MIGRATION_NOTES.md**: list all Hive databases referenced and their proposed UC catalog.schema mapping. This requires customer input on UC structure.

---

## BashOperator spark-submit Detection

On-prem Airflow setups frequently wrap `spark-submit` in a `BashOperator` or `SSHOperator` instead of using `SparkSubmitOperator`. The skill should detect this pattern and convert it to a proper DABs task type.

### Detection Pattern

Look for these patterns in `bash_command` or `command` parameters:

```python
# Pattern 1: Direct spark-submit
BashOperator(
    task_id="run_etl",
    bash_command="spark-submit --master yarn --class com.example.ETL /opt/jars/etl.jar --date {{ ds }}"
)

# Pattern 2: spark-submit via script
BashOperator(
    task_id="run_etl",
    bash_command="/opt/scripts/run_etl.sh {{ ds }}"
)
# Where run_etl.sh contains: spark-submit ...

# Pattern 3: SSHOperator to edge node
SSHOperator(
    task_id="run_etl",
    ssh_conn_id="hadoop_edge_node",
    command="spark-submit --master yarn /opt/spark/jobs/etl.py"
)
```

### Conversion Rules

1. **If `bash_command` contains `spark-submit`**:
   - Parse out the application path (`.py` or `.jar`), `--class`, `--conf` flags, and application arguments
   - Convert to `spark_python_task` (if `.py`) or `spark_jar_task` (if `.jar`)
   - Apply YARN config cleanup (see above)
   - Extract the application file to `src/` and update the path

2. **If `bash_command` calls a shell script** that wraps `spark-submit`:
   - Flag in MIGRATION_NOTES.md: "Shell script `run_etl.sh` wraps spark-submit. Extract the Spark job and convert to a direct task."
   - If the script is available, parse the `spark-submit` command from it

3. **If SSHOperator runs `spark-submit` on a remote host**:
   - Same as pattern 1 -- extract the spark-submit command and convert to a DABs task
   - The SSH hop is no longer needed since Databricks runs the job directly

### Example Conversion

**Airflow:**

```python
run_etl = BashOperator(
    task_id="run_daily_etl",
    bash_command="""
        spark-submit \
            --master yarn \
            --deploy-mode cluster \
            --num-executors 10 \
            --executor-memory 8g \
            --conf spark.yarn.queue=etl \
            --conf spark.sql.shuffle.partitions=200 \
            --class com.example.DailyETL \
            /opt/jars/analytics-1.0.jar \
            --date {{ ds }} \
            --input hdfs:///data/raw/ \
            --output hdfs:///data/silver/
    """,
)
```

**DABs YAML:**

```yaml
- task_key: run_daily_etl
  job_cluster_key: etl-cluster
  spark_jar_task:
    main_class_name: com.example.DailyETL
    parameters:
      - "--date"
      - "{{job.parameters.run_date}}"
      - "--input"
      - "s3://datalake-bucket/data/raw/"
      - "--output"
      - "catalog.silver.daily_output"
  libraries:
    - jar: /Volumes/main/default/libs/analytics-1.0.jar
```

**MIGRATION_NOTES.md entry:**

```
- Task `run_daily_etl`: BashOperator wrapping spark-submit detected and converted to spark_jar_task.
  - HDFS paths `hdfs:///data/raw/` and `hdfs:///data/silver/` need cloud storage mapping.
  - JAR `/opt/jars/analytics-1.0.jar` must be uploaded to a UC volume.
  - YARN configs removed: --master yarn, --deploy-mode cluster, spark.yarn.queue.
```

---

## Data Ingestion Alternatives (Sqoop Replacement)

On-prem Hadoop pipelines commonly use Apache Sqoop to move data between RDBMS and HDFS. Sqoop has no
direct equivalent in Databricks. **Import (RDBMS→lakehouse) and export (lakehouse→RDBMS) are different
problems and map differently** — do not route both to Lakeflow Connect. See
`references/lakeflow-connect.md` for the ingestion-style distinctions.

### Sqoop Operator Mapping

| Sqoop operation | Databricks migration | Notes |
|---|---|---|
| **RDBMS→HDFS import** | Lakeflow Connect (ingestion pipeline), JDBC ingestion notebook, or federation | Connect only for a supported source into a Delta table it owns; else a JDBC read notebook, or federation for read-only query. |
| **Incremental import** (`--incremental append`/`lastmodified`, a cursor column) | **query-based** Lakeflow Connect (cursor), **not** CDC | Sqoop's cursor `--incremental` maps to query-based ingestion — it is NOT log-based change capture. |
| **Log-based change capture** (a true CDC source) | **CDC** Lakeflow Connect where the connector supports it | Only when the source emits a change log (MySQL/PostgreSQL/SQL Server). |
| **HDFS/Hive→RDBMS export** | JDBC/connector write in a notebook, or a reverse-ETL tool | **NOT Lakeflow Connect** — Connect only ingests *into* the lakehouse. |
| Custom file-based ingestion | Auto Loader + `cloudFiles` | For files landing in cloud storage — not a managed connector. |

### Conversion Approach

1. **For Sqoop import tasks**: choose the ingestion style before converting.
   - If the source has a **supported Lakeflow Connect connector** and Connect can own a new destination
     table, emit a `resources.pipelines` ingestion pipeline (query-based for a cursor `--incremental`;
     CDC only for a true log-based source). Document the source connection, target table, cursor/primary
     keys, and networking in MIGRATION_NOTES.md; the UC connection is a manual prerequisite.
   - Otherwise use a **JDBC read notebook** (`spark.read.format("jdbc")`) into Delta, or **federation**
     for read-only query. See `references/lakeflow-connect.md`.

2. **For Sqoop export tasks** (lakehouse→RDBMS): convert to a `notebook_task` using JDBC write:

   ```python
   # Databricks notebook source
   df = spark.read.table("catalog.schema.aggregated_data")
   df.write \
       .format("jdbc") \
       .option("url", dbutils.secrets.get("scope", "jdbc_url")) \
       .option("dbtable", "target_schema.target_table") \
       .option("user", dbutils.secrets.get("scope", "jdbc_user")) \
       .option("password", dbutils.secrets.get("scope", "jdbc_password")) \
       .mode("overwrite") \
       .save()
   ```

---

## Bulk Conversion Guidance (Hundreds of Tasks)

For DAGs with hundreds of Spark tasks on Hadoop, follow these additional practices:

### 1. Group by pattern

Before converting task-by-task, categorize all tasks:

| Pattern | Expected Count | Conversion |
|---|---|---|
| `SparkSubmitOperator` with `.py` | N tasks | Bulk -> `spark_python_task` |
| `SparkSubmitOperator` with `.jar` | N tasks | Bulk -> `spark_jar_task` |
| `BashOperator` wrapping `spark-submit` | N tasks | Parse and convert (see above) |
| `HiveOperator` / SQL tasks | N tasks | Bulk -> `sql_task` |
| Sensors (HDFS, External) | N tasks | Convert to triggers or remove |
| Other | N tasks | Case-by-case |

Present this summary to the user before proceeding with individual task conversion.

### 2. Shared cluster strategy

With hundreds of tasks, avoid creating per-task clusters. Define a small set of shared `job_clusters`:

```yaml
job_clusters:
  - job_cluster_key: small-cluster     # For lightweight tasks
    new_cluster:
      spark_version: ${var.spark_version}
      node_type_id: ${var.node_type_id}
      num_workers: 2

  - job_cluster_key: medium-cluster    # For standard ETL
    new_cluster:
      spark_version: ${var.spark_version}
      node_type_id: ${var.node_type_id}
      autoscale:
        min_workers: 2
        max_workers: 8

  - job_cluster_key: large-cluster     # For heavy processing
    new_cluster:
      spark_version: ${var.spark_version}
      node_type_id: ${var.node_type_id}
      autoscale:
        min_workers: 4
        max_workers: 20
```

Assign tasks to clusters based on their original YARN resource requests (executor count, memory).

### 3. Split large DAGs

If a single Airflow DAG has 200+ tasks, consider splitting into multiple DABs jobs connected via `run_job_task`. Group by:
- Logical pipeline stage (ingest -> transform -> aggregate -> publish)
- Independent branches that can run as separate jobs
- Tasks with different SLAs or ownership

### 4. Dependency file upload

Collect all JARs, Python files, and config files referenced by the Spark jobs. Create an inventory:

```
DEPENDENCY_INVENTORY.md
- /opt/jars/analytics-1.0.jar        -> /Volumes/main/default/libs/analytics-1.0.jar
- /opt/spark/jobs/etl_pipeline.py     -> src/etl_pipeline.py (bundled)
- /opt/spark/jobs/common_utils.py     -> src/common_utils.py (bundled)
- /etc/spark/conf/hive-site.xml       -> Remove (UC replaces Hive metastore)
- /opt/jars/hadoop-aws-3.3.4.jar      -> Remove (built into Databricks runtime)
```
