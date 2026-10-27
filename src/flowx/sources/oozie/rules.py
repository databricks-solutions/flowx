"""Stable IDs for every Oozie translation rule flowx applies.

Translators record the rules they apply in the pipeline's transformation ledger, and the fixture manifest
(``tests/resources/oozie/fixtures.yaml``) names which rules each fixture exercises, so a rule without a fixture,
or a fixture that doesn't actually trigger its rules, fails the tests.
"""

RULES: dict[str, str] = {
    "CF-OK-EDGE": "An ok transition becomes a dependency on the upstream task succeeding.",
    "CF-ERROR-KILL": "An error transition to a kill node is dropped; the failed task already fails the run.",
    "CF-ERROR-ACTION": "An error transition to an action goes through a result_state == failed gate.",
    "CF-ERROR-END": "An error transition to end is covered by the action's success gate, so the run still succeeds.",
    "CF-UPSTREAM-SENTINEL": (
        "A recoverable action gets a sentinel that fails the run when an upstream failure skipped it."
    ),
    "CF-FORK-JOIN": "Fork and join emit no task; the join's successor waits for every path.",
    "CF-DECISION-CHAIN": "A decision becomes a chain of condition tasks, one per case.",
    "CF-OR-MERGE": "A node reached from exclusive branches uses run_if AT_LEAST_ONE_SUCCESS (gates where needed).",
    "CF-KILL-TASK": "A kill node reached on a success path becomes a notebook that fails with the kill message.",
    "CF-RETRY": "retry-max / retry-interval become max_retries / min_retry_interval_millis.",
    "EL-NATIVE-CONDITION": "A simple comparison predicate becomes a native condition_task.",
    "EL-BRIDGE-CONDITION": "A computed predicate is evaluated by a generated bridge notebook.",
    "EL-AGENTIC-CONDITION": "A predicate with no faithful translation becomes an agentic decision.",
    "PROP-JOB-PARAMETER": "A plain property becomes a job parameter with its value as default.",
    "PROP-DERIVED-INLINE": "A property built from other properties is expanded where it's used.",
    "PROP-STRIPPED": "nameNode / jobTracker / resourceManager are removed.",
    "PROP-UNRESOLVED": "An undefined property becomes a required job parameter.",
    "SPARK-JAR": "A spark action with a JAR becomes a spark_jar_task with the JAR as a library.",
    "SPARK-PYTHON": "A spark action with a .py file becomes a spark_python_task.",
    "SPARK-CONF": "--conf and <configuration> become spark_conf.",
    "SPARK-CONF-DROP": "YARN/CDP-only Spark settings are dropped and listed.",
    "SPARK-SIZING": "Executor counts become num_workers; memory/cores are recorded for node-type choice.",
    "SPARK-AUTOSCALE": "Dynamic allocation min/max executors become autoscale min/max workers.",
    "SPARK-LOCAL-MASTER": "master local[*] becomes a single-node cluster.",
    "SPARK-MAVEN": "--packages become Maven libraries.",
    "SPARK-LIB": "Every other JAR in the app's lib/ is attached as a library, as Oozie puts lib/ on the classpath.",
    "SPARK-DEFERRED": "Spark features planned for a later milestone (prepare, py-files, files, archives) are reported.",
    "AGENTIC-ACTION": "An action with no deterministic translation becomes an agentic placeholder.",
}
