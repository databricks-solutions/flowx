"""AWS Glue Workflows source: convert workflow graphs to the flowx Pipeline IR.

A Glue Workflow becomes one Lakeflow Job: each job and crawler node becomes a
task, and each trigger becomes either the job schedule (a scheduled start
trigger) or dependency edges between tasks (a conditional trigger). The
``discover`` and ``convert`` phase modules are thin wrappers over
:func:`flowx.sources.glue.loader.load_pipelines`, mirroring the Step Functions
and Airflow sources, so the source-neutral package phase consumes the result
unchanged.
"""

from __future__ import annotations
