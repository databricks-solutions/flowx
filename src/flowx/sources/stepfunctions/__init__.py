"""AWS Step Functions source: convert state machines to the flowx Pipeline IR.

A Step Functions state machine (Amazon States Language) becomes one Lakeflow
Job. The ``discover`` and ``convert`` phase modules are thin wrappers over
:func:`flowx.sources.stepfunctions.loader.load_pipelines`, mirroring the
Airflow source: ``load_pipelines`` parses the ASL and translates it into the
shared :class:`~flowx.models.ir.Pipeline` IR, so the source-neutral package
phase consumes the result unchanged.
"""

from __future__ import annotations
