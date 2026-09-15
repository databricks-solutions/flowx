"""Source-faithful shared discovery AST.

The Databricks IR in :mod:`flowx.models.ir` is a *target* model: it keeps only
what Databricks needs to run a workflow, so it is deliberately lossy about the
source. The discovery layer needs the opposite -- a source-*faithful*,
standardised model that both Azure Data Factory and Apache Airflow map onto
without either being flattened to a lowest common denominator. This module
defines that model. It is the contract the per-source mappers (#62 for ADF, #63
for Airflow) align to.

Design shape: a small shared **core** of genuinely common concepts as typed
fields, plus an explicit **extension seam** so nothing platform-specific is
lost. Every graph and node carries:

* ``raw`` -- the verbatim source dict (the ADF activity/pipeline JSON, the
  Airflow capture), so a mapper can always fall back to the original; and
* ``properties`` / ``extensions`` -- a free-form bag for platform-specific
  attributes that have no shared typed field yet.

The typed field set is intentionally minimal. Concepts get promoted to typed
fields only once they are genuinely shared; everything else rides in
``raw`` / ``extensions`` until a later PR promotes it. In particular there is
**no typed Connection / linked-service field yet** -- connection and
linked-service details ride in ``properties`` / ``extensions`` for now. That is
a deliberate expansion point, not an oversight.

The lineage primitives from #61 (:class:`~flowx.models.ir.DataAsset`) are
reused here rather than duplicated: a node's reads and writes are lists of
``DataAsset``, so the discovery AST and the lineage substrate share one
vocabulary for physical data references.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from flowx.models.ir import DataAsset, Lineage

# --------------------------------------------------------------------------- #
# Well-known discriminators and concepts (open vocabularies).
#
# ``source`` and ``concept`` are plain strings, not enums, so a source can
# introduce a new value without churning this module -- the extension seam
# applies to the vocabulary too. The constants below name the values the core
# already understands; anything a mapper cannot classify becomes ``CONCEPT_GAP``.
# --------------------------------------------------------------------------- #

SOURCE_ADF = "adf"
SOURCE_AIRFLOW = "airflow"

# Neutral node concepts shared across sources. Not exhaustive by design.
CONCEPT_NOTEBOOK = "notebook"
CONCEPT_SCRIPT = "script"
CONCEPT_COPY_DATA = "copy_data"
CONCEPT_QUERY = "query"
CONCEPT_SET_VARIABLE = "set_variable"
CONCEPT_WAIT = "wait"
CONCEPT_RUN_WORKFLOW = "run_workflow"
CONCEPT_BRANCH = "branch"
CONCEPT_LOOP = "loop"
CONCEPT_SWITCH = "switch"
CONCEPT_GROUP = "group"
CONCEPT_GAP = "gap"


@dataclass(slots=True, kw_only=True)
class ScheduleSpec:
    """A workflow schedule kept in the SOURCE's own shape.

    Deliberately source-faithful, not Databricks-normalised. ``kind`` is a
    neutral trigger category, ``expression`` holds the schedule exactly as the
    source gives it -- an Airflow ``schedule_interval`` cron string or preset,
    an ADF trigger recurrence payload -- and ``timezone`` carries the timezone
    the source itself declares (ADF ``timeZone``, an Airflow DAG timezone).
    Databricks-*target* normalisation -- a Quartz cron string, ``pause_status``
    -- is a convert/target concern and is intentionally **not** typed here; a
    mapper that needs to stash such derived values for now puts them in
    :attr:`extensions`, never as first-class fields. Kept minimal on purpose --
    fields get promoted only once genuinely shared.

    Attributes:
        kind: Neutral trigger category (e.g. ``"schedule"`` / ``"interval"`` /
            ``"file_arrival"`` / ``"continuous"`` / ``"manual"``), ``""`` when
            unknown.
        expression: The source schedule as-given -- a cron / interval / preset
            string, or a structured recurrence payload.
        timezone: The timezone the source declares, verbatim, or ``None``.
        extensions: Overflow for any other source-specific schedule detail.
    """

    kind: str
    expression: Any = None
    timezone: str | None = None
    extensions: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class ParameterSpec:
    """A parameter or variable declaration: an optional type and a default.

    Reused for both graph parameters and graph variables since both sources
    describe them the same way (a declared type plus an optional default).

    Attributes:
        type: Declared type string when the source has one (ADF ``String`` /
            ``Int`` / ...), else ``None`` (Airflow params carry only a default).
        default: Default / initial value, or ``None``.
    """

    type: str | None = None
    default: Any = None


@dataclass(slots=True, kw_only=True)
class PolicySpec:
    """Retry / timeout policy, normalised to seconds.

    Used both per node (:attr:`SourceNode.policy`) and as a cascading graph
    default (:attr:`SourceGraph.default_policy`), e.g. an Airflow DAG's
    ``default_args`` retries/timeout that apply to every task unless a task
    overrides them.

    Only the genuinely shared retry/timeout knobs are typed; source-specific
    policy (ADF ``secure_input`` / ``secure_output``, Airflow retry-delay
    shapes, email-on-failure) rides in :attr:`extensions`.

    Attributes:
        timeout_seconds: Execution timeout in seconds, when resolvable.
        max_retries: Maximum retry count.
        retry_interval_seconds: Delay between retries in seconds.
        extensions: Source-specific policy fields with no shared typed home.
    """

    timeout_seconds: int | None = None
    max_retries: int | None = None
    retry_interval_seconds: int | None = None
    extensions: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class SourceDependency:
    """A dependency edge from a node to one upstream node.

    ``conditions`` is a **per-edge** outcome list -- the natural ADF shape,
    where each individual edge carries the upstream outcome(s) that must hold
    (``["Succeeded", "Skipped"]``). This is distinct from Airflow's
    ``trigger_rule``, which is a single rule on the *downstream* node quantified
    over all its upstreams together and so cannot live on an edge; that rides in
    :attr:`SourceNode.run_condition` instead.

    Attributes:
        upstream: Task key of the upstream node this edge depends on.
        conditions: Required upstream outcome(s) for THIS edge; empty when
            unconditional. Per-edge (ADF); see :attr:`SourceNode.run_condition`
            for Airflow's node-level ``trigger_rule``.
        resolved: ``False`` when the upstream could not be resolved (e.g. a
            partial export); recorded, not dropped.
    """

    upstream: str
    conditions: list[str] = field(default_factory=list)
    resolved: bool = True


@dataclass(slots=True, kw_only=True)
class SourceNode:
    """A single task in a source workflow, standardised but source-faithful.

    Attributes:
        source_id: Stable identifier from the source (ADF activity name, an
            Airflow capture id) used to resolve dependency edges before task
            keys are allocated.
        task_key: Normalised task key unique within the graph.
        concept: Neutral, classified kind (see the ``CONCEPT_*`` constants);
            ``CONCEPT_GAP`` when the source could not classify it.
        source: Source discriminator (``SOURCE_ADF`` / ``SOURCE_AIRFLOW``).
        name: Human-readable display name, when the source has one distinct
            from ``task_key``.
        native_type: The source's own type string, preserved verbatim (ADF
            ``"Copy"`` / ``"DatabricksNotebook"``, Airflow ``"BashOperator"`` /
            a TaskFlow decorator).
        dependencies: Upstream dependency edges, each with its own per-edge
            outcome conditions (the ADF shape).
        run_condition: Node-level aggregate run rule quantified over all this
            node's upstreams together -- Airflow's ``trigger_rule`` (e.g.
            ``"all_success"``, ``"none_failed_min_one_success"``,
            ``"one_failed"``). ``None`` when the source has no such notion
            (ADF, which expresses outcomes per-edge on :attr:`dependencies`).
        policy: Retry / timeout policy, when the source declares one on the
            node; falls back to :attr:`SourceGraph.default_policy` otherwise.
        data_reads: Best-effort description of what this node reads, as
            :class:`~flowx.models.ir.DataAsset` values (reuses #61). Population
            is best-effort and open -- physical tables/files, logical datasets,
            or value hand-offs (an Airflow XCom) alike -- and an empty list is
            valid where the source records nothing.
        data_writes: Best-effort description of what this node writes; same
            open, best-effort semantics as :attr:`data_reads`.
        properties: Free-form bag for platform-specific attributes with no
            shared typed field yet. Connection / linked-service details live
            here for now -- a typed connection field is a deliberate future
            expansion point.
        raw: Verbatim source dict for this node, for lossless fallback.
    """

    source_id: str
    task_key: str
    concept: str
    source: str
    name: str | None = None
    native_type: str | None = None
    dependencies: list[SourceDependency] = field(default_factory=list)
    run_condition: str | None = None
    policy: PolicySpec | None = None
    data_reads: list[DataAsset] = field(default_factory=list)
    data_writes: list[DataAsset] = field(default_factory=list)
    properties: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] | None = None


@dataclass(slots=True, kw_only=True)
class ContainerNode(SourceNode):
    """A control-flow container that nests child nodes under labelled branches.

    One shape covers every source's control flow: an ADF ``ForEach`` /
    ``Until`` becomes ``{"body": [...]}``, an ``IfCondition`` becomes
    ``{"true": [...], "false": [...]}``, a ``Switch`` becomes
    ``{"<case value>": [...], "default": [...]}``, and an Airflow ``TaskGroup``
    becomes ``{"group": [...]}``. The branch label is the source's own, so no
    control-flow structure is flattened away.

    Attributes:
        branches: Branch label -> ordered child nodes.
    """

    branches: dict[str, list[SourceNode]] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class GapNode(SourceNode):
    """A node the source could not classify, kept with its raw payload + reason.

    Its :attr:`concept` defaults to ``CONCEPT_GAP`` so gaps are uniform across
    sources. The unclassifiable payload is preserved in ``raw`` (inherited) and
    the human-readable cause in :attr:`reason`, so discovery can report and
    later resolve it rather than dropping it.

    Attributes:
        reason: Why the node could not be classified.
    """

    concept: str = CONCEPT_GAP
    reason: str | None = None


@dataclass(slots=True, kw_only=True)
class SourceGraph:
    """A source workflow (an ADF pipeline, an Airflow DAG), standardised.

    Attributes:
        name: Workflow name (ADF pipeline name, Airflow ``dag_id``).
        source: Source discriminator (``SOURCE_ADF`` / ``SOURCE_AIRFLOW``).
        description: Human-readable description, when the source has one.
        parameters: Parameter declarations keyed by name.
        variables: Variable declarations keyed by name. Empty for sources with
            no graph-scoped variable concept (Airflow); such sources' global
            variables ride in :attr:`extensions`.
        schedule: Workflow schedule, when one is declared.
        default_policy: Cascading default retry / timeout policy applied to
            every node that does not set its own :attr:`SourceNode.policy` --
            Airflow's DAG ``default_args`` (retries / retry_delay / timeout).
            ``None`` when the source has no graph-level default (ADF, which
            declares policy per activity).
        run_timeout_seconds: Whole-run timeout for one execution of the
            workflow (Airflow's ``dagrun_timeout``), or ``None`` when the source
            declares none.
        tags: Free-form label list (ADF annotations, Airflow user tags).
        tasks: Top-level nodes; control flow nests further nodes via
            :class:`ContainerNode`.
        lineage: Source-neutral lineage block (control/data edges) derived over
            this graph, or ``None`` when lineage has not been derived. Reuses the
            #61 :class:`~flowx.models.ir.Lineage` type so the discovery AST and
            the IR lineage substrate share one vocabulary; populated by
            :mod:`flowx.discovery_lineage` in the discover phase.
        properties: Free-form bag for graph-level platform-specific attributes.
        extensions: Alias-free overflow for anything else source-specific
            (e.g. ADF ``folder``, Airflow global Variables).
        raw: Verbatim source dict for the whole workflow.
    """

    name: str
    source: str
    description: str | None = None
    parameters: dict[str, ParameterSpec] = field(default_factory=dict)
    variables: dict[str, ParameterSpec] = field(default_factory=dict)
    schedule: ScheduleSpec | None = None
    default_policy: PolicySpec | None = None
    run_timeout_seconds: int | None = None
    tags: list[str] = field(default_factory=list)
    tasks: list[SourceNode] = field(default_factory=list)
    lineage: Lineage | None = None
    properties: dict[str, Any] = field(default_factory=dict)
    extensions: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] | None = None
