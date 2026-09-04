"""Source registry: isolate each migration source behind a uniform interface.

flowx converts *from* a source orchestrator (ADF, Airflow, ...) *to* Databricks
Lakeflow Jobs.  The ``discover`` and ``convert`` phases are irreducibly
source-specific (ADF ARM JSON vs. Airflow Python DAGs share no parser), so each
source lives in its own subpackage and registers the phase modules the adapter
should route to.  The ``package`` phase is source-independent -- it consumes the
shared :class:`~flowx.models.ir.Pipeline` IR every source produces -- so it is
not part of a source's registration.

Each source's parser/translator lives under ``flowx.sources.<name>`` (ADF's
loader + translate, Airflow's loader + discover/convert).  The IR, the preparer,
and the bundler stay source-neutral and shared.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Source:
    """A registered migration source and the phase modules it routes to.

    Attributes:
        name: Source identifier used by ``--source`` (e.g. ``"adf"``).
        discover_module: Import path of the discover-phase module (exposes
            ``main(argv)``).
        convert_module: Import path of the convert-phase module (exposes
            ``main(argv)``).
        source_path_flag: The source-specific alias for ``--source-path``
            accepted on the discover/convert runners (e.g. ``--adf-source-path``).
    """

    name: str
    discover_module: str
    convert_module: str
    source_path_flag: str


_REGISTRY: dict[str, Source] = {
    "adf": Source(
        name="adf",
        discover_module="flowx.sources.adf.loader",
        convert_module="flowx.sources.adf.translate",
        source_path_flag="--adf-source-path",
    ),
    "airflow": Source(
        name="airflow",
        discover_module="flowx.sources.airflow.discover",
        convert_module="flowx.sources.airflow.convert",
        source_path_flag="--airflow-source-path",
    ),
}


def available_sources() -> tuple[str, ...]:
    """Returns the registered source names in a stable order."""
    return tuple(sorted(_REGISTRY))


def get_source(name: str) -> Source:
    """Returns the :class:`Source` for *name*.

    Raises:
        KeyError: When *name* is not a registered source.
    """
    try:
        return _REGISTRY[name]
    except KeyError:
        raise KeyError(f"Unknown source {name!r}; available: {', '.join(available_sources())}") from None
