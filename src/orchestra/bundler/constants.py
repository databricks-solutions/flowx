"""String constants for DAB cluster definitions produced by the bundler."""

from __future__ import annotations

from typing import Final

from orchestra.adapter.constants import (
    COMPUTE_MODE_CLASSIC_MULTI_NODE,
    COMPUTE_MODE_CLASSIC_SINGLE_NODE,
)

DEFAULT_JOB_CLUSTER_KEY: Final[str] = "default_cluster"
SINGLE_NODE_JOB_CLUSTER_KEY: Final[str] = "single_node_cluster"
MULTI_NODE_JOB_CLUSTER_KEY: Final[str] = "multi_node_cluster"

MULTI_NODE_CLUSTER_NODE_TYPE_ID: Final[str] = "Standard_D8ds_v5"

COMPUTE_MODE_TO_CLUSTER_KEY: Final[dict[str, str]] = {
    COMPUTE_MODE_CLASSIC_SINGLE_NODE: SINGLE_NODE_JOB_CLUSTER_KEY,
    COMPUTE_MODE_CLASSIC_MULTI_NODE: MULTI_NODE_JOB_CLUSTER_KEY,
}
