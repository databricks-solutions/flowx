"""The profiler package must import without pulling in optional Azure deps."""

from __future__ import annotations

import subprocess
import sys

# Runs in a fresh interpreter so the check doesn't depend on what other tests already imported.
_PROBE = """
import sys
import flowx.sources
import flowx.adapter.__main__
import flowx.sources.adf.profiler
forbidden = {"azure", "aiohttp", "pandas", "tqdm"}
leaked = sorted({name.split(".")[0] for name in sys.modules} & forbidden)
print(",".join(leaked))
"""


def test_importing_profiler_package_does_not_import_azure():
    completed = subprocess.run([sys.executable, "-c", _PROBE], capture_output=True, text=True, check=True)
    leaked = completed.stdout.strip()
    assert not leaked, f"optional deps imported too eagerly: {leaked}"
