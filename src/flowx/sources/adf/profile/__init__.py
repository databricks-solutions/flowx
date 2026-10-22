"""ADF cost/TCO profiler: a live Azure scan that estimates current ADF spend.

Ported from the standalone profiler script. Everything that touches the network or
optional dependencies (azure.*, requests) is imported lazily inside the
modules that need it, so importing this package never requires the ``profile`` extra.
"""
