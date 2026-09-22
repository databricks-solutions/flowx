"""Azure Data Factory / Synapse / Fabric estate profiler.

Standalone pre-migration survey. The heavy third-party imports (pandas, azure-*)
live in :mod:`flowx.profiler.extract_pipelines`; import from there lazily so
loading this package stays cheap for callers that don't profile.
"""
