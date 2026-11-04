"""Live extraction of Azure Data Factory definitions for the ``extract`` phase.

``core`` talks to Azure Resource Manager and writes the native ADF Git layout that
``flowx discover`` reads. It is deliberately self-contained (standard library plus
lazily-imported ``requests``/``azure-identity``) so the same source can be inlined into the
generated notebook. ``runner`` is the phase CLI, ``layout`` checks a path before discover, and
``notebook`` renders the user-run notebook.
"""
