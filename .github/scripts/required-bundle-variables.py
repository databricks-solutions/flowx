"""Print the variables a bundle's dev target needs a value for, one per line."""

from __future__ import annotations

import sys

import yaml


def main() -> int:
    """Print each declared variable that has no default and no dev target value."""
    with open(sys.argv[1], encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}
    target_variables = ((config.get("targets") or {}).get("dev") or {}).get("variables") or {}
    for name, declaration in sorted((config.get("variables") or {}).items()):
        if name not in target_variables and (not isinstance(declaration, dict) or "default" not in declaration):
            print(name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
