#!/usr/bin/env python3
"""Check that .claude-plugin/plugin.json lists exactly the skill folders under skills/."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent


def main() -> int:
    manifest = json.loads((ROOT / ".claude-plugin" / "plugin.json").read_text())
    listed = {Path(path).name for path in manifest.get("skills", [])}
    on_disk = {path.parent.name for path in (ROOT / "skills").glob("*/SKILL.md")}

    missing = sorted(on_disk - listed)
    stale = sorted(listed - on_disk)
    for name in missing:
        print(f"plugin.json is missing skill: ./skills/{name}")
    for name in stale:
        print(f"plugin.json lists a skill with no skills/{name}/SKILL.md")
    return 1 if missing or stale else 0


if __name__ == "__main__":
    sys.exit(main())
