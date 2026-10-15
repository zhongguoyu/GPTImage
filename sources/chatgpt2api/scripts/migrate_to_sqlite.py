from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.storage.sqlite_migration import migrate_legacy_state, verify_sqlite_state


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Migrate and verify chatgpt2api application state")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--db", type=Path)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args(argv)
    db_path = args.db or args.data_dir / "app.db"
    try:
        result = verify_sqlite_state(db_path) if args.verify else migrate_legacy_state(args.data_dir, db_path)
    except Exception as exc:
        print(json.dumps({"valid": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if args.verify and not result.get("valid"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
