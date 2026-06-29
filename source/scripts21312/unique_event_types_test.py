from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


def iter_json_files(root: Path):
    yield from root.rglob("*.json")


EVENT_TYPE_PATTERN = re.compile(r'"event_type"\s*:\s*"((?:\\.|[^"\\])*)"')


def collect_unique_event_types(root: Path) -> set[str]:
    unique_event_types: set[str] = set()

    for json_file in iter_json_files(root):
        content = json_file.read_text(encoding="utf-8", errors="replace")
        for match in EVENT_TYPE_PATTERN.finditer(content):
            raw_value = match.group(1)
            try:
                event_type = json.loads(f'"{raw_value}"')
            except json.JSONDecodeError:
                event_type = raw_value
            unique_event_types.add(str(event_type))

    return unique_event_types


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dem unique event_type values from JSON logs."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("/home/cuong/Desktop/DATN/BK_activity_logs_unzipped"),
        help="Thu muc goc chua cac file JSON.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("scripts/event_types_unique.txt"),
        help="File ghi danh sach event_type unique.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.root.exists():
        print(f"[ERROR] Khong tim thay thu muc: {args.root}")
        return 1

    unique_event_types = collect_unique_event_types(args.root)
    sorted_event_types = sorted(unique_event_types)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(sorted_event_types) + "\n", encoding="utf-8")

    print(f"Unique event_type count: {len(sorted_event_types)}")
    print(f"Written to: {args.output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
