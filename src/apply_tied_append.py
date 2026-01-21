import argparse
import json
import os
import shutil
import sys
from typing import Any, Dict
from database_util import load_json_file


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replace input JSON entries with tied_append overrides by question_id."
    )
    parser.add_argument("--input-json", required=True, help="Path to the base input JSON.")
    parser.add_argument(
        "--append-json",
        required=True,
        help="Path to the *_tied_append.json file containing override entries.",
    )
    args = parser.parse_args()

    append_path = args.append_json
    input_path = args.input_json

    if not os.path.exists(append_path):
        print(f"[tied-append] File not found, skip: {append_path}")
        return 0

    base_data = load_json_file(input_path)
    append_data = load_json_file(append_path)
    backup_path = f"{input_path}.bak"
    shutil.move(input_path, backup_path)


    append_map: Dict[Any, Dict[str, Any]] = {}
    for item in append_data:
        qid = item.get("question_id")
        if qid is None:
            continue
        append_map[qid] = item

    replaced = 0
    for i, item in enumerate(base_data):
        qid = item.get("question_id")
        if qid in append_map:
            base_data[i] = append_map[qid]
            replaced += 1

    with open(input_path, "w", encoding="utf-8") as f:
        json.dump(base_data, f, ensure_ascii=False, indent=2)

    print(
        f"[tied-append] Applied overrides from {append_path}. "
        f"Candidates: {len(append_map)}, replaced: {replaced}. "
        f"Original moved to: {backup_path}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
