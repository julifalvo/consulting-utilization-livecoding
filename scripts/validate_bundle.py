"""Offline check of the Asset Bundle YAML against the CLI's own JSON schema.

`databricks bundle validate` needs a workspace; this runs in CI on every PR without one:

    databricks bundle schema > /tmp/bundle_schema.json
    uv run python scripts/validate_bundle.py /tmp/bundle_schema.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import regex
import yaml
from jsonschema import Draft7Validator, ValidationError, validators

ROOT = Path(__file__).resolve().parents[1]


def _pattern(validator, pattern, instance, schema):
    # The CLI's schema uses Unicode classes (\p{L}) that Python's `re` does not support.
    if validator.is_type(instance, "string") and not regex.search(pattern, instance):
        yield ValidationError(f"{instance!r} does not match {pattern!r}")


BundleValidator = validators.extend(Draft7Validator, {"pattern": _pattern})


def main(schema_path: str) -> int:
    validator = BundleValidator(json.loads(Path(schema_path).read_text(encoding="utf-8")))
    root = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    files = [ROOT / "databricks.yml"]
    for pattern in root.get("include", []):
        files += sorted(ROOT.glob(pattern))

    failures = 0
    for path in files:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        for error in sorted(validator.iter_errors(doc), key=lambda e: list(e.path)):
            failures += 1
            where = "/".join(str(p) for p in error.path) or "<root>"
            print(f"{path.relative_to(ROOT)}: {where}: {error.message}")
        print(f"checked {path.relative_to(ROOT)}")
    print("bundle schema: OK" if not failures else f"bundle schema: {failures} error(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
