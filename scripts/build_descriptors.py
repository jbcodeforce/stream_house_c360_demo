#!/usr/bin/env python
"""Regenerate the Data Products graph descriptors from the dbt manifest.

Run this after ``dbt run`` to rebuild one ``<product>.json`` per data product
(a top-level folder under the dbt project's ``models/``) so the backend graph
page matches the real pipeline.

This is the thin CLI wrapper around ``data_products.descriptor_builder``: it
supplies the three things the pure core deliberately doesn't know about —
where the files are, the real Flink statement-name sanitizer (reused from the
confluent dbt adapter, never reimplemented), and the ``statement_name_prefix``
from the dbt profile.

Because it imports the confluent adapter, run it in the dbt (pipelines) venv:

    cd pipelines && uv run python ../scripts/build_descriptors.py

Options let you point at a different manifest, output dir, or profile, and
``--dry-run`` prints what would change without writing.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_BACKEND_DIR = _REPO_ROOT / "apps" / "backend"

# The builder lives in the backend package (schema parity with the reader);
# make it importable without installing the backend as a dependency.
sys.path.insert(0, str(_BACKEND_DIR))


def _load_statement_prefix(profiles_dir: Path, project_dir: Path) -> str:
    """Read ``statement_name_prefix`` from the dbt profile the project uses,
    falling back to the adapter default ``dbt-`` if anything is missing."""
    import yaml  # provided by dbt in the pipelines venv

    default = "dbt-"
    try:
        project = yaml.safe_load((project_dir / "dbt_project.yml").read_text())
        profile_name = project["profile"]
        profiles = yaml.safe_load((profiles_dir / "profiles.yml").read_text())
        profile = profiles[profile_name]
        target = profile.get("target")
        output = profile["outputs"][target]
        return output.get("statement_name_prefix", default)
    except (OSError, KeyError, TypeError, yaml.YAMLError):
        return default


def _make_statement_name_fn(prefix: str, project_name: str):
    """A model-name -> Flink statement-name function matching the adapter's own
    naming, by reusing its sanitizer (``{prefix}{project}-{model}`` sanitized)."""
    from dbt.adapters.confluent.naming import sanitize_statement_name

    def statement_name(model_name: str) -> str:
        return sanitize_statement_name(f"{prefix}{project_name}-{model_name}")

    return statement_name


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--manifest", type=Path, default=_REPO_ROOT / "pipelines" / "target" / "manifest.json",
        help="dbt manifest.json (default: pipelines/target/manifest.json)",
    )
    parser.add_argument(
        "--out-dir", type=Path,
        default=_BACKEND_DIR / "data_products" / "descriptors",
        help="where to write <product>.json (default: the backend descriptors dir)",
    )
    parser.add_argument(
        "--profiles-dir", type=Path, default=Path.home() / ".dbt",
        help="directory holding profiles.yml (default: ~/.dbt)",
    )
    parser.add_argument(
        "--statement-prefix", default=None,
        help="override the Flink statement_name_prefix (default: read from the dbt profile)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="print the products that would be written without writing files",
    )
    args = parser.parse_args(argv)

    from data_products import descriptor_builder  # after sys.path insert

    manifest = json.loads(args.manifest.read_text())
    project_name = manifest.get("metadata", {}).get("project_name", "")
    prefix = args.statement_prefix
    if prefix is None:
        prefix = _load_statement_prefix(args.profiles_dir, _REPO_ROOT / "pipelines")

    statement_name_fn = _make_statement_name_fn(prefix, project_name)
    descriptors = descriptor_builder.build_descriptors(manifest, statement_name_fn)

    if not descriptors:
        print("No data products found in the manifest.", file=sys.stderr)
        return 1

    args.out_dir.mkdir(parents=True, exist_ok=True)
    for product_id, descriptor in sorted(descriptors.items()):
        statements = sum(1 for n in descriptor["nodes"] if n["type"] == "statement")
        sources = sum(1 for n in descriptor["nodes"] if n["type"] == "source")
        out_path = args.out_dir / f"{product_id}.json"
        if args.dry_run:
            print(f"[dry-run] {out_path} ({statements} statements, {sources} sources)")
            continue
        out_path.write_text(json.dumps(descriptor, indent=2) + "\n")
        print(f"wrote {out_path} ({statements} statements, {sources} sources)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
