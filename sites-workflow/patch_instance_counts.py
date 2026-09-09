#!/usr/bin/env python3
"""Write each extension's observed install count onto its catalog package.

Reads the latest crawl's instances (derived/ckan_instances_clean.csv), maps the
plugins each one reported back to catalog extension packages, and patches
`instances_count` with the number of DISTINCT instances running that extension.

Distinct matters: ckanext-dcat ships dcat, dcat_json_interface and
dcat_rdf_harvester, so summing plugin counts would treat one portal as three.

Extensions with no observed install are left untouched — a crawl only sees
instances that answer status_show and register a plugin whose name maps back to
the package, so a missing value means "not observed", not "nobody runs it".

The mapping itself lives in ecosystem/plugin_map.py, shared with
build_ecosystem_health.py so the two cannot disagree about what runs where.

Usage:
    CKAN_API_KEY=... python patch_instance_counts.py [derived-dir] [--dry-run]
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import CKAN_BASE_URL, SESSION_HEADERS
from ecosystem import plugin_map

CKAN_URL = CKAN_BASE_URL
API_KEY = os.getenv('CKAN_API_KEY', '')
FIELD = 'instances_count'

scraper = plugin_map.scraper
AUTH = {'Authorization': API_KEY}


def patch(package: str, count: int) -> bool:
    resp = scraper.post(f"{CKAN_URL}/api/3/action/package_patch",
                        json={'id': package, FIELD: count}, headers=AUTH, timeout=60)
    if not resp.ok:
        print(f"  ✗ {package}: HTTP {resp.status_code} {resp.text[:200]}")
        return False
    if not resp.json().get('success'):
        print(f"  ✗ {package}: {resp.json().get('error')}")
        return False
    return True


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith('-')]
    dry_run = '--dry-run' in sys.argv
    derived = Path(args[0] if args else 'derived')

    if not (derived / 'ckan_instances_clean.csv').exists():
        print(f"✗ {derived}/ckan_instances_clean.csv not found")
        return 1
    if not API_KEY and not dry_run:
        print("✗ CKAN_API_KEY is not set")
        return 1

    catalog = plugin_map.catalog_extensions(FIELD)
    counts = plugin_map.observed_counts(derived, catalog)
    changed = {p: n for p, n in counts.items()
               if str(catalog[p][FIELD]) != str(n)}

    print(f"Catalog extensions: {len(catalog)}")
    print(f"With an observed install count: {len(counts)}")
    print(f"Needing a patch: {len(changed)}")
    for package, count in sorted(changed.items(), key=lambda kv: -kv[1])[:10]:
        print(f"  {package}: {catalog[package][FIELD]} -> {count}")
    if len(changed) > 10:
        print(f"  ... and {len(changed) - 10} more")

    if dry_run:
        print("Dry run — nothing patched")
        return 0

    failures = sum(0 if patch(p, n) else 1 for p, n in changed.items())
    print(f"{'✓' if not failures else '✗'} {len(changed) - failures}/{len(changed)} extensions patched")
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
