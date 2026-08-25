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

Usage:
    CKAN_API_KEY=... python patch_instance_counts.py [derived-dir] [--dry-run]
"""

import csv
import os
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import cloudscraper
from config import CKAN_BASE_URL, SESSION_HEADERS

CKAN_URL = CKAN_BASE_URL
API_KEY = os.getenv('CKAN_API_KEY', '')
FIELD = 'instances_count'

scraper = cloudscraper.create_scraper()
scraper.headers.update(SESSION_HEADERS)
AUTH = {'Authorization': API_KEY}


def catalog_extensions() -> dict:
    """Map package name -> current instances_count (None when unset)."""
    packages, start = {}, 0
    while True:
        resp = scraper.get(f"{CKAN_URL}/api/3/action/package_search",
                           params={'fq': 'type:extension', 'rows': 1000, 'start': start},
                           headers=AUTH, timeout=120)
        resp.raise_for_status()
        result = resp.json()['result']
        for pkg in result['results']:
            packages[pkg['name']] = pkg.get(FIELD)
        start += len(result['results'])
        if not result['results'] or start >= result['count']:
            break
    return packages


def resolve(plugin: str, families: dict, known: dict) -> str | None:
    """Map a plugin name to the catalog package that provides it, first hit wins."""
    for candidate in (f'ckanext-{plugin}',
                      f"ckanext-{plugin.replace('_', '-')}",
                      families.get(plugin),
                      plugin):
        if candidate and candidate in known:
            return candidate
    return None


def observed_counts(derived: Path, known: dict) -> dict:
    """Package name -> number of distinct instances seen running it."""
    families = {}
    ranking = derived / 'ckan_extension_ranking.csv'
    if ranking.exists():
        with ranking.open(newline='', encoding='utf-8') as fh:
            families = {r['name']: r['family'] for r in csv.DictReader(fh) if r.get('family')}

    instances = defaultdict(set)
    with (derived / 'ckan_instances_clean.csv').open(newline='', encoding='utf-8') as fh:
        for row in csv.DictReader(fh):
            for plugin in filter(None, (row.get('extensions') or '').split('|')):
                package = resolve(plugin, families, known)
                if package:
                    instances[package].add(row['name'])
    return {package: len(names) for package, names in instances.items()}


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

    known = catalog_extensions()
    counts = observed_counts(derived, known)
    changed = {p: n for p, n in counts.items() if str(known.get(p)) != str(n)}

    print(f"Catalog extensions: {len(known)}")
    print(f"With an observed install count: {len(counts)}")
    print(f"Needing a patch: {len(changed)}")
    for package, count in sorted(changed.items(), key=lambda kv: -kv[1])[:10]:
        print(f"  {package}: {known.get(package)} -> {count}")
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
