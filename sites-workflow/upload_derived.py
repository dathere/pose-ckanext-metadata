#!/usr/bin/env python3
"""Upload the derived CSVs to the CKAN dataset as resources.

Each file maps to one resource, matched by name: an existing resource is updated
in place (same UUID, so links and views survive), a missing one is created. Run
after derive_csvs.sh.

Usage:
    CKAN_API_KEY=... python upload_derived.py [derived-dir] [--dry-run]
"""

import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, UTC
from pathlib import Path

import cloudscraper
from config import CKAN_BASE_URL, SESSION_HEADERS

CKAN_URL = CKAN_BASE_URL
API_KEY = os.getenv('CKAN_API_KEY', '')
DATASET_ID = 'ckan-sites-metadata'

# Only these are uploaded, in this order. Anything else in the directory is
# working data, not a deliverable.
DESCRIPTIONS = {
    'ckan_instances_clean.csv':
        'One row per CKAN instance from the latest crawl: dataset/group/organization '
        'counts, version, release branch, support status and installed plugins.',
    'ckan_extension_ranking.csv':
        'Plugin adoption in the latest crawl: install count, share of instances that '
        'reported a plugin list, whether the plugin ships with CKAN, and its repo.',
    'ckan_weekly_long.csv':
        'Every archived crawl stacked, one row per week per instance — the full '
        'time series in analysis-ready form.',
    'ckan_version_changes.csv':
        'CKAN version changes per instance, classified as upgrade, downgrade or '
        'change, with a flag for crossing a release branch.',
    'ckan_extension_changes.csv':
        'Plugins added or removed by an instance from one crawl to the next.',
    'ckan_extension_trends.csv':
        'Per plugin: latest install count and share, plus the change over the cohort '
        'window (instances that reported in every week of it).',
    'ckan_extension_series.csv':
        'Install count per plugin per crawl date — one column per crawl.',
    'ckan_extension_cohort_series.csv':
        'Install count per plugin per crawl date, restricted to the cohort, so a '
        'change reflects a real install rather than a portal answering again.',
}

scraper = cloudscraper.create_scraper()
scraper.headers.update(SESSION_HEADERS)
AUTH = {'Authorization': API_KEY}


def row_count(path: Path) -> int:
    """Data rows, counted with a CSV reader so quoted fields cannot inflate it."""
    with path.open(newline='', encoding='utf-8') as fh:
        return max(sum(1 for _ in csv.reader(fh)) - 1, 0)


def existing_resources() -> dict:
    """Map resource name -> id for the dataset."""
    resp = scraper.get(f"{CKAN_URL}/api/3/action/package_show",
                       params={'id': DATASET_ID}, headers=AUTH, timeout=30)
    resp.raise_for_status()
    result = resp.json()
    if not result.get('success'):
        raise SystemExit(f"package_show failed: {result.get('error')}")
    return {r['name']: r['id'] for r in result['result'].get('resources', []) if r.get('name')}


def upload(path: Path, resource_id: str | None, description: str) -> bool:
    """Update the resource in place when it exists, otherwise create it."""
    stamp = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')
    payload = {'description': f'{description} Last updated: {stamp}'}
    if resource_id:
        action, payload['id'] = 'resource_update', resource_id
    else:
        action = 'resource_create'
        payload |= {'package_id': DATASET_ID, 'name': path.name, 'format': 'CSV'}

    with path.open('rb') as fh:
        resp = scraper.post(f"{CKAN_URL}/api/3/action/{action}",
                            data=payload,
                            files={'upload': (path.name, fh, 'text/csv')},
                            headers=AUTH, timeout=300)
    if not resp.ok:
        print(f"  ✗ {action} HTTP {resp.status_code}: {resp.text[:300]}")
        return False
    if not resp.json().get('success'):
        print(f"  ✗ {action} failed: {resp.json().get('error')}")
        return False
    return True


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith('-')]
    dry_run = '--dry-run' in sys.argv
    derived = Path(args[0] if args else 'derived')

    files = [derived / name for name in DESCRIPTIONS if (derived / name).exists()]
    if not files:
        print(f"✗ No derived CSVs found in {derived}")
        return 1
    if not API_KEY and not dry_run:
        print("✗ CKAN_API_KEY is not set")
        return 1

    resources = existing_resources()
    print(f"Dataset {DATASET_ID}: {len(resources)} existing resources, {len(files)} files to upload")

    failures = 0
    for path in files:
        rid = resources.get(path.name)
        verb = 'update' if rid else 'create'
        size_kb = path.stat().st_size // 1024
        print(f"  {verb:6} {path.name} ({row_count(path)} rows, {size_kb} KB)")
        if dry_run:
            continue
        if not upload(path, rid, DESCRIPTIONS[path.name]):
            failures += 1

    if dry_run:
        print("Dry run — nothing uploaded")
        return 0
    print(f"{'✓' if not failures else '✗'} {len(files) - failures}/{len(files)} resources uploaded")
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
