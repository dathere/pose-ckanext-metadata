#!/usr/bin/env python3
"""Publish a directory of derived CSVs to a CKAN dataset as resources.

Shared by sites-workflow/upload_derived.py and
extensions-workflow/upload_ext_derived.py: the two pipelines publish different
files to different datasets, but the publishing itself is identical, and one
copy of it is one place to fix a CKAN API change.

Each file maps to one resource, matched by name: an existing resource is
updated in place (same UUID, so links, views and bookmarks survive), a missing
one is created.
"""

import csv
import os
import sys
from datetime import datetime, UTC
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import cloudscraper
from config import CKAN_BASE_URL, SESSION_HEADERS

CKAN_URL = CKAN_BASE_URL

scraper = cloudscraper.create_scraper()
scraper.headers.update(SESSION_HEADERS)


def _auth() -> dict:
    return {'Authorization': os.getenv('CKAN_API_KEY', '')}


def row_count(path: Path) -> int:
    """Data rows, counted with a CSV reader so quoted fields cannot inflate it."""
    with path.open(newline='', encoding='utf-8') as fh:
        return max(sum(1 for _ in csv.reader(fh)) - 1, 0)


def existing_resources(dataset_id: str) -> dict:
    """Map resource name -> id for the dataset."""
    resp = scraper.get(f"{CKAN_URL}/api/3/action/package_show",
                       params={'id': dataset_id}, headers=_auth(), timeout=30)
    resp.raise_for_status()
    result = resp.json()
    if not result.get('success'):
        raise SystemExit(f"package_show failed: {result.get('error')}")
    return {r['name']: r['id'] for r in result['result'].get('resources', []) if r.get('name')}


def upload(dataset_id: str, path: Path, resource_id: str | None, description: str) -> bool:
    """Update the resource in place when it exists, otherwise create it."""
    stamp = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')
    payload = {'description': f'{description} Last updated: {stamp}'}
    if resource_id:
        action, payload['id'] = 'resource_update', resource_id
    else:
        action = 'resource_create'
        payload |= {'package_id': dataset_id, 'name': path.name, 'format': 'CSV'}

    with path.open('rb') as fh:
        resp = scraper.post(f"{CKAN_URL}/api/3/action/{action}",
                            data=payload,
                            files={'upload': (path.name, fh, 'text/csv')},
                            headers=_auth(), timeout=300)
    if not resp.ok:
        print(f"  ✗ {action} HTTP {resp.status_code}: {resp.text[:300]}")
        return False
    if not resp.json().get('success'):
        print(f"  ✗ {action} failed: {resp.json().get('error')}")
        return False
    return True


def main(dataset_id: str, descriptions: dict, default_dir: str = 'derived') -> int:
    """CLI entry point: `[derived-dir] [--dry-run]`."""
    args = [a for a in sys.argv[1:] if not a.startswith('-')]
    dry_run = '--dry-run' in sys.argv
    derived = Path(args[0] if args else default_dir)

    # Only the listed files are uploaded, in this order. Anything else in the
    # directory is working data, not a deliverable.
    files = [derived / name for name in descriptions if (derived / name).exists()]
    if not files:
        print(f"✗ No derived CSVs found in {derived}")
        return 1
    if not os.getenv('CKAN_API_KEY', '') and not dry_run:
        print("✗ CKAN_API_KEY is not set")
        return 1

    resources = existing_resources(dataset_id)
    print(f"Dataset {dataset_id}: {len(resources)} existing resources, {len(files)} files to upload")

    failures = 0
    for path in files:
        rid = resources.get(path.name)
        verb = 'update' if rid else 'create'
        size_kb = path.stat().st_size // 1024
        print(f"  {verb:6} {path.name} ({row_count(path)} rows, {size_kb} KB)")
        if dry_run:
            continue
        if not upload(dataset_id, path, rid, descriptions[path.name]):
            failures += 1

    if dry_run:
        print("Dry run — nothing uploaded")
        return 0
    print(f"{'✓' if not failures else '✗'} {len(files) - failures}/{len(files)} resources uploaded")
    return 1 if failures else 0
