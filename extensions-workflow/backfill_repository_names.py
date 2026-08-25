#!/usr/bin/env python3
"""One-off repair: fill in NULL repository_name values in the extensions datastore.

Three legacy rows (tstamp 2025-10-21) were written without a repository_name, which
crashed the weekly duplicate check under pandas 3. The name is recoverable from the
row's GitHub URL, which is always populated.

Usage:
    python backfill_repository_names.py                      # dry run, prints what it would change
    CKAN_API_KEY=... python backfill_repository_names.py --apply
    python backfill_repository_names.py --selftest
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from urllib.parse import urlparse

import cloudscraper
from config import CKAN_BASE_URL, SESSION_HEADERS

CKAN_URL = CKAN_BASE_URL
API_KEY = os.getenv('CKAN_API_KEY', '')
RESOURCE_ID = '9f197f7e-a8f6-41b6-80c7-b14ac9fdbfd7'  # CKAN Extensions Dynamic Metadata

scraper = cloudscraper.create_scraper()
scraper.headers.update(SESSION_HEADERS)
AUTH = {'Authorization': API_KEY}


def name_from_url(url: str) -> str | None:
    """'https://github.com/open-data/ckanext-gcnotify' -> 'open-data/ckanext-gcnotify'."""
    if not url:
        return None
    path = urlparse(url).path.strip('/')
    parts = [p for p in path.split('/') if p]
    if len(parts) < 2:
        return None
    return '/'.join(parts[:2]).removesuffix('.git')


def find_null_names() -> list[dict]:
    """Page through the datastore and return rows whose repository_name is NULL."""
    url = f"{CKAN_URL}/api/3/action/datastore_search"
    rows, offset = [], 0
    while True:
        resp = scraper.post(
            url,
            json={'resource_id': RESOURCE_ID, 'limit': 1000, 'offset': offset, 'sort': '_id'},
            headers=AUTH,
            timeout=30,
        )
        resp.raise_for_status()
        result = resp.json()['result']
        rows.extend(r for r in result['records'] if not r.get('repository_name'))
        offset += 1000
        if offset >= result['total']:
            break
    return rows


def main() -> int:
    apply_changes = '--apply' in sys.argv
    rows = find_null_names()
    if not rows:
        print("✓ No NULL repository_name rows found — nothing to backfill")
        return 0

    updates = []
    for row in rows:
        name = name_from_url(row.get('url'))
        print(f"  _id {row['_id']}: {row.get('url')} -> {name or 'UNRESOLVABLE'}")
        if name:
            updates.append({'_id': row['_id'], 'repository_name': name})

    skipped = len(rows) - len(updates)
    print(f"\n{len(updates)} row(s) to update, {skipped} without a usable URL")

    if not apply_changes:
        print("Dry run — re-run with --apply (and CKAN_API_KEY set) to write these values")
        return 0
    if not API_KEY:
        print("✗ CKAN_API_KEY is not set")
        return 1

    resp = scraper.post(
        f"{CKAN_URL}/api/3/action/datastore_upsert",
        json={'resource_id': RESOURCE_ID, 'method': 'update', 'force': True, 'records': updates},
        headers=AUTH,
        timeout=60,
    )
    if not resp.ok:
        print(f"✗ datastore_upsert HTTP {resp.status_code}: {resp.text[:500]}")
        return 1

    remaining = find_null_names()
    print(f"✓ Updated {len(updates)} row(s); {len(remaining)} NULL repository_name row(s) remain")
    return 0 if not remaining else 1


def selftest() -> int:
    assert name_from_url('https://github.com/open-data/ckanext-gcnotify') == 'open-data/ckanext-gcnotify'
    assert name_from_url('https://github.com/zbw/ckanext-dara/') == 'zbw/ckanext-dara'
    assert name_from_url('https://github.com/zbw/ckanext-dara.git') == 'zbw/ckanext-dara'
    assert name_from_url('https://github.com/open-data/ckanext-foo/tree/main') == 'open-data/ckanext-foo'
    assert name_from_url('https://github.com/open-data') is None  # owner only, not a repo
    assert name_from_url('') is None
    assert name_from_url(None) is None
    print("ok")
    return 0


if __name__ == '__main__':
    sys.exit(selftest() if '--selftest' in sys.argv else main())
