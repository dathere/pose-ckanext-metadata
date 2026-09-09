#!/usr/bin/env python3
"""Seed extensions-history/ from the datastore, once.

The sites pipeline archives every crawl as a CSV in the repo; the extensions
pipeline never did, so its entire history lives only in the CKAN datastore
resource that timeseries_append.py writes to. This script pulls that history
back out and splits it into one file per crawl date, matching the sites layout,
so derive_ext_csvs.sh has something to build a time series from.

Run it once. After that the workflow archives each crawl as it happens and this
script has nothing left to do — existing files are never overwritten.

Usage:
    python seed_history.py [outdir] [--force]

Reads only. CKAN_API_KEY is not required for a public resource, but is sent
when set.
"""

import csv
import os
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import cloudscraper
from config import CKAN_BASE_URL, SESSION_HEADERS

DATASET_ID = 'ckan-extensions-metadata'
RESOURCE_NAME = 'CKAN Extensions Dynamic Metadata'
PAGE = 32000

# The archive keeps the crawl's own columns in the crawl's own order. `_id` is
# a datastore artifact and is dropped.
COLUMNS = ['tstamp', 'repository_name', 'url', 'forks_count', 'total_releases',
           'latest_release', 'release_date', 'stars', 'open_issues',
           'contributors_count', 'discussions']

scraper = cloudscraper.create_scraper()
scraper.headers.update(SESSION_HEADERS)
AUTH = {'Authorization': os.getenv('CKAN_API_KEY', '')}


def resource_id() -> str:
    resp = scraper.get(f"{CKAN_BASE_URL}/api/3/action/package_show",
                       params={'id': DATASET_ID}, headers=AUTH, timeout=60)
    resp.raise_for_status()
    for res in resp.json()['result']['resources']:
        if res.get('name') == RESOURCE_NAME:
            return res['id']
    raise SystemExit(f"✗ resource {RESOURCE_NAME!r} not found on {DATASET_ID}")


def fetch_all(rid: str) -> list:
    """Page through the resource. datastore_search_sql is disabled on this
    catalog (HTTP 400), so aggregate server-side is not an option."""
    rows, offset = [], 0
    while True:
        resp = scraper.get(f"{CKAN_BASE_URL}/api/3/action/datastore_search",
                           params={'resource_id': rid, 'limit': PAGE, 'offset': offset},
                           headers=AUTH, timeout=300)
        resp.raise_for_status()
        result = resp.json()['result']
        batch = result['records']
        rows.extend(batch)
        offset += len(batch)
        print(f"  fetched {offset}/{result['total']}")
        if not batch or offset >= result['total']:
            return rows


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith('-')]
    force = '--force' in sys.argv
    out = Path(args[0] if args else
               Path(__file__).resolve().parent.parent / 'extensions-history')
    out.mkdir(parents=True, exist_ok=True)

    rows = fetch_all(resource_id())
    by_week = defaultdict(list)
    for row in rows:
        stamp = str(row.get('tstamp') or '')[:10]
        if len(stamp) == 10:
            by_week[stamp].append(row)

    written = skipped = deduped = 0
    for week, records in sorted(by_week.items()):
        path = out / f'{week}.csv'
        if path.exists() and not force:
            skipped += 1
            continue
        # Five crawls (2025-07-08/10/16, 2026-04-05 and one more) were appended
        # to the datastore twice, so every repository appears in them twice and
        # every count for that week would double. Keep the last row per
        # repository; a dict preserves insertion order and the later append wins.
        unique = {str(r.get('repository_name') or ''): r for r in records}
        if len(unique) != len(records):
            print(f"  {week}: {len(records)} rows -> {len(unique)} unique repositories")
            deduped += 1
        records = list(unique.values())
        # Sort by repository so a re-seed is byte-identical and diffs stay small.
        records.sort(key=lambda r: str(r.get('repository_name') or ''))
        with path.open('w', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(fh, fieldnames=COLUMNS, extrasaction='ignore')
            writer.writeheader()
            for record in records:
                writer.writerow({c: record.get(c, '') for c in COLUMNS})
        written += 1
        print(f"  wrote {path.name} ({len(records)} rows)")

    print(f"✓ {written} crawls written, {skipped} already present, "
          f"{deduped} de-duplicated ({len(by_week)} in the datastore)")
    return 0


if __name__ == '__main__':
    sys.exit(main())
