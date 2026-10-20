#!/usr/bin/env python3
"""Export each catalogued site's status for the dashboard.

    python fetch_site_status.py <sites-derived>

Writes <sites-derived>/site_status.csv (name,status). The status is the site
schema's own field, which step 3 of the sites datapump sets from the crawl --
active, dormant or unreachable -- or an editor set by hand (staging,
development). The rollup runs after that datapump, so this reads the value the
crawl has just written rather than re-deriving it here.

The crawl archive does not carry it: its `status` column is CKAN support
status (current/behind/eol), a different thing that happens to share the name.
"""

import argparse
import csv
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import cloudscraper  # noqa: E402

from config import CKAN_BASE_URL, SESSION_HEADERS  # noqa: E402

ROWS = 1000


def fetch() -> list:
    session = cloudscraper.create_scraper()
    session.headers.update(SESSION_HEADERS)
    out, start = [], 0
    while True:
        resp = session.get(f"{CKAN_BASE_URL}/api/3/action/package_search", params={
            # Scheming fields are indexed as extras_*; fl must name the
            # index field, though the result still carries it as 'status'.
            'q': 'type:site', 'fl': 'name,extras_status',
            'start': start, 'rows': ROWS, 'include_private': False,
        }, timeout=60)
        resp.raise_for_status()
        result = resp.json()['result']
        out += [(r['name'], (r.get('status') or '').strip().lower()) for r in result['results']]
        start += ROWS
        if start >= result['count']:
            return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('sites_derived', type=Path)
    args = ap.parse_args()

    rows = fetch()
    args.sites_derived.mkdir(parents=True, exist_ok=True)
    path = args.sites_derived / 'site_status.csv'
    with path.open('w', newline='', encoding='utf-8') as fh:
        writer = csv.writer(fh)
        writer.writerow(['name', 'status'])
        writer.writerows(sorted(rows))
    print(f"✓ {path} ({len(rows)} sites)")
    return 0


if __name__ == '__main__':
    sys.exit(main())
