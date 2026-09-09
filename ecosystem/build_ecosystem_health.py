#!/usr/bin/env python3
"""Join what portals actually run against how well it is maintained.

The two pipelines each answer half a question. The sites crawl knows that 219
portals run ckanext-dcat; the extensions crawl knows when it last shipped a
release. Neither alone tells you where the ecosystem is exposed — a widely
installed extension that stopped being maintained is a different kind of
problem from an unmaintained one nobody uses.

    python build_ecosystem_health.py <sites-derived> <ext-derived> [-o outdir]

Writes ckan_ecosystem_health.csv: one row per catalog extension package.

instances_count is left empty, never 0, for an extension no crawl observed. A
crawl only sees portals that answer status_show AND report a plugin whose name
maps back to the package, so absence is ignorance, not evidence.
"""

import argparse
import csv
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ecosystem import plugin_map

# Buckets that mean "upstream has gone quiet". no_release is deliberately not
# here: plenty of healthy CKAN extensions never tag a release, so treating that
# as decay would flag most of the catalog and mean nothing.
STALE = {'dormant', 'abandoned'}

# "Widely installed" is the top decile of the extensions we actually observe
# running somewhere. A quartile puts the bar at 3 portals, which is not wide by
# any reading; the 90th percentile lands near 19, about 4% of the portals that
# report a plugin list, and picks out the ~33 extensions whose breakage would
# be felt across the ecosystem.
WIDELY_INSTALLED_PERCENTILE = 90

FIELDS = ['package', 'title', 'repository_name', 'url', 'instances_count',
          'pct_of_reporting', 'stars', 'forks', 'contributors_count',
          'total_releases', 'days_since_release', 'maintenance', 'risk']


def load_repos(ext_derived: Path) -> dict:
    """repository_name (lowercased) -> the latest crawl's row for it."""
    path = ext_derived / 'ckan_ext_repos_clean.csv'
    if not path.exists():
        raise SystemExit(f"✗ {path} not found — run derive_ext_csvs.sh first")
    with path.open(newline='', encoding='utf-8') as fh:
        return {r['repository_name'].lower(): r for r in csv.DictReader(fh)}


def reporting_instances(sites_derived: Path) -> int:
    """Portals that reported a plugin list in the latest crawl — the honest
    denominator for "share of the ecosystem"."""
    path = sites_derived / 'ckan_instances_clean.csv'
    with path.open(newline='', encoding='utf-8') as fh:
        return sum(1 for r in csv.DictReader(fh) if (r.get('extensions') or '').strip())


def to_int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def build(sites_derived: Path, ext_derived: Path) -> list:
    catalog = plugin_map.catalog_extensions()
    counts = plugin_map.observed_counts(sites_derived, catalog)
    repos = load_repos(ext_derived)
    reporting = reporting_instances(sites_derived)

    # Relative to what we actually observe, so the bar tracks a crawl that
    # improves rather than a number picked in advance.
    observed = sorted(counts.values())
    wide = (observed[int(len(observed) * WIDELY_INSTALLED_PERCENTILE / 100)]
            if observed else 0)

    rows = []
    for package, meta in sorted(catalog.items()):
        repo_key = (meta['repo'] or '').lower()
        repo = repos.get(repo_key, {})
        installs = counts.get(package)
        maintenance = repo.get('maintenance', '')
        risk = ''
        if installs is not None:
            if installs >= wide and maintenance in STALE:
                risk = 'high'
            elif installs >= wide and maintenance == 'no_release':
                risk = 'watch'
            else:
                risk = 'ok'
        rows.append({
            'package': package,
            'title': meta['title'],
            'repository_name': meta['repo'] or '',
            'url': meta['url'],
            'instances_count': '' if installs is None else installs,
            'pct_of_reporting': ('' if installs is None or not reporting
                                 else round(100 * installs / reporting, 1)),
            'stars': repo.get('stars', ''),
            'forks': repo.get('forks_count', ''),
            'contributors_count': repo.get('contributors_count', ''),
            'total_releases': repo.get('total_releases', ''),
            'days_since_release': repo.get('days_since_release', ''),
            'maintenance': maintenance,
            'risk': risk,
        })

    rows.sort(key=lambda r: (-(to_int(r['instances_count']) or 0), r['package']))
    return rows, wide, reporting, len(repos)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('sites_derived', type=Path)
    ap.add_argument('ext_derived', type=Path)
    ap.add_argument('-o', '--out', type=Path, default=Path('.'))
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    rows, wide, reporting, repo_count = build(args.sites_derived, args.ext_derived)

    path = args.out / 'ckan_ecosystem_health.csv'
    with path.open('w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    matched = sum(1 for r in rows if r['maintenance'])
    installed = sum(1 for r in rows if r['instances_count'] != '')
    at_risk = [r for r in rows if r['risk'] == 'high']
    print(f"✓ {path}: {len(rows)} catalog extensions")
    print(f"  {matched} matched to a crawled repository, {installed} observed installed")
    print(f"  denominator: {reporting} portals reported a plugin list; "
          f"widely-installed threshold (P{WIDELY_INSTALLED_PERCENTILE}): {wide} installs")
    print(f"  {len(at_risk)} at risk (widely installed, upstream dormant or abandoned)")
    for row in at_risk[:10]:
        print(f"    {row['package']:35} {row['instances_count']:>4} installs, "
              f"{row['days_since_release']} days since release")
    return 0


if __name__ == '__main__':
    sys.exit(main())
