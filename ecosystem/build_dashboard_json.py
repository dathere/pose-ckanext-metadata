#!/usr/bin/env python3
"""Build the Insights dashboard payload from the derived CSVs.

    python build_dashboard_json.py <sites-derived> <ext-derived> [-o dashboard-data]

Writes dashboard-data/dashboard.json — one pre-aggregated file the page fetches
from raw.githubusercontent, the same way templates/ckan_map/map.html already
fetches map-data/sites.geojson. About 1 MB raw and ~135 KB over the wire once
gzipped, which is a single request against 8 MB of CSV nobody's browser should
be parsing.

Keys are short (`n`, `u`, `d`, `s`) because they repeat once per instance per
week; this is a wire format, not a file anyone reads by hand. The CSVs remain
the download path for that.

Everything here is derived, never recomputed: statuses, families, cohorts and
the quality gate are decided once in derive_csvs.sh and read back out. A number
on the dashboard and the same number in a CSV come from the same place.
"""

import argparse
import collections
import csv
import json
import sys
from pathlib import Path

# Instances hold an index into ext_names rather than the plugin name, and only
# the top slice gets co-install pairs — 1100 plugins squared is 1.2M pairs
# nobody looks at.
TOP_PAIRS = 60
MAX_PAIRS = 1200
MAX_EXTS = 400


def read(path: Path) -> list:
    if not path.exists():
        raise SystemExit(f"✗ {path} not found — run the derive scripts first")
    with path.open(newline='', encoding='utf-8') as fh:
        return list(csv.DictReader(fh))


def num(value, default=0):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def build(sites: Path, ext: Path) -> dict:
    long_rows = read(sites / 'ckan_weekly_long.csv')
    quality = read(sites / 'ckan_crawl_quality.csv')
    trends = read(sites / 'ckan_extension_trends.csv')
    cohort_rows = read(sites / 'ckan_extension_cohort_series.csv')
    upgrades = read(sites / 'ckan_version_changes.csv')
    events = read(sites / 'ckan_extension_changes.csv')

    weeks = sorted({r['week'] for r in long_rows})
    suspect = [r['week'] for r in quality if r.get('quality') != 'ok']
    suspect_set = set(suspect)
    quality_by_week = {r['week']: r for r in quality}

    by_week = collections.defaultdict(list)
    for row in long_rows:
        by_week[row['week']].append(row)

    # The weeks a portal can fairly be judged on. Per-instance uptime strips
    # and the "answered every crawl" cohort are indexed against these, not
    # against every crawl: before 2025-10-29 nothing reported a version, and in
    # 2026-01-26..02-15 the crawl itself was failing. Counting those weeks
    # makes every portal look flaky — with them in, not one of 767 portals
    # answered every crawl; with them out, 251 did.
    ok_weeks = {r['week'] for r in quality if r.get('quality') == 'ok'}
    vweeks = [w for w in weeks
              if w in ok_weeks and any(r['ckan_version'] for r in by_week[w])]
    last = weeks[-1]
    latest = by_week[last]

    cohort_weeks = [c for c in (cohort_rows[0].keys() if cohort_rows else []) if c != 'name']
    cohort_by_name = {r['name']: r for r in cohort_rows}

    # ---- extensions ------------------------------------------------------
    exts = []
    for row in trends:
        cohort = cohort_by_name.get(row['name'], {})
        exts.append({
            'name': row['name'],
            'count': num(row['count']),
            'pct': float(row['pct'] or 0),
            'core': row['core'] == 'true',
            'family': row['family'] or None,
            'delta': num(row['delta']),
            'cohort': [num(cohort.get(w)) for w in cohort_weeks],
        })
    exts.sort(key=lambda e: -e['count'])
    ext_names = [e['name'] for e in exts]
    ext_index = {name: i for i, name in enumerate(ext_names)}

    # ---- timeline --------------------------------------------------------
    events_per_week = collections.Counter(r['week'] for r in events)
    order = ['current', 'behind', 'eol', 'unknown']

    # Portals reachable in EVERY version-bearing week: the only population
    # whose dataset total can be compared across weeks without the number
    # moving because the crawl reached more of them.
    reach = collections.defaultdict(int)
    for week in vweeks:
        for row in by_week[week]:
            if row['reachable'] == '1':
                reach[row['name']] += 1
    stable = {n for n, seen in reach.items() if seen == len(vweeks)}

    timeline = []
    for week in weeks:
        rows = by_week[week]
        statuses = collections.Counter(r['status'] for r in rows)
        qrow = quality_by_week.get(week, {})
        timeline.append({
            'w': week,
            'rows': len(rows),
            'up': sum(1 for r in rows if r['reachable'] == '1'),
            'ds': sum(num(r['num_datasets']) for r in rows),
            **{s: statuses.get(s, 0) for s in order},
            'q': qrow.get('quality', ''),
            'sus': week in suspect_set,
            'ev': events_per_week.get(week, 0),
            'coh_ds': (sum(num(r['num_datasets']) for r in rows if r['name'] in stable)
                       if week in vweeks else None),
        })

    # ---- per instance ----------------------------------------------------
    per_instance = collections.defaultdict(dict)
    for row in long_rows:
        per_instance[row['name']][row['week']] = row

    instances = []
    for name, weekly in per_instance.items():
        seen_weeks = sorted(weekly)
        newest = weekly[seen_weeks[-1]]
        up = [1 if weekly.get(w, {}).get('reachable') == '1' else 0 for w in vweeks]
        live = [num(weekly[w]['num_datasets']) for w in seen_weeks
                if weekly[w]['reachable'] == '1']
        plugins = [p for p in (newest['extensions'] or '').split('|') if p]
        instances.append({
            'n': name,
            'u': newest['url'],
            'h': newest['host'],
            'd': num(newest['num_datasets']),
            'g': num(newest['num_groups']),
            'o': num(newest['num_organizations']),
            'v': newest['ckan_version'] or None,
            'b': newest['branch'] or None,
            's': newest['status'],
            'e': sorted(ext_index[p] for p in plugins if p in ext_index),
            'up': up,
            'seen': sum(up),
            'of': len(vweeks),
            # Present in the archive but absent from the newest crawl: dropped
            # from the catalog rather than merely unreachable.
            'gone': last not in weekly,
            'growth': (live[-1] - live[0]) if len(live) >= 2 else 0,
        })
    instances.sort(key=lambda i: -i['d'])

    # Every bucket is emitted even when empty. The page indexes these by name
    # and a missing key reads as undefined, which took out the whole reliability
    # panel the first time a crawl had no never-answering portal.
    reliability = collections.Counter({'always': 0, 'flaky': 0, 'never': 0})
    for inst in instances:
        reliability['always' if inst['seen'] == inst['of']
                    else 'never' if not inst['seen'] else 'flaky'] += 1

    # ---- latest-crawl breakdowns ----------------------------------------
    statuses = collections.Counter({s: 0 for s in order})
    statuses.update(r['status'] for r in latest)
    branches = collections.Counter(r['branch'] for r in latest if r['branch'])
    versions = collections.Counter(r['ckan_version'] for r in latest if r['ckan_version'])
    tlds = collections.Counter(h.rsplit('.', 1)[-1] for h in
                               (r['host'] for r in latest) if '.' in h)

    families, pairs = collections.Counter(), collections.Counter()
    top = set(ext_names[:TOP_PAIRS])
    family_of = {e['name']: e['family'] for e in exts if e['family']}
    for row in latest:
        plugins = {p for p in (row['extensions'] or '').split('|') if p}
        if not plugins:
            continue
        for fam in {family_of[p] for p in plugins if p in family_of}:
            families[fam] += 1
        shared = sorted(plugins & top)
        for i, a in enumerate(shared):
            for b in shared[i + 1:]:
                pairs[(a, b)] += 1

    all_plugins = set()
    for row in long_rows:
        all_plugins.update(p for p in (row['extensions'] or '').split('|') if p)

    payload = {
        'generated': last,
        'weeks': weeks,
        'nweeks': len(weeks),
        'vweeks': vweeks,
        'cohort_weeks': cohort_weeks,
        'cohort_n': sum(1 for i in instances if i['seen'] == i['of']),
        'suspect_weeks': suspect,
        'total': len(instances),
        'latest_total': len(latest),
        'reachable': sum(1 for r in latest if r['reachable'] == '1'),
        'with_ext': sum(1 for r in latest if r['extensions']),
        'total_datasets': sum(num(r['num_datasets']) for r in latest),
        'unique_ext': len(exts),
        'unique_ext_all': len(all_plugins),
        'timeline': timeline,
        'statuses': dict(statuses),
        'branches': branches.most_common(),
        'versions': versions.most_common(),
        'tlds': tlds.most_common(15),
        'reliability': dict(reliability),
        'exts': exts[:MAX_EXTS],
        'ext_names': ext_names,
        'families': families.most_common(14),
        'pairs': [{'a': a, 'b': b, 'n': n} for (a, b), n in pairs.most_common(MAX_PAIRS)],
        'instances': instances,
        'upgrades': [{'n': r['name'], 'w': r['week'], 'from': r['from'], 'to': r['to'],
                      'k': r['kind'], 'major': r['major'] == 'true',
                      'sus': r['week'] in suspect_set} for r in upgrades],
        'ext_events': [{'n': r['name'], 'w': r['week'],
                        'add': [p for p in r['added'].split('|') if p],
                        'rem': [p for p in r['removed'].split('|') if p],
                        'sus': r['week'] in suspect_set} for r in events],
    }

    health_csv = ext / 'ckan_ecosystem_health.csv'
    if health_csv.exists():
        health = [r for r in read(health_csv) if r['instances_count'] != '']
        payload['health'] = [{
            'p': r['package'],
            'r': r['repository_name'],
            'i': num(r['instances_count']),
            'pct': float(r['pct_of_reporting'] or 0),
            'st': num(r['stars']),
            'c': num(r['contributors_count']),
            'age': num(r['days_since_release'], None),
            'm': r['maintenance'],
            'risk': r['risk'],
        } for r in health]
        payload['at_risk'] = sum(1 for r in health if r['risk'] == 'high')

    ext_maint = ext / 'ckan_ext_maintenance.csv'
    if ext_maint.exists():
        buckets = collections.defaultdict(dict)
        for row in read(ext_maint):
            buckets[row['week']][row['maintenance']] = num(row['count'])
        payload['ext_maintenance'] = [{'w': w, **b} for w, b in sorted(buckets.items())]

    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('sites_derived', type=Path)
    ap.add_argument('ext_derived', type=Path)
    ap.add_argument('-o', '--out', type=Path, default=Path('dashboard-data'))
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    payload = build(args.sites_derived, args.ext_derived)

    path = args.out / 'dashboard.json'
    # allow_nan=False so a stray NaN fails here rather than shipping a file
    # that blows up in JSON.parse at page load.
    with path.open('w', encoding='utf-8') as fh:
        json.dump(payload, fh, separators=(',', ':'), allow_nan=False)

    size = path.stat().st_size
    print(f"✓ {path} ({size // 1024} KB)")
    print(f"  {payload['nweeks']} weeks ({len(payload['suspect_weeks'])} suspect), "
          f"{payload['total']} instances, {payload['unique_ext']} plugins")
    print(f"  latest crawl {payload['generated']}: {payload['latest_total']} portals, "
          f"{payload['with_ext']} reported plugins, "
          f"{payload['total_datasets']:,} datasets")
    if 'at_risk' in payload:
        print(f"  {len(payload['health'])} extensions joined to GitHub health, "
              f"{payload['at_risk']} at risk")
    return 0


if __name__ == '__main__':
    sys.exit(main())
