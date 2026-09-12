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
import datetime
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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




# ---------------------------------------------------------------- extensions
UPKEEP = [(180, 'active'), (365, 'recent'), (730, 'quiet')]
ATTENTION_ROWS = 40
DOWNSTREAM_ROWS = 40


def _age(stamp, today):
    """Whole days between an ISO date and the crawl date, never negative.

    A repository pushed after the crawl ran is a clock artifact, not a
    prediction — the same clamp ext_enrich.luau applies to dates.
    """
    if not stamp:
        return None
    try:
        then = datetime.date.fromisoformat(stamp[:10])
    except ValueError:
        return None
    return max((today - then).days, 0)


def _upkeep(days, archived):
    """How recently the repository was pushed to.

    Deliberately not called maintenance, and the oldest bucket is `still`
    rather than `abandoned`: ckanext-envvars runs on 126 portals and has not
    been touched in two years because it is finished, not because it is broken.
    """
    if archived:
        return 'archived'
    if days is None:
        return 'unknown'
    for limit, label in UPKEEP:
        if days < limit:
            return label
    return 'still'


def build_extensions(sites: Path, ext: Path, generated: str) -> dict:
    """The extension-side tables: attention, livelier downstream, and the lot.

    No release data is read. Most CKAN extensions never tag one, so release
    recency measured maintenance badly enough to be misleading — ckanext-scheming
    runs on 162 portals and has never cut a release.
    """
    from ecosystem import plugin_map

    activity = {r['repository_name']: r for r in read(ext / 'github_activity.csv')}
    repos = {r['repository_name']: r for r in read(ext / 'ckan_ext_repos_clean.csv')}
    catalog = plugin_map.catalog_extensions()
    stars = {k: num(v.get('stars')) for k, v in activity.items()}

    reach = plugin_map.observed_counts(sites, catalog, ext / 'entry_points.csv',
                                       skip_core=True, stars=stars)
    instances = read(sites / 'ckan_instances_clean.csv')
    reporting = sum(1 for r in instances if (r.get('extensions') or '').strip())
    today = datetime.date.fromisoformat(generated)

    # Star history for the sparkline, from whatever crawls each repo appears in.
    history = collections.defaultdict(dict)
    for row in read(ext / 'ckan_ext_weekly_long.csv'):
        history[row['repository_name']][row['week']] = (
            num(row['stars']), num(row['forks_count']), num(row['contributors_count']))

    rows, attention, downstream = [], [], []
    for package, meta in catalog.items():
        if package in plugin_map.CORE_PACKAGES:
            continue
        repo = meta['repo'] or ''
        act, base = activity.get(repo, {}), repos.get(repo, {})
        if not (act or base):
            continue

        push = _age(act.get('pushed_at'), today)
        archived = act.get('is_archived') == 'true'
        got = reach.get(package, 0)
        row = {
            'p': package, 'r': repo, 'u': meta['url'], 'i': got,
            'pct': round(100 * got / reporting, 1) if reporting else 0,
            's': num(act.get('stars'), num(base.get('stars'))),
            'f': num(act.get('forks'), num(base.get('forks_count'))),
            'c': num(base.get('contributors_count')),
            'up': _upkeep(push, archived), 'push': push,
            'br': num(act.get('branches')), 'live': num(act.get('live_branches')),
            'ahead': num(act.get('forks_ahead')),
        }
        rows.append(row)

        series = history.get(repo, {})
        weeks = sorted(series)
        if len(weeks) >= 2:
            first, last = series[weeks[0]], series[weeks[-1]]
            d_stars, d_forks, d_people = (last[0] - first[0], last[1] - first[1],
                                          last[2] - first[2])
            if d_stars or d_forks or d_people:
                attention.append({**row, 'ds': d_stars, 'df': d_forks, 'dc': d_people,
                                  'spark': [series[w][0] for w in weeks[-26:]]})

        if row['ahead'] and act.get('newest_fork'):
            # Every fork that is ahead, newest first. forks_ahead_list is
            # name@date pairs; older archives predate the column, so fall back
            # to the single newest_fork rather than dropping the row.
            forks = []
            for entry in (act.get('forks_ahead_list') or '').split('|'):
                name, _, pushed = entry.partition('@')
                if name:
                    forks.append({'r': name, 'a': _age(pushed, today)})
            if not forks:
                forks = [{'r': act['newest_fork'],
                          'a': _age(act.get('newest_fork_pushed_at'), today)}]
            downstream.append({**row, 'nf': act['newest_fork'],
                               'nfa': _age(act.get('newest_fork_pushed_at'), today),
                               'forks': forks})

    rows.sort(key=lambda r: -r['i'])
    # Forks weighted above stars: forking means someone used the code.
    attention.sort(key=lambda r: -(r['ds'] * 3 + r['df'] * 2 + r['dc']))
    downstream.sort(key=lambda r: (-r['i'], r['nfa'] if r['nfa'] is not None else 10 ** 6))

    return {
        'ext_rows': rows,
        'ext_attention': attention[:ATTENTION_ROWS],
        'ext_downstream': downstream[:DOWNSTREAM_ROWS],
        'ext_meta': {
            'matched': len(rows),
            'observed': sum(1 for r in rows if r['i']),
            'reporting': reporting,
            'downstream': len(downstream),
            'archived': sum(1 for r in rows if r['up'] == 'archived'),
            'core_plugins': sorted(plugin_map.CORE_PLUGINS),
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('sites_derived', type=Path)
    ap.add_argument('ext_derived', type=Path)
    ap.add_argument('-o', '--out', type=Path, default=Path('dashboard-data'))
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    payload = build(args.sites_derived, args.ext_derived)

    # The extension side is optional: if the activity collector has not run,
    # the instance dashboard still publishes rather than failing.
    if (args.ext_derived / 'github_activity.csv').exists():
        payload |= build_extensions(args.sites_derived, args.ext_derived, payload['generated'])
    else:
        print('  no github_activity.csv — extension tables skipped')

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
    if 'ext_meta' in payload:
        m = payload['ext_meta']
        print(f"  extensions: {m['matched']} matched, {m['observed']} observed running, "
              f"{m['downstream']} with a livelier fork, {m['archived']} archived")
    return 0


if __name__ == '__main__':
    sys.exit(main())
