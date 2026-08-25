#!/usr/bin/env python3
"""
Build the derived CSVs (and optional dashboard JSON payloads) from the CKAN crawl dumps.

Two input shapes, same enrichment logic:

  snapshot   one crawl, one row per instance          -> ckan_instances_clean.csv
                                                         ckan_extension_ranking.csv
  weekly     N crawls stacked, one row per week/inst  -> ckan_weekly_long.csv
                                                         ckan_version_changes.csv
                                                         ckan_extension_changes.csv
                                                         ckan_extension_trends.csv

Usage
    python build_ckan_csvs.py snapshot ckan_stats.csv          -o out/
    python build_ckan_csvs.py weekly   weekly_crawl.csv        -o out/
    python build_ckan_csvs.py weekly   weekly_crawl.csv -o out/ --json panel.json

A header row is detected automatically (ckan_stats.csv has one, the weekly
export does not); override with --has-header / --no-header.
"""

import argparse
import ast
import collections
import json
import re
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

COLS = ['tstamp', 'name', 'url', 'num_datasets', 'num_groups',
        'num_organizations', 'ckan_version', 'extensions']

# Newest patch on each still-supported branch. Bump these when CKAN ships a
# release, otherwise every instance silently drops out of "current".
LATEST = {'2.11': '2.11.5', '2.10': '2.10.10'}

# Plugins that ship inside CKAN itself. Everything else is a third-party
# extension. Kept explicit rather than inferred; the list moves between
# releases and guessing produces worse answers than a stale list does.
CORE = {
    'activity', 'datastore', 'datapusher', 'image_view', 'text_view',
    'recline_view', 'recline_grid_view', 'recline_graph_view',
    'recline_map_view', 'datatables_view', 'webpage_view', 'audio_view',
    'video_view', 'resource_proxy', 'stats', 'multilingual',
    'expire_api_token', 'tracking',
}

# Maps an individual plugin name back to the repo that provides it, so
# dcat + dcat_json_interface + dcat_rdf_harvester roll up to one package.
FAMILY = [
    (r'^dcat', 'ckanext-dcat'),
    (r'_harvester$|^harvest$|^ckan_harvester$', 'ckanext-harvest'),
    (r'^spatial_|^csw_|^spatial$', 'ckanext-spatial'),
    (r'^geo_view$|^geojson_view$|^wmts_view$|^shp_view$|^ags_', 'ckanext-geoview'),
    (r'^scheming_', 'ckanext-scheming'),
    (r'^hierarchy_', 'ckanext-hierarchy'),
    (r'^pdf_view$', 'ckanext-pdfview'),
    (r'^officedocs_view$', 'ckanext-officedocs'),
    (r'^xloader$', 'ckanext-xloader'),
    (r'^pages$', 'ckanext-pages'),
    (r'^showcase$', 'ckanext-showcase'),
    (r'^envvars$', 'ckanext-envvars'),
    (r'^googleanalytics$', 'ckanext-googleanalytics'),
]


# --------------------------------------------------------------------------
# field-level helpers
# --------------------------------------------------------------------------

def family(plugin: str):
    for pattern, repo in FAMILY:
        if re.search(pattern, plugin):
            return repo
    return None


def parse_extensions(cell):
    """The extensions column is a Python/JSON list literal. Returns a sorted
    list of unique names, or None when the portal reported nothing at all.
    Sorting matters: the weekly diff compares sets, but a stable order keeps
    the pipe-delimited output diffable."""
    if pd.isna(cell):
        return None
    try:
        names = {str(x).strip() for x in ast.literal_eval(cell) if str(x).strip()}
    except (ValueError, SyntaxError):
        return None
    return sorted(names) or None


def branch(version):
    """2.11.5 -> '2.11'. None when unparseable."""
    if pd.isna(version):
        return None
    m = re.match(r'^(\d+)\.(\d+)', str(version))
    return f'{m.group(1)}.{m.group(2)}' if m else None


def support_status(version, br):
    """current  = newest patch on a supported branch (or newer than we track)
       behind   = supported branch, stale patch
       eol      = branch no longer receives fixes
       unknown  = portal reported no version"""
    # 'unknown' means the portal told us nothing. A version we can't parse
    # into a branch (a bare '2', a fork string) still means the portal
    # answered, so it lands in 'eol' rather than being counted as silent.
    if pd.isna(version):
        return 'unknown'
    if not isinstance(br, str):
        return 'eol'
    if br in LATEST:
        return 'current' if str(version) == LATEST[br] else 'behind'
    try:
        major, minor = (int(x) for x in br.split('.'))
        if (major, minor) > max((int(a), int(b)) for a, b in
                                (k.split('.') for k in LATEST)):
            return 'current'
    except ValueError:
        pass
    return 'eol'


def version_tuple(version):
    """Sortable (major, minor, patch). Trailing junk like '2.12.0b0' is
    truncated to its digits, which is fine for ordering."""
    if not version or pd.isna(version):
        return None
    parts = re.findall(r'\d+', str(version))[:3]
    return tuple(int(x) for x in (parts + ['0', '0', '0'])[:3])


def host(url):
    try:
        return urlparse(url if '//' in url else 'http://' + url).netloc.replace('www.', '')
    except ValueError:
        return url


def has_header_row(path):
    """The dumps are inconsistent: ckan_stats.csv carries a header, the
    weekly export does not. Guessing beats making the caller remember."""
    with open(path, encoding='utf-8', errors='replace') as fh:
        first = fh.readline()
    return first.split(',')[:3] == COLS[:3]


def load(path, has_header=None):
    if has_header is None:
        has_header = has_header_row(path)
    df = pd.read_csv(path, header=0 if has_header else None,
                     names=None if has_header else COLS)
    missing = set(COLS) - set(df.columns)
    if missing:
        raise SystemExit(f'{path}: missing columns {sorted(missing)}')
    df['ext'] = df['extensions'].apply(parse_extensions)
    df['branch'] = df['ckan_version'].apply(branch)
    df['status'] = [support_status(v, b) for v, b in zip(df.ckan_version, df.branch)]
    # A portal counts as reachable if it gave us EITHER a version or a plugin
    # list. Some return one without the other depending on how status_show is
    # exposed, and treating those as down understates coverage.
    df['reachable'] = df.ckan_version.notna() | df.ext.notna()
    df['host'] = df['url'].apply(host)
    df['num_extensions'] = df.ext.apply(lambda e: len(e) if e else 0)
    df['ext_flat'] = df.ext.apply(lambda e: '|'.join(e) if e else '')
    return df


# --------------------------------------------------------------------------
# snapshot mode
# --------------------------------------------------------------------------

def build_snapshot(df, out: Path):
    instances = df[['tstamp', 'name', 'url', 'host', 'num_datasets', 'num_groups',
                    'num_organizations', 'ckan_version', 'branch', 'status',
                    'num_extensions']].copy()
    instances['extensions'] = df['ext_flat']
    instances.to_csv(out / 'ckan_instances_clean.csv', index=False)

    reporting = int(df.ext.notna().sum())
    counts = collections.Counter()
    for names in df.ext.dropna():
        counts.update(names)

    ranking = pd.DataFrame([
        {'name': name,
         'count': n,
         # Denominator is instances that reported a plugin list, not all
         # instances. "Of portals that told us, how many run this."
         'pct': round(100 * n / reporting, 1) if reporting else 0.0,
         'core': name in CORE,
         'family': family(name)}
        for name, n in counts.most_common()
    ])
    ranking.to_csv(out / 'ckan_extension_ranking.csv', index=False)

    print(f'snapshot: {len(df)} instances, {int(df.reachable.sum())} reachable, '
          f'{reporting} reporting plugins, {len(counts)} distinct plugins')
    return {'instances': len(df), 'reporting': reporting}


# --------------------------------------------------------------------------
# weekly mode
# --------------------------------------------------------------------------

def build_weekly(df, out: Path, json_path=None):
    df['week'] = pd.to_datetime(df.tstamp).dt.strftime('%Y-%m-%d')
    weeks = sorted(df.week.unique())
    widx = {w: i for i, w in enumerate(weeks)}
    nweeks = len(weeks)

    long = df[['week', 'name', 'url', 'num_datasets', 'num_groups',
               'num_organizations', 'ckan_version', 'branch', 'status',
               'num_extensions']].copy()
    long['reachable'] = df.reachable.astype(int)
    long['extensions'] = df['ext_flat']
    long.to_csv(out / 'ckan_weekly_long.csv', index=False)

    per_instance = {name: g.sort_values('week') for name, g in df.groupby('name')}

    # ---- change detection -------------------------------------------------
    # Both loops skip weeks where the portal was silent, comparing each
    # reported value against the previous REPORTED one. Otherwise every
    # flaky portal emits a spurious "removed everything / added it back"
    # pair, which would swamp the real events.
    version_changes, extension_changes = [], []
    for name, g in per_instance.items():
        prev_version = None
        for row in g.itertuples():
            v = row.ckan_version
            if pd.isna(v):
                continue
            if prev_version is not None and v != prev_version:
                a, b = version_tuple(prev_version), version_tuple(v)
                if a and b:
                    kind = 'upgrade' if b > a else 'downgrade' if b < a else 'change'
                    major = (a[0], a[1]) != (b[0], b[1])
                else:
                    kind, major = 'change', False
                version_changes.append({'n': name, 'w': row.week, 'from': prev_version,
                                        'to': v, 'k': kind, 'major': major})
            prev_version = v

        prev_ext = None
        for row in g.itertuples():
            e = row.ext
            if e is None:
                continue
            if prev_ext is not None:
                added = sorted(set(e) - set(prev_ext))
                removed = sorted(set(prev_ext) - set(e))
                if added or removed:
                    extension_changes.append({'week': row.week, 'name': name,
                                              'added': '|'.join(added),
                                              'removed': '|'.join(removed)})
            prev_ext = e

    pd.DataFrame(version_changes, columns=['n', 'w', 'from', 'to', 'k', 'major']) \
        .sort_values(['w', 'n']).to_csv(out / 'ckan_version_changes.csv', index=False)
    pd.DataFrame(extension_changes, columns=['week', 'name', 'added', 'removed']) \
        .sort_values(['week', 'name']).to_csv(out / 'ckan_extension_changes.csv', index=False)

    # ---- adoption series --------------------------------------------------
    # Raw weekly counts move with crawl reachability (442-511 responding
    # instances per week here), so a plugin can "gain 30 installs" purely
    # because more portals answered. The cohort series fixes the denominator
    # to instances that reported a plugin list in EVERY week, which makes a
    # change in the series a real install or uninstall. Deltas use the cohort.
    cohort = [n for n, g in per_instance.items()
              if len(g) == nweeks and g.ext.notna().all()]

    raw = collections.defaultdict(lambda: [0] * nweeks)
    coh = collections.defaultdict(lambda: [0] * nweeks)
    for row in df.itertuples():
        if not row.ext:
            continue
        i = widx[row.week]
        in_cohort = row.name in cohort
        for plugin in row.ext:
            raw[plugin][i] += 1
            if in_cohort:
                coh[plugin][i] += 1

    reporting = [int(df[df.week == w].ext.notna().sum()) for w in weeks]
    exts = []
    for plugin, series in raw.items():
        c = coh.get(plugin, [0] * nweeks)
        exts.append({'name': plugin,
                     'count': series[-1],
                     'pct': round(100 * series[-1] / reporting[-1], 1) if reporting[-1] else 0.0,
                     'delta': c[-1] - c[0],
                     'core': plugin in CORE,
                     'family': family(plugin),
                     'series': series,
                     'cohort': c})
    exts.sort(key=lambda e: -e['count'])

    pd.DataFrame([
        {k: v for k, v in e.items() if k not in ('series', 'cohort')}
        | {f'w{i+1}': v for i, v in enumerate(e['series'])}
        | {f'c{i+1}': v for i, v in enumerate(e['cohort'])}
        for e in exts
    ]).to_csv(out / 'ckan_extension_trends.csv', index=False)

    print(f'weekly: {nweeks} crawls {weeks[0]}..{weeks[-1]}, '
          f'{df.name.nunique()} instances, cohort {len(cohort)}')
    print(f'  {len(version_changes)} version changes '
          f'({sum(1 for v in version_changes if v["major"])} crossed a minor release)')
    print(f'  {len(extension_changes)} plugin change events, {len(raw)} distinct plugins')

    if json_path:
        write_panel_json(df, per_instance, weeks, widx, exts, cohort,
                         reporting, version_changes, extension_changes, Path(json_path))


def write_panel_json(df, per_instance, weeks, widx, exts, ext_cohort,
                     reporting, version_changes, extension_changes, path):
    """Payload embedded in the dashboard HTML. Not needed for the CSVs."""
    nweeks = len(weeks)
    order = ['current', 'behind', 'eol', 'unknown']

    stable = [n for n, g in per_instance.items()
              if len(g) == nweeks and g.reachable.all()]
    timeline = []
    for w in weeks:
        g = df[df.week == w]
        sc = collections.Counter(g.status)
        sub = g[g.name.isin(stable)]
        timeline.append({'w': w, 'up': int(g.reachable.sum()), 'total': len(g),
                         **{s: sc.get(s, 0) for s in order},
                         'datasets': int(g.num_datasets.sum()),
                         'cohort_ds': int(sub.num_datasets.sum()),
                         'cohort_n': len(sub)})

    ext_id = {e['name']: i for i, e in enumerate(exts)}
    instances = []
    for name, g in per_instance.items():
        last = g.iloc[-1]
        live = [int(d) for d, up in zip(g.num_datasets, g.reachable) if up]
        instances.append({
            'n': name, 'u': last.url, 'h': last.host,
            'd': int(last.num_datasets), 'o': int(last.num_organizations),
            'gr': int(last.num_groups),
            'v': None if pd.isna(last.ckan_version) else str(last.ckan_version),
            'b': last.branch if isinstance(last.branch, str) else None,
            's': last.status,
            'e': [ext_id[x] for x in (last.ext or []) if x in ext_id],
            'seen': int(g.reachable.sum()), 'of': len(g),
            'ds_series': [int(x) for x in g.num_datasets],
            'up_series': [int(x) for x in g.reachable],
            'growth': (live[-1] - live[0]) if len(live) >= 2 else 0,
        })

    top = {e['name'] for e in exts[:60]}
    pairs, fam = collections.Counter(), collections.Counter()
    for row in df[df.week == weeks[-1]].itertuples():
        if not row.ext:
            continue
        for f in {family(e) for e in row.ext} - {None}:
            fam[f] += 1
        s = set(row.ext) & top
        for a in s:
            for b in s:
                if a < b:
                    pairs[(a, b)] += 1

    rel = collections.Counter()
    for i in instances:
        rel['always' if i['seen'] == i['of'] else 'never' if not i['seen'] else 'flaky'] += 1

    payload = {
        'weeks': weeks, 'timeline': timeline, 'total': len(instances),
        'reporting': reporting,
        'ext_names': [e['name'] for e in exts],
        'exts': exts[:400],
        'movers': sorted([e for e in exts if e['delta']],
                         key=lambda e: -abs(e['delta']))[:20],
        'families': fam.most_common(14),
        'pairs': [{'a': a, 'b': b, 'n': n} for (a, b), n in pairs.most_common(400)],
        'instances': instances,
        'upgrades': sorted(version_changes, key=lambda x: (x['w'], x['n'])),
        'ext_events': sorted(
            [{'w': e['week'], 'n': e['name'],
              'add': e['added'].split('|') if e['added'] else [],
              'rem': e['removed'].split('|') if e['removed'] else []}
             for e in extension_changes], key=lambda x: (x['w'], x['n'])),
        'reliability': dict(rel),
        'ext_cohort_n': len(ext_cohort), 'stable_n': len(stable),
        'unique_ext': len(exts),
    }
    # allow_nan=False so a stray pandas NaN fails here instead of shipping a
    # file that blows up in JSON.parse at page load.
    with open(path, 'w') as fh:
        json.dump(payload, fh, separators=(',', ':'), allow_nan=False)
    print(f'  wrote {path} ({path.stat().st_size // 1024} KB)')


# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('mode', choices=['snapshot', 'weekly'])
    ap.add_argument('csv', help='crawl dump')
    ap.add_argument('-o', '--out', default='.', help='output directory')
    ap.add_argument('--has-header', dest='has_header', action='store_true',
                    default=None, help='force: input has a header row')
    ap.add_argument('--no-header', dest='has_header', action='store_false',
                    help='force: input has no header row')
    ap.add_argument('--json', help='(weekly only) also write the dashboard payload')
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    df = load(args.csv, args.has_header)

    if args.mode == 'snapshot':
        build_snapshot(df, out)
    else:
        build_weekly(df, out, args.json)


if __name__ == '__main__':
    main()
