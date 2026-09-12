#!/usr/bin/env python3
"""Map reported plugin names to catalog extension packages, and those to repos.

Two pipelines need the same mapping and must not drift apart:

  patch_instance_counts.py    plugin -> package, to write instances_count
  build_ecosystem_health.py   plugin -> package -> repo, to join installs
                              against GitHub health

The plugin side is fuzzy by necessity: an instance reports `dcat_rdf_harvester`
and the catalog holds `ckanext-dcat`. The repo side is not — every extension
package carries its GitHub URL in `url`, so package -> repo is a lookup, not a
guess.
"""

import csv
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import cloudscraper
from config import CKAN_BASE_URL, SESSION_HEADERS

GITHUB_RE = re.compile(r'github\.com/([\w.-]+)/([\w.-]+)', re.I)

# Plugin names that ship inside CKAN itself. Matched on the exact entry-point
# name, never a prefix: `datapusher` is core, `datapusher_plus` is a separate
# extension and must survive the filter.
CORE_PLUGINS = {
    'image_view', 'text_view', 'datastore', 'stats', 'recline_view',
    'resource_proxy', 'datapusher', 'datatables_view', 'webpage_view',
    'activity', 'recline_map_view', 'recline_grid_view', 'recline_graph_view',
    'tracking', 'video_view', 'audio_view', 'expire_api_token',
}

# Catalogue entries that are really the core plugins above. Filtering the plugin
# names alone would leave these listed at zero reach.
CORE_PACKAGES = {'ckanext-datastore', 'ckanext-stats', 'ckanext-datatablesview'}

scraper = cloudscraper.create_scraper()
scraper.headers.update(SESSION_HEADERS)


def _auth() -> dict:
    return {'Authorization': os.getenv('CKAN_API_KEY', '')}


def repo_from_url(url: str) -> str | None:
    """https://github.com/ckan/ckanext-dcat[.git][/tree/...] -> 'ckan/ckanext-dcat'."""
    match = GITHUB_RE.search(url or '')
    if not match:
        return None
    owner, repo = match.group(1), match.group(2)
    return f"{owner}/{repo[:-4] if repo.lower().endswith('.git') else repo}"


def catalog_extensions(field: str = 'instances_count') -> dict:
    """Map package name -> {field, url, repo} for every type:extension package."""
    packages, start = {}, 0
    while True:
        resp = scraper.get(f"{CKAN_BASE_URL}/api/3/action/package_search",
                           params={'fq': 'type:extension', 'rows': 1000, 'start': start},
                           headers=_auth(), timeout=120)
        resp.raise_for_status()
        result = resp.json()['result']
        for pkg in result['results']:
            url = pkg.get('url') or ''
            packages[pkg['name']] = {
                field: pkg.get(field),
                'url': url,
                'repo': repo_from_url(url),
                'title': pkg.get('title') or pkg['name'],
            }
        start += len(result['results'])
        if not result['results'] or start >= result['count']:
            break
    return packages


def families(ranking_csv: Path) -> dict:
    """plugin -> upstream package, from the `family` column qsv/plugin_meta.luau
    writes onto ckan_extension_ranking.csv. Empty when the file is absent."""
    if not ranking_csv.exists():
        return {}
    with ranking_csv.open(newline='', encoding='utf-8') as fh:
        return {r['name']: r['family'] for r in csv.DictReader(fh) if r.get('family')}


def entry_point_packages(entry_points_csv: Path, known: dict,
                         stars: dict | None = None) -> dict:
    """plugin -> package, from what each repository actually declares.

    status_show reports ENTRY POINT names, not packages: a portal running
    ckanext-dcat reports dcat, dcat_rdf_harvester, dcat_json_interface and
    structured_data. Reading the declarations resolves that by fact rather than
    by regex.

    Names collide more than expected — not through GitHub forks (almost none of
    the catalogue are) but through independent republished copies, so
    ckanext-dcat-rev declares the same five names as ckanext-dcat. Stars break
    the tie, which reliably picks the upstream.
    """
    if not entry_points_csv.exists():
        return {}
    repo_to_pkg = {(m.get('repo') or '').lower(): p for p, m in known.items() if m.get('repo')}
    stars = {k.lower(): v for k, v in (stars or {}).items()}

    declared = defaultdict(list)
    with entry_points_csv.open(newline='', encoding='utf-8') as fh:
        for row in csv.DictReader(fh):
            repo = (row['repository_name'] or '').lower()
            if repo in repo_to_pkg:
                declared[row['plugin']].append(repo)

    return {plugin: repo_to_pkg[max(repos, key=lambda r: (stars.get(r, 0), -len(r)))]
            for plugin, repos in declared.items()}


def resolve(plugin: str, family_map: dict, known: dict, entry_points: dict | None = None) -> str | None:
    """Map a plugin name to the catalog package that provides it, first hit wins.

    Order is by descending certainty: an exact package name is unambiguous, a
    declared entry point is a statement of fact, the family regexes are a
    backstop for repositories that declare nothing readable.
    """
    for candidate in (f'ckanext-{plugin}', f"ckanext-{plugin.replace('_', '-')}"):
        if candidate in known:
            return candidate
    if entry_points and plugin in entry_points:
        return entry_points[plugin]
    for candidate in (family_map.get(plugin), plugin):
        if candidate and candidate in known:
            return candidate
    return None


def observed_instances(instances_csv: Path, family_map: dict, known: dict,
                       entry_points: dict | None = None, skip_core: bool = False) -> dict:
    """Package name -> set of instance names seen running it.

    Distinct instances, not plugin hits: ckanext-dcat ships dcat,
    dcat_json_interface and dcat_rdf_harvester, so counting hits would treat one
    portal as three.
    """
    instances = defaultdict(set)
    with instances_csv.open(newline='', encoding='utf-8') as fh:
        for row in csv.DictReader(fh):
            for plugin in filter(None, (row.get('extensions') or '').split('|')):
                if skip_core and plugin in CORE_PLUGINS:
                    continue
                package = resolve(plugin, family_map, known, entry_points)
                if package and not (skip_core and package in CORE_PACKAGES):
                    instances[package].add(row['name'])
    return instances


def observed_counts(derived: Path, known: dict, entry_points_csv: Path | None = None,
                    skip_core: bool = False, stars: dict | None = None) -> dict:
    """Package name -> number of distinct instances seen running it."""
    family_map = families(derived / 'ckan_extension_ranking.csv')
    eps = entry_point_packages(entry_points_csv, known, stars) if entry_points_csv else {}
    seen = observed_instances(derived / 'ckan_instances_clean.csv', family_map, known,
                              eps, skip_core)
    return {package: len(names) for package, names in seen.items()}
