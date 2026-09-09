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


def resolve(plugin: str, family_map: dict, known: dict) -> str | None:
    """Map a plugin name to the catalog package that provides it, first hit wins."""
    for candidate in (f'ckanext-{plugin}',
                      f"ckanext-{plugin.replace('_', '-')}",
                      family_map.get(plugin),
                      plugin):
        if candidate and candidate in known:
            return candidate
    return None


def observed_instances(instances_csv: Path, family_map: dict, known: dict) -> dict:
    """Package name -> set of instance names seen running it.

    Distinct instances, not plugin hits: ckanext-dcat ships dcat,
    dcat_json_interface and dcat_rdf_harvester, so counting hits would treat one
    portal as three.
    """
    instances = defaultdict(set)
    with instances_csv.open(newline='', encoding='utf-8') as fh:
        for row in csv.DictReader(fh):
            for plugin in filter(None, (row.get('extensions') or '').split('|')):
                package = resolve(plugin, family_map, known)
                if package:
                    instances[package].add(row['name'])
    return instances


def observed_counts(derived: Path, known: dict) -> dict:
    """Package name -> number of distinct instances seen running it."""
    family_map = families(derived / 'ckan_extension_ranking.csv')
    seen = observed_instances(derived / 'ckan_instances_clean.csv', family_map, known)
    return {package: len(names) for package, names in seen.items()}
