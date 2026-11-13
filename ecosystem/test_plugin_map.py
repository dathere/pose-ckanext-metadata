"""plugin_map: a package that declares entry points is only credited with those.

Run: python ecosystem/test_plugin_map.py
plugin_map imports cloudscraper and config (which wants a Cloudflare token) at
import time; neither is used here, so both are stubbed.
"""
import csv
import importlib.util
import os
import sys
import tempfile
import types
from pathlib import Path

sys.modules['cloudscraper'] = types.SimpleNamespace(
    create_scraper=lambda: types.SimpleNamespace(headers={}))
sys.modules['config'] = types.SimpleNamespace(CKAN_BASE_URL='', SESSION_HEADERS={})

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location('plugin_map', os.path.join(HERE, 'plugin_map.py'))
pm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pm)

KNOWN = {
    'ckanext-charts': {'repo': 'DataShades/ckanext-charts'},
    'ckanext-foo': {'repo': 'x/ckanext-foo'},                  # declares nothing readable
    'ckanext-bar': {'repo': 'x/ckanext-bar'},                  # declares a plugin named bar
    'ckanext-sitemap': {'repo': 'kata-csc/ckanext-sitemap'},   # a copy of datopian's
    'ckanext-sitemap-datopian': {'repo': 'datopian/ckanext-sitemap'},
}
DECLARED_ROWS = [
    ('DataShades/ckanext-charts', 'charts_view'),
    ('DataShades/ckanext-charts', 'charts_builder_view'),
    ('x/ckanext-bar', 'bar'),
    ('kata-csc/ckanext-sitemap', 'sitemap'),
    ('datopian/ckanext-sitemap', 'sitemap'),
]

with tempfile.TemporaryDirectory() as tmp:
    ep_csv = Path(tmp) / 'entry_points.csv'
    with ep_csv.open('w', newline='', encoding='utf-8') as fh:
        w = csv.writer(fh)
        w.writerow(['repository_name', 'plugin'])
        w.writerows(DECLARED_ROWS)
    # More stars, so republished copies tie-break to datopian's.
    stars = {'datopian/ckanext-sitemap': 50, 'kata-csc/ckanext-sitemap': 5}
    EPS = pm.entry_point_packages(ep_csv, KNOWN, stars)
    DECL = pm.declared_plugins(ep_csv, KNOWN)

    assert EPS['sitemap'] == 'ckanext-sitemap-datopian'        # the tie-break, unchanged
    assert DECL['ckanext-charts'] == {'charts_view', 'charts_builder_view'}
    assert DECL['ckanext-sitemap'] == {'sitemap'}              # raw: both copies declare it

    # ckanext-charts declares two entry points; a plugin merely called `charts` isn't it.
    assert pm.resolve('charts_view', {}, KNOWN, EPS, DECL) == 'ckanext-charts'
    assert pm.resolve('charts_builder_view', {}, KNOWN, EPS, DECL) == 'ckanext-charts'
    assert pm.resolve('charts', {}, KNOWN, EPS, DECL) is None
    # Nothing declared: the name match stands. Own-named declared plugin: stands.
    assert pm.resolve('foo', {}, KNOWN, EPS, DECL) == 'ckanext-foo'
    assert pm.resolve('bar', {}, KNOWN, EPS, DECL) == 'ckanext-bar'
    # Republished copies both declare `sitemap`; the exact name still picks its own copy
    # rather than losing it to the stars tie-break.
    assert pm.resolve('sitemap', {}, KNOWN, EPS, DECL) == 'ckanext-sitemap'
    # Callers that pass no declarations (the catalogue's instances_count) are unchanged.
    assert pm.resolve('charts', {}, KNOWN) == 'ckanext-charts'
    assert pm.resolve('charts', {}, KNOWN, {}, {}) == 'ckanext-charts'

    path = Path(tmp) / 'ckan_instances_clean.csv'
    with path.open('w', newline='', encoding='utf-8') as fh:
        w = csv.writer(fh)
        w.writerow(['name', 'extensions'])
        w.writerow(['a', 'charts_view|dcat'])
        w.writerow(['b', 'charts_builder_view|charts_view'])   # two entry points, one portal
        w.writerow(['c', 'charts'])                            # not ckanext-charts
        w.writerow(['d', 'foo|sitemap'])
    plugins = {}
    seen = pm.observed_instances(path, {}, KNOWN, EPS, plugins_out=plugins, declared=DECL)
    assert seen == {'ckanext-charts': {'a', 'b'}, 'ckanext-foo': {'d'},
                    'ckanext-sitemap': {'d'}}, seen
    assert plugins == {'ckanext-charts': {'charts_view', 'charts_builder_view'},
                       'ckanext-foo': {'foo'}, 'ckanext-sitemap': {'sitemap'}}, plugins
    # Without declarations the old name match still counts the portal that runs `charts`.
    assert pm.observed_instances(path, {}, KNOWN)['ckanext-charts'] == {'c'}

print('ok')
