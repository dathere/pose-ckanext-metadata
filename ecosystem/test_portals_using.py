import importlib.util
import os

spec = importlib.util.spec_from_file_location(
    'builder', os.path.join(os.path.dirname(__file__), 'build_dashboard_json.py'))
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)

instances = [{'n': 'big'}, {'n': 'mid'}, {'n': 'gone-from-latest'}, {'n': 'small'}]
latest = [
    {'name': 'small', 'extensions': 'charts_view|dcat'},
    {'name': 'big', 'extensions': 'dcat|dcat'},       # listed twice, one portal
    {'name': 'mid', 'extensions': ''},                # reported nothing
]

assert builder.portals_using(latest, instances) == {'charts_view': [3], 'dcat': [0, 3]}
assert builder.portals_using([], instances) == {}
print('ok')

# Package -> portal indexes, against the same instances list.
assert builder.package_portals({'ckanext-x': {'small', 'big'}, 'ckanext-y': {'nobody'}}, {'big': 0, 'small': 3}) \
    == {'ckanext-x': [0, 3], 'ckanext-y': []}
assert builder.package_portals({}, {}) == {}
print('ok')
