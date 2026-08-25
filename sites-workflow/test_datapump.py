"""Self-check for datapump's datastore schema builder. Run: python test_datapump.py"""
import pandas as pd
from datapump import build_fields

df = pd.DataFrame({
    'tstamp': ['2026-08-23 0:00:00'],
    'num_datasets': [5],
    'ckan_version': [None],   # all-null column: the one the old code dropped
    'new_col': [1.5],
})
existing = [
    {'id': 'tstamp', 'type': 'text'},
    {'id': 'num_datasets', 'type': 'numeric'},
    {'id': 'ckan_version', 'type': 'text'},
    {'id': 'gone_col', 'type': 'text'},
]

fields = build_fields(df, existing)
ids = [f['id'] for f in fields]
types = {f['id']: f['type'] for f in fields}

# Every column must be declared, including the all-null one that caused the 409.
assert set(ids) == set(df.columns), ids
# Old types survive (num_datasets stays numeric, not int), dropped columns disappear.
assert types['num_datasets'] == 'numeric', types
assert 'gone_col' not in types, types
# New columns are inferred from dtype.
assert types['new_col'] == 'numeric', types

# No prior resource: schema comes purely from dtypes.
assert build_fields(df, []) == [
    {'id': 'tstamp', 'type': 'text'},
    {'id': 'num_datasets', 'type': 'int'},
    {'id': 'ckan_version', 'type': 'text'},
    {'id': 'new_col', 'type': 'numeric'},
]
print("ok")
