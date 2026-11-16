"""Self-check for datapump's datastore schema builder. Run: python test_datapump.py"""
import pandas as pd
import datapump
from datapump import build_fields, push_to_datastore

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

# push_to_datastore: create first batch with schema, upsert the rest, stop on failure.
class Resp:
    def __init__(self, ok): self.ok, self.status_code, self.text = ok, 200 if ok else 502, ''
    def json(self): return {'success': True}

calls = []
fail_at = None
def fake_post(url, json, headers, timeout):
    calls.append((url.rsplit('/', 1)[1], json))
    return Resp(len(calls) != fail_at)
datapump.scraper.post = fake_post
datapump.PUSH_BATCH = 2

big = pd.DataFrame({'a': [1, 2, 3, 4, 5]})
assert push_to_datastore('r1', big, [{'id': 'a', 'type': 'int'}])
assert [c[0] for c in calls] == ['datastore_create', 'datastore_upsert', 'datastore_upsert'], calls
assert 'fields' in calls[0][1] and 'fields' not in calls[1][1]
assert calls[1][1]['method'] == 'insert'
assert sum(len(c[1]['records']) for c in calls) == 5
assert [c[1]['calculate_record_count'] for c in calls] == [False, False, True]

calls.clear(); fail_at = 2
assert not push_to_datastore('r1', big, [])
assert len(calls) == 2  # stops after the failed batch

calls.clear(); fail_at = None
assert push_to_datastore('r1', big.iloc[:0], [])  # empty df still creates the table
assert [c[0] for c in calls] == ['datastore_create']
print("ok")
