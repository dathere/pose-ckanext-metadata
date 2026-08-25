"""Self-check for the duplicate-detection key. Run: python test_timeseries_append.py"""
import pandas as pd
from timeseries_append import composite_key, UNIQUE_COLUMNS

# CSV side: dates as written by 2refresh.py
new_df = pd.DataFrame({
    'repository_name': ['ckanext-spatial', 'ckanext-dcat'],
    'tstamp': ['2026-08-23', '2026-08-23'],
})
# Datastore side: same rows read back, plus a legacy row with a null name (_id 16975 in prod)
existing_df = pd.DataFrame({
    'repository_name': ['ckanext-spatial', None],
    'tstamp': ['2026-08-23 0:00:00', '2025-10-21 0:00:00'],
})

new_keys = composite_key(new_df, UNIQUE_COLUMNS)
existing_keys = composite_key(existing_df, UNIQUE_COLUMNS)

# A null name must not blow up '-'.join (pandas 3 keeps NaN through astype(str)).
assert list(existing_keys) == ['ckanext-spatial-2026-08-23', '-2025-10-21'], list(existing_keys)
# Same row on both sides must match despite the different timestamp format.
assert new_keys[0] in set(existing_keys), (new_keys[0], list(existing_keys))
# A genuinely new row must not match.
assert new_keys[1] not in set(existing_keys), new_keys[1]

# End to end through filter_duplicates: 1 new, 1 duplicate.
from timeseries_append import filter_duplicates
truly_new, duplicates = filter_duplicates(new_df.copy(), existing_df.copy(), UNIQUE_COLUMNS)
assert len(truly_new) == 1 and truly_new.iloc[0]['repository_name'] == 'ckanext-dcat', truly_new
assert len(duplicates) == 1, duplicates
print("ok")
