import importlib.util
import os
from datetime import date

os.environ.setdefault('CF_BYPASS_TOKEN', 'test')
spec = importlib.util.spec_from_file_location(
    'updater', os.path.join(os.path.dirname(__file__), '3updateSitesCatalog.py'))
updater = importlib.util.module_from_spec(spec)
spec.loader.exec_module(updater)
classify = updater.classify_status

TODAY = date(2026, 10, 19)

assert classify('production', '2026-04-22', TODAY) is None          # 180 days: still active
assert classify('production', '2026-04-21', TODAY) == 'dormant'     # 181 days
assert classify(None, '2025-01-01', TODAY) == 'dormant'
assert classify('dormant', '2025-01-01', TODAY) is None             # already dormant
assert classify('dormant', '2026-10-01', TODAY) == 'production'     # revived
assert classify('staging', '2026-10-01', TODAY) is None             # active, untouched
assert classify('production', '', TODAY) is None                    # no date
assert classify('production', 'garbage', TODAY) is None
print('ok')
