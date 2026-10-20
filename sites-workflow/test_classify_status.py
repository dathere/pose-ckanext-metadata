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
FRESH, OLD = '2026-04-22', '2026-04-21'                               # 180 / 181 days

assert classify('active', True, FRESH, TODAY) is None                 # unchanged
assert classify('active', True, OLD, TODAY) == 'dormant'
assert classify('dormant', True, OLD, TODAY) is None
assert classify('dormant', True, FRESH, TODAY) == 'active'            # revived
assert classify('Production', True, FRESH, TODAY) == 'active'         # normalised
assert classify('production', True, FRESH, TODAY) == 'active'
assert classify(None, True, FRESH, TODAY) == 'active'                 # blank
assert classify('production', True, '', TODAY) == 'active'            # reachable, no public datasets
assert classify('production', True, 'garbage', TODAY) == 'active'
assert classify('active', False, '', TODAY) == 'unreachable'
assert classify('unreachable', False, '', TODAY) is None
assert classify('unreachable', True, FRESH, TODAY) == 'active'
assert classify('staging', True, OLD, TODAY) is None                  # manual, untouched
assert classify('Development', False, '', TODAY) is None
assert classify('active', None, OLD, TODAY) is None                   # reachability unknown
print('ok')
