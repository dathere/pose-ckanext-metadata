#!/usr/bin/env python3
"""Upload the derived extensions CSVs to `ckan-extensions-metadata` as resources.

The counterpart to sites-workflow/upload_derived.py; both call
ecosystem/ckan_upload.py. Run after derive_ext_csvs.sh.

Usage:
    CKAN_API_KEY=... python upload_ext_derived.py [derived-dir] [--dry-run]
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ecosystem import ckan_upload

DATASET_ID = 'ckan-extensions-metadata'

DESCRIPTIONS = {
    'ckan_ext_repos_clean.csv':
        'One row per catalogued extension from the latest crawl: stars, forks, open '
        'issues, contributors, release count and how long since the last release, '
        'with a maintenance bucket derived from that age.',
    'ckan_ext_weekly_long.csv':
        'Every archived extensions crawl stacked, one row per week per repository — '
        'the full GitHub time series in analysis-ready form.',
    'ckan_ext_growth.csv':
        'Change in stars, forks, contributors and releases over the cohort window, '
        'restricted to repositories seen in every week of it so a missed crawl '
        'cannot read as a collapse.',
    'ckan_ext_release_cadence.csv':
        'Release activity per repository: total releases, how many appeared while we '
        'were watching, over how many crawls, and how stale the newest one is.',
    'ckan_ext_maintenance.csv':
        'How many extensions fall in each maintenance bucket per crawl week — the '
        'shape of the ecosystem, not of any one project.',
    'ckan_ext_crawl_quality.csv':
        'Per crawl week: how many repositories the crawl returned against the median '
        'crawl, and whether that week was complete enough to include.',
    'ckan_ecosystem_health.csv':
        'The two pipelines joined: how many catalogued CKAN instances were observed '
        'running each extension, next to its GitHub health, with a risk flag for '
        'extensions that are widely installed and no longer maintained upstream.',
}


if __name__ == '__main__':
    sys.exit(ckan_upload.main(DATASET_ID, DESCRIPTIONS))
