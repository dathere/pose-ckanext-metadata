#!/usr/bin/env python3
"""Upload the derived sites CSVs to `ckan-sites-metadata` as resources.

The publishing itself lives in ecosystem/ckan_upload.py, shared with the
extensions pipeline. This file is just the manifest: which files ship, in what
order, described how. Run after derive_csvs.sh.

Usage:
    CKAN_API_KEY=... python upload_derived.py [derived-dir] [--dry-run]
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ecosystem import ckan_upload

DATASET_ID = 'ckan-sites-metadata'

DESCRIPTIONS = {
    'ckan_instances_clean.csv':
        'One row per CKAN instance from the latest crawl: dataset/group/organization '
        'counts, version, release branch, support status and installed plugins.',
    'ckan_extension_ranking.csv':
        'Plugin adoption in the latest crawl: install count, share of instances that '
        'reported a plugin list, whether the plugin ships with CKAN, and its repo.',
    'ckan_weekly_long.csv':
        'Every archived crawl stacked, one row per week per instance — the full '
        'time series in analysis-ready form.',
    'ckan_version_changes.csv':
        'CKAN version changes per instance, classified as upgrade, downgrade or '
        'change, with a flag for crossing a release branch.',
    'ckan_extension_changes.csv':
        'Plugins added or removed by an instance from one crawl to the next.',
    'ckan_extension_trends.csv':
        'Per plugin: latest install count and share, plus the change over the cohort '
        'window (instances that reported in every week of it).',
    'ckan_extension_series.csv':
        'Install count per plugin per crawl date — one column per crawl.',
    'ckan_extension_cohort_series.csv':
        'Install count per plugin per crawl date, restricted to the cohort, so a '
        'change reflects a real install rather than a portal answering again.',
    'ckan_crawl_quality.csv':
        'Per crawl week: how many instances were seen, how many reported a plugin '
        'list, and whether that week was good enough to include in the adoption '
        'series. Read this before reading a dip as a real change.',
}


if __name__ == '__main__':
    sys.exit(ckan_upload.main(DATASET_ID, DESCRIPTIONS))
