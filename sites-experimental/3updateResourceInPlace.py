#!/usr/bin/env python3
"""
Merge new ckan_stats.csv into the existing CKAN resource file in-place:
  1. Download the current resource CSV from CKAN
  2. Append new rows (dedup on name + tstamp)
  3. Upload the merged CSV back via resource_update (preserves resource ID/UUID)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import io
import logging
import cloudscraper
import pandas as pd
from datetime import datetime, UTC
from config import CKAN_BASE_URL, SESSION_HEADERS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

CKAN_URL = CKAN_BASE_URL
API_KEY = os.getenv('CKAN_API_KEY', '')
RESOURCE_ID = 'efa7d10b-4259-4dc7-bf40-36a9172b268e'
NEW_STATS_FILE = 'ckan_stats.csv'

scraper = cloudscraper.create_scraper()
scraper.headers.update(SESSION_HEADERS)
AUTH = {'Authorization': API_KEY}


# ── Helpers ────────────────────────────────────────────────────────────────────

def get_resource_info() -> dict | None:
    """Fetch current resource metadata by UUID."""
    url = f"{CKAN_URL}/api/3/action/resource_show"
    try:
        resp = scraper.get(url, params={'id': RESOURCE_ID}, headers=AUTH, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        if not result.get('success'):
            logger.error(f"resource_show failed: {result.get('error')}")
            return None
        resource = result['result']
        logger.info(f"Found resource '{resource.get('name')}' — id: {resource['id']}")
        return resource
    except Exception as e:
        logger.error(f"Error fetching resource {RESOURCE_ID}: {e}")
        return None


def download_existing_csv(resource: dict) -> pd.DataFrame:
    """Download the CSV currently attached to the resource."""
    # CKAN sets the url field to the full download URL after a file upload
    download_url = resource.get('url', '')

    if not download_url:
        logger.warning("Resource has no URL — starting with empty dataset")
        return pd.DataFrame()

    logger.info(f"Downloading existing CSV from: {download_url}")
    try:
        resp = scraper.get(download_url, headers=AUTH, timeout=60, allow_redirects=True)
        logger.info(f"  HTTP {resp.status_code} | Content-Type: {resp.headers.get('Content-Type', 'unknown')}")
        resp.raise_for_status()

        content_type = resp.headers.get('Content-Type', '')
        if 'text/html' in content_type:
            logger.error(f"  Got HTML instead of CSV — download URL may be blocked or requires login")
            logger.error(f"  Response preview: {resp.text[:300]}")
            return pd.DataFrame()

        df = pd.read_csv(io.StringIO(resp.text))
        logger.info(f"  Downloaded {len(df)} existing rows, columns: {list(df.columns)}")
        return df
    except Exception as e:
        logger.error(f"Failed to download existing CSV: {e}")
        return pd.DataFrame()


def merge_data(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """Append new rows to existing — no deduplication, every run is a new snapshot."""
    if existing.empty:
        logger.info(f"No existing data — using all {len(new)} new rows")
        return new.copy()

    combined = pd.concat([existing, new], ignore_index=True)
    logger.info(f"Appended: {len(existing)} existing + {len(new)} new = {len(combined)} total rows")
    return combined


def update_resource_in_place(merged: pd.DataFrame) -> bool:
    """Replace the resource file via resource_update, keeping the same UUID."""
    timestamp = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')
    csv_bytes = merged.to_csv(index=False).encode('utf-8')

    url = f"{CKAN_URL}/api/3/action/resource_update"
    data = {
        'id': RESOURCE_ID,
        'description': (
            f'Time series stats for CKAN sites (experimental, 10-site test run). '
            f'Last updated: {timestamp}'
        ),
    }
    files = {'upload': ('ckan-sites-timeseries.csv', csv_bytes, 'text/csv')}

    try:
        resp = scraper.post(url, data=data, files=files, headers=AUTH, timeout=60)
        resp.raise_for_status()
        result = resp.json()
        if result.get('success'):
            confirmed_id = result['result']['id']
            logger.info(f"✓ Resource updated in-place — id unchanged: {confirmed_id}")
            logger.info(f"  Rows: {len(merged)}")
            return True
        logger.error(f"resource_update failed: {result.get('error')}")
        return False
    except Exception as e:
        logger.error(f"Error updating resource: {e}")
        return False


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=== CKAN RESOURCE IN-PLACE UPDATER (experimental) ===")
    print(f"Resource ID : {RESOURCE_ID}")
    print(f"New data    : {NEW_STATS_FILE}\n")

    if not API_KEY:
        logger.error("CKAN_API_KEY is not set")
        sys.exit(1)

    # 1. Load new stats
    try:
        new_df = pd.read_csv(NEW_STATS_FILE)
        logger.info(f"Read {len(new_df)} rows from {NEW_STATS_FILE}")
    except Exception as e:
        logger.error(f"Cannot read {NEW_STATS_FILE}: {e}")
        sys.exit(1)

    for col in UNIQUE_COLUMNS:
        if col not in new_df.columns:
            logger.error(f"Required column '{col}' missing from {NEW_STATS_FILE}")
            sys.exit(1)

    # 2. Fetch current resource metadata
    resource = get_resource_info()
    if not resource:
        logger.error("Cannot proceed without a valid resource — check RESOURCE_ID")
        sys.exit(1)

    # 3. Download existing CSV
    existing_df = download_existing_csv(resource)

    # 4. Merge
    merged_df = merge_data(existing_df, new_df)

    # 5. Upload merged CSV back in-place
    if not update_resource_in_place(merged_df):
        logger.error("In-place update failed")
        sys.exit(1)

    print(f"\n✓ Done. Resource ID is unchanged: {RESOURCE_ID}")
    print(f"  Total rows : {len(merged_df)}")
    print(f"  View at    : {CKAN_URL}/dataset/ckan-time-series-dataset-experimental/resource/{RESOURCE_ID}")


if __name__ == '__main__':
    main()
