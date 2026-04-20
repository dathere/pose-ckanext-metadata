#!/usr/bin/env python3
"""
Merge new ckan_stats.csv into the CKAN resource, then replace the file:
  1. Download the existing resource CSV from CKAN
  2. Append new rows (dedup on name + tstamp)
  3. Delete the old resource
  4. Upload the merged CSV as a fresh resource
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
DATASET_ID = 'ckan-time-series-dataset-experimental'
RESOURCE_ID = '6d217f9e-8efa-48cf-86f3-8b4fdbc7083d'
RESOURCE_NAME = 'ckan-extensions-dynamic-metadata.csv'
NEW_STATS_FILE = 'ckan_stats.csv'
UNIQUE_COLUMNS = ['name', 'tstamp']

scraper = cloudscraper.create_scraper()
scraper.headers.update(SESSION_HEADERS)
AUTH = {'Authorization': API_KEY}


# ── Helpers ────────────────────────────────────────────────────────────────────

def find_resource():
    """Fetch resource metadata directly by UUID. Returns None if not found."""
    url = f"{CKAN_URL}/api/3/action/resource_show"
    try:
        resp = scraper.get(url, params={'id': RESOURCE_ID}, headers=AUTH, timeout=30)
        if resp.status_code == 404:
            logger.info(f"Resource {RESOURCE_ID} not found (404) — will upload fresh")
            return None
        resp.raise_for_status()
        result = resp.json()
        if not result.get('success'):
            logger.error(f"resource_show failed: {result.get('error')}")
            return None
        resource = result['result']
        logger.info(f"Found resource '{resource.get('name')}' — id: {resource['id']}")
        return resource
    except Exception as e:
        logger.error(f"Error looking up resource {RESOURCE_ID}: {e}")
        return None


def download_existing_csv(resource: dict) -> pd.DataFrame:
    """Download the CSV file attached to the resource."""
    download_url = resource.get('url', '')
    if not download_url:
        logger.warning("Resource has no URL — starting fresh")
        return pd.DataFrame()

    logger.info(f"Downloading existing CSV from: {download_url}")
    try:
        resp = scraper.get(download_url, headers=AUTH, timeout=60)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        logger.info(f"  Downloaded {len(df)} existing rows")
        return df
    except Exception as e:
        logger.error(f"Failed to download existing CSV: {e}")
        return pd.DataFrame()


def merge_data(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """Append new rows to existing, deduplicate on UNIQUE_COLUMNS."""
    if existing.empty:
        logger.info(f"No existing data — using all {len(new)} new rows")
        return new.copy()

    combined = pd.concat([existing, new], ignore_index=True)
    before = len(combined)
    combined.drop_duplicates(subset=UNIQUE_COLUMNS, keep='last', inplace=True)
    combined.reset_index(drop=True, inplace=True)
    dupes = before - len(combined)
    logger.info(
        f"Merged: {len(existing)} existing + {len(new)} new "
        f"= {before} rows, {dupes} duplicates removed → {len(combined)} total"
    )
    return combined


def delete_resource(resource_id: str) -> bool:
    """Delete the CKAN resource."""
    url = f"{CKAN_URL}/api/3/action/resource_delete"
    try:
        resp = scraper.post(url, json={'id': resource_id}, headers=AUTH, timeout=30)
        resp.raise_for_status()
        if resp.json().get('success'):
            logger.info(f"✓ Deleted resource {resource_id}")
            return True
        logger.error(f"resource_delete failed: {resp.json().get('error')}")
        return False
    except Exception as e:
        logger.error(f"Error deleting resource: {e}")
        return False


def upload_resource(merged: pd.DataFrame) -> str | None:
    """Upload merged CSV as a new resource, return new resource id."""
    timestamp = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')
    csv_bytes = merged.to_csv(index=False).encode('utf-8')
    filename = 'ckan-sites-timeseries.csv'

    url = f"{CKAN_URL}/api/3/action/resource_create"
    data = {
        'package_id': DATASET_ID,
        'name': RESOURCE_NAME,
        'format': 'CSV',
        'description': (
            f'Time series stats for CKAN sites (experimental, 10-site test run). '
            f'Last updated: {timestamp}'
        ),
    }
    files = {'upload': (filename, csv_bytes, 'text/csv')}

    try:
        resp = scraper.post(url, data=data, files=files, headers=AUTH, timeout=60)
        resp.raise_for_status()
        result = resp.json()
        if result.get('success'):
            new_id = result['result']['id']
            logger.info(f"✓ Uploaded merged CSV — new resource id: {new_id}")
            logger.info(f"  Rows: {len(merged)}")
            return new_id
        logger.error(f"resource_create failed: {result.get('error')}")
        return None
    except Exception as e:
        logger.error(f"Error uploading resource: {e}")
        return None


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=== CKAN RESOURCE UPDATER (experimental) ===")
    print(f"Dataset  : {DATASET_ID}")
    print(f"Resource : {RESOURCE_ID}")
    print(f"New data : {NEW_STATS_FILE}\n")

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

    # 2. Find existing resource
    resource = find_resource()

    # 3. Download existing CSV (if resource exists)
    existing_df = download_existing_csv(resource) if resource else pd.DataFrame()

    # 4. Merge
    merged_df = merge_data(existing_df, new_df)

    # 5. Delete old resource (if it exists)
    if resource:
        if not delete_resource(resource['id']):
            logger.error("Failed to delete old resource — aborting to avoid data loss")
            sys.exit(1)

    # 6. Upload merged CSV
    new_id = upload_resource(merged_df)
    if not new_id:
        logger.error("Upload failed")
        sys.exit(1)

    print(f"\n✓ Done. Dataset: {CKAN_URL}/dataset/{DATASET_ID}")
    print(f"  New resource id : {new_id}")
    print(f"  Total rows      : {len(merged_df)}")


if __name__ == '__main__':
    main()
