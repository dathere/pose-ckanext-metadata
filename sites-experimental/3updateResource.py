#!/usr/bin/env python3
"""
Merge new ckan_stats.csv into the CKAN resource, then replace the file:
  1. Find the existing resource by name within the dataset
  2. Download the existing resource CSV from CKAN
  3. Append new rows (dedup on name + tstamp)
  4. Delete old resource views, then delete the old resource
  5. Upload the merged CSV as a fresh resource
  6. Create a new resource view for the new resource
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import io
import time
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
RESOURCE_NAME = 'CKAN Sites Dynamic Metadata'
NEW_STATS_FILE = 'ckan_stats.csv'
UNIQUE_COLUMNS = ['name', 'tstamp']

scraper = cloudscraper.create_scraper()
scraper.headers.update(SESSION_HEADERS)
AUTH = {'Authorization': API_KEY}


# ── Helpers ────────────────────────────────────────────────────────────────────

def find_resource() -> dict | None:
    """Find resource by name within the dataset. Returns resource dict or None."""
    url = f"{CKAN_URL}/api/3/action/package_show"
    try:
        resp = scraper.get(url, params={'id': DATASET_ID}, headers=AUTH, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        if not result.get('success'):
            logger.error(f"package_show failed: {result.get('error')}")
            return None
        for resource in result['result'].get('resources', []):
            if resource.get('name') == RESOURCE_NAME:
                logger.info(f"Found resource '{resource['name']}' — id: {resource['id']}")
                return resource
        logger.info(f"No resource named '{RESOURCE_NAME}' in dataset — will upload fresh")
        return None
    except Exception as e:
        logger.error(f"Error looking up resource in dataset: {e}")
        return None


def download_existing_csv(resource: dict) -> pd.DataFrame:
    """Download the CSV file attached to the resource."""
    download_url = resource.get('url', '')
    if not download_url:
        logger.warning("Resource has no URL — starting fresh")
        return pd.DataFrame()

    logger.info(f"Downloading existing CSV from: {download_url}")
    try:
        resp = scraper.get(download_url, headers=AUTH, timeout=60, allow_redirects=True)
        logger.info(f"  HTTP {resp.status_code} | Content-Type: {resp.headers.get('Content-Type', 'unknown')}")
        resp.raise_for_status()

        content_type = resp.headers.get('Content-Type', '')
        if 'text/html' in content_type:
            logger.error("  Got HTML instead of CSV — download URL may require login")
            logger.error(f"  Response preview: {resp.text[:300]}")
            return pd.DataFrame()

        df = pd.read_csv(io.StringIO(resp.text))
        logger.info(f"  Downloaded {len(df)} existing rows, columns: {list(df.columns)}")
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


def get_resource_views(resource_id: str) -> list:
    """Return list of view dicts for a resource."""
    url = f"{CKAN_URL}/api/3/action/resource_view_list"
    try:
        resp = scraper.get(url, params={'id': resource_id}, headers=AUTH, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        if result.get('success'):
            views = result['result']
            logger.info(f"  Found {len(views)} resource view(s) for {resource_id}")
            return views
        logger.warning(f"resource_view_list failed: {result.get('error')}")
        return []
    except Exception as e:
        logger.error(f"Error listing resource views: {e}")
        return []


def delete_resource_views(resource_id: str) -> None:
    """Delete all views attached to a resource."""
    views = get_resource_views(resource_id)
    if not views:
        return

    url = f"{CKAN_URL}/api/3/action/resource_view_delete"
    for view in views:
        view_id = view['id']
        try:
            resp = scraper.post(url, json={'id': view_id}, headers=AUTH, timeout=30)
            resp.raise_for_status()
            if resp.json().get('success'):
                logger.info(f"  ✓ Deleted resource view {view_id} ('{view.get('title', '')}')")
            else:
                logger.warning(f"  resource_view_delete failed for {view_id}: {resp.json().get('error')}")
        except Exception as e:
            logger.error(f"  Error deleting resource view {view_id}: {e}")


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
    files = {'upload': ('ckan-sites-timeseries.csv', csv_bytes, 'text/csv')}

    try:
        resp = scraper.post(url, data=data, files=files, headers=AUTH, timeout=60)
        if not resp.ok:
            logger.error(f"Upload HTTP {resp.status_code}: {resp.text[:500]}")
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


def create_resource_view(resource_id: str) -> bool:
    """Create a DataExplorer view for the new resource."""
    url = f"{CKAN_URL}/api/3/action/resource_view_create"
    data = {
        'resource_id': resource_id,
        'title': 'Data Explorer',
        'view_type': 'recline_view',
    }
    try:
        resp = scraper.post(url, json=data, headers=AUTH, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        if result.get('success'):
            view_id = result['result']['id']
            logger.info(f"✓ Created resource view {view_id} for resource {resource_id}")
            return True
        logger.warning(f"resource_view_create failed: {result.get('error')}")
        return False
    except Exception as e:
        logger.error(f"Error creating resource view: {e}")
        return False


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=== CKAN RESOURCE UPDATER (experimental) ===")
    print(f"Dataset  : {DATASET_ID}")
    print(f"Resource : {RESOURCE_NAME}")
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

    # 2. Find existing resource by name
    resource = find_resource()

    # 3. Download existing CSV (if resource exists)
    existing_df = download_existing_csv(resource) if resource else pd.DataFrame()

    # 4. Merge
    merged_df = merge_data(existing_df, new_df)

    # 5. Delete old resource views + old resource (if it exists)
    if resource:
        logger.info("Deleting old resource views...")
        delete_resource_views(resource['id'])

        if not delete_resource(resource['id']):
            logger.error("Failed to delete old resource — aborting to avoid data loss")
            sys.exit(1)

        # Brief pause to allow CKAN to fully process the deletion
        time.sleep(2)

    # 6. Upload merged CSV
    logger.info("Uploading merged CSV as new resource...")
    new_id = upload_resource(merged_df)
    if not new_id:
        logger.error("Upload failed")
        sys.exit(1)

    # 7. Create resource view for new resource
    logger.info("Creating resource view...")
    create_resource_view(new_id)

    print(f"\n✓ Done. Dataset: {CKAN_URL}/dataset/{DATASET_ID}")
    print(f"  New resource id : {new_id}")
    print(f"  Total rows      : {len(merged_df)}")


if __name__ == '__main__':
    main()
