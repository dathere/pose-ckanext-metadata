#!/usr/bin/env python3
"""
Merge new ckan_stats.csv into the CKAN datastore resource, then replace it:
  1. Find the existing resource by name within the dataset
  2. Download existing datastore records
  3. Append new rows (no deduplication)
  4. Delete old resource views, then delete the old resource
  5. Create a new resource (JSON, no file upload)
  6. Push merged data to datastore via datastore_create
  7. Create a new resource view
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
RESOURCE_NAME = 'ckan sites timeseries data 10'
NEW_STATS_FILE = 'ckan_stats.csv'

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
        logger.info(f"No resource named '{RESOURCE_NAME}' in dataset — will create fresh")
        return None
    except Exception as e:
        logger.error(f"Error looking up resource in dataset: {e}")
        return None


def download_existing_datastore(resource_id: str) -> pd.DataFrame:
    """Download all existing records from the datastore."""
    url = f"{CKAN_URL}/api/3/action/datastore_search"
    all_records = []
    offset = 0
    batch_size = 1000

    logger.info(f"Downloading existing datastore records from {resource_id}...")
    try:
        while True:
            params = {'resource_id': resource_id, 'limit': batch_size, 'offset': offset}
            resp = scraper.post(url, json=params, headers=AUTH, timeout=30)
            resp.raise_for_status()
            result = resp.json()
            if not result.get('success'):
                logger.warning(f"datastore_search failed: {result.get('error')}")
                break
            records = result['result']['records']
            if not records:
                break
            for r in records:
                r.pop('_id', None)
            all_records.extend(records)
            total = result['result'].get('total', 0)
            offset += batch_size
            if len(all_records) >= total:
                break
        logger.info(f"  Downloaded {len(all_records)} existing records")
    except Exception as e:
        logger.error(f"Error downloading datastore records: {e}")

    return pd.DataFrame(all_records) if all_records else pd.DataFrame()


def get_resource_views(resource_id: str) -> list:
    """Return list of view dicts for a resource."""
    url = f"{CKAN_URL}/api/3/action/resource_view_list"
    try:
        resp = scraper.get(url, params={'id': resource_id}, headers=AUTH, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        if result.get('success'):
            views = result['result']
            logger.info(f"  Found {len(views)} resource view(s)")
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
                logger.info(f"  ✓ Deleted view {view_id} ('{view.get('title', '')}')")
            else:
                logger.warning(f"  resource_view_delete failed for {view_id}: {resp.json().get('error')}")
        except Exception as e:
            logger.error(f"  Error deleting view {view_id}: {e}")


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


def create_resource() -> str | None:
    """Create a new empty resource (no file upload). Returns new resource id."""
    timestamp = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')
    url = f"{CKAN_URL}/api/3/action/resource_create"
    data = {
        'package_id': DATASET_ID,
        'name': RESOURCE_NAME,
        'format': 'CSV',
        'url_type': 'datastore',
        'description': (
            f'Time series stats for CKAN sites (experimental, 10-site test run). '
            f'Last updated: {timestamp}'
        ),
    }
    try:
        resp = scraper.post(url, json=data, headers=AUTH, timeout=30)
        if not resp.ok:
            logger.error(f"resource_create HTTP {resp.status_code}: {resp.text[:500]}")
            resp.raise_for_status()
        result = resp.json()
        if result.get('success'):
            new_id = result['result']['id']
            logger.info(f"✓ Created new resource — id: {new_id}")
            return new_id
        logger.error(f"resource_create failed: {result.get('error')}")
        return None
    except Exception as e:
        logger.error(f"Error creating resource: {e}")
        return None


def push_to_datastore(resource_id: str, df: pd.DataFrame) -> bool:
    """Push dataframe to CKAN datastore via datastore_create."""
    records = df.to_dict(orient='records')
    cleaned = []
    for r in records:
        cleaned_r = {}
        for k, v in r.items():
            if pd.notna(v) and v is not None:
                cleaned_r[k] = v.item() if hasattr(v, 'item') else v
        cleaned.append(cleaned_r)

    logger.info(f"Pushing {len(cleaned)} records to datastore...")
    url = f"{CKAN_URL}/api/3/action/datastore_create"
    data = {
        'resource_id': resource_id,
        'records': cleaned,
        'force': True,
        'calculate_record_count': True,
    }
    try:
        resp = scraper.post(url, json=data, headers=AUTH, timeout=60)
        if not resp.ok:
            logger.error(f"datastore_create HTTP {resp.status_code}: {resp.text[:500]}")
            resp.raise_for_status()
        result = resp.json()
        if result.get('success'):
            logger.info(f"✓ Pushed {len(cleaned)} records to datastore")
            return True
        logger.error(f"datastore_create failed: {result.get('error')}")
        return False
    except Exception as e:
        logger.error(f"Error pushing to datastore: {e}")
        return False


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
        if resp.status_code == 409:
            logger.info("Resource view already exists (auto-created by CKAN) — skipping")
            return True
        resp.raise_for_status()
        result = resp.json()
        if result.get('success'):
            view_id = result['result']['id']
            logger.info(f"✓ Created resource view {view_id}")
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

    # 2. Find existing resource by name
    resource = find_resource()

    # 3. Download existing datastore records
    existing_df = download_existing_datastore(resource['id']) if resource else pd.DataFrame()

    # 4. Append new rows (no deduplication)
    if existing_df.empty:
        merged_df = new_df.copy()
        logger.info(f"No existing data — using all {len(merged_df)} new rows")
    else:
        merged_df = pd.concat([existing_df, new_df], ignore_index=True)
        logger.info(f"Appended: {len(existing_df)} existing + {len(new_df)} new = {len(merged_df)} total rows")

    # 5. Delete old resource views + old resource
    if resource:
        logger.info("Deleting old resource views...")
        delete_resource_views(resource['id'])

        if not delete_resource(resource['id']):
            logger.error("Failed to delete old resource — aborting to avoid data loss")
            sys.exit(1)

        time.sleep(2)

    # 6. Create new resource (no file)
    logger.info("Creating new resource...")
    new_id = create_resource()
    if not new_id:
        logger.error("Resource creation failed")
        sys.exit(1)

    # 7. Push merged data to datastore
    if not push_to_datastore(new_id, merged_df):
        logger.error("Datastore push failed")
        sys.exit(1)

    # 8. Create resource view
    logger.info("Creating resource view...")
    create_resource_view(new_id)

    print(f"\n✓ Done. Dataset: {CKAN_URL}/dataset/{DATASET_ID}")
    print(f"  New resource id : {new_id}")
    print(f"  Total rows      : {len(merged_df)}")
    print(f"  View at         : {CKAN_URL}/dataset/{DATASET_ID}/resource/{new_id}")


if __name__ == '__main__':
    main()
