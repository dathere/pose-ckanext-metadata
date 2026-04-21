#!/usr/bin/env python3
"""
CKAN Sites Datastore Updater
Replaces the existing resource in-place:
  1. Find the existing resource by name within the dataset
  2. Download existing datastore records
  3. Append new rows
  4. Delete old resource views, then delete the old resource
  5. Create a new resource (datastore-type, no file upload)
  6. Push merged data to datastore via datastore_create
  7. Create a datatables_view for the new resource
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
    filename='datapump.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CKAN_URL = CKAN_BASE_URL
API_KEY = os.getenv('CKAN_API_KEY', 'CKAN_API_KEY')
DATASET_ID = 'ckan-sites-metadata'
RESOURCE_NAME = 'CKAN Sites Dynamic Metadata'
CSV_FILE_PATH = 'ckan_stats.csv'

scraper = cloudscraper.create_scraper()
scraper.headers.update(SESSION_HEADERS)
AUTH = {'Authorization': API_KEY}


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
            if resource.get('name', '').startswith(RESOURCE_NAME):
                print(f"✓ Found resource: {resource['name']}")
                print(f"  Resource ID: {resource['id']}")
                logger.info(f"Found resource '{resource['name']}' — id: {resource['id']}")
                return resource
        print(f"✗ No resource found matching '{RESOURCE_NAME}'")
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

    print(f"  Downloading existing data to check for duplicates...")
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
        print(f"  Downloaded {len(all_records)} existing records")
        logger.info(f"Downloaded {len(all_records)} existing records")
    except Exception as e:
        logger.error(f"Error downloading datastore records: {e}")

    return pd.DataFrame(all_records) if all_records else pd.DataFrame()


def get_resource_views(resource_id: str) -> list:
    url = f"{CKAN_URL}/api/3/action/resource_view_list"
    try:
        resp = scraper.get(url, params={'id': resource_id}, headers=AUTH, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        if result.get('success'):
            return result['result']
        return []
    except Exception as e:
        logger.error(f"Error listing resource views: {e}")
        return []


def delete_resource_views(resource_id: str) -> None:
    views = get_resource_views(resource_id)
    if not views:
        return
    url = f"{CKAN_URL}/api/3/action/resource_view_delete"
    for view in views:
        try:
            resp = scraper.post(url, json={'id': view['id']}, headers=AUTH, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Error deleting view {view['id']}: {e}")


def delete_resource(resource_id: str) -> bool:
    url = f"{CKAN_URL}/api/3/action/resource_delete"
    try:
        resp = scraper.post(url, json={'id': resource_id}, headers=AUTH, timeout=30)
        resp.raise_for_status()
        if resp.json().get('success'):
            logger.info(f"Deleted resource {resource_id}")
            return True
        logger.error(f"resource_delete failed: {resp.json().get('error')}")
        return False
    except Exception as e:
        logger.error(f"Error deleting resource: {e}")
        return False


def create_resource() -> str | None:
    """Create a new datastore-type resource (no file upload). Returns new resource id."""
    timestamp = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')
    url = f"{CKAN_URL}/api/3/action/resource_create"
    data = {
        'package_id': DATASET_ID,
        'name': RESOURCE_NAME,
        'format': 'CSV',
        'url_type': 'datastore',
        'description': f'Dynamic metadata for CKAN sites - time series data. Last updated: {timestamp}',
    }
    try:
        resp = scraper.post(url, json=data, headers=AUTH, timeout=30)
        if not resp.ok:
            logger.error(f"resource_create HTTP {resp.status_code}: {resp.text[:500]}")
            resp.raise_for_status()
        result = resp.json()
        if result.get('success'):
            new_id = result['result']['id']
            print(f"✓ Created new resource — id: {new_id}")
            logger.info(f"Created new resource {new_id}")
            return new_id
        logger.error(f"resource_create failed: {result.get('error')}")
        return None
    except Exception as e:
        logger.error(f"Error creating resource: {e}")
        return None


def push_to_datastore(resource_id: str, df: pd.DataFrame) -> bool:
    """Push full dataframe to CKAN datastore via datastore_create."""
    records = df.to_dict(orient='records')
    cleaned = []
    for r in records:
        cleaned_r = {}
        for k, v in r.items():
            if pd.notna(v) and v is not None:
                cleaned_r[k] = v.item() if hasattr(v, 'item') else v
        cleaned.append(cleaned_r)

    print(f"\nInserting {len(cleaned)} records...")
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
            print(f"✓ Successfully inserted {len(cleaned)} records")
            logger.info(f"Inserted {len(cleaned)} records to resource {resource_id}")
            return True
        logger.error(f"datastore_create failed: {result.get('error')}")
        return False
    except Exception as e:
        logger.error(f"Error pushing to datastore: {e}")
        return False


def create_resource_view(resource_id: str) -> bool:
    url = f"{CKAN_URL}/api/3/action/resource_view_create"
    data = {
        'resource_id': resource_id,
        'title': 'Data Table',
        'view_type': 'datatables_view',
    }
    try:
        resp = scraper.post(url, json=data, headers=AUTH, timeout=30)
        if resp.status_code == 409:
            logger.info("Resource view already exists (auto-created by CKAN) — skipping")
            return True
        resp.raise_for_status()
        result = resp.json()
        if result.get('success'):
            print("✓ Resource metadata and download URL updated")
            logger.info(f"Created datatables_view for resource {resource_id}")
            return True
        logger.warning(f"resource_view_create failed: {result.get('error')}")
        return False
    except Exception as e:
        logger.error(f"Error creating resource view: {e}")
        return False


def main():
    print("=== CKAN SITES DATASTORE APPENDER ===")
    print(f"Dataset: {DATASET_ID}")
    print(f"CSV file: {CSV_FILE_PATH}")

    if not os.path.exists(CSV_FILE_PATH):
        print(f"✗ CSV file '{CSV_FILE_PATH}' not found!")
        return False

    print("Reading CSV file...")
    try:
        new_df = pd.read_csv(CSV_FILE_PATH)
        print(f"  Rows: {len(new_df)}")
        print(f"  Columns: {', '.join(new_df.columns)}")
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")
        logger.error(f"Cannot read {CSV_FILE_PATH}: {e}")
        return False

    resource = find_resource()

    existing_df = download_existing_datastore(resource['id']) if resource else pd.DataFrame()

    if existing_df.empty:
        merged_df = new_df.copy()
        print(f"  No existing data, all {len(merged_df)} records are new")
    else:
        merged_df = pd.concat([existing_df, new_df], ignore_index=True)
        print(f"  Appended: {len(existing_df)} existing + {len(new_df)} new = {len(merged_df)} total rows")

    if resource:
        delete_resource_views(resource['id'])
        if not delete_resource(resource['id']):
            print("✗ Failed to delete old resource — aborting to avoid data loss")
            logger.error("Failed to delete old resource")
            return False
        time.sleep(2)

    new_id = create_resource()
    if not new_id:
        print("✗ Resource creation failed")
        return False

    success = push_to_datastore(new_id, merged_df)
    if not success:
        print("✗ Failed to push data to datastore")
        return False

    create_resource_view(new_id)

    print(f"\n✓ Data successfully appended!")
    print(f"View dataset: {CKAN_URL}/dataset/{DATASET_ID}")
    return True


if __name__ == '__main__':
    if len(sys.argv) > 1:
        CSV_FILE_PATH = sys.argv[1]
        print(f"Using CSV file: {CSV_FILE_PATH}")

    success = main()

    if success:
        print("\n=== APPEND COMPLETE ===")
        sys.exit(0)
    else:
        print("\n=== APPEND FAILED ===")
        sys.exit(1)
