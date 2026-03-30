#!/usr/bin/env python3
"""
CKAN Sites Datastore Appender
Reads CSV and appends to existing datastore with duplicate checking.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
import cloudscraper
import pandas as pd
from datetime import datetime
from config import CKAN_BASE_URL, SESSION_HEADERS

# Set up logging
logging.basicConfig(
    filename='datapump.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CKAN_URL = CKAN_BASE_URL
API_KEY = os.getenv('CKAN_API_KEY', 'CKAN_API_KEY')
DATASET_ID = 'ckan-sites-metadata'
RESOURCE_NAME = 'CKAN Sites Dynamic Metadata'
CSV_FILE_PATH = 'ckan_stats.csv'
UNIQUE_COLUMNS = ['name', 'tstamp']  # Columns to check for duplicates

# Create a cloudscraper session for all requests
scraper = cloudscraper.create_scraper()
scraper.headers.update(SESSION_HEADERS)


def get_resource_id(dataset_id, resource_name):
    """Find resource ID by name"""

    package_show_url = f"{CKAN_URL}/api/3/action/package_show"
    headers = {'Authorization': API_KEY}

    try:
        response = scraper.get(
            package_show_url,
            params={'id': dataset_id},
            headers=headers
        )

        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                resources = result['result'].get('resources', [])

                for resource in resources:
                    if resource.get('name', '').startswith(resource_name):
                        resource_id = resource['id']
                        has_datastore = resource.get('datastore_active', False)
                        print(f"✓ Found resource: {resource['name']}")
                        print(f"  Resource ID: {resource_id}")
                        return {'id': resource_id, 'has_datastore': has_datastore}

                print(f"✗ No resource found matching '{resource_name}'")
                return None

    except Exception as e:
        print(f"✗ Error finding resource: {e}")
        logger.error(f"Error finding resource: {e}")
        return None


def download_existing_data(resource_id, limit=100000):
    """Download all existing data from datastore to check for duplicates"""

    print(f"  Downloading existing data to check for duplicates...")

    search_url = f"{CKAN_URL}/api/3/action/datastore_search"
    headers = {'Authorization': API_KEY}

    all_records = []
    offset = 0
    batch_size = 1000

    try:
        while offset < limit:
            params = {
                'resource_id': resource_id,
                'limit': batch_size,
                'offset': offset
            }

            response = scraper.post(
                search_url,
                json=params,
                headers=headers,
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    records = result['result']['records']

                    if not records:
                        break

                    for record in records:
                        record.pop('_id', None)

                    all_records.extend(records)
                    offset += batch_size

                    total = result['result'].get('total', 0)
                    if len(all_records) >= total:
                        break
                else:
                    break
            else:
                break

    except Exception as e:
        logger.error(f"Error downloading existing data: {e}")
        return None

    print(f"  Downloaded {len(all_records)} existing records")

    if all_records:
        return pd.DataFrame(all_records)
    else:
        return pd.DataFrame()


def filter_duplicates(new_df, existing_df, unique_columns):
    """Remove records from new_df that already exist in existing_df"""

    if existing_df.empty:
        print(f"  No existing data, all {len(new_df)} records are new")
        return new_df, new_df.copy()

    print(f"  Checking for duplicates based on: {', '.join(unique_columns)}")

    for col in unique_columns:
        if col not in new_df.columns:
            print(f"  Warning: Column '{col}' not in new data")
            return new_df, new_df.copy()
        if col not in existing_df.columns:
            print(f"  Warning: Column '{col}' not in existing data")
            return new_df, new_df.copy()

    new_df['_composite_key'] = new_df[unique_columns].astype(str).agg('-'.join, axis=1)
    existing_df['_composite_key'] = existing_df[unique_columns].astype(str).agg('-'.join, axis=1)

    existing_keys = set(existing_df['_composite_key'])
    mask = ~new_df['_composite_key'].isin(existing_keys)

    truly_new = new_df[mask].copy()
    duplicates = new_df[~mask].copy()

    truly_new.drop('_composite_key', axis=1, inplace=True)
    if not new_df.empty:
        new_df.drop('_composite_key', axis=1, inplace=True)
    if not existing_df.empty:
        existing_df.drop('_composite_key', axis=1, inplace=True)
    if not duplicates.empty:
        duplicates.drop('_composite_key', axis=1, inplace=True)

    print(f"  Found {len(truly_new)} new records, {len(duplicates)} duplicates")

    return truly_new, duplicates


def append_to_datastore(resource_id, df):
    """Append CSV data to datastore with duplicate checking"""

    print(f"\nChecking datastore for existing data...")

    existing_df = download_existing_data(resource_id)

    if existing_df is None:
        print("  ✗ Could not download existing data")
        return False

    new_records_df, duplicates_df = filter_duplicates(df, existing_df, UNIQUE_COLUMNS)

    if len(new_records_df) == 0:
        print(f"\n  No new records to insert (all {len(df)} records already exist)")
        return True

    print(f"\nInserting {len(new_records_df)} new records...")

    records = new_records_df.to_dict(orient='records')
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for k, v in record.items():
            if pd.notna(v) and v is not None:
                if hasattr(v, 'item'):
                    v = v.item()
                cleaned_record[k] = v
        cleaned_records.append(cleaned_record)

    upsert_url = f"{CKAN_URL}/api/3/action/datastore_upsert"
    headers = {'Authorization': API_KEY}

    data = {
        'resource_id': resource_id,
        'records': cleaned_records,
        'method': 'insert',
        'force': True,
        'calculate_record_count': True
    }

    try:
        response = scraper.post(
            upsert_url,
            json=data,
            headers=headers,
            timeout=60
        )

        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                print(f"✓ Successfully inserted {len(cleaned_records)} new records")
                logger.info(f"Inserted {len(cleaned_records)} records to resource {resource_id}")

                if len(duplicates_df) > 0:
                    print(f"  (Skipped {len(duplicates_df)} duplicate records)")

                return True
            else:
                error = result.get('error', {})
                print(f"✗ Failed to insert data:")
                print(f"  Error: {json.dumps(error, indent=2)}")
                logger.error(f"Insert failed: {error}")
                return False
        else:
            print(f"✗ HTTP Error {response.status_code}")
            print(f"  {response.text[:500]}")
            logger.error(f"HTTP {response.status_code}: {response.text}")
            return False

    except Exception as e:
        print(f"✗ Error: {e}")
        logger.error(f"Exception during insert: {e}", exc_info=True)
        return False


def update_resource_metadata(resource_id):
    """Update resource description with timestamp"""

    timestamp = datetime.utcnow().isoformat() + 'Z'

    resource_patch_url = f"{CKAN_URL}/api/3/action/resource_patch"
    headers = {'Authorization': API_KEY}

    data = {
        'id': resource_id,
        'description': f'Dynamic metadata for CKAN sites - time series data. Last updated: {timestamp}'
    }

    try:
        response = scraper.post(
            resource_patch_url,
            json=data,
            headers=headers
        )

        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                print("✓ Resource metadata updated")
                return True
    except Exception as e:
        logger.error(f"Error updating metadata: {e}")

    return False


def main():
    """Main function"""

    print("=== CKAN SITES DATASTORE APPENDER ===")
    print(f"Dataset: {DATASET_ID}")
    print(f"CSV file: {CSV_FILE_PATH}")
    print(f"Duplicate check columns: {', '.join(UNIQUE_COLUMNS)}\n")

    if not os.path.exists(CSV_FILE_PATH):
        print(f"✗ CSV file '{CSV_FILE_PATH}' not found!")
        return False

    print("Reading CSV file...")
    try:
        df = pd.read_csv(CSV_FILE_PATH)
        print(f"  Rows: {len(df)}")
        print(f"  Columns: {', '.join(df.columns)}")
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")
        return False

    for col in UNIQUE_COLUMNS:
        if col not in df.columns:
            print(f"✗ ERROR: Unique column '{col}' not found in CSV!")
            return False

    print()

    resource_info = get_resource_id(DATASET_ID, RESOURCE_NAME)
    if not resource_info:
        return False

    resource_id = resource_info['id']
    success = append_to_datastore(resource_id, df)

    if success:
        update_resource_metadata(resource_id)
        print(f"\n✓ Data successfully appended!")
        print(f"View dataset: {CKAN_URL}/dataset/{DATASET_ID}")
        return True
    else:
        print(f"\n✗ Failed to append data")
        return False


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
