#!/usr/bin/env python3
"""
CKAN Time-Series Data Appender - No Primary Key Version
Works with datastores that don't have primary keys defined
Checks for duplicates before inserting to avoid duplicate data
"""

import os
import requests
import pandas as pd
import json
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    filename='timeseries_append.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CKAN_URL = 'https://catalog.civicdataecosystem.org'
API_KEY = os.getenv('CKAN_API_KEY', 'CKAN_API_KEY')
DATASET_ID = 'ckan-extensions-metadata'
RESOURCE_NAME = 'CKAN Extensions Dynamic Metadata'
CSV_FILE_PATH = 'dynamic_metadata_update.csv'
UNIQUE_COLUMNS = ['repository_name', 'tstamp']  # Columns to check for duplicates

def get_resource_info(dataset_id, resource_name):
    """Find resource by name and check if it has datastore"""
    
    package_show_url = f"{CKAN_URL}/api/3/action/package_show"
    headers = {'Authorization': API_KEY}
    
    try:
        response = requests.get(
            package_show_url,
            params={'id': dataset_id},
            headers=headers
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                dataset = result['result']
                resources = dataset.get('resources', [])
                
                for resource in resources:
                    if resource.get('name', '').startswith(resource_name):
                        resource_id = resource['id']
                        print(f"✓ Found resource: {resource['name']}")
                        print(f"  Resource ID: {resource_id}")
                        
                        has_datastore = resource.get('datastore_active', False)
                        return {
                            'id': resource_id,
                            'has_datastore': has_datastore,
                            'package_id': dataset['id']
                        }
                
                print(f"No resource found matching '{resource_name}'")
                return None
                
    except Exception as e:
        print(f"Error finding resource: {e}")
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
            
            response = requests.post(
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
                    
                    # Remove _id field if present
                    for record in records:
                        record.pop('_id', None)
                    
                    all_records.extend(records)
                    offset += batch_size
                    
                    # Check if we got all records
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
    
    # Ensure unique columns exist in both dataframes
    for col in unique_columns:
        if col not in new_df.columns:
            print(f"  Warning: Column '{col}' not in new data")
            return new_df, new_df.copy()
        if col not in existing_df.columns:
            print(f"  Warning: Column '{col}' not in existing data")
            return new_df, new_df.copy()
    
    # Convert timestamps to strings for comparison if they exist
    for df in [new_df, existing_df]:
        for col in unique_columns:
            if col in df.columns and df[col].dtype == 'object':
                # Try to normalize timestamp format
                try:
                    df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')
                    df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
                except:
                    pass
    
    # Create composite key for comparison
    new_df['_composite_key'] = new_df[unique_columns].astype(str).agg('-'.join, axis=1)
    existing_df['_composite_key'] = existing_df[unique_columns].astype(str).agg('-'.join, axis=1)
    
    # Find records that don't exist
    existing_keys = set(existing_df['_composite_key'])
    mask = ~new_df['_composite_key'].isin(existing_keys)
    
    truly_new = new_df[mask].copy()
    duplicates = new_df[~mask].copy()
    
    # Remove the composite key column
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
    """Append data to existing datastore using insert method (no primary key needed)"""
    
    print(f"\nChecking datastore for existing data...")
    
    # Download existing data to check for duplicates
    existing_df = download_existing_data(resource_id)
    
    if existing_df is None:
        print("  ✗ Could not download existing data")
        return False
    
    # Filter out duplicates
    new_records_df, duplicates_df = filter_duplicates(df, existing_df, UNIQUE_COLUMNS)
    
    if len(new_records_df) == 0:
        print(f"\n  No new records to insert (all {len(df)} records already exist)")
        return True
    
    print(f"\nInserting {len(new_records_df)} new records...")
    
    # Convert datetime columns to ISO format strings
    for col in ['tstamp', 'release_date']:
        if col in new_records_df.columns:
            new_records_df[col] = pd.to_datetime(new_records_df[col], format='%Y-%m-%d', errors='coerce')
            new_records_df[col] = new_records_df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    # Convert dataframe to records and clean
    records = new_records_df.to_dict(orient='records')
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for k, v in record.items():
            if pd.notna(v) and v is not None:
                # Convert numpy types to Python native types
                if hasattr(v, 'item'):
                    v = v.item()
                cleaned_record[k] = v
        cleaned_records.append(cleaned_record)
    
    # Use datastore_upsert with 'insert' method (doesn't require primary key)
    upsert_url = f"{CKAN_URL}/api/3/action/datastore_upsert"
    headers = {'Authorization': API_KEY}
    
    data = {
        'resource_id': resource_id,
        'records': cleaned_records,
        'method': 'insert',  # Use 'insert' instead of 'upsert' (no primary key needed)
        'force': True,
        'calculate_record_count': True
    }
    
    try:
        response = requests.post(
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
            print(f"  Response: {response.text[:500]}")
            logger.error(f"HTTP {response.status_code}: {response.text}")
            return False
            
    except Exception as e:
        print(f"✗ Error inserting data: {e}")
        logger.error(f"Exception during insert: {e}", exc_info=True)
        return False

def create_datastore_without_primary_key(resource_id, df):
    """Create datastore without primary key (just for initial setup)"""
    
    print("Creating datastore structure (without primary key)...")
    
    # First, try to delete existing datastore
    delete_url = f"{CKAN_URL}/api/3/action/datastore_delete"
    headers = {'Authorization': API_KEY}
    
    try:
        response = requests.post(
            delete_url,
            json={'resource_id': resource_id, 'force': True},
            headers=headers
        )
        if response.status_code == 200:
            print("  ✓ Deleted existing datastore")
    except:
        pass
    
    # Prepare fields
    fields = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        
        if 'int' in dtype:
            field_type = 'int'
        elif 'float' in dtype:
            field_type = 'numeric'
        elif 'bool' in dtype:
            field_type = 'bool'
        elif col in ['tstamp', 'release_date']:
            field_type = 'timestamp'
        else:
            field_type = 'text'
        
        fields.append({
            'id': col,
            'type': field_type
        })
    
    # Convert datetime columns
    for col in ['tstamp', 'release_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    # Convert to records
    records = df.to_dict(orient='records')
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for k, v in record.items():
            if pd.notna(v) and v is not None:
                if hasattr(v, 'item'):
                    v = v.item()
                cleaned_record[k] = v
        cleaned_records.append(cleaned_record)
    
    # Create datastore WITHOUT primary key
    datastore_create_url = f"{CKAN_URL}/api/3/action/datastore_create"
    
    data = {
        'resource_id': resource_id,
        'fields': fields,
        'records': cleaned_records,
        'force': True,
        'calculate_record_count': True
        # NOTE: No primary_key specified!
    }
    
    try:
        response = requests.post(
            datastore_create_url,
            json=data,
            headers=headers,
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                print(f"✓ Datastore created with {len(cleaned_records)} records")
                return True
            else:
                print(f"✗ Failed: {result.get('error')}")
                return False
        else:
            print(f"✗ HTTP Error {response.status_code}")
            print(f"  {response.text[:500]}")
            return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

def create_resource_with_datastore(package_id, df):
    """Create new resource with datastore (no primary key)"""
    
    print("Creating new resource with datastore...")
    
    fields = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        
        if 'int' in dtype:
            field_type = 'int'
        elif 'float' in dtype:
            field_type = 'numeric'
        elif 'bool' in dtype:
            field_type = 'bool'
        elif col in ['tstamp', 'release_date']:
            field_type = 'timestamp'
        else:
            field_type = 'text'
        
        fields.append({
            'id': col,
            'type': field_type
        })
    
    # Convert datetime columns
    for col in ['tstamp', 'release_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    records = df.to_dict(orient='records')
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for k, v in record.items():
            if pd.notna(v) and v is not None:
                if hasattr(v, 'item'):
                    v = v.item()
                cleaned_record[k] = v
        cleaned_records.append(cleaned_record)
    
    datastore_create_url = f"{CKAN_URL}/api/3/action/datastore_create"
    headers = {'Authorization': API_KEY}
    
    timestamp = datetime.utcnow().isoformat() + 'Z'
    
    resource = {
        'package_id': package_id,
        'name': RESOURCE_NAME,
        'description': f'Dynamic metadata for CKAN extensions - time series data. Last updated: {timestamp}',
        'format': 'CSV'
    }
    
    data = {
        'resource': resource,
        'fields': fields,
        'records': cleaned_records,
        'force': True,
        'calculate_record_count': True
        # No primary_key!
    }
    
    try:
        response = requests.post(
            datastore_create_url,
            json=data,
            headers=headers,
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                resource_id = result['result']['resource_id']
                print(f"✓ Created resource: {resource_id}")
                return resource_id
            else:
                print(f"✗ Failed: {result.get('error')}")
                return None
        else:
            print(f"✗ HTTP Error {response.status_code}")
            return None
    except Exception as e:
        print(f"✗ Error: {e}")
        return None

def update_resource_metadata(resource_id):
    """Update resource description with timestamp"""
    
    timestamp = datetime.utcnow().isoformat() + 'Z'
    
    resource_patch_url = f"{CKAN_URL}/api/3/action/resource_patch"
    headers = {'Authorization': API_KEY}
    
    data = {
        'id': resource_id,
        'description': f'Dynamic metadata for CKAN extensions - time series data. Last updated: {timestamp}'
    }
    
    try:
        response = requests.post(
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
    """Main function - append time-series data without requiring primary key"""
    
    print("=== CKAN TIME-SERIES DATA APPENDER (No Primary Key) ===")
    print(f"Dataset: {DATASET_ID}")
    print(f"CSV file: {CSV_FILE_PATH}")
    print(f"Duplicate check columns: {', '.join(UNIQUE_COLUMNS)}")
    print()
    
    # Check CSV file exists
    if not os.path.exists(CSV_FILE_PATH):
        print(f"✗ ERROR: CSV file '{CSV_FILE_PATH}' not found!")
        return False
    
    # Read the CSV
    print("Reading CSV file...")
    try:
        df = pd.read_csv(CSV_FILE_PATH)
        print(f"  Rows: {len(df)}")
        print(f"  Columns: {', '.join(df.columns)}")
    except Exception as e:
        print(f"✗ ERROR reading CSV: {e}")
        return False
    
    # Validate unique columns exist
    for col in UNIQUE_COLUMNS:
        if col not in df.columns:
            print(f"✗ ERROR: Unique column '{col}' not found in CSV!")
            return False
    
    print()
    
    # Check if resource exists
    resource_info = get_resource_info(DATASET_ID, RESOURCE_NAME)
    
    if resource_info:
        resource_id = resource_info['id']
        
        if resource_info['has_datastore']:
            # Append to existing datastore
            success = append_to_datastore(resource_id, df)
        else:
            # Create datastore for existing resource
            print("Resource exists but has no datastore, creating it...")
            success = create_datastore_without_primary_key(resource_id, df)
        
        if success:
            update_resource_metadata(resource_id)
            
    else:
        # Create new resource with datastore
        print("Resource not found, creating new resource with datastore...")
        
        try:
            package_show_url = f"{CKAN_URL}/api/3/action/package_show"
            headers = {'Authorization': API_KEY}
            
            response = requests.get(
                package_show_url,
                params={'id': DATASET_ID},
                headers=headers
            )
            
            if response.status_code != 200:
                print(f"✗ Dataset '{DATASET_ID}' not found")
                return False
            
            package = response.json()['result']
            resource_id = create_resource_with_datastore(package['id'], df)
            
            success = resource_id is not None
        except Exception as e:
            print(f"✗ Error: {e}")
            return False
    
    if success:
        print(f"\n✓ Time-series data successfully appended!")
        print(f"View dataset: {CKAN_URL}/dataset/{DATASET_ID}")
        return True
    else:
        print(f"\n✗ Failed to append time-series data")
        return False

if __name__ == '__main__':
    import sys
    
    # Allow custom CSV file path
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
