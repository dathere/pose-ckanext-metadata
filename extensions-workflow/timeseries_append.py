#!/usr/bin/env python3
"""
CKAN Time-Series Data Appender using Datapump Logic
Directly appends CSV data to CKAN datastore using upsert
Replaces the download-merge-delete-upload cycle
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
PRIMARY_KEY = ['repository_name', 'tstamp']  # Composite key for time-series

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
                        
                        # Check if resource has datastore
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
        return None

def check_datastore_structure(resource_id):
    """Check if datastore exists and has primary key defined"""
    
    datastore_info_url = f"{CKAN_URL}/api/3/action/datastore_info"
    headers = {'Authorization': API_KEY}
    
    try:
        response = requests.post(
            datastore_info_url,
            json={'id': resource_id},
            headers=headers
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                info = result.get('result', {})
                schema = info.get('schema', {})
                
                # Check if primary key exists
                primary_keys = []
                for field in schema.get('fields', []):
                    if field.get('is_primary_key'):
                        primary_keys.append(field['name'])
                
                return {
                    'exists': True,
                    'has_primary_key': len(primary_keys) > 0,
                    'primary_keys': primary_keys,
                    'fields': schema.get('fields', [])
                }
        return {'exists': False}
        
    except:
        return {'exists': False}

def recreate_datastore_with_primary_key(resource_id, df):
    """Delete and recreate datastore with proper primary key"""
    
    print("Recreating datastore with primary key...")
    
    # First, try to delete existing datastore
    delete_url = f"{CKAN_URL}/api/3/action/datastore_delete"
    headers = {'Authorization': API_KEY}
    
    try:
        response = requests.post(
            delete_url,
            json={'resource_id': resource_id, 'force': True},
            headers=headers
        )
        print("  Deleted existing datastore")
    except:
        pass  # It's okay if delete fails
    
    # Now create new datastore with primary key
    fields = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        
        if 'int' in dtype:
            field_type = 'int'
        elif 'float' in dtype:
            field_type = 'float'
        elif 'bool' in dtype:
            field_type = 'boolean'
        elif col in ['tstamp', 'release_date']:
            field_type = 'timestamp'
        else:
            field_type = 'text'
        
        fields.append({
            'id': col,
            'type': field_type
        })
    
    # Convert datetime columns to string
    for col in ['tstamp', 'release_date']:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    # Convert dataframe to records and clean NaN values
    records = df.to_dict(orient='records')
    cleaned_records = []
    for record in records:
        cleaned_record = {k: v for k, v in record.items() if pd.notna(v)}
        cleaned_records.append(cleaned_record)
    
    # Create datastore with primary key
    datastore_create_url = f"{CKAN_URL}/api/3/action/datastore_create"
    
    data = {
        'resource_id': resource_id,
        'fields': fields,
        'records': cleaned_records,
        'primary_key': PRIMARY_KEY,
        'indexes': ','.join(PRIMARY_KEY),
        'force': True
    }
    
    try:
        response = requests.post(
            datastore_create_url,
            json=data,
            headers=headers
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                print("✓ Datastore recreated with primary key and initial data")
                return True
            else:
                print(f"✗ Failed to recreate datastore: {result.get('error')}")
                return False
    except Exception as e:
        print(f"✗ Error recreating datastore: {e}")
        return False

def append_to_datastore(resource_id, df, force_recreate=False):
    """Append data to existing datastore using upsert"""
    
    # Check datastore structure first
    ds_info = check_datastore_structure(resource_id)
    
    if not ds_info['exists'] or not ds_info['has_primary_key'] or force_recreate:
        print("Datastore needs to be recreated with primary key...")
        return recreate_datastore_with_primary_key(resource_id, df)
    
    print(f"Appending {len(df)} records to datastore...")
    
    # Convert datetime columns to string for JSON serialization
    for col in ['tstamp', 'release_date']:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    # Convert dataframe to records
    records = df.to_dict(orient='records')
    
    # Clean records - remove NaN values
    cleaned_records = []
    for record in records:
        cleaned_record = {k: v for k, v in record.items() 
                         if pd.notna(v)}
        cleaned_records.append(cleaned_record)
    
    # Use datastore_upsert to append/update
    upsert_url = f"{CKAN_URL}/api/3/action/datastore_upsert"
    headers = {'Authorization': API_KEY}
    
    data = {
        'resource_id': resource_id,
        'records': cleaned_records,
        'method': 'upsert',
        'force': True,
        'calculate_record_count': True
    }
    
    try:
        response = requests.post(
            upsert_url,
            json=data,
            headers=headers
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                print(f"✓ Successfully appended data to datastore")
                logger.info(f"Appended {len(cleaned_records)} records to resource {resource_id}")
                return True
            else:
                error = result.get('error')
                print(f"✗ Failed to append data: {error}")
                
                # If error is about primary key, recreate datastore
                if 'unique key must be passed' in str(error):
                    print("Primary key issue detected, recreating datastore...")
                    return recreate_datastore_with_primary_key(resource_id, df)
                
                logger.error(f"Upsert failed: {error}")
                return False
        else:
            print(f"✗ HTTP Error {response.status_code}: {response.text}")
            
            # If 409 error about primary key, recreate
            if response.status_code == 409:
                print("Primary key conflict, recreating datastore...")
                return recreate_datastore_with_primary_key(resource_id, df)
            
            return False
            
    except Exception as e:
        print(f"✗ Error appending data: {e}")
        logger.error(f"Exception during upsert: {e}")
        return False

def create_resource_with_datastore(package_id, df):
    """Create new resource with datastore in one operation"""
    
    print("Creating new resource with datastore...")
    
    # Prepare fields
    fields = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        
        if 'int' in dtype:
            field_type = 'int'
        elif 'float' in dtype:
            field_type = 'float'
        elif 'bool' in dtype:
            field_type = 'boolean'
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
            df[col] = df[col].astype(str)
    
    # Convert dataframe to records
    records = df.to_dict(orient='records')
    cleaned_records = []
    for record in records:
        cleaned_record = {k: v for k, v in record.items() if pd.notna(v)}
        cleaned_records.append(cleaned_record)
    
    # Create resource with datastore in one call
    datastore_create_url = f"{CKAN_URL}/api/3/action/datastore_create"
    headers = {'Authorization': API_KEY}
    
    timestamp = datetime.utcnow().isoformat() + '+00:00'
    
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
        'primary_key': PRIMARY_KEY,
        'indexes': ','.join(PRIMARY_KEY),
        'force': True
    }
    
    try:
        response = requests.post(
            datastore_create_url,
            json=data,
            headers=headers
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                resource_id = result['result']['resource_id']
                print(f"✓ Created resource with datastore: {resource_id}")
                return resource_id
            else:
                print(f"✗ Failed to create resource: {result.get('error')}")
                return None
    except Exception as e:
        print(f"✗ Error creating resource: {e}")
        return None

def update_resource_metadata(resource_id):
    """Update resource description with timestamp"""
    
    timestamp = datetime.utcnow().isoformat() + '+00:00'
    
    resource_update_url = f"{CKAN_URL}/api/3/action/resource_update"
    headers = {'Authorization': API_KEY}
    
    data = {
        'id': resource_id,
        'description': f'Dynamic metadata for CKAN extensions - time series data. Last updated: {timestamp}'
    }
    
    try:
        response = requests.post(
            resource_update_url,
            json=data,
            headers=headers
        )
        
        if response.status_code == 200:
            print("✓ Resource metadata updated")
            return True
    except:
        pass
    
    return False

def main():
    """Main function - append time-series data using datapump logic"""
    
    print("=== CKAN TIME-SERIES DATA APPENDER ===")
    print(f"Dataset: {DATASET_ID}")
    print(f"CSV file: {CSV_FILE_PATH}")
    print(f"Primary key: {', '.join(PRIMARY_KEY)}")
    print()
    
    # Check CSV file exists
    if not os.path.exists(CSV_FILE_PATH):
        print(f"✗ ERROR: CSV file '{CSV_FILE_PATH}' not found!")
        return False
    
    # Read the CSV
    print("Reading CSV file...")
    df = pd.read_csv(CSV_FILE_PATH)
    print(f"  Rows: {len(df)}")
    print(f"  Columns: {', '.join(df.columns)}")
    
    # Validate primary key columns exist
    for key_col in PRIMARY_KEY:
        if key_col not in df.columns:
            print(f"✗ ERROR: Primary key column '{key_col}' not found in CSV!")
            return False
    
    print()
    
    # Check if resource exists
    resource_info = get_resource_info(DATASET_ID, RESOURCE_NAME)
    
    if resource_info:
        resource_id = resource_info['id']
        
        # Append data using upsert (will handle primary key issues internally)
        success = append_to_datastore(resource_id, df)
        
        if success:
            update_resource_metadata(resource_id)
            
    else:
        # Resource doesn't exist, create it with datastore
        print("Resource not found, creating new resource with datastore...")
        
        # Get package info first
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
        
        if resource_id:
            success = True
        else:
            success = False
    
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
