#!/usr/bin/env python3
"""
Simple CKAN Sites Datastore Appender
Just reads CSV and appends to existing datastore - no deduplication, no processing
"""

import os
import requests
import pandas as pd
import sys

# Configuration
CKAN_URL = 'https://catalog.civicdataecosystem.org'
API_KEY = os.getenv('CKAN_API_KEY', 'CKAN_API_KEY')
DATASET_ID = 'ckan-sites-metadata'
RESOURCE_NAME = 'CKAN Sites Dynamic Metadata'
CSV_FILE_PATH = 'ckan_stats.csv'

def get_resource_id(dataset_id, resource_name):
    """Find resource ID by name"""
    
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
                resources = result['result'].get('resources', [])
                
                for resource in resources:
                    if resource.get('name', '').startswith(resource_name):
                        resource_id = resource['id']
                        print(f"✓ Found resource: {resource['name']}")
                        print(f"  Resource ID: {resource_id}")
                        return resource_id
                
                print(f"✗ No resource found matching '{resource_name}'")
                return None
                
    except Exception as e:
        print(f"✗ Error finding resource: {e}")
        return None

def append_to_datastore(resource_id, csv_file):
    """Simply append CSV data to datastore"""
    
    # Read CSV
    print(f"\nReading CSV: {csv_file}")
    try:
        df = pd.read_csv(csv_file)
        print(f"  Rows: {len(df)}")
        print(f"  Columns: {', '.join(df.columns)}")
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")
        return False
    
    # Convert to records
    records = df.to_dict(orient='records')
    
    # Clean None values
    cleaned_records = []
    for record in records:
        cleaned_record = {k: v for k, v in record.items() if pd.notna(v)}
        cleaned_records.append(cleaned_record)
    
    print(f"\nAppending {len(cleaned_records)} records to datastore...")
    
    # Append to datastore
    upsert_url = f"{CKAN_URL}/api/3/action/datastore_upsert"
    headers = {'Authorization': API_KEY}
    
    data = {
        'resource_id': resource_id,
        'records': cleaned_records,
        'method': 'insert',
        'force': True
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
                print(f"✓ Successfully appended {len(cleaned_records)} records")
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

def main():
    """Main function"""
    
    print("=== SIMPLE CKAN SITES DATASTORE APPENDER ===")
    print(f"Dataset: {DATASET_ID}")
    print(f"CSV file: {CSV_FILE_PATH}\n")
    
    # Check CSV exists
    if not os.path.exists(CSV_FILE_PATH):
        print(f"✗ CSV file '{CSV_FILE_PATH}' not found!")
        return False
    
    # Get resource ID
    resource_id = get_resource_id(DATASET_ID, RESOURCE_NAME)
    if not resource_id:
        return False
    
    # Append data
    success = append_to_datastore(resource_id, CSV_FILE_PATH)
    
    if success:
        print(f"\n✓ Data successfully appended!")
        print(f"View dataset: {CKAN_URL}/dataset/{DATASET_ID}")
        return True
    else:
        print(f"\n✗ Failed to append data")
        return False

if __name__ == '__main__':
    # Allow custom CSV file path
    if len(sys.argv) > 1:
        CSV_FILE_PATH = sys.argv[1]
        print(f"Using CSV file: {CSV_FILE_PATH}")
    
    success = main()
    sys.exit(0 if success else 1)
