#!/usr/bin/env python3
"""
CSV Uploader for CKAN Dataset
Uploads a CSV file as a resource to a specific CKAN dataset
"""

import os
import requests
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    filename='csv_upload.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CKAN_URL = 'https://catalog.civicdataecosystem.org'
API_KEY = os.getenv('CKAN_API_KEY', 'CKAN_API_KEY')
DATASET_ID = 'ckan-extensions-metadata'
CSV_FILE_PATH = 'dynamic_metadata_update.csv'  # Your output CSV file

def test_ckan_connection():
    """Test CKAN connection and configuration"""
    print("=== TESTING CKAN CONNECTION ===")
    print(f"API URL: {CKAN_URL}")
    print(f"API Key: {'✓ Found' if API_KEY else '✗ Missing'}")
    
    if not API_KEY:
        print("ERROR: No API key found!")
        return False
    
    # Test with site_read action
    test_url = f"{CKAN_URL}/api/3/action/site_read"
    try:
        response = requests.get(test_url)
        if response.status_code == 200:
            print("✓ CKAN instance is accessible")
            return True
        else:
            print(f"✗ CKAN instance returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Cannot connect to CKAN: {e}")
        return False

def test_dataset_exists(dataset_id):
    """Test if the target dataset exists"""
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
                print(f"✓ Dataset '{dataset_id}' found")
                return True
            else:
                print(f"✗ Dataset '{dataset_id}' not found: {result.get('error')}")
                return False
        else:
            print(f"✗ HTTP {response.status_code} checking dataset: {response.text}")
            return False
            
    except Exception as e:
        print(f"✗ Error checking dataset: {e}")
        return False

def upload_csv_resource(csv_path, dataset_id):
    """Upload CSV file as a resource to CKAN dataset"""
    
    if not os.path.exists(csv_path):
        print(f"✗ CSV file not found: {csv_path}")
        return False
    
    # Check if dataset exists
    if not test_dataset_exists(dataset_id):
        return False
    
    resource_create_url = f"{CKAN_URL}/api/3/action/resource_create"
    
    # Get file info
    file_size = os.path.getsize(csv_path)
    timestamp = datetime.utcnow().isoformat() + '+00:00'
    date_str = datetime.utcnow().strftime('%Y-%m-%d')
    
    resource_data = {
        'package_id': dataset_id,
        'name': f'CKAN Extensions Dynamic Metadata',
        'description': f'Dynamic metadata for CKAN extensions including forks, stars, releases, and contributor information. Last updated: {timestamp}',
        'format': 'CSV',
        'resource_type': 'file'
    }
    
    headers = {'Authorization': API_KEY}
    
    try:
        print(f"Uploading {os.path.basename(csv_path)} ({file_size} bytes) to dataset '{dataset_id}'...")
        
        with open(csv_path, 'rb') as file_obj:
            response = requests.post(
                resource_create_url,
                data=resource_data,
                headers=headers,
                files={'upload': (os.path.basename(csv_path), file_obj, 'text/csv')}
            )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                resource_id = result['result']['id']
                resource_url = result['result'].get('url', 'N/A')
                print(f"✓ SUCCESS: CSV uploaded successfully!")
                print(f"  Resource ID: {resource_id}")
                print(f"  Resource URL: {resource_url}")
                print(f"  Dataset URL: {CKAN_URL}/dataset/{dataset_id}")
                
                logger.info(f"Successfully uploaded {csv_path} to dataset {dataset_id} - Resource ID: {resource_id}")
                return True
            else:
                error_msg = result.get('error', 'Unknown error')
                print(f"✗ API ERROR: {error_msg}")
                logger.error(f"API error uploading {csv_path}: {error_msg}")
                return False
        else:
            print(f"✗ HTTP ERROR {response.status_code}: {response.text}")
            logger.error(f"HTTP {response.status_code} uploading {csv_path}: {response.text}")
            return False
            
    except Exception as e:
        print(f"✗ EXCEPTION: {str(e)}")
        logger.error(f"Exception uploading {csv_path}: {str(e)}")
        return False

def update_existing_resource(csv_path, dataset_id, resource_name_pattern="CKAN Extensions Dynamic Metadata"):
    """
    Check if a resource with the same name pattern exists and update it, otherwise create new
    """
    # First get the dataset to see existing resources
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
                existing_resources = dataset.get('resources', [])
                
                # Look for existing resource with same name pattern
                target_resource = None
                for resource in existing_resources:
                    resource_name = resource.get('name', '')
                    if resource_name.startswith(resource_name_pattern):
                        target_resource = resource
                        break
                
                if target_resource:
                    print(f"Found existing resource '{target_resource['name']}', updating...")
                    return update_resource(csv_path, target_resource['id'])
                else:
                    print(f"No existing resource matching '{resource_name_pattern}', creating new...")
                    return upload_csv_resource(csv_path, dataset_id)
            
    except Exception as e:
        print(f"Error checking existing resources: {e}")
        # Fall back to creating new resource
        return upload_csv_resource(csv_path, dataset_id)

def update_resource(csv_path, resource_id):
    """Update an existing resource with new CSV file"""
    
    if not os.path.exists(csv_path):
        print(f"✗ CSV file not found: {csv_path}")
        return False
    
    resource_update_url = f"{CKAN_URL}/api/3/action/resource_update"
    
    file_size = os.path.getsize(csv_path)
    timestamp = datetime.utcnow().isoformat() + '+00:00'
    
    resource_data = {
        'id': resource_id,
        'description': f'Dynamic metadata for CKAN extensions including forks, stars, releases, and contributor information. Last updated: {timestamp}',
        'format': 'CSV'
    }
    
    headers = {'Authorization': API_KEY}
    
    try:
        print(f"Updating resource {resource_id} with {os.path.basename(csv_path)} ({file_size} bytes)...")
        
        with open(csv_path, 'rb') as file_obj:
            response = requests.post(
                resource_update_url,
                data=resource_data,
                headers=headers,
                files={'upload': (os.path.basename(csv_path), file_obj, 'text/csv')}
            )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                resource_url = result['result'].get('url', 'N/A')
                print(f"✓ SUCCESS: Resource updated successfully!")
                print(f"  Resource ID: {resource_id}")
                print(f"  Resource URL: {resource_url}")
                
                logger.info(f"Successfully updated resource {resource_id} with {csv_path}")
                return True
            else:
                error_msg = result.get('error', 'Unknown error')
                print(f"✗ API ERROR: {error_msg}")
                return False
        else:
            print(f"✗ HTTP ERROR {response.status_code}: {response.text}")
            return False
            
    except Exception as e:
        print(f"✗ EXCEPTION: {str(e)}")
        return False

def main():
    """Main function to upload CSV to CKAN dataset"""
    
    print("=== CKAN CSV UPLOADER ===")
    print(f"Target dataset: {DATASET_ID}")
    print(f"CSV file: {CSV_FILE_PATH}")
    print(f"CKAN URL: {CKAN_URL}")
    print()
    
    # Check if CSV file exists
    if not os.path.exists(CSV_FILE_PATH):
        print(f"✗ ERROR: CSV file '{CSV_FILE_PATH}' not found!")
        print("Please ensure your CSV file exists before uploading.")
        return False
    
    # Test connection
    if not test_ckan_connection():
        print("Cannot connect to CKAN instance. Aborting.")
        return False
    
    print()
    
    # Upload/update the CSV
    success = update_existing_resource(CSV_FILE_PATH, DATASET_ID)
    
    if success:
        print(f"\n✓ CSV successfully uploaded to CKAN dataset!")
        print(f"View the dataset at: {CKAN_URL}/dataset/{DATASET_ID}")
    else:
        print(f"\n✗ Failed to upload CSV to CKAN dataset.")
    
    return success

if __name__ == '__main__':
    import sys
    
    # Allow custom CSV file path as command line argument
    if len(sys.argv) > 1:
        CSV_FILE_PATH = sys.argv[1]
        print(f"Using custom CSV file: {CSV_FILE_PATH}")
    
    success = main()
    
    if success:
        print("\n=== UPLOAD COMPLETE ===")
        sys.exit(0)
    else:
        print("\n=== UPLOAD FAILED ===")
        sys.exit(1)