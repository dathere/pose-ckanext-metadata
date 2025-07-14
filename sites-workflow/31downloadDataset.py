#!/usr/bin/env python3
"""
Smart CKAN Sites CSV Downloader
Downloads CSV by finding resource ID using the resource name
"""

import requests
import sys
import os

# Configuration
CKAN_URL = 'https://catalog.civicdataecosystem.org'
DATASET_ID = 'ckan-sites-metadata'
RESOURCE_NAME = 'CKAN Sites Dynamic Metadata'
DEFAULT_OUTPUT_PATH = 'existing_sites_metadata.csv'

def get_resource_id_by_name(dataset_id, resource_name):
    """
    Find resource ID by searching for resource name in the dataset
    
    Args:
        dataset_id (str): CKAN dataset ID
        resource_name (str): Name of the resource to find
    
    Returns:
        str: Resource ID if found, None otherwise
    """
    
    print(f"Finding resource ID for '{resource_name}'...")
    
    # CKAN API endpoint to get dataset info
    package_show_url = f"{CKAN_URL}/api/3/action/package_show"
    
    try:
        response = requests.get(
            package_show_url,
            params={'id': dataset_id},
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                dataset = result['result']
                resources = dataset.get('resources', [])
                
                # Search for resource by name
                for resource in resources:
                    if resource.get('name', '').startswith(resource_name):
                        resource_id = resource['id']
                        print(f"✓ Found resource: {resource['name']}")
                        print(f"  Resource ID: {resource_id}")
                        return resource_id
                
                print(f"✗ No resource found with name starting with '{resource_name}'")
                print("Available resources:")
                for resource in resources:
                    print(f"  - {resource.get('name', 'Unnamed')}")
                return None
            else:
                print(f"✗ API Error: {result.get('error')}")
                return None
        else:
            print(f"✗ HTTP Error {response.status_code}")
            return None
            
    except Exception as e:
        print(f"✗ Error finding resource: {str(e)}")
        return None

def download_csv_by_resource_id(resource_id, output_path):
    """Download CSV using resource ID"""
    
    download_url = f"{CKAN_URL}/dataset/{DATASET_ID}/resource/{resource_id}/download"
    
    print(f"Downloading from: {download_url}")
    
    try:
        response = requests.get(download_url, timeout=30)
        
        if response.status_code == 200:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            file_size = os.path.getsize(output_path)
            print(f"✓ Successfully downloaded {file_size} bytes to {output_path}")
            return True
            
        else:
            print(f"✗ Error downloading: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"✗ Error downloading CSV: {str(e)}")
        return False

def download_csv(output_path=DEFAULT_OUTPUT_PATH):
    """
    Download CSV by finding resource ID using resource name
    
    Args:
        output_path (str): Path where to save the downloaded CSV
    
    Returns:
        bool: True if successful, False otherwise
    """
    
    print(f"=== SMART CKAN SITES CSV DOWNLOADER ===")
    print(f"Dataset: {DATASET_ID}")
    print(f"Resource name: {RESOURCE_NAME}")
    print(f"Output file: {output_path}")
    print()
    
    # Step 1: Find resource ID by name
    resource_id = get_resource_id_by_name(DATASET_ID, RESOURCE_NAME)
    
    if not resource_id:
        print("Cannot proceed without resource ID")
        return False
    
    print()
    
    # Step 2: Download the CSV using the resource ID
    success = download_csv_by_resource_id(resource_id, output_path)
    
    return success

def main():
    """Main function"""
    
    # Use command line argument if provided
    output_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_PATH
    
    success = download_csv(output_path)
    
    if success:
        print("Download complete!")
        sys.exit(0)
    else:
        print("Download failed!")
        sys.exit(1)

if __name__ == '__main__':
    main()