#!/usr/bin/env python3
"""
Smart CKAN Resource Deleter
Deletes a resource by finding it using the resource name
"""

import os
import requests
import sys

# Configuration
CKAN_URL = 'https://catalog.civicdataecosystem.org'
API_KEY = os.getenv('CKAN_API_KEY')
DATASET_ID = 'ckan-sites-metadata'
RESOURCE_NAME = 'CKAN Sites Dynamic Metadata'

def get_resource_id_by_name(dataset_id, resource_name, api_key):
    """
    Find resource ID by searching for resource name in the dataset
    
    Args:
        dataset_id (str): CKAN dataset ID
        resource_name (str): Name of the resource to find
        api_key (str): CKAN API key for authentication
    
    Returns:
        str: Resource ID if found, None otherwise
    """
    
    print(f"Finding resource ID for '{resource_name}'...")
    
    # CKAN API endpoint to get dataset info
    package_show_url = f"{CKAN_URL}/api/3/action/package_show"
    
    headers = {'Authorization': api_key} if api_key else {}
    
    try:
        response = requests.get(
            package_show_url,
            params={'id': dataset_id},
            headers=headers,
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

def delete_resource(resource_id, api_key):
    """
    Delete a resource from CKAN dataset
    
    Args:
        resource_id (str): The ID of the resource to delete
        api_key (str): CKAN API key for authentication
    
    Returns:
        bool: True if successful, False otherwise
    """
    
    print(f"Deleting resource from CKAN...")
    print(f"Resource ID: {resource_id}")
    print(f"CKAN URL: {CKAN_URL}")
    
    if not api_key:
        print("✗ Error: No API key found!")
        print("Set the CKAN_API_KEY environment variable")
        return False
    
    # CKAN API endpoint for resource deletion
    delete_url = f"{CKAN_URL}/api/3/action/resource_delete"
    
    # Request headers
    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }
    
    # Request data
    data = {
        'id': resource_id
    }
    
    try:
        print("Sending delete request...")
        response = requests.post(delete_url, json=data, headers=headers, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                print(f"✓ Successfully deleted resource {resource_id}")
                return True
            else:
                error_msg = result.get('error', 'Unknown error')
                print(f"✗ API Error: {error_msg}")
                return False
                
        elif response.status_code == 403:
            print("✗ Error: Permission denied (403)")
            print("You must be a sysadmin or the owner of the resource to delete it")
            return False
            
        elif response.status_code == 404:
            print("✗ Error: Resource not found (404)")
            print("The resource may have already been deleted or the ID is incorrect")
            return False
            
        else:
            print(f"✗ HTTP Error {response.status_code}: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        print("✗ Error: Request timed out")
        return False
        
    except requests.exceptions.ConnectionError:
        print("✗ Error: Could not connect to CKAN")
        return False
        
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return False

def main():
    """Main function"""
    
    print("=== SMART CKAN RESOURCE DELETER ===")
    print(f"Dataset: {DATASET_ID}")
    print(f"Resource name: {RESOURCE_NAME}")
    print()
    
    if not API_KEY:
        print("✗ Error: No API key found!")
        print("Set the CKAN_API_KEY environment variable")
        sys.exit(1)
    
    # Step 1: Find resource ID by name
    resource_id = get_resource_id_by_name(DATASET_ID, RESOURCE_NAME, API_KEY)
    
    if not resource_id:
        print("Cannot proceed without resource ID")
        sys.exit(1)
    
    print()
    
    # Step 2: Delete the resource using the found resource ID
    success = delete_resource(resource_id, API_KEY)
    
    if success:
        print("Resource deletion complete!")
        sys.exit(0)
    else:
        print("Resource deletion failed!")
        sys.exit(1)

if __name__ == '__main__':
    main()