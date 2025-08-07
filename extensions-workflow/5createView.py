#!/usr/bin/env python3
"""
Simple CKAN Table View Creator
Creates a table view for a resource in a CKAN dataset using the CKAN API.
"""

import requests
import json
import os


def create_ckan_table_view(ckan_url, dataset_id, resource_name, api_key):
    """
    Create a table view for a resource in CKAN.
    
    Args:
        ckan_url: Base URL of the CKAN instance (e.g., 'https://demo.ckan.org')
        dataset_id: ID or name of the dataset
        resource_name: Name of the resource
        api_key: API key for authentication
        
    Returns:
        Dictionary containing the created view information
    """
    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }
    
    # Get dataset to find resource ID
    dataset_url = f"{ckan_url.rstrip('/')}/api/3/action/package_show"
    dataset_response = requests.post(dataset_url, json={'id': dataset_id}, headers=headers)
    dataset_data = dataset_response.json()
    
    # Find resource ID by name
    resource_id = None
    for resource in dataset_data['result']['resources']:
        if resource['name'].lower() == resource_name.lower():
            resource_id = resource['id']
            break
    
    if not resource_id:
        raise Exception(f"Resource '{resource_name}' not found in dataset")
    
    # Create table view
    view_url = f"{ckan_url.rstrip('/')}/api/3/action/resource_view_create"
    view_data = {
        'resource_id': resource_id,
        'title': 'Data Table View',
        'view_type': 'datatables_view',
        'description': 'Interactive table view of the resource data'
    }
    
    view_response = requests.post(view_url, json=view_data, headers=headers)
    view_result = view_response.json()
    
    if view_result['success']:
        return view_result['result']
    else:
        raise Exception(f"Failed to create view: {view_result['error']}")


# Example usage:
if __name__ == "__main__":
    # Set your parameters here
    CKAN_URL = "https://catalog.civicdataecosystem.org"
    DATASET_ID = "ckan-extensions-metadata"
    RESOURCE_NAME = "CKAN Extensions Dynamic Metadata"
    API_KEY = os.getenv('CKAN_API_KEY')
    
    try:
        view = create_ckan_table_view(CKAN_URL, DATASET_ID, RESOURCE_NAME, API_KEY)
        print(f"Successfully created view with ID: {view['id']}")
    except Exception as e:
        print(f"Error: {e}")