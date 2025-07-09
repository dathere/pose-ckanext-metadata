#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import requests
import json
from datetime import datetime
import time
import traceback
from typing import Dict, Optional, List
import sys
import os

# Configuration
CKAN_BASE_URL = "https://catalog.civicdataecosystem.org"
CKAN_API_BASE = f"{CKAN_BASE_URL}/api/3/action"
CKAN_API_KEY = os.getenv('CKAN_API_KEY', 'CKAN_API_KEY')

class CKANMetadataUpdater:
    def __init__(self, api_key: str, base_url: str = CKAN_BASE_URL):
        """Initialize the CKAN metadata updater"""
        self.api_key = api_key
        self.base_url = base_url
        self.api_base = f"{base_url}/api/3/action"
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': api_key,
            'Content-Type': 'application/json'
        })
        self.processed_count = 0
        self.error_count = 0
        self.start_time = datetime.now()

    def print_status(self, current: int, total: int, package_name: str = ""):
        """Print processing status with ETA"""
        if current == 0:
            self.start_time = datetime.now()
            print(f"Starting to process {total} packages...")
            return
            
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = elapsed / current if current > 0 else 0
        remaining = rate * (total - current)
        
        hours, remainder = divmod(int(remaining), 3600)
        minutes, seconds = divmod(remainder, 60)
        eta = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        elapsed_formatted = f"{int(elapsed//3600):02d}:{int((elapsed%3600)//60):02d}:{int(elapsed%60):02d}"
        
        print(f"Processing {current}/{total} | {package_name} | " +
              f"Elapsed: {elapsed_formatted} | ETA: {eta} | " +
              f"Success: {self.processed_count} | Errors: {self.error_count}")

    def extract_package_name_from_url(self, catalog_url: str) -> Optional[str]:
        """Extract package name from catalog URL"""
        try:
            # Expected format: https://catalog.civicdataecosystem.org/extension/package-name
            if '/extension/' in catalog_url:
                return catalog_url.split('/extension/')[-1].strip()
            return None
        except Exception as e:
            print(f"Error extracting package name from URL {catalog_url}: {str(e)}")
            return None

    def get_package_info(self, package_name: str) -> Optional[Dict]:
        """Get current package information from CKAN"""
        try:
            response = self.session.get(
                f"{self.api_base}/package_show",
                params={'id': package_name}
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    return data['result']
                else:
                    print(f"API error for {package_name}: {data.get('error', {})}")
                    return None
            else:
                print(f"HTTP error {response.status_code} for package {package_name}")
                return None
                
        except Exception as e:
            print(f"Error getting package info for {package_name}: {str(e)}")
            return None

    def prepare_metadata_update(self, metadata: Dict) -> Dict:
        """Prepare metadata for CKAN update using proper schema fields"""
        
        # Map dynamic metadata fields to CKAN schema fields
        field_mapping = {
            'forks_count': 'forks_count',           # Direct field in schema
            'total_releases': 'total_releases',     # Direct field in schema
            'latest_release': 'latest_release',     # Direct field in schema
            'release_date': 'release_date',         # Direct field in schema
            'stars': 'stars',                       # Direct field in schema
            'open_issues': 'open_issues',           # Direct field in schema
            'contributors_count': 'contributors_count',  # Direct field in schema
            'discussions': 'discussions',           # Direct field in schema
            'last_update': 'last_update'            # Direct field in schema
        }
        
        # Convert metadata to CKAN format
        update_data = {}
        for original_key, ckan_field in field_mapping.items():
            if original_key in metadata and metadata[original_key] is not None:
                value = metadata[original_key]
                
                # Handle boolean values for discussions field
                if original_key == 'discussions':
                    # Convert None/null to False, then to string
                    if value is None or pd.isna(value):
                        value = 'FALSE'
                    else:
                        value = 'TRUE' if value else 'FALSE'
                
                # Handle date formatting for release_date and last_update
                elif original_key in ['release_date', 'last_update']:
                    # Ensure date is in proper format
                    if isinstance(value, str) and value != 'No releases':
                        try:
                            # Try to parse and reformat the date
                            from datetime import datetime
                            if original_key == 'release_date' and value != 'No releases':
                                # Parse GitHub date format and convert to YYYY-MM-DD
                                parsed_date = datetime.strptime(value, '%Y-%m-%d')
                                value = parsed_date.strftime('%Y-%m-%d')
                            elif original_key == 'last_update':
                                # Parse the timestamp format from your script
                                parsed_date = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                                value = parsed_date.isoformat()
                        except ValueError:
                            # If parsing fails, keep original value
                            pass
                    elif value == 'No releases':
                        value = None  # Skip this field if no releases
                        continue
                
                # Handle integer fields
                elif original_key in ['forks_count', 'total_releases', 'stars', 'open_issues', 'contributors_count']:
                    try:
                        value = int(value) if value is not None and not pd.isna(value) else 0
                    except (ValueError, TypeError):
                        value = 0
                
                # Only add non-None values
                if value is not None:
                    update_data[ckan_field] = value
        
        return update_data

    def update_package_metadata(self, package_name: str, metadata: Dict) -> bool:
        """Update a single package with dynamic metadata"""
        try:
            print(f"Updating package: {package_name}")
            
            # Get current package info
            current_package = self.get_package_info(package_name)
            if not current_package:
                print(f"Could not retrieve package info for {package_name}")
                return False
            
            # Prepare metadata update using schema fields
            update_data = self.prepare_metadata_update(metadata)
            
            if not update_data:
                print(f"No valid metadata to update for {package_name}")
                return False
            
            # Add package ID to update data
            update_data['id'] = package_name
            
            print(f"Updating fields: {list(update_data.keys())}")
            
            # Make the patch request
            response = self.session.post(
                f"{self.api_base}/package_patch",
                data=json.dumps(update_data)
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    print(f"Successfully updated {package_name}")
                    return True
                else:
                    print(f"API error updating {package_name}: {data.get('error', {})}")
                    return False
            else:
                print(f"HTTP error {response.status_code} updating {package_name}: {response.text}")
                return False
                
        except Exception as e:
            print(f"Error updating package {package_name}: {str(e)}")
            traceback.print_exc()
            return False

    def load_and_merge_data(self, url_list_file: str, metadata_file: str) -> List[Dict]:
        """Load and merge URL list with dynamic metadata"""
        try:
            # Load URL list
            print(f"Loading URL list from {url_list_file}...")
            url_df = pd.read_csv(url_list_file)
            
            # Load dynamic metadata
            print(f"Loading dynamic metadata from {metadata_file}...")
            metadata_df = pd.read_csv(metadata_file)
            
            # Create a mapping from GitHub URL to catalog URL
            url_mapping = {}
            for _, row in url_df.iterrows():
                if pd.notna(row['github_url']) and pd.notna(row['catalog_url']):
                    url_mapping[row['github_url']] = row['catalog_url']
            
            # Merge data
            merged_data = []
            for _, metadata_row in metadata_df.iterrows():
                github_url = metadata_row.get('url', '')
                
                if github_url in url_mapping:
                    catalog_url = url_mapping[github_url]
                    package_name = self.extract_package_name_from_url(catalog_url)
                    
                    if package_name:
                        merged_data.append({
                            'package_name': package_name,
                            'catalog_url': catalog_url,
                            'github_url': github_url,
                            'metadata': metadata_row.to_dict()
                        })
                    else:
                        print(f"Could not extract package name from URL: {catalog_url}")
                else:
                    print(f"No catalog URL found for GitHub URL: {github_url}")
            
            print(f"Successfully merged data for {len(merged_data)} packages")
            return merged_data
            
        except Exception as e:
            print(f"Error loading and merging data: {str(e)}")
            return []

    def update_all_packages(self, url_list_file: str, metadata_file: str):
        """Update all packages with dynamic metadata"""
        
        # Load and merge data
        merged_data = self.load_and_merge_data(url_list_file, metadata_file)
        
        if not merged_data:
            print("No data to process. Exiting.")
            return
        
        total_count = len(merged_data)
        print(f"\nStarting to update {total_count} packages...")
        print("=" * 60)
        
        success_list = []
        error_list = []
        
        for idx, item in enumerate(merged_data, 1):
            package_name = item['package_name']
            metadata = item['metadata']
            
            self.print_status(idx, total_count, package_name)
            
            try:
                success = self.update_package_metadata(package_name, metadata)
                
                if success:
                    self.processed_count += 1
                    success_list.append(package_name)
                else:
                    self.error_count += 1
                    error_list.append(package_name)
                
                # Brief pause between requests to be respectful to the API
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Unexpected error processing {package_name}: {str(e)}")
                self.error_count += 1
                error_list.append(package_name)
                continue
        
        # Print final summary
        print("\n" + "=" * 60)
        print("UPDATE SUMMARY")
        print("=" * 60)
        print(f"Total packages processed: {total_count}")
        print(f"Successfully updated: {self.processed_count}")
        print(f"Errors encountered: {self.error_count}")
        print(f"Success rate: {(self.processed_count / total_count * 100):.1f}%")
        
        if success_list:
            print(f"\nSuccessfully updated packages:")
            for pkg in success_list:
                print(f"  ✓ {pkg}")
        
        if error_list:
            print(f"\nPackages with errors:")
            for pkg in error_list:
                print(f"  ✗ {pkg}")
        
        print(f"\nTotal processing time: {datetime.now() - self.start_time}")

    def test_api_connection(self) -> bool:
        """Test CKAN API connection and authentication"""
        try:
            print("Testing CKAN API connection...")
            
            response = self.session.get(f"{self.api_base}/site_read")
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    print("✓ CKAN API connection successful")
                    
                    # Test authentication by trying to access user info
                    auth_response = self.session.get(f"{self.api_base}/user_show", params={'id': 'default'})
                    if auth_response.status_code == 200:
                        print("✓ API authentication successful")
                        return True
                    else:
                        print("⚠ API connection works but authentication may have issues")
                        print("This might be okay if your API key has limited permissions")
                        return True
                else:
                    print("✗ CKAN API returned error")
                    return False
            else:
                print(f"✗ CKAN API connection failed with status {response.status_code}")
                return False
                
        except Exception as e:
            print(f"✗ Error testing CKAN API connection: {str(e)}")
            return False


def main():
    """Main function"""
    print("CKAN Extensions Dynamic Metadata Updater")
    print("=" * 50)
    
    # Configuration
    url_list_file = "url_list.csv"
    metadata_file = "dynamic_metadata_update.csv"
    
    print(f"URL list file: {url_list_file}")
    print(f"Metadata file: {metadata_file}")
    print(f"CKAN base URL: {CKAN_BASE_URL}")
    print()
    
    # Check if input files exist
    if not os.path.exists(url_list_file):
        print(f"Error: URL list file '{url_list_file}' not found.")
        return
    
    if not os.path.exists(metadata_file):
        print(f"Error: Metadata file '{metadata_file}' not found.")
        return
    
    # Validate API key
    if not CKAN_API_KEY or CKAN_API_KEY == "CKAN_API_KEY":
        print("Error: Please set a valid CKAN API key in the CKAN_API_KEY variable.")
        print("You can get your API key from your CKAN user profile page.")
        return
    
    try:
        # Initialize updater
        updater = CKANMetadataUpdater(CKAN_API_KEY, CKAN_BASE_URL)
        
        # Test API connection
        if not updater.test_api_connection():
            print("Failed to connect to CKAN API. Please check your configuration.")
            return
        
        print()
        
        # Confirm before proceeding
        #response = input("Do you want to proceed with updating the metadata? (y/N): ")
        #if response.lower() not in ['y', 'yes']:
        #    print("Update cancelled by user.")
        #    return
        print("Starting metadata update process...")
        # Update packages
        updater.update_all_packages(url_list_file, metadata_file)
        
    except KeyboardInterrupt:
        print("\nProcess interrupted by user!")
        
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
