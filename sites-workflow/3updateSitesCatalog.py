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

class CKANSiteStatsUpdater:
    def __init__(self, api_key: str, base_url: str = CKAN_BASE_URL):
        """Initialize the CKAN site statistics updater"""
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

    def prepare_stats_update(self, stats: Dict) -> Dict:
        """Prepare statistics for CKAN update using proper schema fields"""
        
        # Map statistics fields to CKAN schema fields
        field_mapping = {
            'num_datasets': 'num_datasets',
            'num_groups': 'num_groups', 
            'num_organizations': 'num_organizations'
        }
        
        # Convert stats to CKAN format
        update_data = {}
        for original_key, ckan_field in field_mapping.items():
            if original_key in stats and stats[original_key] is not None:
                value = stats[original_key]
                
                # Handle integer fields
                try:
                    value = int(value) if value is not None and not pd.isna(value) else 0
                except (ValueError, TypeError):
                    value = 0
                
                # Only add non-negative values
                if value >= 0:
                    update_data[ckan_field] = value
        
        return update_data

    def update_package_stats(self, package_name: str, stats: Dict) -> bool:
        """Update a single package with statistics"""
        try:
            print(f"Updating package: {package_name}")
            
            # Get current package info
            current_package = self.get_package_info(package_name)
            if not current_package:
                print(f"Could not retrieve package info for {package_name}")
                return False
            
            # Prepare stats update using schema fields
            update_data = self.prepare_stats_update(stats)
            
            if not update_data:
                print(f"No valid statistics to update for {package_name}")
                return False
            
            # Add package ID to update data
            update_data['id'] = package_name
            
            print(f"Updating stats: datasets={update_data.get('num_datasets', 0)}, " +
                  f"groups={update_data.get('num_groups', 0)}, " +
                  f"orgs={update_data.get('num_organizations', 0)}")
            
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

    def load_stats_data(self, stats_file: str) -> List[Dict]:
        """Load statistics data from CSV"""
        try:
            print(f"Loading statistics from {stats_file}...")
            stats_df = pd.read_csv(stats_file)
            
            # Check required columns
            required_columns = ['name', 'num_datasets', 'num_groups', 'num_organizations']
            missing_columns = [col for col in required_columns if col not in stats_df.columns]
            if missing_columns:
                print(f"Error: Missing required columns: {missing_columns}")
                return []
            
            # Convert to list of dictionaries
            stats_data = []
            for _, row in stats_df.iterrows():
                if pd.notna(row['name']):  # Only process rows with valid names
                    stats_data.append({
                        'package_name': row['name'],
                        'url': row.get('url', ''),
                        'stats': {
                            'num_datasets': row.get('num_datasets', 0),
                            'num_groups': row.get('num_groups', 0),
                            'num_organizations': row.get('num_organizations', 0)
                        }
                    })
            
            print(f"Successfully loaded statistics for {len(stats_data)} packages")
            return stats_data
            
        except Exception as e:
            print(f"Error loading statistics data: {str(e)}")
            return []

    def update_all_packages(self, stats_file: str):
        """Update all packages with statistics"""
        
        # Load statistics data
        stats_data = self.load_stats_data(stats_file)
        
        if not stats_data:
            print("No data to process. Exiting.")
            return
        
        total_count = len(stats_data)
        print(f"\nStarting to update {total_count} packages...")
        print("=" * 60)
        
        success_list = []
        error_list = []
        
        for idx, item in enumerate(stats_data, 1):
            package_name = item['package_name']
            stats = item['stats']
            
            self.print_status(idx, total_count, package_name)
            
            try:
                success = self.update_package_stats(package_name, stats)
                
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
    print("CKAN Site Statistics Updater")
    print("=" * 50)
    
    # Configuration
    stats_file = "ckan_stats.csv"
    
    print(f"Statistics file: {stats_file}")
    print(f"CKAN base URL: {CKAN_BASE_URL}")
    print()
    
    # Check if input file exists
    if not os.path.exists(stats_file):
        print(f"Error: Statistics file '{stats_file}' not found.")
        return
    
    # Validate API key
    if not CKAN_API_KEY or CKAN_API_KEY == "CKAN_API_KEY":
        print("Error: Please set a valid CKAN API key in the CKAN_API_KEY variable.")
        print("You can get your API key from your CKAN user profile page.")
        print("Set it as an environment variable: export CKAN_API_KEY='your-api-key-here'")
        return
    
    try:
        # Initialize updater
        updater = CKANSiteStatsUpdater(CKAN_API_KEY, CKAN_BASE_URL)
        
        # Test API connection
        if not updater.test_api_connection():
            print("Failed to connect to CKAN API. Please check your configuration.")
            return
        
        print()
        
        print("Starting statistics update process...")
        
        # Update packages
        updater.update_all_packages(stats_file)
        
    except KeyboardInterrupt:
        print("\nProcess interrupted by user!")
        
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
