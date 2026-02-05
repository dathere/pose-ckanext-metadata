#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CKAN Ecosystem YAML Metadata Updater
-------------------------------------
Fetches ckan_ecosystem.yaml from extension repositories and updates
the CKAN ecosystem catalog with the metadata.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import cloudscraper
import yaml
import json
import time
from typing import Dict, Optional, List
from urllib.parse import urlparse
from config import USER_AGENT, CKAN_BASE_URL

# Configuration
CKAN_API_KEY = os.getenv('CKAN_API_KEY', '')


class EcosystemYAMLUpdater:
    def __init__(self):
        self.base_url = CKAN_BASE_URL
        self.api_base = f"{self.base_url}/api/3/action"
        self.session = cloudscraper.create_scraper()

        headers = {'User-Agent': USER_AGENT}
        if CKAN_API_KEY:
            headers['Authorization'] = CKAN_API_KEY
        self.session.headers.update(headers)

        self.processed_count = 0
        self.error_count = 0

    def get_all_extensions(self) -> List[str]:
        """Fetch all extension package names from CKAN catalog"""
        print("Fetching all extensions from catalog...")

        all_names = []
        start = 0
        rows = 1000

        while True:
            response = self.session.get(
                f"{self.api_base}/package_search",
                params={
                    'fq': 'type:extension',
                    'start': start,
                    'rows': rows,
                    'include_private': False
                }
            )

            if response.status_code != 200:
                print(f"API failed with status {response.status_code}")
                break

            data = response.json()
            if not data.get('success'):
                print("API returned error")
                break

            result = data['result']
            batch = result.get('results', [])
            total_count = result.get('count', 0)

            for pkg in batch:
                all_names.append(pkg.get('name', ''))

            print(f"Fetched {len(all_names)}/{total_count} extensions")

            if len(batch) < rows or len(all_names) >= total_count:
                break

            start += rows

        print(f"Total extensions found: {len(all_names)}")
        return all_names

    def get_extension_from_catalog(self, extension_identifier: str) -> Optional[Dict]:
        """
        Fetch extension details from CKAN catalog.

        Args:
            extension_identifier: Either extension name or full catalog URL.

        Returns:
            Extension metadata dict, or None if not found.
        """
        # Extract extension name from URL if full URL provided
        if extension_identifier.startswith('http'):
            parsed = urlparse(extension_identifier)
            extension_name = parsed.path.split('/')[-1]
        else:
            extension_name = extension_identifier

        print(f"  Fetching catalog data for: {extension_name}")

        try:
            response = self.session.get(
                f"{self.api_base}/package_show",
                params={'id': extension_name}
            )

            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    print(f"  Found extension in catalog: {extension_name}")
                    return data['result']

            print(f"  Extension not found in catalog: {extension_name}")
            return None

        except Exception as e:
            print(f"  Error fetching extension from catalog: {str(e)}")
            return None

    def construct_raw_yaml_urls(self, github_url: str) -> List[str]:
        """
        Convert GitHub repository URL to raw ckan_ecosystem.yaml URL candidates.

        Tries main branch first, then master.
        """
        github_url = github_url.rstrip('/')
        parsed = urlparse(github_url)
        path_parts = parsed.path.strip('/').split('/')

        if len(path_parts) >= 2:
            owner = path_parts[0]
            repo = path_parts[1]
            return [
                f"https://raw.githubusercontent.com/{owner}/{repo}/main/ckan_ecosystem.yaml",
                f"https://raw.githubusercontent.com/{owner}/{repo}/master/ckan_ecosystem.yaml"
            ]

        return []

    def fetch_yaml_from_github(self, github_url: str) -> Optional[Dict]:
        """
        Fetch and parse ckan_ecosystem.yaml from a GitHub repository.

        Returns:
            Parsed YAML content dict, or None if not found.
        """
        print(f"  Looking for ckan_ecosystem.yaml in repository...")

        raw_urls = self.construct_raw_yaml_urls(github_url)

        for raw_url in raw_urls:
            try:
                print(f"    Trying: {raw_url}")
                response = self.session.get(raw_url, timeout=10)

                if response.status_code == 200:
                    yaml_content = yaml.safe_load(response.text)
                    print(f"  Found and parsed ckan_ecosystem.yaml")
                    return yaml_content

            except Exception:
                continue

        print(f"  ckan_ecosystem.yaml not found in repository")
        return None

    def map_yaml_to_ckan_fields(self, yaml_data: Dict, existing_data: Dict) -> Dict:
        """
        Map YAML metadata fields to CKAN package fields.

        Args:
            yaml_data: Parsed YAML metadata.
            existing_data: Existing package data from CKAN.

        Returns:
            Updated package data ready for CKAN API.
        """
        print("  Mapping YAML metadata to CKAN fields...")

        updated_data = existing_data.copy()

        # Direct field mappings
        field_mapping = {
            'title': 'title',
            'notes': 'notes',
            'detailed_info': 'detailed_info',
            'publisher': 'publisher',
            'extension_type': 'extension_type',
            'license': 'license',
            'contact_name': 'contact_name',
            'contact_email': 'contact_email',
            'url': 'url',
            'organization_url': 'organization_url',
        }

        for yaml_key, ckan_key in field_mapping.items():
            if yaml_key in yaml_data and yaml_data[yaml_key]:
                value = yaml_data[yaml_key]
                if isinstance(value, str):
                    value = value.strip()
                if value:
                    updated_data[ckan_key] = value
                    print(f"    Updated {ckan_key}")

        # Handle tags (list -> CKAN tag dicts)
        if 'tags' in yaml_data and yaml_data['tags']:
            tags = yaml_data['tags']
            if isinstance(tags, list):
                tags = [tag for tag in tags if tag and str(tag).strip()]
                if tags:
                    updated_data['tags'] = [{'name': str(tag).strip()} for tag in tags]
                    print(f"    Updated tags: {', '.join(str(t) for t in tags)}")

        # Handle CKAN version compatibility
        if 'ckan_version' in yaml_data and yaml_data['ckan_version']:
            versions = yaml_data['ckan_version']
            if isinstance(versions, list):
                versions = [str(v) for v in versions if v]
                if versions:
                    updated_data['ckan_version'] = versions
                    print(f"    Updated CKAN versions: {', '.join(versions)}")

        return updated_data

    def update_catalog_extension(self, package_data: Dict) -> bool:
        """
        Update extension in CKAN catalog via package_patch API.

        Returns:
            True if successful.
        """
        print(f"  Updating catalog for: {package_data.get('name')}")

        try:
            response = self.session.post(
                f"{self.api_base}/package_patch",
                data=json.dumps(package_data),
                headers={'Content-Type': 'application/json'}
            )

            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    print(f"  Successfully updated extension in catalog")
                    return True
                else:
                    error = data.get('error', {})
                    print(f"  API returned error: {error}")
                    return False
            else:
                print(f"  HTTP error: {response.status_code}")
                print(f"    Response: {response.text[:200]}")
                return False

        except Exception as e:
            print(f"  Error updating catalog: {str(e)}")
            return False

    def process_extension(self, extension_identifier: str, auto_confirm: bool = False) -> bool:
        """
        Complete workflow to process a single extension.

        Args:
            extension_identifier: Extension name or catalog URL.
            auto_confirm: Skip interactive confirmation prompt.

        Returns:
            True if successful.
        """
        print(f"\n{'='*70}")
        print(f"Processing: {extension_identifier}")
        print(f"{'='*70}\n")

        # Step 1: Get extension from catalog
        catalog_data = self.get_extension_from_catalog(extension_identifier)
        if not catalog_data:
            return False

        # Step 2: Get GitHub URL
        github_url = catalog_data.get('url', '')
        if not github_url or 'github.com' not in github_url:
            print(f"  No GitHub URL found in catalog metadata")
            return False

        print(f"  GitHub URL: {github_url}")

        # Step 3: Fetch YAML from GitHub
        yaml_data = self.fetch_yaml_from_github(github_url)
        if not yaml_data:
            print(f"  Skipping update - no ckan_ecosystem.yaml found")
            return False

        # Step 4: Map YAML to CKAN fields
        updated_data = self.map_yaml_to_ckan_fields(yaml_data, catalog_data)

        # Step 5: Preview changes
        print(f"\n  Metadata Preview:")
        print(f"    Title: {updated_data.get('title', 'N/A')}")
        print(f"    Type: {updated_data.get('extension_type', 'N/A')}")
        print(f"    License: {updated_data.get('license', 'N/A')}")
        print(f"    Publisher: {updated_data.get('publisher', 'N/A')}")
        ckan_versions = updated_data.get('ckan_version', [])
        if isinstance(ckan_versions, list):
            print(f"    CKAN Versions: {', '.join(str(v) for v in ckan_versions) or 'N/A'}")

        # Step 6: Confirm update
        if not auto_confirm:
            confirm = input(f"\n  Update this extension in the catalog? (y/n): ").strip().lower()
            if confirm != 'y':
                print(f"  Skipped by user")
                return False

        # Step 7: Update catalog
        success = self.update_catalog_extension(updated_data)

        if success:
            print(f"\n  Extension updated successfully!")
            print(f"    View at: {self.base_url}/extension/{updated_data.get('name')}")

        # Brief pause between API calls
        time.sleep(0.5)

        return success


def get_extensions_to_process(updater: EcosystemYAMLUpdater) -> List[str]:
    """
    Interactive prompt to get list of extensions to process.

    Returns:
        List of extension identifiers (names or URLs).
    """
    print("\n" + "="*70)
    print("CKAN Ecosystem YAML Metadata Updater")
    print("="*70)
    print("\nThis tool fetches metadata from ckan_ecosystem.yaml files in")
    print("extension repositories and updates the CKAN ecosystem catalog.")
    print("\nYou can provide:")
    print("  - Extension names (e.g., ckanext-ecosystem-test)")
    print("  - Catalog URLs (e.g., https://ecosystem.ckan.org/extension/ckanext-ecosystem-test)")
    print("  - Multiple extensions separated by commas")
    print("  - 'all' to process all extensions in the catalog")
    print("\n" + "="*70 + "\n")

    user_input = input("Which extension(s) would you like to update? ").strip()

    if not user_input:
        print("No input provided")
        return []

    if user_input.lower() == 'all':
        return updater.get_all_extensions()

    # Split by comma and clean up
    extensions = [ext.strip() for ext in user_input.split(',')]
    extensions = [ext for ext in extensions if ext]

    return extensions


def main():
    """Main execution function"""

    # Check for API key
    if not CKAN_API_KEY:
        print("WARNING: CKAN_API_KEY not set!")
        print("  Set it via environment variable or you may not be able to update the catalog.")
        print("  Example: export CKAN_API_KEY='your-api-key-here'\n")

    # Initialize updater
    updater = EcosystemYAMLUpdater()

    auto_confirm = os.getenv('AUTO_CONFIRM', '').lower() == 'true'

    # Get extensions to process (from stdin in CI, or interactive prompt)
    if not sys.stdin.isatty():
        # Non-interactive mode: read from stdin
        user_input = sys.stdin.read().strip()
        if not user_input:
            print("No extensions provided via stdin")
            return
        if user_input.lower() == 'all':
            extensions = updater.get_all_extensions()
        else:
            extensions = [ext.strip() for ext in user_input.split(',') if ext.strip()]
        auto_confirm = True
    else:
        extensions = get_extensions_to_process(updater)

    if not extensions:
        print("No extensions to process")
        return

    print(f"\nExtensions to process: {len(extensions)}")
    for ext in extensions:
        print(f"  - {ext}")

    # Process each extension
    success_count = 0
    failed_count = 0

    for extension in extensions:
        try:
            success = updater.process_extension(extension, auto_confirm=auto_confirm)
            if success:
                success_count += 1
            else:
                failed_count += 1
        except KeyboardInterrupt:
            print("\n\nProcess interrupted by user")
            break
        except Exception as e:
            print(f"\nUnexpected error processing {extension}: {str(e)}")
            failed_count += 1

    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(f"Successfully updated: {success_count}")
    print(f"Failed: {failed_count}")
    print(f"Total processed: {success_count + failed_count}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
