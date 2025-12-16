#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import re
from urllib.parse import urlparse

class SimpleSiteURLExtractor:
    def __init__(self):
        self.base_url = "https://ecosystem.ckan.org"
        self.api_base = f"{self.base_url}/api/3/action"
        
    def clean_url(self, url):
        """Clean and validate URL"""
        if not url or not isinstance(url, str):
            return ""
        
        url = url.strip()
        if not url:
            return ""
        
        # Add protocol if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        try:
            # Parse and validate URL
            parsed = urlparse(url)
            if parsed.netloc:
                return url
        except Exception:
            pass
        
        return ""
    
    def extract_visit_url(self, package):
        """Extract the main visit URL from a site package"""
        # Primary URL field
        main_url = package.get('url', '')
        if main_url:
            cleaned = self.clean_url(main_url)
            if cleaned:
                return cleaned
        
        # Check resources for website URLs
        for resource in package.get('resources', []):
            resource_url = resource.get('url', '')
            if resource_url:
                cleaned = self.clean_url(resource_url)
                if cleaned:
                    # Skip obvious file downloads
                    if not any(ext in cleaned.lower() for ext in ['.csv', '.json', '.xml', '.pdf', '.zip', '.xlsx']):
                        return cleaned
        
        # Check extras for website/homepage URLs
        for extra in package.get('extras', []):
            if extra.get('key', '').lower() in ['website', 'homepage', 'site_url', 'portal_url']:
                extra_url = extra.get('value', '')
                if extra_url:
                    cleaned = self.clean_url(extra_url)
                    if cleaned:
                        return cleaned
        
        # Look for URLs in notes/description
        notes = package.get('notes', '')
        if notes:
            # Simple URL extraction from text
            url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+[^\s<>"{}|\\^`\[\].,;!?]'
            urls = re.findall(url_pattern, notes, re.IGNORECASE)
            for url in urls:
                cleaned = self.clean_url(url)
                if cleaned and not any(ext in cleaned.lower() for ext in ['.csv', '.json', '.xml', '.pdf', '.zip', '.xlsx']):
                    return cleaned
        
        return ""
    
    def get_all_sites(self):
        """Get all sites with their visit URLs"""
        print("Fetching all sites...")
        
        all_packages = []
        start = 0
        rows = 100
        
        while True:
            print(f"Fetching batch starting at {start}...")
            
            response = requests.get(
                f"{self.api_base}/package_search",
                params={
                    'q': 'type:site',
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
            batch_packages = result.get('results', [])
            total_count = result.get('count', 0)
            
            all_packages.extend(batch_packages)
            
            print(f"Fetched {len(all_packages)}/{total_count} sites")

            
            # Stop if we got fewer results than requested or reached the total
            if len(batch_packages) < rows or len(all_packages) >= total_count:
                break
                
            start += rows
        
        print(f"Total sites found: {len(all_packages)}")
        
        results = []
        
        for i, pkg in enumerate(all_packages, 1):
            site_name = pkg.get('name', '')
            site_title = pkg.get('title', '')
            print(f"Processing {i}/{len(all_packages)}: {site_name}")
            
            # Extract the visit URL
            visit_url = self.extract_visit_url(pkg)
            
            # Get organization info
            org_info = pkg.get('organization', {})
            organization = org_info.get('name', '') if org_info else ''
            
            results.append({
                'name': site_name,
                'url': visit_url
            })
        
        return results
    
    def save_to_csv(self, results, filename):
        """Save results to CSV"""
        # Filter out empty URLs
        filtered_results = [r for r in results if r['url']]
        
        df = pd.DataFrame(filtered_results)
        df.to_csv(filename, index=False, encoding='utf-8')
        
        # Print summary
        total = len(results)
        with_urls = len(filtered_results)
        
        print(f"\nResults saved to {filename}")
        print(f"Total sites processed: {total}")
        print(f"Sites with URLs: {with_urls}")
        print(f"Success rate: {with_urls/total*100:.1f}%")
        
        # Show some examples
        print(f"\nFirst 5 sites with URLs:")
        for i, result in enumerate(filtered_results[:5], 1):
            print(f"  {i}. {result['name']}: {result['url']}")

def main():
    print("Simple CKAN Site URL Extractor")
    print("=" * 40)
    
    output_file = "site_urls.csv"
    
    extractor = SimpleSiteURLExtractor()
    results = extractor.get_all_sites()
    
    if results:
        extractor.save_to_csv(results, output_file)
    else:
        print("No results found!")

if __name__ == "__main__":
    main()
