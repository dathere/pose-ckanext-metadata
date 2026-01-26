#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import pandas as pd
import re
from config import USER_AGENT, CKAN_BASE_URL

class SimpleGitHubExtractor:
    def __init__(self):
        self.base_url = CKAN_BASE_URL
        self.api_base = f"{self.base_url}/api/3/action"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': USER_AGENT
        })
        
    def extract_github_url(self, text):
        """Extract first GitHub URL from text"""
        if not text:
            return ""
        
        # Simple regex for GitHub URLs
        pattern = r'https?://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/?'
        match = re.search(pattern, str(text), re.IGNORECASE)
        
        if match:
            url = match.group(0)
            # Clean URL
            url = url.rstrip('/')
            url = url.replace('http://github.com', 'https://github.com')
            return url
        
        return ""
    
    def get_all_extensions(self):
        """Get all extensions with their GitHub URLs"""
        print("Fetching all extensions...")
        
        all_packages = []
        start = 0
        rows = 1000
        
        while True:
            print(f"Fetching batch starting at {start}...")
            
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
            batch_packages = result.get('results', [])
            total_count = result.get('count', 0)
            
            all_packages.extend(batch_packages)
            
            print(f"Fetched {len(all_packages)}/{total_count} extensions")
            
            # Stop if we got fewer results than requested or reached the total
            if len(batch_packages) < rows or len(all_packages) >= total_count:
                break
                
            start += rows
        
        print(f"Total extensions found: {len(all_packages)}")
        
        results = []
        
        for i, pkg in enumerate(all_packages, 1):
            print(f"Processing {i}/{len(all_packages)}: {pkg.get('name', '')}")
            
            # Search all text fields for GitHub URL
            github_url = ""
            
            # Check multiple fields for GitHub URL
            fields_to_check = [
                pkg.get('url', ''),
                pkg.get('notes', ''),
                str(pkg.get('extras', []))
            ]
            
            # Check resources too
            for resource in pkg.get('resources', []):
                fields_to_check.append(resource.get('url', ''))
            
            # Find first GitHub URL
            for field in fields_to_check:
                github_url = self.extract_github_url(field)
                if github_url:
                    break
            
            results.append({
                'catalog_url': f"{self.base_url}/extension/{pkg.get('name', '')}",
                'github_url': github_url
            })
        
        return results
    
    def save_to_csv(self, results, filename):
        """Save results to CSV"""
        df = pd.DataFrame(results)
        df.to_csv(filename, index=False)
        
        # Print summary
        total = len(results)
        with_github = len([r for r in results if r['github_url']])
        
        print(f"\nResults saved to {filename}")
        print(f"Total extensions: {total}")
        print(f"Extensions with GitHub URLs: {with_github}")
        print(f"Success rate: {with_github/total*100:.1f}%")

def main():
    print("Simple CKAN Extension GitHub URL Extractor")
    print("=" * 50)
    
    output_file = f"url_list.csv"
    
    extractor = SimpleGitHubExtractor()
    results = extractor.get_all_extensions()
    
    if results:
        extractor.save_to_csv(results, output_file)
    else:
        print("No results found!")

if __name__ == "__main__":
    main()
