#!/usr/bin/env python3
"""
Fetch the first 10 CKAN site URLs from ecosystem.ckan.org.
Outputs site_urls.csv with columns: name, url
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import re
import cloudscraper
import pandas as pd
from urllib.parse import urlparse
from config import CKAN_BASE_URL, SESSION_HEADERS

SITE_LIMIT = 10
OUTPUT_FILE = 'site_urls.csv'


class SiteURLExtractor:
    def __init__(self):
        self.base_url = CKAN_BASE_URL
        self.session = cloudscraper.create_scraper()
        self.session.headers.update(SESSION_HEADERS)

    def clean_url(self, url):
        if not url or not isinstance(url, str):
            return ''
        url = url.strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        try:
            if urlparse(url).netloc:
                return url
        except Exception:
            pass
        return ''

    def extract_visit_url(self, package):
        main_url = package.get('url', '')
        if main_url:
            cleaned = self.clean_url(main_url)
            if cleaned:
                return cleaned

        for resource in package.get('resources', []):
            resource_url = resource.get('url', '')
            if resource_url:
                cleaned = self.clean_url(resource_url)
                if cleaned and not any(
                    ext in cleaned.lower()
                    for ext in ['.csv', '.json', '.xml', '.pdf', '.zip', '.xlsx']
                ):
                    return cleaned

        for extra in package.get('extras', []):
            if extra.get('key', '').lower() in ['website', 'homepage', 'site_url', 'portal_url']:
                cleaned = self.clean_url(extra.get('value', ''))
                if cleaned:
                    return cleaned

        notes = package.get('notes', '')
        if notes:
            url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+[^\s<>"{}|\\^`\[\].,;!?]'
            for url in re.findall(url_pattern, notes, re.IGNORECASE):
                cleaned = self.clean_url(url)
                if cleaned and not any(
                    ext in cleaned.lower()
                    for ext in ['.csv', '.json', '.xml', '.pdf', '.zip', '.xlsx']
                ):
                    return cleaned
        return ''

    def get_sites(self, limit):
        print(f"Fetching up to {limit} sites from {self.base_url}...")
        response = self.session.get(
            f"{self.base_url}/api/3/action/package_search",
            params={'q': 'type:site', 'start': 0, 'rows': limit, 'include_private': False}
        )
        if response.status_code != 200 or not response.json().get('success'):
            print(f"API error: {response.status_code}")
            return []

        packages = response.json()['result'].get('results', [])
        print(f"Got {len(packages)} site packages")

        results = []
        for i, pkg in enumerate(packages, 1):
            name = pkg.get('name', '')
            url = self.extract_visit_url(pkg)
            print(f"  {i}. {name}: {url or '(no URL)'}")
            if url:
                results.append({'name': name, 'url': url})

        return results


def main():
    print("=== SITES URL EXTRACTOR (experimental, limit=10) ===")
    extractor = SiteURLExtractor()
    results = extractor.get_sites(SITE_LIMIT)

    if not results:
        print("No sites with URLs found.")
        sys.exit(1)

    df = pd.DataFrame(results)
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"\n✓ Saved {len(df)} sites to {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
