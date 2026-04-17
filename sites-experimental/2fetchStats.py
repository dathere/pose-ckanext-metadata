#!/usr/bin/env python3
"""
Fetch CKAN stats for each site in site_urls.csv.
Outputs ckan_stats.csv with columns: tstamp, name, url, num_datasets,
num_groups, num_organizations, ckan_version, extensions
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ssl
import csv
import json
import logging
import urllib3
import cloudscraper
import requests.adapters
from pathlib import Path
from urllib.parse import urljoin
from datetime import datetime, UTC
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List
from config import SESSION_HEADERS

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

INPUT_FILE = 'site_urls.csv'
OUTPUT_FILE = 'ckan_stats.csv'
MAX_WORKERS = 10
REQUEST_TIMEOUT = 20
RETRY_ATTEMPTS = 2

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class SSLIgnoreAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        kwargs['ssl_context'] = ctx
        super().init_poolmanager(*args, **kwargs)


class CKANStatsExtractor:
    def __init__(self):
        self.session = self._create_session()

    def _create_session(self):
        session = cloudscraper.create_scraper()
        session.headers.update(SESSION_HEADERS)
        adapter = SSLIgnoreAdapter()
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        return session

    def normalize_url(self, url: str) -> str:
        url = url.strip()
        if not url:
            return url
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url.rstrip('/')

    def make_api_call(self, base_url: str, endpoint: str, params: Dict = None) -> Dict:
        urls_to_try = [base_url]
        if base_url.startswith('https://'):
            urls_to_try.append('http://' + base_url[len('https://'):])

        for url in urls_to_try:
            api_url = urljoin(url + '/', f'api/3/action/{endpoint}')
            for attempt in range(1, RETRY_ATTEMPTS + 2):
                try:
                    response = self.session.get(api_url, timeout=REQUEST_TIMEOUT, params=params)
                    response.raise_for_status()
                    data = response.json()
                    if data.get('success', False):
                        return data
                    return None
                except Exception as e:
                    logger.debug(f"Attempt {attempt} failed for {api_url}: {type(e).__name__}: {e}")
                    if attempt <= RETRY_ATTEMPTS:
                        continue
        return None

    def empty_stats(self) -> Dict:
        return {
            'num_datasets': '0',
            'num_groups': '0',
            'num_organizations': '0',
            'ckan_version': '',
            'extensions': '',
            'tstamp': datetime.now(UTC).strftime('%Y-%m-%d')
        }

    def get_stats(self, url: str) -> Dict:
        normalized = self.normalize_url(url)
        if not normalized:
            return self.empty_stats()

        stats = self.empty_stats()

        package_data = self.make_api_call(normalized, 'package_search', params={'rows': 0})
        if package_data and isinstance(package_data.get('result'), dict):
            stats['num_datasets'] = str(package_data['result'].get('count', 0))

        group_data = self.make_api_call(normalized, 'group_list')
        if group_data and isinstance(group_data.get('result'), list):
            stats['num_groups'] = str(len(group_data['result']))

        org_data = self.make_api_call(normalized, 'organization_list')
        if org_data and isinstance(org_data.get('result'), list):
            stats['num_organizations'] = str(len(org_data['result']))

        status_data = self.make_api_call(normalized, 'status_show')
        if status_data and isinstance(status_data.get('result'), dict):
            result = status_data['result']
            stats['ckan_version'] = result.get('ckan_version', '')
            extensions = result.get('extensions', [])
            if extensions:
                stats['extensions'] = json.dumps(extensions)

        stats['tstamp'] = datetime.now(UTC).strftime('%Y-%m-%d')
        return stats

    def process_row(self, row: Dict, index: int, total: int) -> Dict:
        url = row.get('url', '').strip()
        result = row.copy()
        try:
            stats = self.get_stats(url) if url else self.empty_stats()
            result.update(stats)
            logger.info(
                f"[{index}/{total}] {url[:50]:50s} | "
                f"D:{stats['num_datasets']:>4s} G:{stats['num_groups']:>3s} "
                f"O:{stats['num_organizations']:>3s} V:{stats['ckan_version']}"
            )
        except Exception as e:
            logger.error(f"[{index}/{total}] Failed: {url} — {e}")
            result.update(self.empty_stats())
        return result

    def process_csv(self, input_file: str, output_file: str):
        with open(input_file, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError(f"'{input_file}' is empty or has no header")
            if 'url' not in reader.fieldnames:
                raise ValueError("'url' column not found in input file")
            rows = list(reader)

        stats_columns = ['tstamp', 'num_datasets', 'num_groups', 'num_organizations', 'ckan_version', 'extensions']
        fieldnames = ['tstamp'] + [c for c in reader.fieldnames if c != 'tstamp']
        for col in stats_columns:
            if col not in fieldnames:
                fieldnames.append(col)

        logger.info(f"Processing {len(rows)} sites with {MAX_WORKERS} workers...")
        processed = [None] * len(rows)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(self.process_row, row, i + 1, len(rows)): i
                for i, row in enumerate(rows)
            }
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    processed[idx] = future.result()
                except Exception as e:
                    logger.error(f"Task failed for row {idx + 1}: {e}")
                    processed[idx] = rows[idx].copy()
                    processed[idx].update(self.empty_stats())

        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(processed)

        logger.info(f"✓ Saved {len(processed)} rows to {output_file}")


def main():
    logger.info("=== CKAN STATS FETCHER (experimental) ===")
    if not Path(INPUT_FILE).exists():
        logger.error(f"'{INPUT_FILE}' not found")
        sys.exit(1)

    extractor = CKANStatsExtractor()
    extractor.process_csv(INPUT_FILE, OUTPUT_FILE)


if __name__ == '__main__':
    main()
