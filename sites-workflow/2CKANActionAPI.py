import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import csv
import cloudscraper
import json
from pathlib import Path
from urllib.parse import urljoin
from datetime import datetime, UTC
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List
import logging
from config import USER_AGENT

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

INPUT_CSV_FILE = "site_urls.csv"
OUTPUT_CSV_FILE = "ckan_stats.csv"
MAX_WORKERS = 10  # Number of concurrent threads
REQUEST_TIMEOUT = 15  # Reduced timeout for faster failures
RETRY_ATTEMPTS = 2  # Number of retries for failed requests

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class SimpleCKANExtractor:
    def __init__(self):
        self.session = self._create_session()

    def _create_session(self) -> cloudscraper.CloudScraper:
        """Create a cloudscraper session to bypass Cloudflare"""
        session = cloudscraper.create_scraper()
        session.headers.update({'User-Agent': USER_AGENT})
        session.verify = False

        return session
    
    def normalize_url(self, url: str) -> str:
        """Normalize URL format"""
        url = url.strip()
        if not url:
            return url
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url.rstrip('/')
    
    def make_api_call(self, base_url: str, endpoint: str) -> Dict:
        """Make API call with error handling"""
        try:
            api_url = urljoin(base_url + '/', f'api/3/action/{endpoint}')
            response = self.session.get(api_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            if data.get('success', False):
                return data
        except Exception:
            pass
        return None
    
    def get_ckan_stats(self, url: str) -> Dict:
        """Extract CKAN statistics from a single URL"""
        normalized_url = self.normalize_url(url)
        if not normalized_url:
            return self.get_empty_stats()
        
        stats = self.get_empty_stats()
        
        # Get number of datasets
        package_data = self.make_api_call(normalized_url, 'package_list')
        if package_data and isinstance(package_data.get('result'), list):
            stats['num_datasets'] = str(len(package_data['result']))
        
        # Get number of groups
        group_data = self.make_api_call(normalized_url, 'group_list')
        if group_data and isinstance(group_data.get('result'), list):
            stats['num_groups'] = str(len(group_data['result']))
        
        # Get number of organizations
        org_data = self.make_api_call(normalized_url, 'organization_list')
        if org_data and isinstance(org_data.get('result'), list):
            stats['num_organizations'] = str(len(org_data['result']))
        
        # Get CKAN version and extensions
        status_data = self.make_api_call(normalized_url, 'status_show')
        if status_data and isinstance(status_data.get('result'), dict):
            result = status_data['result']
            stats['ckan_version'] = result.get('ckan_version', '')
            extensions = result.get('extensions', [])
            if extensions:
                stats['extensions'] = json.dumps(extensions)
        
        stats['tstamp'] = datetime.now(UTC).strftime('%Y-%m-%d')
        
        return stats
    
    def get_empty_stats(self) -> Dict:
        """Return empty stats structure"""
        return {
            'num_datasets': '0',
            'num_groups': '0',
            'num_organizations': '0',
            'ckan_version': '',
            'extensions': '',
            'tstamp': datetime.now(UTC).strftime('%Y-%m-%d')
        }
    
    def process_single_row(self, row: Dict, index: int, total: int) -> Dict:
        """Process a single row"""
        url = row.get('url', '').strip()
        processed_row = row.copy()
        
        if url:
            try:
                stats = self.get_ckan_stats(url)
                processed_row.update(stats)
                logger.info(
                    f"[{index}/{total}] {url[:50]:50s} | "
                    f"D:{stats['num_datasets']:>4s} G:{stats['num_groups']:>3s} "
                    f"O:{stats['num_organizations']:>3s} V:{stats['ckan_version']}"
                )
            except Exception as e:
                logger.error(f"[{index}/{total}] Failed: {url} - {str(e)}")
                processed_row.update(self.get_empty_stats())
        else:
            logger.warning(f"[{index}/{total}] Empty URL, skipping")
            processed_row.update(self.get_empty_stats())
        
        return processed_row
    
    def process_csv(self, input_file: str, output_file: str):
        """Process CSV file with concurrent execution"""
        # Read input file
        with open(input_file, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            
            if reader.fieldnames is None:
                raise ValueError(f"CSV file '{input_file}' appears to be empty or has no header row")
            
            original_fieldnames = list(reader.fieldnames)
            
            if 'url' not in original_fieldnames:
                raise ValueError(f"Required 'url' column not found in input file")
            
            rows = list(reader)
        
        # Define stats columns
        stats_columns = [
            'tstamp',
            'num_datasets', 
            'num_groups', 
            'num_organizations',
            'ckan_version',
            'extensions'
        ]
        
        # Create final fieldnames
        final_fieldnames = ['tstamp']
        for col in original_fieldnames:
            if col != 'tstamp' and col not in final_fieldnames:
                final_fieldnames.append(col)
        for col in stats_columns:
            if col != 'tstamp' and col not in final_fieldnames:
                final_fieldnames.append(col)
        
        logger.info(f"Processing {len(rows)} rows with {MAX_WORKERS} workers...")
        
        # Process rows concurrently
        processed_rows = [None] * len(rows)  # Pre-allocate to maintain order
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_index = {
                executor.submit(self.process_single_row, row, i + 1, len(rows)): i
                for i, row in enumerate(rows)
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    processed_rows[index] = future.result()
                except Exception as e:
                    logger.error(f"Task failed for row {index + 1}: {str(e)}")
                    # Add empty stats for failed rows
                    processed_rows[index] = rows[index].copy()
                    processed_rows[index].update(self.get_empty_stats())
        
        # Write output file
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=final_fieldnames)
            writer.writeheader()
            writer.writerows(processed_rows)
        
        # Calculate and display summary
        self._print_summary(input_file, output_file, processed_rows)
    
    def _print_summary(self, input_file: str, output_file: str, processed_rows: List[Dict]):
        """Print processing summary"""
        total_datasets = sum(int(row.get('num_datasets', 0)) for row in processed_rows)
        total_groups = sum(int(row.get('num_groups', 0)) for row in processed_rows)
        total_orgs = sum(int(row.get('num_organizations', 0)) for row in processed_rows)
        
        sites_with_data = sum(1 for row in processed_rows if int(row.get('num_datasets', 0)) > 0)
        
        logger.info("\n" + "=" * 70)
        logger.info("PROCESSING COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Input file:  {input_file}")
        logger.info(f"Output file: {output_file}")
        logger.info(f"Total rows:  {len(processed_rows)}")
        logger.info(f"Sites with data: {sites_with_data}")
        logger.info(f"\nTotal datasets:      {total_datasets:,}")
        logger.info(f"Total groups:        {total_groups:,}")
        logger.info(f"Total organizations: {total_orgs:,}")
        logger.info("=" * 70)


def main():
    logger.info("=" * 70)
    logger.info("CKAN Statistics Extractor v2.0 (Concurrent)")
    logger.info("=" * 70)
    logger.info(f"Configuration: {MAX_WORKERS} workers, {REQUEST_TIMEOUT}s timeout")
    logger.info("=" * 70)
    
    if not Path(INPUT_CSV_FILE).exists():
        logger.error(f"Input file '{INPUT_CSV_FILE}' not found!")
        return
    
    try:
        extractor = SimpleCKANExtractor()
        extractor.process_csv(INPUT_CSV_FILE, OUTPUT_CSV_FILE)
        logger.info(f"\nSuccess! Results saved to: {OUTPUT_CSV_FILE}")
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
