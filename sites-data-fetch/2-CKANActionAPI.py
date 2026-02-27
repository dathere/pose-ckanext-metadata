import csv
import json
import cloudscraper
import time
import logging
import backoff
import argparse
import os
from pathlib import Path
from urllib.parse import urljoin

INPUT_CSV_FILE  = "1.csv"
OUTPUT_CSV_FILE = "2.csv"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ── Retry helper ─────────────────────────────────────────────────────────────

@backoff.on_exception(
    backoff.expo,
    Exception,
    max_tries=4,
    max_time=90,
    on_backoff=lambda d: logger.warning(f"API retry {d['tries']}: {d['exception']}")
)
def _get(session, url, **kwargs):
    """HTTP GET with exponential-backoff retry."""
    return session.get(url, **kwargs)


# ── Main class ───────────────────────────────────────────────────────────────

class CKANMetadataExtractor:
    def __init__(self):
        self.session = cloudscraper.create_scraper()
        self.session.headers.update({'User-Agent': 'CKAN-Metadata-Extractor/1.0'})

    def normalize_url(self, url: str) -> str:
        url = url.strip()
        if not url:
            return url
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url.rstrip('/')

    def make_api_call(self, base_url: str, endpoint: str):
        """Call a CKAN API endpoint, return parsed JSON or None on failure."""
        api_url = urljoin(base_url + '/', f'api/3/action/{endpoint}')
        try:
            response = _get(self.session, api_url, timeout=30, verify=False)
            response.raise_for_status()
            data = response.json()
            if data.get('success', False):
                return data
            logger.warning(f"API returned success=false for {api_url}")
        except Exception as e:
            logger.warning(f"API call failed [{endpoint}] {base_url}: {e}")
        return None

    def process_ckan_instance(self, url: str):
        logger.info(f"Processing: {url}")

        normalized_url = self.normalize_url(url)
        if not normalized_url:
            return self.get_empty_result()

        result = self.get_empty_result()

        status_data = self.make_api_call(normalized_url, 'status_show')
        if status_data and status_data.get('result'):
            api_result = status_data['result']
            result['ckan_version']    = str(api_result.get('ckan_version', ''))
            result['description']     = str(api_result.get('site_description', ''))
            result['api_title']       = str(api_result.get('site_title', ''))
            contact_email             = api_result.get('error_emails_to')
            result['contact_email']   = str(contact_email) if contact_email else ''
            result['primary_language'] = str(api_result.get('locale_default', ''))
            extensions = api_result.get('extensions', [])
            result['extensions'] = ', '.join(extensions) if isinstance(extensions, list) else str(extensions or '')

        time.sleep(1)

        group_data = self.make_api_call(normalized_url, 'group_list')
        if group_data and isinstance(group_data.get('result'), list):
            result['num_groups'] = str(len(group_data['result']))

        time.sleep(1)

        org_data = self.make_api_call(normalized_url, 'organization_list')
        if org_data and isinstance(org_data.get('result'), list):
            result['num_organizations'] = str(len(org_data['result']))

        time.sleep(1)

        package_data = self.make_api_call(normalized_url, 'package_list')
        if package_data and isinstance(package_data.get('result'), list):
            result['num_datasets'] = str(len(package_data['result']))

        return result

    def get_empty_result(self):
        return {
            'ckan_version': '', 'description': '', 'api_title': '',
            'contact_email': '', 'primary_language': '', 'extensions': '',
            'num_groups': '0', 'num_organizations': '0', 'num_datasets': '0'
        }

    def process_csv(self, input_file: str, output_file: str, rows: int = None):
        """Process CSV. Supports resume (skips URLs already present in output)."""

        # ── Load input ───────────────────────────────────────────────────────
        with open(input_file, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise ValueError(f"'{input_file}' is empty or has no header")
            if 'url' not in reader.fieldnames:
                raise ValueError(f"'url' column not found. Got: {reader.fieldnames}")

            original_fieldnames = list(reader.fieldnames)
            all_rows = list(reader)

        if rows:
            all_rows = all_rows[:rows]

        metadata_columns = [
            'ckan_version', 'description', 'api_title', 'contact_email',
            'primary_language', 'extensions', 'num_groups', 'num_organizations', 'num_datasets'
        ]
        final_fieldnames = original_fieldnames.copy()
        for col in metadata_columns:
            if col not in final_fieldnames:
                final_fieldnames.append(col)

        # ── Resume support ───────────────────────────────────────────────────
        processed_urls = set()
        if os.path.exists(output_file):
            with open(output_file, 'r', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    processed_urls.add(row.get('url', '').strip())
            logger.info(f"Resume: {len(processed_urls)} URLs already done, skipping")

        mode = 'a' if processed_urls else 'w'

        logger.info(f"Processing {len(all_rows)} rows...")

        # ── Process rows incrementally ───────────────────────────────────────
        with open(output_file, mode, encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=final_fieldnames)
            if not processed_urls:
                writer.writeheader()

            for i, row in enumerate(all_rows, 1):
                url = row.get('url', '').strip()

                if url and url in processed_urls:
                    continue

                logger.info(f"Row {i}/{len(all_rows)}: {url or '(empty)'}")

                processed_row = row.copy()
                if url:
                    try:
                        metadata = self.process_ckan_instance(url)
                        processed_row.update(metadata)
                    except Exception as e:
                        logger.error(f"Failed on {url}: {e}")
                        processed_row.update(self.get_empty_result())
                else:
                    processed_row.update(self.get_empty_result())

                writer.writerow(processed_row)
                f.flush()
                time.sleep(5)

        logger.info(f"Done. Results saved to {output_file}")


# ── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description='CKAN Metadata Extractor')
    parser.add_argument('--rows', type=int, default=None,
                        help='Maximum number of rows to process')
    parser.add_argument('--input',  default=INPUT_CSV_FILE,  help='Input CSV file')
    parser.add_argument('--output', default=OUTPUT_CSV_FILE, help='Output CSV file')
    return parser.parse_args()


def main():
    args = parse_args()

    print("CKAN Metadata Extractor")
    print("=" * 50)
    print(f"Input:  {args.input}")
    print(f"Output: {args.output}")
    if args.rows:
        print(f"Limit:  {args.rows} rows")

    if not Path(args.input).exists():
        print(f"ERROR: Input file '{args.input}' not found!")
        return

    try:
        extractor = CKANMetadataExtractor()
        extractor.process_csv(args.input, args.output, rows=args.rows)
        print(f"\nSuccess! Results saved to: {args.output}")
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
