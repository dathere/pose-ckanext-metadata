import csv
import json
import requests
import time
from pathlib import Path
from urllib.parse import urljoin

INPUT_CSV_FILE = "site_urls.csv"
OUTPUT_CSV_FILE = "ckan_stats.csv"

class SimpleCKANExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'CKAN-Stats-Extractor/1.0'})
    
    def normalize_url(self, url: str) -> str:
        url = url.strip()
        if not url:
            return url
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url.rstrip('/')
    
    def make_api_call(self, base_url: str, endpoint: str):
        try:
            api_url = urljoin(base_url + '/', f'api/3/action/{endpoint}')
            response = self.session.get(api_url, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data.get('success', False):
                return data
        except:
            pass
        return None
    
    def get_ckan_stats(self, url: str):
        print(f"Processing: {url}")
        
        normalized_url = self.normalize_url(url)
        if not normalized_url:
            return self.get_empty_stats()
        
        stats = self.get_empty_stats()
        
        # Get number of datasets
        package_data = self.make_api_call(normalized_url, 'package_list')
        if package_data and isinstance(package_data.get('result'), list):
            stats['num_datasets'] = str(len(package_data['result']))
        
        time.sleep(0.5)
        
        # Get number of groups
        group_data = self.make_api_call(normalized_url, 'group_list')
        if group_data and isinstance(group_data.get('result'), list):
            stats['num_groups'] = str(len(group_data['result']))
        
        time.sleep(0.5)
        
        # Get number of organizations
        org_data = self.make_api_call(normalized_url, 'organization_list')
        if org_data and isinstance(org_data.get('result'), list):
            stats['num_organizations'] = str(len(org_data['result']))
        
        return stats
    
    def get_empty_stats(self):
        return {
            'num_datasets': '0',
            'num_groups': '0',
            'num_organizations': '0'
        }
    
    def process_csv(self, input_file: str, output_file: str):
        # Read input file
        with open(input_file, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            
            # Check if fieldnames exist
            if reader.fieldnames is None:
                raise ValueError(f"CSV file '{input_file}' appears to be empty or has no header row")
            
            # Get original fieldnames from input file
            original_fieldnames = list(reader.fieldnames)
            print(f"Original columns: {original_fieldnames}")
            
            # Check if 'url' column exists
            if 'url' not in original_fieldnames:
                raise ValueError(f"Required 'url' column not found in input file. Available columns: {original_fieldnames}")
            
            # Read all rows from input
            rows = list(reader)
        
        # Define the new stats columns that will be added
        stats_columns = [
            'num_datasets', 
            'num_groups', 
            'num_organizations'
        ]
        
        # Create final fieldnames: original columns + new stats columns
        final_fieldnames = original_fieldnames.copy()
        for col in stats_columns:
            if col not in final_fieldnames:
                final_fieldnames.append(col)
        
        print(f"Final columns: {final_fieldnames}")
        print(f"Processing {len(rows)} rows...")
        
        processed_rows = []
        for i, row in enumerate(rows, 1):
            print(f"Processing row {i}/{len(rows)}")
            
            url = row.get('url', '').strip()
            
            # Preserve all original data from the row
            processed_row = row.copy()
            
            if url:
                # Get CKAN stats
                stats = self.get_ckan_stats(url)
                
                # Add stats to the row
                processed_row.update(stats)
                
                print(f"  Datasets: {stats['num_datasets']}, Groups: {stats['num_groups']}, Organizations: {stats['num_organizations']}")
            else:
                print(f"  Warning: Empty URL in row {i}, skipping stats extraction")
                # Add empty stats for rows with no URL
                empty_stats = self.get_empty_stats()
                processed_row.update(empty_stats)
            
            processed_rows.append(processed_row)
            time.sleep(0.5)
        
        # Write output file
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=final_fieldnames)
            writer.writeheader()
            writer.writerows(processed_rows)
        
        print(f"\nProcessing complete!")
        print(f"Input file: {input_file}")
        print(f"Output file: {output_file}")
        print(f"Rows processed: {len(processed_rows)}")
        
        # Show summary stats
        total_datasets = sum(int(row.get('num_datasets', 0)) for row in processed_rows)
        total_groups = sum(int(row.get('num_groups', 0)) for row in processed_rows)
        total_orgs = sum(int(row.get('num_organizations', 0)) for row in processed_rows)
        
        print(f"\nSummary Statistics:")
        print(f"Total CKAN sites processed: {len(processed_rows)}")
        print(f"Total datasets across all sites: {total_datasets:,}")
        print(f"Total groups across all sites: {total_groups:,}")
        print(f"Total organizations across all sites: {total_orgs:,}")

def main():
    print("Simple CKAN Statistics Extractor")
    print("=" * 50)
    
    # Check if input file exists
    if not Path(INPUT_CSV_FILE).exists():
        print(f"ERROR: Input file '{INPUT_CSV_FILE}' not found!")
        return
    
    try:
        extractor = SimpleCKANExtractor()
        extractor.process_csv(INPUT_CSV_FILE, OUTPUT_CSV_FILE)
        print(f"\nSuccess! Results saved to: {OUTPUT_CSV_FILE}")
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()