import csv
import json
import requests
import time
from pathlib import Path
from urllib.parse import urljoin

INPUT_CSV_FILE = "1.csv"
OUTPUT_CSV_FILE = "2.csv"

class CKANMetadataExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'CKAN-Metadata-Extractor/1.0'})
    
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
    
    def process_ckan_instance(self, url: str):
        print(f"Processing: {url}")
        
        normalized_url = self.normalize_url(url)
        if not normalized_url:
            return self.get_empty_result()
        
        result = self.get_empty_result()
        
        status_data = self.make_api_call(normalized_url, 'status_show')
        if status_data and status_data.get('result'):
            api_result = status_data['result']
            result['ckan_version'] = str(api_result.get('ckan_version', ''))
            result['description'] = str(api_result.get('site_description', ''))
            result['api_title'] = str(api_result.get('site_title', ''))
            contact_email = api_result.get('error_emails_to')
            result['contact_email'] = str(contact_email) if contact_email else ''
            result['primary_language'] = str(api_result.get('locale_default', ''))
            extensions = api_result.get('extensions', [])
            if isinstance(extensions, list):
                result['extensions'] = ', '.join(extensions)
            else:
                result['extensions'] = str(extensions) if extensions else ''
        
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
            'ckan_version': '',
            'description': '',
            'api_title': '',
            'contact_email': '',
            'primary_language': '',
            'extensions': '',
            'num_groups': '0',
            'num_organizations': '0',
            'num_datasets': '0'
        }
    
    def process_csv(self, input_file: str, output_file: str):
        # Read input file and preserve original structure
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
        
        # Define the new metadata columns that will be added
        metadata_columns = [
            'ckan_version', 
            'description', 
            'api_title', 
            'contact_email',
            'primary_language', 
            'extensions', 
            'num_groups', 
            'num_organizations', 
            'num_datasets'
        ]
        
        # Create final fieldnames: original columns + new metadata columns
        # Only add metadata columns that don't already exist
        final_fieldnames = original_fieldnames.copy()
        for col in metadata_columns:
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
                # Get CKAN metadata
                metadata = self.process_ckan_instance(url)
                
                # Add metadata to the row (will overwrite if columns already exist)
                processed_row.update(metadata)
            else:
                print(f"  Warning: Empty URL in row {i}, skipping metadata extraction")
                # Add empty metadata for rows with no URL
                empty_metadata = self.get_empty_result()
                processed_row.update(empty_metadata)
            
            processed_rows.append(processed_row)
            time.sleep(5)
        
        # Write output file with preserved structure + new metadata
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=final_fieldnames)
            writer.writeheader()
            writer.writerows(processed_rows)
        
        print(f"\nProcessing complete!")
        print(f"Input file: {input_file}")
        print(f"Output file: {output_file}")
        print(f"Rows processed: {len(processed_rows)}")
        print(f"Original columns: {len(original_fieldnames)}")
        print(f"Final columns: {len(final_fieldnames)}")
        
        # Show which columns were added
        added_columns = [col for col in final_fieldnames if col not in original_fieldnames]
        if added_columns:
            print(f"Added columns: {added_columns}")

def main():
    print("CKAN Metadata Extractor")
    print("=" * 50)
    
    # Check if input file exists
    if not Path(INPUT_CSV_FILE).exists():
        print(f"ERROR: Input file '{INPUT_CSV_FILE}' not found!")
        return
    
    try:
        extractor = CKANMetadataExtractor()
        extractor.process_csv(INPUT_CSV_FILE, OUTPUT_CSV_FILE)
        print(f"\nSuccess! Results saved to: {OUTPUT_CSV_FILE}")
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()