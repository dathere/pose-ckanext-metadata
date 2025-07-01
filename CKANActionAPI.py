import csv
import json
import requests
import time
from pathlib import Path
from urllib.parse import urljoin

INPUT_CSV_FILE = "10Url.csv"
OUTPUT_CSV_FILE = "output3.csv"

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
        with open(input_file, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        processed_rows = []
        for row in rows:
            url = row.get('URL', '').strip()
            if not url:
                continue
            
            metadata = self.process_ckan_instance(url)
            combined_row = {'URL': url, **metadata}
            processed_rows.append(combined_row)
            time.sleep(1)
        
        fieldnames = ['URL', 'ckan_version', 'description', 'api_title', 'contact_email', 
                     'primary_language', 'extensions', 'num_groups', 'num_organizations', 'num_datasets']
        
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(processed_rows)

def main():
    extractor = CKANMetadataExtractor()
    extractor.process_csv(INPUT_CSV_FILE, OUTPUT_CSV_FILE)

if __name__ == '__main__':
    main()