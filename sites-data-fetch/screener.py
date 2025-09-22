import os
import time
import logging
import glob
import requests
from datetime import datetime
from config import Config

# Set up basic logging
logging.basicConfig(
    filename='screenshot_upload.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define screenshots directory
SCREENSHOTS_DIR = 'screenshots'

def test_ckan_connection():
    """
    Test CKAN connection and configuration
    """
    api_url = Config.get('ckan_url', 'https://catalog.civicdataecosystem.org')
    api_key = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJHNW00ZUF6RFM1SWxzaXRPekVpMWNWcDJaS3hlR25Rc3pUQWlNWkxmMlAydkdwamV1Qk56WlZweEFRQ2lIRVZSZUpfRlB2VGlhQ0plNHZ4QiIsImlhdCI6MTc1MDcxMjYyMn0.fgTa-v9DzMH0UN-F3NZhPYQCITfZOl9SgZeXKgxTTMU'
    
    print("=== TESTING CKAN CONNECTION ===")
    print(f"API URL: {api_url}")
    print(f"API Key: {'✓ Found' if api_key else '✗ Missing'}")
    
    if not api_key:
        print("ERROR: No API key found in configuration!")
        return False
    
    # Test with site_read action (doesn't require authentication)
    test_url = f"{api_url}/api/3/action/site_read"
    try:
        response = requests.get(test_url)
        if response.status_code == 200:
            print("✓ CKAN instance is accessible")
            return True
        else:
            print(f"✗ CKAN instance returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Cannot connect to CKAN: {e}")
        return False

def test_dataset_exists(dataset_id, api_url, api_key):
    """
    Test if a dataset exists before trying to add resources
    """
    package_show_url = f"{api_url}/api/3/action/package_show"
    headers = {'Authorization': api_key}
    
    try:
        response = requests.get(
            package_show_url,
            params={'id': dataset_id},
            headers=headers
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                return True
            else:
                logger.warning(f"Dataset '{dataset_id}' not found: {result.get('error')}")
                return False
        else:
            logger.warning(f"HTTP {response.status_code} checking dataset '{dataset_id}': {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error checking dataset '{dataset_id}': {e}")
        return False

def upload_screenshot_resource(dataset_id, screenshot_path, api_url, api_key):
    """
    Upload a single screenshot as a resource to a dataset
    """
    if not os.path.exists(screenshot_path):
        logger.error(f"Screenshot file not found: {screenshot_path}")
        return False
    
    # Check if dataset exists first
    if not test_dataset_exists(dataset_id, api_url, api_key):
        logger.error(f"Dataset '{dataset_id}' does not exist or is not accessible")
        return False
    
    resource_create_url = f"{api_url}/api/3/action/resource_create"
    
    resource_data = {
        'package_id': dataset_id,
        'name': f"Screenshot of {dataset_id}",
        'description': f"Visual representation of the {dataset_id} data portal",
        'format': 'PNG'
    }
    
    headers = {'Authorization': api_key}
    
    try:
        with open(screenshot_path, 'rb') as file_obj:
            file_size = os.path.getsize(screenshot_path)
            logger.info(f"Uploading {os.path.basename(screenshot_path)} ({file_size} bytes) for dataset '{dataset_id}'")
            
            response = requests.post(
                resource_create_url,
                data=resource_data,
                headers=headers,
                files={'upload': (os.path.basename(screenshot_path), file_obj, 'image/png')}
            )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                resource_id = result['result']['id']
                resource_url = result['result'].get('url', 'N/A')
                logger.info(f"✓ Successfully uploaded screenshot for '{dataset_id}' - Resource ID: {resource_id}")
                print(f"✓ SUCCESS: {dataset_id} - Resource ID: {resource_id}")
                return True
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"✗ API error for '{dataset_id}': {error_msg}")
                print(f"✗ API ERROR: {dataset_id} - {error_msg}")
                return False
        else:
            logger.error(f"✗ HTTP {response.status_code} for '{dataset_id}': {response.text}")
            print(f"✗ HTTP ERROR {response.status_code}: {dataset_id}")
            return False
            
    except Exception as e:
        logger.error(f"✗ Exception uploading '{dataset_id}': {str(e)}")
        print(f"✗ EXCEPTION: {dataset_id} - {str(e)}")
        return False

def add_screenshot_resources(screenshots_dir=SCREENSHOTS_DIR, rate_limit_seconds=1.0, test_mode=False):
    """
    Add screenshot PNG files as resources to existing datasets
    """
    # Get configuration
    api_url = Config.get('ckan_url', 'https://catalog.civicdataecosystem.org')
    api_key = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJHNW00ZUF6RFM1SWxzaXRPekVpMWNWcDJaS3hlR25Rc3pUQWlNWkxmMlAydkdwamV1Qk56WlZweEFRQ2lIRVZSZUpfRlB2VGlhQ0plNHZ4QiIsImlhdCI6MTc1MDcxMjYyMn0.fgTa-v9DzMH0UN-F3NZhPYQCITfZOl9SgZeXKgxTTMU'
    
    if not api_key:
        print("ERROR: No CKAN API key found in configuration!")
        return 0, 1
    
    # Ensure API URL format
    if not api_url.startswith('http'):
        api_url = f"https://{api_url}"
    if api_url.endswith('/'):
        api_url = api_url.rstrip('/')
    
    print(f"Using CKAN URL: {api_url}")
    print(f"Screenshots directory: {screenshots_dir}")
    
    # Test connection first
    if not test_ckan_connection():
        print("Cannot connect to CKAN instance. Aborting.")
        return 0, 1
    
    added = 0
    failed = 0
    total = 0
    
    logger.info(f"Starting screenshot resource additions from {screenshots_dir}")
    
    # Get all PNG files in the screenshots directory
    png_files = glob.glob(os.path.join(screenshots_dir, "*.png"))
    
    if not png_files:
        print(f"No PNG files found in {screenshots_dir}")
        return 0, 0
    
    print(f"Found {len(png_files)} PNG files to process")
    
    if test_mode:
        print("TEST MODE: Processing only first 3 files")
        png_files = png_files[:3]
    
    for png_path in png_files:
        total += 1
        # Extract the dataset name from the filename (without extension)
        filename = os.path.basename(png_path)
        dataset_name = os.path.splitext(filename)[0]
        
        if not dataset_name:
            logger.warning(f"Unable to determine dataset name from {png_path}, skipping")
            failed += 1
            continue
        
        print(f"Processing {total}/{len(png_files)}: {dataset_name}")
        
        # Upload the screenshot
        if upload_screenshot_resource(dataset_name, png_path, api_url, api_key):
            added += 1
        else:
            failed += 1
        
        # Rate limiting
        if rate_limit_seconds > 3:
            time.sleep(rate_limit_seconds)
    
    print(f"\n=== FINAL RESULTS ===")
    print(f"Total files processed: {total}")
    print(f"Successfully added: {added}")
    print(f"Failed: {failed}")
    print(f"Success rate: {(added/total*100):.1f}%" if total > 0 else "N/A")
    
    logger.info(f"Final results - Total: {total}, Added: {added}, Failed: {failed}")
    return added, failed

def debug_single_upload(dataset_id=None, screenshot_path=None):
    """
    Debug a single upload for testing
    """
    if not dataset_id:
        dataset_id = input("Enter dataset ID to test: ").strip()
    if not screenshot_path:
        screenshot_path = input("Enter screenshot path (or press Enter for 'test.png'): ").strip()
        if not screenshot_path:
            screenshot_path = "test.png"
    
    api_url = Config.get('ckan_url', 'https://catalog.civicdataecosystem.org')
    api_key = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJHNW00ZUF6RFM1SWxzaXRPekVpMWNWcDJaS3hlR25Rc3pUQWlNWkxmMlAydkdwamV1Qk56WlZweEFRQ2lIRVZSZUpfRlB2VGlhQ0plNHZ4QiIsImlhdCI6MTc1MDcxMjYyMn0.fgTa-v9DzMH0UN-F3NZhPYQCITfZOl9SgZeXKgxTTMU'
    
    if not api_url.startswith('http'):
        api_url = f"https://{api_url}"
    if api_url.endswith('/'):
        api_url = api_url.rstrip('/')
    
    print(f"\n=== DEBUGGING SINGLE UPLOAD ===")
    print(f"Dataset ID: {dataset_id}")
    print(f"Screenshot: {screenshot_path}")
    print(f"API URL: {api_url}")
    print(f"File exists: {os.path.exists(screenshot_path)}")
    
    if not test_ckan_connection():
        return
    
    success = upload_screenshot_resource(dataset_id, screenshot_path, api_url, api_key)
    print(f"Result: {'SUCCESS' if success else 'FAILED'}")

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "debug":
            # Debug mode - single upload
            debug_single_upload()
        elif sys.argv[1] == "test":
            # Test mode - only process first few files
            rate_limit = float(Config.get('rate_limit_seconds', 1.0))
            added, failed = add_screenshot_resources(rate_limit_seconds=rate_limit, test_mode=True)
        else:
            print("Usage: python script.py [debug|test]")
            print("  debug - Test single upload")
            print("  test  - Process only first 3 files")
            print("  (no args) - Process all files")
    else:
        # Normal mode - process all files
        rate_limit = float(Config.get('rate_limit_seconds', 1.0))
        added, failed = add_screenshot_resources(rate_limit_seconds=rate_limit)
        
        print(f"\n=== SUMMARY ===")
        print(f"Added screenshots to {added} datasets, {failed} failures")