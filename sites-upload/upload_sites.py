import os
import requests
from requests_toolbelt import MultipartEncoder
from config_sites import Config
import csv
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ckanConfig = Config.get('ckan')


def action(action, data_dict, file_dict=None):
    """
    Perform CKAN API action with optional file upload
    """
    fields = data_dict

    if file_dict:
        data_dict['upload'] = (
            file_dict.get('file_name'),
            open(os.path.abspath(file_dict.get('path')), 'rb'),
            'application/octet-stream'
        )
        fields = dict(data_dict)
        m = MultipartEncoder(fields=fields)
        r = requests.post(
            ckanConfig.get('url') + '/api/action/' + action,
            data=m,
            headers={
                'content-type': m.content_type,
                'Authorization': ckanConfig.get('api_key')
            }
        )
    else:
        fields = {}
        for key, value in data_dict.items():
            if key == 'tags' or key == 'extensions':
                fields[key] = value
            else:
                fields[key] = str(value) if value else ''
        
        r = requests.post(
            ckanConfig.get('url') + '/api/action/' + action,
            json=fields,
            headers={
                'Content-Type': 'application/json',
                'Authorization': ckanConfig.get('api_key')
            }
        )

    print(r.json())
    print("\n")

    return r


def parse_extensions(extensions_string):
    """
    Parse extensions from comma-separated string or list
    """
    if not extensions_string:
        return []
    
    if isinstance(extensions_string, list):
        return extensions_string
    
    # Split by commas and clean up
    extensions_list = [ext.strip() for ext in extensions_string.split(',') if ext.strip()]
    return extensions_list


def validate_boolean(value):
    """
    Convert various boolean representations to string 'true' or 'false'
    """
    if isinstance(value, bool):
        return 'true' if value else 'false'
    
    if isinstance(value, str):
        value_lower = value.lower().strip()
        if value_lower in ['true', 'yes', '1', 'y']:
            return 'true'
        elif value_lower in ['false', 'no', '0', 'n']:
            return 'false'
    
    return value


def validate_int(value):
    """
    Validate and convert to integer, return None if invalid
    """
    if not value or value == '':
        return None
    
    try:
        return int(value)
    except (ValueError, TypeError):
        logger.warning(f"Invalid integer value: {value}")
        return None


def check_organization_exists(org_name):
    """
    Check if an organization exists in the CKAN instance
    """
    if not org_name:
        return False
    
    try:
        response = action('organization_show', {'id': org_name})
        return True
    except Exception as e:
        logger.warning(f"Organization {org_name} not found: {e}")
        return False


if __name__ == '__main__':
    CSVFilePath = Config.get('sites_metadata_filepath', 'sites_metadata.csv')
    
    logger.info(f"Starting CKAN sites upload from {CSVFilePath}")
    
    with open(CSVFilePath, encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        row_count = 0
        success_count = 0
        error_count = 0
        
        for row in reader:
            row_count += 1
            time.sleep(5)  # Rate limiting
            
            try:
                logger.info(f"Processing row {row_count}: {row.get('title', 'Unknown')}")
                
                # Parse tags
                tags_list = []
                if row.get('tag_string'):
                    tags_list = [{'name': tag.strip()} for tag in row['tag_string'].split(',') if tag.strip()]
                
                # Parse extensions as multiple_text preset expects a list
                extensions_list = parse_extensions(row.get('extensions', ''))
                
                # Validate numeric fields
                num_datasets = validate_int(row.get('num_datasets'))
                num_organizations = validate_int(row.get('num_organizations'))
                num_groups = validate_int(row.get('num_groups'))
                
                # Validate latitude and longitude
                latitude = row.get('latitude', '').strip()
                longitude = row.get('longitude', '').strip()
                
                # Build the dataset dictionary
                dataset_dict = {
                    'type': 'site',  # Important: specify the dataset type
                    'name': row.get('name', ''),  # URL slug
                    'title': row.get('title', ''),
                    'notes': row.get('notes', ''),
                    'url': row.get('url', ''),
                    'public_site': validate_boolean(row.get('public_site', '')),
                    'location': row.get('location', ''),
                    'region': row.get('region', ''),
                    'latitude': latitude,
                    'longitude': longitude,
                    'site_type': row.get('site_type', ''),
                    'status': row.get('status', ''),
                    'git_repo': row.get('git_repo', ''),
                    'ckan_version': row.get('ckan_version', ''),
                    'extensions': extensions_list,
                    'contact_email': row.get('contact_email', ''),
                    'tags': tags_list,
                    'language': row.get('language', ''),
                    'is_featured': row.get('is_featured', ''),
                    'topic_id': row.get('topic_id', ''),
                }
                
                # Add numeric fields only if they have valid values
                if num_datasets is not None:
                    dataset_dict['num_datasets'] = num_datasets
                if num_organizations is not None:
                    dataset_dict['num_organizations'] = num_organizations
                if num_groups is not None:
                    dataset_dict['num_groups'] = num_groups
                
                # Handle organization if provided
                if row.get('owner_org'):
                    org_name = row.get('owner_org').strip()
                    if check_organization_exists(org_name):
                        dataset_dict['owner_org'] = org_name
                    else:
                        logger.warning(f"Organization '{org_name}' not found, creating site without organization")
                
                # Remove empty string values to avoid CKAN validation issues
                dataset_dict = {k: v for k, v in dataset_dict.items() if v != '' and v is not None}
                
                # Check if we're updating or creating
                if row.get('package_id'):
                    # Update existing dataset
                    dataset_dict['id'] = row['package_id']
                    response = action('package_patch', dataset_dict)
                    logger.info(f"Updated site: {dataset_dict.get('name')}")
                else:
                    # Create new dataset
                    response = action('package_create', dataset_dict)
                    logger.info(f"Created site: {dataset_dict.get('name')}")
                
                # Handle screenshot resource if provided
                if row.get('screenshot_url') or row.get('screenshot_path'):
                    resource_dict = {
                        'package_id': dataset_dict.get('name'),
                        'name': row.get('screenshot_caption', 'Screenshot'),
                        'url': row.get('screenshot_url', ''),
                    }
                    
                    # If local file path is provided, upload it
                    if row.get('screenshot_path') and os.path.exists(row.get('screenshot_path')):
                        file_dict = {
                            'file_name': os.path.basename(row.get('screenshot_path')),
                            'path': row.get('screenshot_path')
                        }
                        response = action('resource_create', resource_dict, file_dict)
                    else:
                        response = action('resource_create', resource_dict)
                    
                    logger.info(f"Added screenshot resource for: {dataset_dict.get('name')}")
                
                success_count += 1
                
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing row {row_count}: {e}")
                logger.error(f"Failed row data: {row}")
        
        # Summary
        logger.info("\n" + "="*50)
        logger.info(f"Upload Summary:")
        logger.info(f"Total rows processed: {row_count}")
        logger.info(f"Successful uploads: {success_count}")
        logger.info(f"Failed uploads: {error_count}")
        logger.info("="*50)