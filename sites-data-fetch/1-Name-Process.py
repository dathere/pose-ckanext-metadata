import csv
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
from googletrans import Translator
import time
from typing import Optional, Tuple
import logging
from slugify import slugify
import unicodedata
from langdetect import detect_langs, LangDetectException
import string

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CKANInstanceNameExtractor:
    def __init__(self):
        self.translator = Translator()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # English default patterns to check AFTER translation
        self.english_default_patterns = [
            r'^ckan$',
            r'^welcome\s*(to\s*)?(the\s*)?ckan$',
            r'^ckan\s*[-–]\s*welcome$',
            r'^welcome\s*[-–]\s*ckan$',
            r'^home\s*[-–]\s*ckan$',
            r'^ckan\s*[-–]\s*home$',
            r'^(welcome|home|start|enter|portal|website|site|data|platform|system)$',
            r'^ckan\s*(portal|site|website|data|platform|instance|system)$',
            r'^(portal|site|website|data|platform|instance|system)\s*ckan$',
            r'^(welcome|home)\s*(page|site|portal)?$',
            r'^(data|open\s*data)\s*(portal|platform)?$',
            r'^default\s*(site|portal|title)?$',
            r'^untitled(\s*site)?$',
            r'^no\s*title$',
            r'^example(\s*site)?$',
            r'^test(\s*site)?$',
            r'^demo(\s*site)?$'
        ]
    
    def is_non_english(self, text: str) -> bool:
        """Robustly detect if text is non-English"""
        if not text or not text.strip():
            return False
        
        try:
            # Method 1: Check for non-ASCII characters (quick check)
            non_ascii_chars = sum(1 for char in text if ord(char) > 127)
            total_chars = len(text.strip())
            
            # If more than 10% non-ASCII, likely non-English
            if total_chars > 0 and (non_ascii_chars / total_chars) > 0.1:
                return True
            
            # Method 2: Check for Latin extended characters (covers European languages)
            latin_extended_pattern = r'[àáäâèéëêìíïîòóöôùúüûñçßøåæœÀÁÄÂÈÉËÊÌÍÏÎÒÓÖÔÙÚÜÛÑÇØÅÆŒ]'
            if re.search(latin_extended_pattern, text):
                return True
            
            # Method 3: Use langdetect for more accurate detection
            try:
                # Get language probabilities
                langs = detect_langs(text)
                if langs:
                    # Check if English is the most probable language
                    top_lang = langs[0]
                    if top_lang.lang != 'en' and top_lang.prob > 0.7:
                        return True
                    # If English probability is low, consider it non-English
                    if top_lang.lang == 'en' and top_lang.prob < 0.8:
                        # Check if there are other language candidates
                        if len(langs) > 1:
                            return True
            except LangDetectException:
                pass
            
            # Method 4: Check for common non-English words/patterns
            non_english_indicators = [
                # Spanish
                r'\b(el|la|los|las|de|del|para|por|con|sin|sobre|bajo|entre)\b',
                # French  
                r'\b(le|la|les|de|du|des|pour|avec|sans|sur|sous|dans|entre)\b',
                # German
                r'\b(der|die|das|den|dem|des|für|mit|ohne|auf|unter|zwischen)\b',
                # Italian
                r'\b(il|lo|la|gli|le|di|del|della|per|con|senza|su|sotto|tra)\b',
                # Portuguese
                r'\b(o|a|os|as|do|da|dos|das|para|com|sem|sobre|sob|entre)\b',
                # Dutch
                r'\b(de|het|een|van|voor|met|zonder|op|onder|tussen)\b'
            ]
            
            text_lower = text.lower()
            for pattern in non_english_indicators:
                if re.search(pattern, text_lower):
                    return True
            
        except Exception as e:
            logger.debug(f"Language detection error: {str(e)}")
        
        return False
    
    def is_default_value(self, value: str) -> bool:
        """Check if a value is a default CKAN value (should be called AFTER translation)"""
        if not value:
            return True
        
        cleaned_value = value.lower().strip()
        
        # Remove extra spaces and normalize
        cleaned_value = ' '.join(cleaned_value.split())
        
        # Check against English default patterns
        for pattern in self.english_default_patterns:
            if re.match(pattern, cleaned_value, re.IGNORECASE):
                return True
        
        # Check if it contains CKAN with default context words
        if 'ckan' in cleaned_value:
            # If it's just variations of CKAN with common words, it's likely default
            ckan_with_defaults = [
                'welcome', 'home', 'portal', 'site', 'website', 'platform',
                'data', 'open', 'system', 'instance', 'catalog', 'repository'
            ]
            
            # Remove CKAN and see what's left
            without_ckan = cleaned_value.replace('ckan', '').strip(' -–—')
            
            # If what's left is just a default word, it's a default title
            if without_ckan in ckan_with_defaults:
                return True
        
        # Check if too short to be meaningful (less than 4 chars)
        if len(cleaned_value) < 4 and cleaned_value not in ['nyc', 'la', 'sf', 'uk', 'usa', 'eu']:
            return True
        
        # Check if it's just punctuation and/or CKAN
        if re.match(r'^[^a-zA-Z0-9]*ckan[^a-zA-Z0-9]*$', cleaned_value, re.IGNORECASE):
            return True
        
        return False
    
    def translate_if_needed(self, text: str, locale: Optional[str] = None) -> Tuple[str, str, bool]:
        """Translate text to English if needed. Returns (translated_text, original_text, was_translated)"""
        try:
            # If we have a locale from the API, use it
            if locale and locale != 'en':
                try:
                    # Map common locale codes to language codes if needed
                    lang_code = locale.split('_')[0].split('-')[0].lower()
                    
                    # Only translate if not English
                    if lang_code != 'en':
                        translation = self.translator.translate(text, src=lang_code, dest='en')
                        translated_text = translation.text
                        
                        logger.info(f"Translated '{text}' from locale '{locale}' to '{translated_text}'")
                        return translated_text, text, True
                        
                except Exception as e:
                    logger.warning(f"Translation error using locale '{locale}' for '{text}': {str(e)}")
                    # Fall back to auto-detection
            
            # If no locale or translation failed, use auto-detection
            if self.is_non_english(text):
                try:
                    # Translate to English
                    translation = self.translator.translate(text, dest='en')
                    translated_text = translation.text
                    
                    logger.info(f"Translated '{text}' to '{translated_text}' (auto-detected)")
                    return translated_text, text, True
                    
                except Exception as e:
                    logger.warning(f"Translation error for '{text}': {str(e)}")
                    return text, text, False
            
            # Text is already in English
            return text, text, False
            
        except Exception as e:
            logger.warning(f"Language detection error for '{text}': {str(e)}")
            return text, text, False
    
    def extract_from_html(self, url: str) -> Optional[str]:
        """Extract title from HTML page"""
        try:
            response = self.session.get(url, timeout=10, verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Try to find title tag
            title_tag = soup.find('title')
            if title_tag and title_tag.text:
                title = title_tag.text.strip()
                logger.info(f"Found title from HTML for {url}: {title}")
                return title
            
            # Try meta tags as fallback
            meta_tags = [
                {'property': 'og:title'},
                {'name': 'og:title'},
                {'property': 'og:site_name'},
                {'name': 'og:site_name'},
                {'name': 'title'},
                {'property': 'twitter:title'}
            ]
            
            for meta_attrs in meta_tags:
                meta = soup.find('meta', attrs=meta_attrs)
                if meta and meta.get('content'):
                    content = meta.get('content').strip()
                    logger.info(f"Found title from meta tag for {url}: {content}")
                    return content
            
            # Try h1 tag as last resort
            h1 = soup.find('h1')
            if h1 and h1.text:
                h1_text = h1.text.strip()
                logger.info(f"Found title from h1 tag for {url}: {h1_text}")
                return h1_text
            
        except Exception as e:
            logger.warning(f"Error extracting HTML title from {url}: {str(e)}")
        
        return None
    
    def extract_from_api(self, url: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract title and locale from CKAN API. Returns (title, locale)"""
        try:
            # Try different API endpoints
            api_endpoints = [
                '/api/3/action/status_show',
                '/api/action/status_show',
                '/api/2/util/status',
                '/api/util/status'
            ]
            
            for endpoint in api_endpoints:
                api_url = urljoin(url.rstrip('/') + '/', endpoint.lstrip('/'))
                
                try:
                    response = self.session.get(api_url, timeout=10, verify=False)
                    if response.status_code == 200:
                        data = response.json()
                        
                        # Try to find site_title and locale in different locations
                        site_title = None
                        locale = None
                        
                        if isinstance(data, dict):
                            # Standard CKAN API response
                            if 'result' in data and isinstance(data['result'], dict):
                                site_title = data['result'].get('site_title')
                                locale = data['result'].get('locale_default')
                            # Direct response
                            elif 'site_title' in data:
                                site_title = data['site_title']
                                locale = data.get('locale_default')
                        
                        if site_title:
                            logger.info(f"Found title from API for {url}: {site_title}, locale: {locale}")
                            return str(site_title).strip(), locale
                
                except Exception as e:
                    logger.debug(f"API endpoint {api_url} failed: {str(e)}")
                    continue
            
        except Exception as e:
            logger.warning(f"Error extracting API data from {url}: {str(e)}")
        
        return None, None
    
    def extract_instance_name(self, url: str) -> str:
        """Extract instance name from URL"""
        try:
            # Clean URL
            url = url.strip()
            original_url = url  # Keep the original URL
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            
            logger.info(f"Processing URL: {url}")
            
            # Try API first (usually more reliable) - now also gets locale
            title, locale = self.extract_from_api(url)
            
            # If API fails, try HTML
            if not title:
                title = self.extract_from_html(url)
                locale = None  # No locale from HTML
            
            # If we found a title
            if title:
                # Translate if needed (now with locale information)
                translated_title, original_title, was_translated = self.translate_if_needed(title, locale)
                
                # Check if the translated title is a default value
                if not self.is_default_value(translated_title):
                    # If it was translated, return formatted version
                    if was_translated:
                        return f"{translated_title} ({original_title})"
                    else:
                        return translated_title
                else:
                    logger.info(f"Title '{translated_title}' identified as default value")
            
            # If all else fails (both methods returned default values), return the exact URL without https://
            clean_url = url
            if clean_url.startswith('https://'):
                clean_url = clean_url[8:]
            elif clean_url.startswith('http://'):
                clean_url = clean_url[7:]
            
            # Remove trailing slashes
            clean_url = clean_url.rstrip('/')
            
            logger.info(f"Using exact URL as title for {url}: {clean_url}")
            return clean_url
            
        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")
            # Return URL without protocol as fallback
            fallback = url
            if fallback.startswith('https://'):
                fallback = fallback[8:]
            elif fallback.startswith('http://'):
                fallback = fallback[7:]
            return fallback.rstrip('/')
    
    def create_url_friendly_name(self, title: str) -> str:
        """Convert title to URL-friendly format"""
        # Use slugify for robust conversion
        return slugify(title, lowercase=True)
    
    def process_csv(self, input_file: str, output_file: str, url_column: str = 'url'):
        """Process CSV file with CKAN URLs"""
        results = []
        
        try:
            # Read input CSV
            with open(input_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames.copy()
                
                # Add new columns if not present
                if 'title' not in fieldnames:
                    fieldnames.append('title')
                if 'name' not in fieldnames:
                    fieldnames.append('name')
                
                # Process each row
                for i, row in enumerate(reader, 1):
                    url = row.get(url_column, '').strip()
                    
                    if url:
                        logger.info(f"Processing {i}: {url}")
                        
                        # Extract title
                        title = self.extract_instance_name(url)
                        row['title'] = title
                        
                        # Create URL-friendly name
                        row['name'] = self.create_url_friendly_name(title)
                        
                        # Add small delay to avoid overwhelming servers
                        time.sleep(0.5)
                    else:
                        row['title'] = ''
                        row['name'] = ''
                    
                    results.append(row)
            
            # Write output CSV
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
            
            logger.info(f"Processing complete. Results saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Error processing CSV: {str(e)}")
            raise


def main():
    """Main function to run the extractor"""
    # Configuration
    INPUT_FILE = '0.csv'  # Change this to your input file
    OUTPUT_FILE = '1.csv'  # Change this to your desired output file
    URL_COLUMN = 'url'  # Change this if your URL column has a different name
    
    print(f"Starting CKAN Instance Name Extractor...")
    print(f"Input file: {INPUT_FILE}")
    print(f"Output file: {OUTPUT_FILE}")
    print(f"URL column: {URL_COLUMN}")
    
    # Check if input file exists
    import os
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: Input file '{INPUT_FILE}' not found!")
        print("Please make sure the CSV file exists in the current directory.")
        return
    
    try:
        # Create extractor instance
        extractor = CKANInstanceNameExtractor()
        
        # Process the CSV file
        extractor.process_csv(INPUT_FILE, OUTPUT_FILE, URL_COLUMN)
        
        print(f"\nProcessing completed successfully!")
        print(f"Results saved to: {OUTPUT_FILE}")
        
    except Exception as e:
        print(f"\nERROR during processing: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()