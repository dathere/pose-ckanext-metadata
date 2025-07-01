import requests
from bs4 import BeautifulSoup
import re
import csv
import time
import logging
from urllib.parse import urljoin, urlparse
from typing import Optional, Tuple
from googletrans import Translator
from langdetect import detect_langs, LangDetectException
import signal
from contextlib import contextmanager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TimeoutException(Exception):
    pass


@contextmanager
def timeout(seconds):
    """Context manager for timeouts"""
    def timeout_handler(signum, frame):
        raise TimeoutException()
    
    # Set the signal handler and a timeout alarm
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)  # Disable the alarm


class CKANAboutExtractor:
    def __init__(self, page_timeout: int = 10, total_timeout: int = 30):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.translator = Translator()
        self.page_timeout = page_timeout  # Timeout for individual page requests
        self.total_timeout = total_timeout  # Total timeout for processing one site
    
    def is_default_description(self, text: str) -> bool:
        """Check if the description is a default CKAN description."""
        if not text:
            return True
        
        text_lower = text.lower().strip()
        
        # Check if it starts with "CKAN is the" - very common default
        if text_lower.startswith("ckan is the"):
            logger.info("Description starts with 'CKAN is the' - identified as default")
            return True
        
        # Check against known default patterns
        default_patterns = [
            r"^ckan is the world's leading open[- ]?source",
            r"^ckan is a powerful data management system",
            r"^welcome to ckan",
            r"^this is a ckan instance",
            r"^ckan is an open[- ]?source data portal",
            r"^ckan is a tool for making open data websites",
            r"^comprehensive knowledge archive network",
            r"^ckan is a registry of open knowledge",
            r"^ckan, the world's leading open source data portal platform",
            r"^ckan is the open source data management system",
            r"^ckan is the leading open source data portal",
            r"^ckan is a data catalogue software",
            r"^ckan is free and open source software"
        ]
        
        for pattern in default_patterns:
            if re.search(pattern, text_lower):
                logger.info(f"Description matches default pattern: {pattern}")
                return True
        
        # Check if it's too short to be meaningful
        if len(text.strip()) < 50:
            return True
        
        return False
    
    def detect_and_translate(self, text: str) -> Tuple[str, str, bool]:
        """Detect language and translate if needed. Returns (translated_text, original_language, was_translated)"""
        try:
            # Detect language
            detected_langs = detect_langs(text)
            if detected_langs:
                lang_code = detected_langs[0].lang
                confidence = detected_langs[0].prob
                
                # Only translate if we're confident about the language and it's not English
                if confidence > 0.7 and lang_code != 'en':
                    try:
                        # Get the language name
                        lang_name = self.translator.translate(lang_code, src='en', dest='en').text
                        
                        # Translate to English
                        translation = self.translator.translate(text, src=lang_code, dest='en')
                        translated_text = translation.text
                        
                        logger.info(f"Translated from {lang_name} to English")
                        return translated_text, lang_name, True
                        
                    except Exception as e:
                        logger.warning(f"Translation error: {str(e)}")
                        return text, "Unknown", False
            
            # Text is already in English or couldn't detect language
            return text, "English", False
            
        except Exception as e:
            logger.warning(f"Language detection error: {str(e)}")
            return text, "Unknown", False
    
    def format_description(self, original_text: str, translated_text: str, original_language: str) -> str:
        """Format the description with translation information."""
        if original_language == "English" or original_language == "Unknown":
            return original_text
        
        formatted = f"{translated_text}\n\n"
        formatted += f"*Translated from {original_language}*\n"
        formatted += "---\n"
        formatted += "**Original Text:**\n"
        formatted += original_text
        
        return formatted
    
    def normalize_url(self, url: str) -> str:
        """Normalize URL with proper protocol handling."""
        url = url.strip().rstrip('/')
        
        # If no protocol, try https first, then http
        if not url.startswith(('http://', 'https://')):
            return url  # Will be handled in get_detailed_description
        
        return url
    
    def try_url_with_protocols(self, url: str) -> Optional[requests.Response]:
        """Try accessing URL with both HTTPS and HTTP."""
        # If URL doesn't have protocol, try both
        if not url.startswith(('http://', 'https://')):
            # Try HTTPS first
            try:
                https_url = 'https://' + url
                response = self.session.get(https_url, timeout=self.page_timeout, verify=False)
                if response.status_code == 200:
                    return response
            except:
                pass
            
            # Try HTTP if HTTPS fails
            try:
                http_url = 'http://' + url
                response = self.session.get(http_url, timeout=self.page_timeout, verify=False)
                if response.status_code == 200:
                    return response
            except:
                pass
        else:
            # URL has protocol, use as is
            try:
                response = self.session.get(url, timeout=self.page_timeout, verify=False)
                if response.status_code == 200:
                    return response
            except:
                pass
        
        return None
    
    def get_detailed_description(self, base_url: str) -> Optional[str]:
        """Extract detailed description from the About page with timeout."""
        try:
            with timeout(self.total_timeout):
                # Normalize the base URL
                base_url = self.normalize_url(base_url)
                
                # Try different possible About page URLs
                about_paths = [
                    "/about",
                    "/about/about",
                    "/about-us",
                    "/pages/about",
                    "/en/about",
                    "/about.html",
                    "/about/",
                    "/info/about"
                ]
                
                detailed_description = ""
                
                for path in about_paths:
                    try:
                        # Construct full URL
                        if base_url.startswith(('http://', 'https://')):
                            url = base_url + path
                        else:
                            url = base_url + path  # Will be handled by try_url_with_protocols
                        
                        logger.info(f"Trying about page: {url}")
                        
                        # Try to get the page with both protocols if needed
                        response = self.try_url_with_protocols(url)
                        
                        if response:
                            soup = BeautifulSoup(response.text, 'html.parser')
                            
                            # Strategy 1: Look for main content area
                            main_content = None
                            content_selectors = [
                                '.main-content', '#main-content', '.content', '#content',
                                'main', 'article', '.page-content', '#page-content',
                                '.about-content', '#about-content', '.primary', '#primary',
                                '.col-md-9', '.span9', '[role="main"]'
                            ]
                            
                            for selector in content_selectors:
                                main_content = soup.select_one(selector)
                                if main_content:
                                    logger.debug(f"Found main content with selector: {selector}")
                                    break
                            
                            # If we found a main content area
                            if main_content:
                                # Extract all paragraphs
                                paragraphs = main_content.find_all('p')
                                
                                # Combine paragraphs into a single text
                                if paragraphs:
                                    detailed_description = " ".join([p.get_text().strip() for p in paragraphs])
                                    logger.info(f"Found description from main content: {len(detailed_description)} chars")
                                    break
                            
                            # Strategy 2: If main content area not found or no paragraphs
                            if not detailed_description:
                                body_paragraphs = soup.find_all('p')
                                
                                # Filter out very short paragraphs
                                meaningful_paragraphs = [p.get_text().strip() for p in body_paragraphs 
                                                    if len(p.get_text().strip()) > 50]
                                
                                if meaningful_paragraphs:
                                    detailed_description = " ".join(meaningful_paragraphs)
                                    logger.info(f"Found description from body paragraphs: {len(detailed_description)} chars")
                                    break
                            
                    except TimeoutException:
                        logger.warning(f"Timeout while processing {url}")
                        raise
                    except Exception as e:
                        logger.debug(f"Error accessing {path}: {str(e)}")
                        continue
                
                # Clean up the text
                if detailed_description:
                    # Remove extra whitespace
                    detailed_description = re.sub(r'\s+', ' ', detailed_description)
                    # Remove multiple newlines
                    detailed_description = re.sub(r'\n+', ' ', detailed_description)
                    # Trim
                    detailed_description = detailed_description.strip()
                    
                    # Check if it's meaningful length
                    if len(detailed_description) > 100:
                        # Detect language and translate if needed
                        translated_text, original_language, was_translated = self.detect_and_translate(detailed_description)
                        
                        # IMPORTANT: Check if the TRANSLATED text is a default description
                        if self.is_default_description(translated_text):
                            logger.info("Translated description is default CKAN text, returning None")
                            return None
                        
                        # Format the description
                        if was_translated:
                            formatted_description = self.format_description(
                                detailed_description, translated_text, original_language
                            )
                            return formatted_description
                        else:
                            # Still need to check if English text is default
                            if self.is_default_description(detailed_description):
                                logger.info("Description is default CKAN text, returning None")
                                return None
                            return detailed_description
                    else:
                        logger.warning(f"Description too short for {base_url}, ignoring")
                
                logger.warning(f"No detailed description found for {base_url}")
                return None
                
        except TimeoutException:
            logger.error(f"Total timeout exceeded for {base_url}")
            return None
        except Exception as e:
            logger.error(f"Error getting detailed description for {base_url}: {str(e)}")
            return None
    
    def process_csv(self, input_file: str, output_file: str, url_column: str = 'url', 
                    description_column: str = 'description'):
        """Process a CSV file with CKAN URLs and extract descriptions."""
        results = []
        
        try:
            # Read input CSV
            with open(input_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames.copy()
                
                # Add description column if not present
                if description_column not in fieldnames:
                    fieldnames.append(description_column)
                
                # Process each row
                for i, row in enumerate(reader, 1):
                    url = row.get(url_column, '').strip()
                    
                    if url:
                        logger.info(f"\nProcessing {i}: {url}")
                        
                        # Extract description
                        description = self.get_detailed_description(url)
                        row[description_column] = description if description else ''
                        
                        # Add small delay to be respectful to servers
                        time.sleep(1)
                    else:
                        row[description_column] = ''
                    
                    results.append(row)
            
            # Write output CSV
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
            
            logger.info(f"\nProcessing complete. Results saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Error processing CSV: {str(e)}")
            raise
    
    def extract_single_url(self, url: str) -> Optional[str]:
        """Extract description from a single URL."""
        return self.get_detailed_description(url)


def main():
    """Main function to run the extractor."""
    # Configuration
    INPUT_FILE = 'ckan_instances.csv'  # Change this to your input file
    OUTPUT_FILE = 'ckan_instances_with_descriptions.csv'  # Change this to your desired output file
    URL_COLUMN = 'url'  # Change this if your URL column has a different name
    DESCRIPTION_COLUMN = 'description'  # Name for the description column
    PAGE_TIMEOUT = 10  # Timeout for individual page requests (seconds)
    TOTAL_TIMEOUT = 30  # Total timeout for processing one site (seconds)
    
    print(f"Starting CKAN About Page Description Extractor...")
    print(f"Input file: {INPUT_FILE}")
    print(f"Output file: {OUTPUT_FILE}")
    print(f"URL column: {URL_COLUMN}")
    print(f"Description column: {DESCRIPTION_COLUMN}")
    print(f"Page timeout: {PAGE_TIMEOUT}s")
    print(f"Total timeout per site: {TOTAL_TIMEOUT}s")
    
    # Check if input file exists
    import os
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: Input file '{INPUT_FILE}' not found!")
        print("Please make sure the CSV file exists in the current directory.")
        return
    
    try:
        # Create extractor instance with timeout settings
        extractor = CKANAboutExtractor(page_timeout=PAGE_TIMEOUT, total_timeout=TOTAL_TIMEOUT)
        
        # Process the CSV file
        extractor.process_csv(INPUT_FILE, OUTPUT_FILE, URL_COLUMN, DESCRIPTION_COLUMN)
        
        print(f"\nProcessing completed successfully!")
        print(f"Results saved to: {OUTPUT_FILE}")
        
    except Exception as e:
        print(f"\nERROR during processing: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
    