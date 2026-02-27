import csv
import cloudscraper
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
import backoff
import argparse
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ── Retry helpers ────────────────────────────────────────────────────────────

@backoff.on_exception(
    backoff.expo,
    Exception,
    max_tries=4,
    max_time=60,
    on_backoff=lambda d: logger.warning(f"HTTP retry {d['tries']}: {d['exception']}")
)
def _fetch(session, url, **kwargs):
    """HTTP GET with exponential-backoff retry."""
    return session.get(url, **kwargs)


@backoff.on_exception(
    backoff.expo,
    Exception,
    max_tries=3,
    max_time=30,
    on_backoff=lambda d: logger.warning(f"Translation retry {d['tries']}: {d['exception']}")
)
def _translate(translator, text, **kwargs):
    """translate() call with retry."""
    return translator.translate(text, **kwargs)


# ── Main class ───────────────────────────────────────────────────────────────

class CKANInstanceNameExtractor:
    def __init__(self):
        self.translator = Translator()
        self.session = cloudscraper.create_scraper()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

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
        if not text or not text.strip():
            return False
        try:
            non_ascii_chars = sum(1 for char in text if ord(char) > 127)
            total_chars = len(text.strip())
            if total_chars > 0 and (non_ascii_chars / total_chars) > 0.1:
                return True
            latin_extended_pattern = r'[àáäâèéëêìíïîòóöôùúüûñçßøåæœÀÁÄÂÈÉËÊÌÍÏÎÒÓÖÔÙÚÜÛÑÇØÅÆŒ]'
            if re.search(latin_extended_pattern, text):
                return True
            try:
                langs = detect_langs(text)
                if langs:
                    top_lang = langs[0]
                    if top_lang.lang != 'en' and top_lang.prob > 0.7:
                        return True
                    if top_lang.lang == 'en' and top_lang.prob < 0.8 and len(langs) > 1:
                        return True
            except LangDetectException:
                pass
            non_english_indicators = [
                r'\b(el|la|los|las|de|del|para|por|con|sin|sobre|bajo|entre)\b',
                r'\b(le|la|les|de|du|des|pour|avec|sans|sur|sous|dans|entre)\b',
                r'\b(der|die|das|den|dem|des|für|mit|ohne|auf|unter|zwischen)\b',
                r'\b(il|lo|la|gli|le|di|del|della|per|con|senza|su|sotto|tra)\b',
                r'\b(o|a|os|as|do|da|dos|das|para|com|sem|sobre|sob|entre)\b',
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
        if not value:
            return True
        cleaned_value = ' '.join(value.lower().strip().split())
        for pattern in self.english_default_patterns:
            if re.match(pattern, cleaned_value, re.IGNORECASE):
                return True
        if 'ckan' in cleaned_value:
            ckan_with_defaults = [
                'welcome', 'home', 'portal', 'site', 'website', 'platform',
                'data', 'open', 'system', 'instance', 'catalog', 'repository'
            ]
            without_ckan = cleaned_value.replace('ckan', '').strip(' -–—')
            if without_ckan in ckan_with_defaults:
                return True
        if len(cleaned_value) < 4 and cleaned_value not in ['nyc', 'la', 'sf', 'uk', 'usa', 'eu']:
            return True
        if re.match(r'^[^a-zA-Z0-9]*ckan[^a-zA-Z0-9]*$', cleaned_value, re.IGNORECASE):
            return True
        return False

    def translate_if_needed(self, text: str, locale: Optional[str] = None) -> Tuple[str, str, bool]:
        """Translate text to English if needed. Returns (translated, original, was_translated)."""
        try:
            if locale and locale != 'en':
                try:
                    lang_code = locale.split('_')[0].split('-')[0].lower()
                    if lang_code != 'en':
                        translation = _translate(self.translator, text, src=lang_code, dest='en')
                        logger.info(f"Translated '{text}' from '{locale}' to '{translation.text}'")
                        return translation.text, text, True
                except Exception as e:
                    logger.warning(f"Locale-based translation failed for '{text}': {e}")

            if self.is_non_english(text):
                try:
                    translation = _translate(self.translator, text, dest='en')
                    logger.info(f"Auto-translated '{text}' to '{translation.text}'")
                    return translation.text, text, True
                except Exception as e:
                    logger.warning(f"Auto-translation failed for '{text}': {e}")
                    return text, text, False

            return text, text, False
        except Exception as e:
            logger.warning(f"translate_if_needed error for '{text}': {e}")
            return text, text, False

    def extract_from_html(self, url: str) -> Optional[str]:
        """Extract title from HTML page with retry."""
        try:
            response = _fetch(self.session, url, timeout=10, verify=False)
            response.raise_for_status()
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')

            title_tag = soup.find('title')
            if title_tag and title_tag.text:
                return title_tag.text.strip()

            meta_tags = [
                {'property': 'og:title'}, {'name': 'og:title'},
                {'property': 'og:site_name'}, {'name': 'og:site_name'},
                {'name': 'title'}, {'property': 'twitter:title'}
            ]
            for meta_attrs in meta_tags:
                meta = soup.find('meta', attrs=meta_attrs)
                if meta and meta.get('content'):
                    return meta.get('content').strip()

            h1 = soup.find('h1')
            if h1 and h1.text:
                return h1.text.strip()

        except Exception as e:
            logger.warning(f"HTML title extraction failed for {url}: {e}")
        return None

    def extract_from_api(self, url: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract title and locale from CKAN API. Returns (title, locale)."""
        api_endpoints = [
            '/api/3/action/status_show',
            '/api/action/status_show',
            '/api/2/util/status',
            '/api/util/status'
        ]
        for endpoint in api_endpoints:
            api_url = urljoin(url.rstrip('/') + '/', endpoint.lstrip('/'))
            try:
                response = _fetch(self.session, api_url, timeout=10, verify=False)
                if response.status_code == 200:
                    data = response.json()
                    site_title = None
                    locale = None
                    if isinstance(data, dict):
                        if 'result' in data and isinstance(data['result'], dict):
                            site_title = data['result'].get('site_title')
                            locale = data['result'].get('locale_default')
                        elif 'site_title' in data:
                            site_title = data['site_title']
                            locale = data.get('locale_default')
                    if site_title:
                        logger.info(f"API title for {url}: {site_title}, locale: {locale}")
                        return str(site_title).strip(), locale
            except Exception as e:
                logger.debug(f"API endpoint {api_url} failed: {e}")
                continue
        return None, None

    def extract_instance_name(self, url: str) -> str:
        """Extract instance name from URL."""
        try:
            url = url.strip()
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url

            logger.info(f"Processing URL: {url}")

            title, locale = self.extract_from_api(url)
            if not title:
                title = self.extract_from_html(url)
                locale = None

            if title:
                translated_title, original_title, was_translated = self.translate_if_needed(title, locale)
                if not self.is_default_value(translated_title):
                    if was_translated:
                        return f"{translated_title} ({original_title})"
                    return translated_title
                logger.info(f"Title '{translated_title}' is a default value, falling back to URL")

            clean_url = url
            for prefix in ('https://', 'http://'):
                if clean_url.startswith(prefix):
                    clean_url = clean_url[len(prefix):]
                    break
            return clean_url.rstrip('/')

        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
            fallback = url
            for prefix in ('https://', 'http://'):
                if fallback.startswith(prefix):
                    fallback = fallback[len(prefix):]
                    break
            return fallback.rstrip('/')

    def create_url_friendly_name(self, title: str) -> str:
        return slugify(title, lowercase=True)

    def process_csv(self, input_file: str, output_file: str,
                    url_column: str = 'url', rows: int = None):
        """Process CSV file with CKAN URLs. Supports resume (skips already-processed URLs)."""

        # ── Load input ───────────────────────────────────────────────────────
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames)
            if 'title' not in fieldnames:
                fieldnames.append('title')
            if 'name' not in fieldnames:
                fieldnames.append('name')
            all_rows = list(reader)

        if rows:
            all_rows = all_rows[:rows]

        # ── Resume support ───────────────────────────────────────────────────
        processed_urls = set()
        if os.path.exists(output_file):
            with open(output_file, 'r', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    processed_urls.add(row.get(url_column, '').strip())
            logger.info(f"Resume: {len(processed_urls)} URLs already processed, skipping them")

        mode = 'a' if processed_urls else 'w'

        # ── Process rows incrementally ───────────────────────────────────────
        with open(output_file, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not processed_urls:
                writer.writeheader()

            for i, row in enumerate(all_rows, 1):
                url = row.get(url_column, '').strip()

                if url and url in processed_urls:
                    continue

                logger.info(f"Processing {i}/{len(all_rows)}: {url}")

                if url:
                    try:
                        title = self.extract_instance_name(url)
                        row['title'] = title
                        row['name'] = self.create_url_friendly_name(title)
                    except Exception as e:
                        logger.error(f"Failed on {url}: {e}")
                        row['title'] = ''
                        row['name'] = ''
                    time.sleep(0.5)
                else:
                    row['title'] = ''
                    row['name'] = ''

                writer.writerow(row)
                f.flush()

        logger.info(f"Done. Results saved to {output_file}")


# ── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description='CKAN Instance Name Extractor')
    parser.add_argument('--rows', type=int, default=None,
                        help='Maximum number of rows to process')
    parser.add_argument('--input', default='0.csv', help='Input CSV file')
    parser.add_argument('--output', default='1.csv', help='Output CSV file')
    return parser.parse_args()


def main():
    args = parse_args()

    print(f"Starting CKAN Instance Name Extractor...")
    print(f"Input file:  {args.input}")
    print(f"Output file: {args.output}")
    if args.rows:
        print(f"Row limit:   {args.rows}")

    if not os.path.exists(args.input):
        print(f"ERROR: Input file '{args.input}' not found!")
        return

    try:
        extractor = CKANInstanceNameExtractor()
        extractor.process_csv(args.input, args.output, rows=args.rows)
        print(f"\nProcessing completed. Results saved to: {args.output}")
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
