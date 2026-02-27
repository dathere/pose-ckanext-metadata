import cloudscraper
from bs4 import BeautifulSoup
import re
import csv
import time
import logging
import backoff
import argparse
import os
from urllib.parse import urljoin, urlparse
from typing import Optional, Tuple
from googletrans import Translator
from langdetect import detect_langs, LangDetectException
import signal
from contextlib import contextmanager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ── Timeout helper ────────────────────────────────────────────────────────────

class TimeoutException(Exception):
    pass


@contextmanager
def timeout(seconds):
    def _handler(signum, frame):
        raise TimeoutException()
    signal.signal(signal.SIGALRM, _handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)


# ── Retry helpers ─────────────────────────────────────────────────────────────

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


# ── Main class ────────────────────────────────────────────────────────────────

class CKANAboutExtractor:
    def __init__(self, page_timeout: int = 10, total_timeout: int = 30):
        self.session = cloudscraper.create_scraper()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.translator = Translator()
        self.page_timeout = page_timeout
        self.total_timeout = total_timeout

    def is_default_description(self, text: str) -> bool:
        if not text:
            return True
        text_lower = text.lower().strip()
        if text_lower.startswith("ckan is the"):
            return True
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
                return True
        if len(text.strip()) < 50:
            return True
        return False

    def detect_and_translate(self, text: str) -> Tuple[str, str, bool]:
        """Detect language and translate if needed."""
        try:
            detected_langs = detect_langs(text)
            if detected_langs:
                lang_code = detected_langs[0].lang
                confidence = detected_langs[0].prob
                if confidence > 0.7 and lang_code != 'en':
                    try:
                        translation = _translate(self.translator, text, src=lang_code, dest='en')
                        return translation.text, lang_code, True
                    except Exception as e:
                        logger.warning(f"Translation error: {e}")
                        return text, "Unknown", False
            return text, "English", False
        except Exception as e:
            logger.warning(f"Language detection error: {e}")
            return text, "Unknown", False

    def format_description(self, original_text: str, translated_text: str, original_language: str) -> str:
        if original_language in ("English", "Unknown"):
            return original_text
        formatted = f"{translated_text}\n\n"
        formatted += f"*Translated from {original_language}*\n"
        formatted += "---\n**Original Text:**\n"
        formatted += original_text
        return formatted

    def normalize_url(self, url: str) -> str:
        return url.strip().rstrip('/')

    def try_url(self, url: str) -> Optional[object]:
        """Try fetching a URL (HTTPS first, then HTTP), return response or None."""
        candidates = []
        if url.startswith(('http://', 'https://')):
            candidates = [url]
        else:
            candidates = ['https://' + url, 'http://' + url]

        for candidate in candidates:
            try:
                response = _fetch(self.session, candidate,
                                  timeout=self.page_timeout, verify=False)
                if response.status_code == 200:
                    return response
            except Exception as e:
                logger.debug(f"Fetch failed for {candidate}: {e}")
        return None

    def get_detailed_description(self, base_url: str) -> Optional[str]:
        """Extract detailed description from About page."""
        try:
            with timeout(self.total_timeout):
                base_url = self.normalize_url(base_url)

                about_paths = [
                    "/about", "/about/about", "/about-us", "/pages/about",
                    "/en/about", "/about.html", "/about/", "/info/about"
                ]

                detailed_description = ""

                for path in about_paths:
                    try:
                        url = base_url + path
                        logger.info(f"Trying: {url}")
                        response = self.try_url(url)
                        if not response:
                            continue

                        soup = BeautifulSoup(response.text, 'html.parser')

                        # Strategy 1: named content area
                        content_selectors = [
                            '.main-content', '#main-content', '.content', '#content',
                            'main', 'article', '.page-content', '#page-content',
                            '.about-content', '#about-content', '.primary', '#primary',
                            '.col-md-9', '.span9', '[role="main"]'
                        ]
                        main_content = None
                        for selector in content_selectors:
                            main_content = soup.select_one(selector)
                            if main_content:
                                break

                        if main_content:
                            paragraphs = main_content.find_all('p')
                            if paragraphs:
                                detailed_description = " ".join(
                                    p.get_text().strip() for p in paragraphs
                                )
                                break

                        # Strategy 2: all body paragraphs
                        if not detailed_description:
                            body_paragraphs = [
                                p.get_text().strip()
                                for p in soup.find_all('p')
                                if len(p.get_text().strip()) > 50
                            ]
                            if body_paragraphs:
                                detailed_description = " ".join(body_paragraphs)
                                break

                    except TimeoutException:
                        logger.warning(f"Page timeout: {path}")
                        raise
                    except Exception as e:
                        logger.debug(f"Error on {path}: {e}")
                        continue

                if detailed_description:
                    detailed_description = re.sub(r'\s+', ' ', detailed_description).strip()
                    if len(detailed_description) > 100:
                        translated_text, original_language, was_translated = \
                            self.detect_and_translate(detailed_description)
                        if self.is_default_description(translated_text):
                            logger.info("Translated description is default CKAN text — skipping")
                            return None
                        if was_translated:
                            return self.format_description(
                                detailed_description, translated_text, original_language
                            )
                        if self.is_default_description(detailed_description):
                            return None
                        return detailed_description
                    logger.warning(f"Description too short for {base_url}")

                logger.warning(f"No description found for {base_url}")
                return None

        except TimeoutException:
            logger.error(f"Total timeout exceeded for {base_url}")
            return None
        except Exception as e:
            logger.error(f"Error for {base_url}: {e}")
            return None

    def process_csv(self, input_file: str, output_file: str,
                    url_column: str = 'url',
                    description_column: str = 'detailed_description',
                    rows: int = None):
        """Process CSV. Supports resume (skips URLs already in output)."""

        # ── Load input ───────────────────────────────────────────────────────
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames)
            if description_column not in fieldnames:
                fieldnames.append(description_column)
            all_rows = list(reader)

        if rows:
            all_rows = all_rows[:rows]

        # ── Resume support ───────────────────────────────────────────────────
        processed_urls = set()
        if os.path.exists(output_file):
            with open(output_file, 'r', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    processed_urls.add(row.get(url_column, '').strip())
            logger.info(f"Resume: {len(processed_urls)} URLs already done, skipping")

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

                logger.info(f"\nProcessing {i}/{len(all_rows)}: {url}")

                if url:
                    try:
                        description = self.get_detailed_description(url)
                        row[description_column] = description if description else ''
                    except Exception as e:
                        logger.error(f"Failed on {url}: {e}")
                        row[description_column] = ''
                    time.sleep(1)
                else:
                    row[description_column] = ''

                writer.writerow(row)
                f.flush()

        logger.info(f"Done. Results saved to {output_file}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description='CKAN About Page Description Extractor')
    parser.add_argument('--rows', type=int, default=None,
                        help='Maximum number of rows to process')
    parser.add_argument('--input',  default='3.csv', help='Input CSV file')
    parser.add_argument('--output', default='4.csv', help='Output CSV file')
    parser.add_argument('--page-timeout',  type=int, default=10,
                        help='Timeout per page request (seconds)')
    parser.add_argument('--total-timeout', type=int, default=30,
                        help='Total timeout per site (seconds)')
    return parser.parse_args()


def main():
    args = parse_args()

    print(f"Starting CKAN About Page Description Extractor...")
    print(f"Input file:     {args.input}")
    print(f"Output file:    {args.output}")
    print(f"Page timeout:   {args.page_timeout}s")
    print(f"Total timeout:  {args.total_timeout}s")
    if args.rows:
        print(f"Row limit:      {args.rows}")

    if not os.path.exists(args.input):
        print(f"ERROR: Input file '{args.input}' not found!")
        return

    try:
        extractor = CKANAboutExtractor(
            page_timeout=args.page_timeout,
            total_timeout=args.total_timeout
        )
        extractor.process_csv(args.input, args.output, rows=args.rows)
        print(f"\nProcessing completed. Results saved to: {args.output}")
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
