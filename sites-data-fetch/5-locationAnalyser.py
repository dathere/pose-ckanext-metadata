import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv
import time
import re
import logging
import sys
from tqdm import tqdm
import backoff
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import pycountry
from urllib.parse import urlparse
import langdetect
import langcodes

# Set up logging with thread safety
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ckan_location_analyzer.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# Thread-local storage for request tracking
thread_local = threading.local()

# Load environment variables (for API key)
load_dotenv()

# Configuration - Edit these values as needed
INPUT_FILE = '4.csv'        # Input CSV file path
OUTPUT_FILE = '5.csv'       # Output CSV file path
ROWS_TO_PROCESS = None      # Number of rows to process (None = all rows)
MODEL_NAME = 'google/gemini-2.0-flash-001'  # OpenRouter model to use
RETRY_FAILED_ONLY = False   # Set to True to retry only previously failed requests
MAX_RETRIES = 3             # Maximum number of retries for API calls
TEMPERATURE = 0.7           # Temperature for LLM generation (0.0-1.0)
MAX_TOKENS = 2000          # Maximum tokens in LLM response
NUM_THREADS = 10           # Number of concurrent threads to use

def detect_language(text):
    """Detect the language of the provided text and return its name."""
    if not text or pd.isna(text) or len(str(text).strip()) < 10:
        return None
    
    try:
        lang_code = langdetect.detect(str(text))
        # Get the language name
        language = langcodes.Language.get(lang_code).display_name()
        return language
    except:
        return None

def extract_location_from_tld(url):
    """Extract location hints from top-level domain."""
    if pd.isna(url) or not url:
        return None
    
    try:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        
        # Extract TLD
        tld_match = re.search(r'\.([a-z]{2,})$', domain)
        if not tld_match:
            return None
            
        tld = tld_match.group(1)
        
        # Skip common generic TLDs
        if tld in ['com', 'org', 'net', 'edu', 'gov', 'io', 'co', 'info', 'ai']:
            return None
            
        # Try to match TLD to country
        try:
            country = pycountry.countries.get(alpha_2=tld.upper())
            if country:
                return country.name
        except:
            pass
            
        # Specific cases for regional TLDs
        # Comprehensive TLD to Country mapping
        tld_country_map = {
            # Europe
            'uk': 'United Kingdom',
            'gb': 'United Kingdom',
            'eu': 'European Union',
            'de': 'Germany',
            'fr': 'France',
            'es': 'Spain',
            'it': 'Italy',
            'nl': 'Netherlands',
            'be': 'Belgium',
            'ch': 'Switzerland',
            'at': 'Austria',
            'se': 'Sweden',
            'no': 'Norway',
            'dk': 'Denmark',
            'fi': 'Finland',
            'pl': 'Poland',
            'pt': 'Portugal',
            'ie': 'Ireland',
            'gr': 'Greece',
            'cz': 'Czech Republic',
            'hu': 'Hungary',
            'ro': 'Romania',
            'bg': 'Bulgaria',
            'hr': 'Croatia',
            'si': 'Slovenia',
            'sk': 'Slovakia',
            'lt': 'Lithuania',
            'lv': 'Latvia',
            'ee': 'Estonia',
            'lu': 'Luxembourg',
            'mt': 'Malta',
            'cy': 'Cyprus',
            'is': 'Iceland',
            'li': 'Liechtenstein',
            'mc': 'Monaco',
            'sm': 'San Marino',
            'va': 'Vatican City',
            'ad': 'Andorra',
            'al': 'Albania',
            'ba': 'Bosnia and Herzegovina',
            'by': 'Belarus',
            'fo': 'Faroe Islands',
            'gg': 'Guernsey',
            'im': 'Isle of Man',
            'je': 'Jersey',
            'md': 'Moldova',
            'me': 'Montenegro',
            'mk': 'North Macedonia',
            'rs': 'Serbia',
            'ua': 'Ukraine',
            'xk': 'Kosovo',
            
            # Americas
            'us': 'United States',
            'ca': 'Canada',
            'mx': 'Mexico',
            'br': 'Brazil',
            'ar': 'Argentina',
            'cl': 'Chile',
            'co': 'Colombia',
            'pe': 'Peru',
            've': 'Venezuela',
            'ec': 'Ecuador',
            'bo': 'Bolivia',
            'py': 'Paraguay',
            'uy': 'Uruguay',
            'gy': 'Guyana',
            'sr': 'Suriname',
            'gf': 'French Guiana',
            'cr': 'Costa Rica',
            'pa': 'Panama',
            'cu': 'Cuba',
            'do': 'Dominican Republic',
            'gt': 'Guatemala',
            'hn': 'Honduras',
            'sv': 'El Salvador',
            'ni': 'Nicaragua',
            'bz': 'Belize',
            'jm': 'Jamaica',
            'ht': 'Haiti',
            'tt': 'Trinidad and Tobago',
            'bb': 'Barbados',
            'bs': 'Bahamas',
            'ag': 'Antigua and Barbuda',
            'dm': 'Dominica',
            'gd': 'Grenada',
            'kn': 'Saint Kitts and Nevis',
            'lc': 'Saint Lucia',
            'vc': 'Saint Vincent and the Grenadines',
            'ai': 'Anguilla',
            'aw': 'Aruba',
            'cw': 'Curaçao',
            'sx': 'Sint Maarten',
            'bq': 'Caribbean Netherlands',
            'ky': 'Cayman Islands',
            'vg': 'British Virgin Islands',
            'vi': 'US Virgin Islands',
            'pr': 'Puerto Rico',
            'mq': 'Martinique',
            'gp': 'Guadeloupe',
            'bl': 'Saint Barthélemy',
            'mf': 'Saint Martin',
            'pm': 'Saint Pierre and Miquelon',
            'fk': 'Falkland Islands',
            
            # Asia
            'cn': 'China',
            'jp': 'Japan',
            'kr': 'South Korea',
            'kp': 'North Korea',
            'in': 'India',
            'id': 'Indonesia',
            'my': 'Malaysia',
            'sg': 'Singapore',
            'th': 'Thailand',
            'vn': 'Vietnam',
            'ph': 'Philippines',
            'tw': 'Taiwan',
            'hk': 'Hong Kong',
            'mo': 'Macau',
            'kh': 'Cambodia',
            'la': 'Laos',
            'mm': 'Myanmar',
            'bn': 'Brunei',
            'tl': 'Timor-Leste',
            'mn': 'Mongolia',
            
            # South Asia
            'pk': 'Pakistan',
            'bd': 'Bangladesh',
            'lk': 'Sri Lanka',
            'np': 'Nepal',
            'bt': 'Bhutan',
            'mv': 'Maldives',
            'af': 'Afghanistan',
            
            # Central Asia
            'kz': 'Kazakhstan',
            'uz': 'Uzbekistan',
            'tm': 'Turkmenistan',
            'kg': 'Kyrgyzstan',
            'tj': 'Tajikistan',
            
            # Middle East
            'ae': 'United Arab Emirates',
            'sa': 'Saudi Arabia',
            'qa': 'Qatar',
            'kw': 'Kuwait',
            'bh': 'Bahrain',
            'om': 'Oman',
            'ye': 'Yemen',
            'jo': 'Jordan',
            'lb': 'Lebanon',
            'sy': 'Syria',
            'iq': 'Iraq',
            'ir': 'Iran',
            'il': 'Israel',
            'ps': 'Palestine',
            'tr': 'Turkey',
            'az': 'Azerbaijan',
            'am': 'Armenia',
            'ge': 'Georgia',
            
            # Oceania
            'au': 'Australia',
            'nz': 'New Zealand',
            'fj': 'Fiji',
            'pg': 'Papua New Guinea',
            'sb': 'Solomon Islands',
            'vu': 'Vanuatu',
            'nc': 'New Caledonia',
            'pf': 'French Polynesia',
            'gu': 'Guam',
            'mp': 'Northern Mariana Islands',
            'as': 'American Samoa',
            'ck': 'Cook Islands',
            'nu': 'Niue',
            'tk': 'Tokelau',
            'nf': 'Norfolk Island',
            'cc': 'Cocos Islands',
            'cx': 'Christmas Island',
            'tv': 'Tuvalu',
            'to': 'Tonga',
            'ws': 'Samoa',
            'ki': 'Kiribati',
            'nr': 'Nauru',
            'fm': 'Micronesia',
            'mh': 'Marshall Islands',
            'pw': 'Palau',
            'wf': 'Wallis and Futuna',
            
            # Africa
            'za': 'South Africa',
            'eg': 'Egypt',
            'ma': 'Morocco',
            'tn': 'Tunisia',
            'dz': 'Algeria',
            'ly': 'Libya',
            'sd': 'Sudan',
            'et': 'Ethiopia',
            'ke': 'Kenya',
            'ug': 'Uganda',
            'tz': 'Tanzania',
            'rw': 'Rwanda',
            'bi': 'Burundi',
            'so': 'Somalia',
            'dj': 'Djibouti',
            'er': 'Eritrea',
            'ss': 'South Sudan',
            'ng': 'Nigeria',
            'gh': 'Ghana',
            'ci': 'Ivory Coast',
            'sn': 'Senegal',
            'ml': 'Mali',
            'bf': 'Burkina Faso',
            'ne': 'Niger',
            'tg': 'Togo',
            'bj': 'Benin',
            'lr': 'Liberia',
            'sl': 'Sierra Leone',
            'gn': 'Guinea',
            'gw': 'Guinea-Bissau',
            'gm': 'Gambia',
            'mr': 'Mauritania',
            'cv': 'Cape Verde',
            'ga': 'Gabon',
            'cg': 'Republic of Congo',
            'cd': 'Democratic Republic of Congo',
            'cm': 'Cameroon',
            'cf': 'Central African Republic',
            'td': 'Chad',
            'ao': 'Angola',
            'zm': 'Zambia',
            'zw': 'Zimbabwe',
            'mz': 'Mozambique',
            'bw': 'Botswana',
            'na': 'Namibia',
            'sz': 'Eswatini',
            'ls': 'Lesotho',
            'mg': 'Madagascar',
            'mu': 'Mauritius',
            'sc': 'Seychelles',
            'km': 'Comoros',
            'yt': 'Mayotte',
            're': 'Réunion',
            'st': 'São Tomé and Príncipe',
            'gq': 'Equatorial Guinea',
            'eh': 'Western Sahara',
            
            # Special/Other
            'io': 'British Indian Ocean Territory',
            'bm': 'Bermuda',
            'gi': 'Gibraltar',
            'gs': 'South Georgia',
            'sh': 'Saint Helena',
            'tc': 'Turks and Caicos',
            'pn': 'Pitcairn Islands',
            'um': 'US Minor Islands',
            'aq': 'Antarctica',
            'bv': 'Bouvet Island',
            'hm': 'Heard Island',
            'tf': 'French Southern Territories',
            'sj': 'Svalbard and Jan Mayen',
            'ax': 'Åland Islands',
        }
        
        return tld_country_map.get(tld)
            
    except:
        return None

def extract_location_from_domain(url):
    """Extract location hints from domain name parts."""
    if pd.isna(url) or not url:
        return None
    
    try:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        
        # Remove www. and subdomains
        main_domain = re.sub(r'^www\.', '', domain)
        
        # Look for city/state patterns
        # Example: data.sugarlandtx.gov -> "Sugarland, Texas"
        city_state_match = re.search(r'([a-z]+)([a-z]{2})\.gov', main_domain)
        if city_state_match:
            city = city_state_match.group(1)
            state_code = city_state_match.group(2)
            
            # US state abbreviation map (simplified)
            us_states = {
                'tx': 'Texas', 'ca': 'California', 'ny': 'New York',
                'fl': 'Florida', 'il': 'Illinois', 'pa': 'Pennsylvania',
                # Add more as needed
            }
            
            state = us_states.get(state_code.lower())
            if state:
                # Format city name properly (capitalize each word)
                city = ' '.join(word.capitalize() for word in re.findall(r'[a-z]+', city))
                return f"{city}, {state}"
        
        # Look for city names in domain
        common_cities = [
            'london', 'paris', 'berlin', 'tokyo', 'newyork', 'madrid',
            'rome', 'amsterdam', 'dublin', 'chicago', 'boston', 'sydney',
            # Add more as needed
        ]
        
        for city in common_cities:
            if city in main_domain:
                return city.capitalize()
                
        return None
            
    except:
        return None

# Use backoff decorator to implement retries with exponential backoff
@backoff.on_exception(
    backoff.expo, 
    (requests.exceptions.RequestException, json.JSONDecodeError), 
    max_tries=3, 
    giveup=lambda e: isinstance(e, requests.exceptions.HTTPError) and e.response.status_code in [400, 401, 403]
)
def call_openrouter_api(prompt, api_key, model_name, max_tokens=2000, temperature=0.7):
    """Make API call to OpenRouter with backoff retry logic."""
    url = "https://openrouter.ai/api/v1/chat/completions"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": model_name,
        "messages": [
            {
                "role": "user", 
                "content": prompt
            }
        ],
        "max_tokens": max_tokens,
        "temperature": temperature
    }
    
    thread_id = threading.get_ident()
    logging.info(f"Thread {thread_id}: Calling API with model: {model_name}, max_tokens: {max_tokens}")
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()

def extract_content_between_markers(text, start_marker, end_marker, default_value=""):
    """Extract content between specific markers in text."""
    if not text:
        return default_value
    
    pattern = f"{re.escape(start_marker)}(.*?){re.escape(end_marker)}"
    match = re.search(pattern, text, re.DOTALL)
    
    if match:
        return match.group(1).strip()
    return default_value

def normalize_region(region):
    """Map any region name to one of the allowed values."""
    region = region.strip() if region else ""
    
    # Define mappings of various region names to the allowed values
    region_mappings = {
        # Latin America & Caribbean
        "latin america": "Latin America & Caribbean",
        "south america": "Latin America & Caribbean",
        "central america": "Latin America & Caribbean",
        "caribbean": "Latin America & Caribbean",
        
        # Sub-Saharan Africa
        "sub-saharan africa": "Sub-Saharan Africa",
        "southern africa": "Sub-Saharan Africa",
        "central africa": "Sub-Saharan Africa",
        "east africa": "Sub-Saharan Africa",
        "west africa": "Sub-Saharan Africa",
        
        # Europe
        "europe": "Europe",
        "western europe": "Europe",
        "eastern europe": "Europe",
        "northern europe": "Europe",
        "southern europe": "Europe",
        "eu": "Europe",
        
        # Asia-Pacific
        "asia-pacific": "Asia-Pacific",
        "asia pacific": "Asia-Pacific",
        "east asia": "Asia-Pacific",
        "southeast asia": "Asia-Pacific",
        "south asia": "Asia-Pacific",
        "oceania": "Asia-Pacific",
        "pacific": "Asia-Pacific",
        "australasia": "Asia-Pacific",
        
        # North Africa & Middle East
        "north africa": "North Africa & Middle East",
        "middle east": "North Africa & Middle East",
        "mena": "North Africa & Middle East",
        
        # Central Asia
        "central asia": "Central Asia"
    }
    
    # List of allowed values
    allowed_regions = [
        "North America",
        "Latin America & Caribbean",
        "Sub-Saharan Africa",
        "Europe",
        "Asia-Pacific",
        "North Africa & Middle East",
        "Central Asia",
        "Global / Uncertain"
    ]
    
    # Check if the region is already in the allowed values
    if region in allowed_regions:
        return region
    
    # Check if region matches any of our mappings (case insensitive)
    region_lower = region.lower()
    for key, value in region_mappings.items():
        if key in region_lower:
            return value
    
    # Country-based fallbacks
    country_region_map = {
        "united states": "North America",
        "canada": "North America",
        "mexico": "Latin America & Caribbean",
        "brazil": "Latin America & Caribbean",
        "argentina": "Latin America & Caribbean",
        "colombia": "Latin America & Caribbean",
        "chile": "Latin America & Caribbean",
        "peru": "Latin America & Caribbean",
        "united kingdom": "Europe",
        "france": "Europe",
        "germany": "Europe",
        "italy": "Europe",
        "spain": "Europe",
        "china": "Asia-Pacific",
        "japan": "Asia-Pacific",
        "india": "Asia-Pacific",
        "australia": "Asia-Pacific",
        "new zealand": "Asia-Pacific",
        "indonesia": "Asia-Pacific",
        "egypt": "North Africa & Middle East",
        "saudi arabia": "North Africa & Middle East",
        "israel": "North Africa & Middle East",
        "south africa": "Sub-Saharan Africa",
        "nigeria": "Sub-Saharan Africa",
        "kenya": "Sub-Saharan Africa",
        "kazakhstan": "Central Asia",
        "uzbekistan": "Central Asia"
    }
    
    
    # Return "Global / Uncertain" if we couldn't determine the region
    return "Global / Uncertain"

def parse_llm_response(response_text):
    """Parse the LLM response to extract location information."""
    try:
        # First try to parse as JSON in case the model returned JSON
        try:
            json_data = json.loads(response_text)
            if isinstance(json_data, dict) and "location" in json_data:
                json_data["region"] = normalize_region(json_data.get("region", ""))
                # Remove latitude and longitude if present
                if "latitude" in json_data:
                    json_data.pop("latitude")
                if "longitude" in json_data:
                    json_data.pop("longitude")
                return json_data
        except (json.JSONDecodeError, TypeError):
            pass
        
        # Extract each field between markers
        location = extract_content_between_markers(response_text, "<LOCATION>", "</LOCATION>", "")
        region = extract_content_between_markers(response_text, "<REGION>", "</REGION>", "")
        place = extract_content_between_markers(response_text, "<PLACE>", "</PLACE>", "")
        country = extract_content_between_markers(response_text, "<COUNTRY>", "</COUNTRY>", "")
        
        # Normalize the region to one of the allowed values
        normalized_region = normalize_region(region)
        
        # Create and return the result dictionary
        result = {
            "location": location,
            "region": normalized_region,
            "place": place,
            "country": country
        }
        
        return result
    
    except Exception as e:
        logging.error(f"Error parsing LLM response: {e}")
        return {
            "location": "",
            "region": "Global / Uncertain",
            "place": "",
            "country": ""
        }

def get_llm_response(url, name, description, domain_location, name_location, language, 
                     api_key, model_name, temperature=0.7, max_tokens=2000, max_retries=3):
    """Get location information from LLM via OpenRouter API."""
    
    # Prepare contextual hints
    context = []
    if domain_location:
        context.append(f"Domain analysis suggests a connection to: {domain_location}")
    if name_location:
        context.append(f"Name analysis suggests a connection to: {name_location}")
    if language and language != "English":
        context.append(f"The content is in {language} language")
    
    context_text = "\n".join(context) if context else "No preliminary location hints detected."
    
    prompt = f"""
    You are analyzing a CKAN data portal to determine its geographic location based on the following information:
    
    URL: {url}
    Name: {name}
    Description: {description}
    
    Context from preliminary analysis:
    {context_text}
    
    Based ONLY on this information, determine the most likely geographic location of this data portal.
    The location should be as specific as possible (city/town, region, country).
    
    IMPORTANT: 
    - Focus on the actual location of the organization/entity that operates this portal, not just where the data is about
    - Look for mentions of cities, regions, countries, government agencies, or organizations with known locations
    - Consider language clues, domain names, and regional terminology
    - DO NOT MAKE UP information that isn't supported by the provided text
    - If you're not sure, provide your best educated guess and indicate low confidence
    
    For the region field, you MUST categorize the location into EXACTLY ONE of these allowed values:
    - Latin America & Caribbean
    - Sub-Saharan Africa
    - Europe
    - Asia-Pacific
    - North Africa & Middle East
    - Central Asia
    - North America
    - Global / Uncertain (use this if you cannot determine a specific region or if it's a global/multi-regional site)
    
    Return your answer in the following format:
    
    <LOCATION>Full location (City, Region, Country)</LOCATION>
    <REGION>ONE OF THE ALLOWED REGION VALUES LISTED ABOVE</REGION>
    <PLACE>City or specific place</PLACE>
    <COUNTRY>Country name</COUNTRY>
    """
    
    for attempt in range(max_retries):
        try:
            result = call_openrouter_api(
                prompt, 
                api_key, 
                model_name, 
                max_tokens=max_tokens,
                temperature=temperature
            )
            
            content = result['choices'][0]['message']['content']

            # Handle potential control characters in JSON
            content = content.replace('\x00', '')  # Remove null bytes
            
            location_data = parse_llm_response(content)
            
            # Validate results (at minimum we should have country or place)
            if not location_data.get("country") and not location_data.get("place"):
                raise ValueError(f"No location information generated")
            
            return location_data, None
        
        except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, ValueError) as e:
            error_msg = f"Attempt {attempt+1}/{max_retries} failed: {str(e)}"
            logging.warning(error_msg)
            
            if attempt == max_retries - 1:  # Last attempt
                logging.error(f"All {max_retries} attempts failed")
                return {
                    "location": "",
                    "region": "",
                    "place": "",
                    "country": "",
                    "latitude": "",
                    "longitude": ""
                }, error_msg
            
            # Wait before retrying, with increasing delay
            time.sleep(2 ** attempt)
    
    # This should never happen due to the loop above, but just in case
    return {
        "location": "",
        "region": "",
        "place": "",
        "country": "",
        "latitude": "",
        "longitude": ""
    }, "Unknown error"

def process_site(api_key, row_data):
    """Process a single CKAN site with all necessary context."""
    index, row = row_data
    
    url = row.get('url', '')
    name = row.get('name', '')
    description = row.get('detailed_description', '')
    
    thread_id = threading.get_ident()
    logging.info(f"Thread {thread_id}: Processing site: {name} ({url})")
    
    # Skip if all fields are empty
    if (pd.isna(url) or url == "") and (pd.isna(name) or name == "") and (pd.isna(description) or description == ""):
        logging.warning(f"Thread {thread_id}: Skipping {name if not pd.isna(name) else 'unnamed site'}: No data")
        return index, {
            'location': "",
            'region': "Global / Uncertain",
            'place': "",
            'country': "",
            'error': "No data available"
        }
    
    # Preliminary analysis to provide context for the LLM
    domain_location = extract_location_from_tld(url)
    name_location = extract_location_from_domain(url)
    language = detect_language(description)
    
    # Get response from LLM
    result, error = get_llm_response(
        url, 
        name, 
        description,
        domain_location,
        name_location,
        language,
        api_key, 
        MODEL_NAME,
        temperature=TEMPERATURE,
        max_tokens=MAX_TOKENS,
        max_retries=MAX_RETRIES
    )
    
    # Prepare result
    if error:
        return index, {
            'location': "",
            'region': "Global / Uncertain",
            'place': "",
            'country': "",
            'error': error
        }
    else:
        return index, {
            'location': result.get("location", ""),
            'region': result.get("region", "Global / Uncertain"),
            'place': result.get("place", ""),
            'country': result.get("country", ""),
            'error': ""  # Clear any previous error
        }

def main():
    print("CKAN Location Analyzer")
    print("=" * 50)
    print(f"Input file: {INPUT_FILE}")
    print(f"Output file: {OUTPUT_FILE}")
    print(f"Model: {MODEL_NAME}")
    print(f"Threads: {NUM_THREADS}")
    print(f"Max retries: {MAX_RETRIES}")
    print()
    
    # Get API key from environment variable or hardcoded value
    api_key = os.getenv("OPEN_ROUTER_KEY") 
    
    # Check if input file exists
    if not os.path.exists(INPUT_FILE):
        logging.error(f"Input file '{INPUT_FILE}' not found!")
        return

    # Read the input CSV
    logging.info(f"Reading CSV from {INPUT_FILE}")
    try:
        df = pd.read_csv(INPUT_FILE)
    except Exception as e:
        logging.error(f"Error reading CSV: {e}")
        return
    
    # Validate required columns exist (or create them if missing)
    required_input_columns = ['url', 'name', 'detailed_description']
    missing_columns = [col for col in required_input_columns if col not in df.columns]
    
    if missing_columns:
        logging.warning(f"Missing columns: {', '.join(missing_columns)}. Creating empty columns.")
        for col in missing_columns:
            df[col] = ""
    
    # Limit rows if specified
    if ROWS_TO_PROCESS is not None:
        df = df.head(ROWS_TO_PROCESS)
        logging.info(f"Processing first {ROWS_TO_PROCESS} rows")
    else:
        logging.info(f"Processing all {len(df)} rows")
    
    # Create new columns if they don't exist
    output_columns = ['location', 'region', 'place', 'country', 'error']
    for col in output_columns:
        if col not in df.columns:
            df[col] = ""
    
    # Initialize region with "Global / Uncertain" for all rows
    if 'region' in df.columns:
        df['region'] = df['region'].apply(lambda x: x if x and not pd.isna(x) else "Global / Uncertain")
    
    # Setup for retry logic
    if RETRY_FAILED_ONLY:
        # Only process rows that previously failed
        to_process = df[df['error'].notna() & (df['error'] != "")]
        if len(to_process) == 0:
            logging.info("No failed rows to retry. Exiting.")
            return
        logging.info(f"Retrying {len(to_process)} previously failed rows")
    else:
        to_process = df
    
    # Create a lock for dataframe updates
    df_lock = threading.Lock()
    # Create a counter for tracking progress
    processed_count = 0
    total_count = len(to_process)
    
    # Create a progress bar
    progress_bar = tqdm(total=total_count, desc="Processing CKAN sites")
    
    # Function to update progress bar
    def update_progress():
        nonlocal processed_count
        with df_lock:
            processed_count += 1
            progress_bar.update(1)
    
    # Function to save the dataframe periodically
    def save_dataframe():
        with df_lock:
            df.to_csv(OUTPUT_FILE, index=False)
            logging.info(f"Intermediate save to {OUTPUT_FILE} completed")
    
    # Set up ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        # Submit tasks
        futures = {}
        for index, row in to_process.iterrows():
            future = executor.submit(process_site, api_key, (index, row))
            futures[future] = index
        
        # Track last save time
        last_save_time = time.time()
        
        # Process as they complete
        for future in as_completed(futures):
            try:
                index, result = future.result()
                
                # Update the dataframe with the result
                with df_lock:
                    for key, value in result.items():
                        df.at[index, key] = value
                
                update_progress()
                
                # Save periodically (every 5 items or 60 seconds)
                current_time = time.time()
                if processed_count % 5 == 0 or (current_time - last_save_time) > 60:
                    save_dataframe()
                    last_save_time = current_time
                    
                # Add a small delay to avoid rate limiting if needed
                time.sleep(0.1)
                
            except Exception as e:
                logging.error(f"Error processing row: {str(e)}")
                with df_lock:
                    df.at[futures[future], 'error'] = f"Processing error: {str(e)}"
                update_progress()
    
    # Close progress bar
    progress_bar.close()
    
    # Save the final updated dataframe
    logging.info(f"Saving final results to {OUTPUT_FILE}")
    df.to_csv(OUTPUT_FILE, index=False)
    
    # Report statistics
    successful = len(df[df['error'] == ""])
    failed = len(df[df['error'] != ""])
    countries_found = df['country'].notna() & (df['country'] != "")
    countries_count = len(df[countries_found])
    
    # Count regions by category
    region_counts = df['region'].value_counts()
    
    logging.info(f"Processing complete. Successfully processed: {successful}, Failed: {failed}")
    logging.info(f"Countries identified: {countries_count} ({countries_count/len(df)*100:.1f}%)")
    
    # Log distribution of regions
    logging.info("Region distribution:")
    for region, count in region_counts.items():
        logging.info(f"  {region}: {count} ({count/len(df)*100:.1f}%)")
    
    logging.info("Done!")

if __name__ == "__main__":
    main()