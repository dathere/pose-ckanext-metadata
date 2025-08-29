import requests
from urllib.parse import urlparse
import re
import csv
import logging
from typing import Dict, List, Tuple
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CKANSiteTypeDetector:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Enhanced domain patterns with priority scores
        self.domain_patterns = {
            'Government': {
                'patterns': [
                    r'\.gov(\.|$)', r'\.gob(\.|$)', r'\.govt(\.|$)', 
                    r'\.go\.[a-z]{2}', r'\.gc\.', r'\.mil(\.|$)',
                    r'government', r'federal', r'state\.', r'city\.',
                    r'ministry', r'municipal', r'council', r'administration',
                    r'public-data', r'opendata\.government', r'data\.gov',
                    r'datos\.gob', r'donnees\.gouv', r'dati\.gov',
                    r'\.admin\.', r'\.public\.', r'\.ciudad\.',
                    r'prefecture', r'region\.', r'departement\.',
                    r'provincia\.', r'comune\.', r'canton\.'
                ],
                'priority': 1
            },
            'Educational': {
                'patterns': [
                    r'\.edu(\.|$)', r'\.ac\.[a-z]{2}', r'\.edu\.[a-z]{2}',
                    r'university', r'universit', r'universidad',
                    r'college', r'school', r'academy', r'academia',
                    r'institute', r'education', r'educational',
                    r'campus', r'faculty', r'department',
                    r'hochschule', r'universiteit', r'universite',
                    r'politecnico', r'politechnic'
                ],
                'priority': 2
            },
            'Research': {
                'patterns': [
                    r'research', r'science', r'scientific', r'ciencia',
                    r'laboratory', r'laboratorio', r'labo',
                    r'innovation', r'technology', r'tech\.',
                    r'experiment', r'discovery', r'institut',
                    r'observatory', r'center-for', r'centre-for',
                    r'biomedical', r'genomics', r'physics',
                    r'chemistry', r'biology', r'ecology',
                    r'climate', r'weather', r'space', r'nasa',
                    r'cern', r'noaa', r'nih\.', r'nsf\.'
                ],
                'priority': 3
            },
            'Healthcare': {
                'patterns': [
                    r'health', r'hospital', r'medical', r'medicine',
                    r'clinic', r'healthcare', r'salud', r'sante',
                    r'nhs', r'medicare', r'medicaid', r'patient',
                    r'disease', r'treatment', r'therapy',
                    r'pharmaceutical', r'drug', r'nursing'
                ],
                'priority': 4
            },
            'Non-profit': {
                'patterns': [
                    r'\.org(\.|$)', r'foundation', r'fundacion',
                    r'ngo', r'nonprofit', r'non-profit',
                    r'charity', r'charitable', r'trust',
                    r'association', r'society', r'community',
                    r'humanitarian', r'volunteer', r'donation',
                    r'wwf', r'greenpeace', r'amnesty',
                    r'red-cross', r'united-nations', r'unesco'
                ],
                'priority': 5
            },
            'Commercial': {
                'patterns': [
                    r'\.com(\.|$)', r'\.io(\.|$)', r'\.biz(\.|$)',
                    r'enterprise', r'company', r'corporation',
                    r'ltd', r'inc', r'corp', r'gmbh', r'sarl',
                    r'business', r'commercial', r'market',
                    r'sales', r'product', r'service',
                    r'consulting', r'solutions', r'technologies'
                ],
                'priority': 6
            },
            'Transportation': {
                'patterns': [
                    r'transport', r'transit', r'railway', r'rail',
                    r'airport', r'aviation', r'traffic',
                    r'highway', r'roads', r'mobility',
                    r'logistics', r'shipping', r'freight',
                    r'metro', r'subway', r'bus', r'ferry'
                ],
                'priority': 7
            },
            'Environmental': {
                'patterns': [
                    r'environment', r'environmental', r'ecology',
                    r'climate', r'weather', r'meteorolog',
                    r'conservation', r'wildlife', r'nature',
                    r'sustainability', r'renewable', r'energy',
                    r'pollution', r'emissions', r'recycling',
                    r'biodiversity', r'ecosystem', r'forest'
                ],
                'priority': 8
            },
            'Agriculture': {
                'patterns': [
                    r'agriculture', r'agricultural', r'farming',
                    r'farm', r'crop', r'livestock', r'cattle',
                    r'fisheries', r'aquaculture', r'forestry',
                    r'food', r'nutrition', r'harvest',
                    r'irrigation', r'soil', r'seed'
                ],
                'priority': 9
            },
            'Regional': {
                'patterns': [
                    r'regional', r'local', r'district', r'county',
                    r'borough', r'township', r'parish',
                    r'metropolitan', r'urban', r'rural',
                    r'geographic', r'spatial', r'mapping',
                    r'cadastre', r'territory', r'zone'
                ],
                'priority': 10
            }
        }
    
    def analyze_domain(self, url: str) -> List[Tuple[str, float]]:
        """Analyze domain and return matching categories with confidence scores."""
        domain = urlparse(url).netloc.lower()
        matches = []
        
        for category, config in self.domain_patterns.items():
            score = 0
            matched_patterns = []
            
            for pattern in config['patterns']:
                if re.search(pattern, domain):
                    score += 1
                    matched_patterns.append(pattern)
            
            if score > 0:
                # Normalize score based on number of patterns
                confidence = min(score / len(config['patterns']) * 100, 100)
                # Boost confidence for exact TLD matches
                if category == 'Government' and re.search(r'\.gov(\.|$)', domain):
                    confidence = max(confidence, 90)
                elif category == 'Educational' and re.search(r'\.edu(\.|$)', domain):
                    confidence = max(confidence, 90)
                elif category == 'Non-profit' and re.search(r'\.org(\.|$)', domain):
                    confidence = max(confidence, 70)
                
                matches.append((category, confidence, matched_patterns))
        
        # Sort by confidence and priority
        matches.sort(key=lambda x: (x[1], -self.domain_patterns[x[0]]['priority']), reverse=True)
        return [(m[0], m[1]) for m in matches]
    
    def check_country_tld(self, url: str) -> Tuple[str, float]:
        """Check country-specific TLDs that often indicate government sites."""
        domain = urlparse(url).netloc.lower()
        
        # Country TLDs that often indicate government when combined with 'data' or 'open'
        gov_country_patterns = {
            r'\.uk$': ('Government', 60),
            r'\.ca$': ('Government', 60),
            r'\.au$': ('Government', 60),
            r'\.nz$': ('Government', 60),
            r'\.in$': ('Government', 55),
            r'\.za$': ('Government', 55),
            r'\.sg$': ('Government', 60),
            r'\.my$': ('Government', 55),
            r'\.jp$': ('Government', 50),
            r'\.kr$': ('Government', 50),
            r'\.cn$': ('Government', 60),
            r'\.de$': ('Regional', 45),
            r'\.fr$': ('Government', 50),
            r'\.es$': ('Government', 50),
            r'\.it$': ('Government', 50),
            r'\.nl$': ('Government', 50),
            r'\.be$': ('Government', 50),
            r'\.ch$': ('Government', 50),
            r'\.at$': ('Government', 50),
            r'\.eu$': ('Government', 65),
        }
        
        # Check if domain contains data-related keywords
        data_keywords = ['data', 'datos', 'donnees', 'daten', 'open', 'portal']
        has_data_keyword = any(keyword in domain for keyword in data_keywords)
        
        for pattern, (category, base_confidence) in gov_country_patterns.items():
            if re.search(pattern, domain):
                # Boost confidence if data keyword present
                confidence = base_confidence + 20 if has_data_keyword else base_confidence
                return category, min(confidence, 80)
        
        return "Unknown", 0
    
    def analyze_subdomain(self, url: str) -> Tuple[str, float]:
        """Analyze subdomain patterns for classification."""
        domain = urlparse(url).netloc.lower()
        subdomain_parts = domain.split('.')
        
        if len(subdomain_parts) > 2:
            subdomain = subdomain_parts[0]
            
            subdomain_patterns = {
                'data': ('Government', 65),
                'opendata': ('Government', 70),
                'datos': ('Government', 70),
                'research': ('Research', 75),
                'science': ('Research', 70),
                'health': ('Healthcare', 70),
                'transport': ('Transportation', 75),
                'environment': ('Environmental', 75),
                'education': ('Educational', 70),
                'portal': ('Government', 60),
                'geo': ('Regional', 65),
                'maps': ('Regional', 65),
                'statistics': ('Government', 65),
                'census': ('Government', 70),
            }
            
            for pattern, (category, confidence) in subdomain_patterns.items():
                if pattern in subdomain:
                    return category, confidence
        
        return "Unknown", 0
    
    def check_data_portal_patterns(self, url: str) -> Tuple[str, float]:
        """Check for common data portal patterns."""
        domain = urlparse(url).netloc.lower()
        
        # Generic data portal indicators
        if any(keyword in domain for keyword in ['dataplatform', 'dataportal', 'openplatform']):
            # Try to determine sector from other parts
            if 'gov' in domain or 'public' in domain:
                return "Government", 65
            elif 'research' in domain or 'science' in domain:
                return "Research", 65
            else:
                return "Regional", 55  # Generic data portals often serve regions
        
        # Check for numbered or geographic identifiers
        if re.search(r'\d{2,}', domain) or re.search(r'[a-z]{2}\d{2,}', domain):
            return "Regional", 50  # Often regional portals have codes
        
        return "Unknown", 0
    
    def statistical_classification(self, url: str) -> Tuple[str, float]:
        """Use statistical patterns for classification."""
        domain = urlparse(url).netloc.lower()
        
        # Length and structure analysis
        domain_parts = domain.split('.')
        
        # Short domains with data keywords likely government
        if len(domain_parts) == 2 and any(kw in domain for kw in ['data', 'open']):
            return "Government", 55
        
        # Complex subdomains often indicate institutional use
        if len(domain_parts) >= 4:
            if any(kw in domain for kw in ['univ', 'edu', 'acad']):
                return "Educational", 60
            else:
                return "Government", 50  # Complex government structures
        
        # Check for geographic identifiers (common in regional portals)
        if re.search(r'[a-z]+-[a-z]+', domain) or re.search(r'[a-z]+\.[a-z]+-[a-z]+', domain):
            return "Regional", 55
        
        return "Unknown", 0
    
    def default_classification(self, url: str) -> Tuple[str, float]:
        """Final fallback classification based on most common CKAN usage."""
        domain = urlparse(url).netloc.lower()
        
        # Most CKAN instances are government or regional
        # Make educated guess based on domain structure
        
        # If it has 'data' in domain, likely government
        if 'data' in domain or 'datos' in domain or 'donnees' in domain:
            return "Government", 45
        
        # If .org domain, likely non-profit
        if domain.endswith('.org'):
            return "Non-profit", 45
        
        # If .com domain, could be commercial or regional
        if domain.endswith('.com') or domain.endswith('.io'):
            # Check if it seems like a geographic/regional service
            if any(geo in domain for geo in ['map', 'geo', 'city', 'region', 'local']):
                return "Regional", 45
            else:
                return "Commercial", 40
        
        # Default: Regional (many CKAN instances serve regional data)
        return "Regional", 40
    
    def apply_fallback_methods(self, url: str) -> Tuple[str, float]:
        """Apply fallback methods when primary classification returns Unknown."""
        logger.info("Applying fallback classification methods...")
        
        # Method 1: Check TLD country codes for likely government sites
        site_type, confidence = self.check_country_tld(url)
        if site_type != "Unknown":
            return site_type, confidence
        
        # Method 2: Analyze subdomain patterns
        site_type, confidence = self.analyze_subdomain(url)
        if site_type != "Unknown":
            return site_type, confidence
        
        # Method 3: Check for data portal keywords
        site_type, confidence = self.check_data_portal_patterns(url)
        if site_type != "Unknown":
            return site_type, confidence
        
        # Method 4: Statistical analysis based on common patterns
        site_type, confidence = self.statistical_classification(url)
        if site_type != "Unknown":
            return site_type, confidence
        
        # Method 5: Default classification based on generic indicators
        return self.default_classification(url)
    
    def get_site_type(self, url: str) -> Tuple[str, float, Dict[str, any]]:
        """
        Determine the type of CKAN site with confidence score.
        Returns: (site_type, confidence, metadata)
        """
        try:
            logger.info(f"Analyzing site type for: {url}")
            
            # Analyze domain
            domain_matches = self.analyze_domain(url)
            
            # Combine scores (only domain scores now)
            combined_scores = {}
            
            # Add domain scores
            for category, score in domain_matches:
                combined_scores[category] = score
            
            # Get the best match
            if combined_scores:
                best_category = max(combined_scores.items(), key=lambda x: x[1])
                site_type = best_category[0]
                confidence = min(best_category[1], 100)
            else:
                site_type = "Unknown"
                confidence = 0
            
            # If Unknown, try fallback methods
            if site_type == "Unknown":
                site_type, confidence = self.apply_fallback_methods(url)
            
            # Prepare metadata
            metadata = {
                'domain': urlparse(url).netloc,
                'domain_matches': domain_matches[:3],  # Top 3 domain matches
            }
            
            logger.info(f"Site type: {site_type} (confidence: {confidence:.1f}%)")
            return site_type, confidence, metadata
            
        except Exception as e:
            logger.error(f"Error determining site type for {url}: {str(e)}")
            # Even on error, return a default classification
            return "Regional", 30, {'error': str(e)}
    
    def process_csv(self, input_file: str, output_file: str, 
                    url_column: str = 'url',
                    type_column: str = 'site_type',
                    confidence_column: str = 'type_confidence'):
        """Process CSV file and add site type information."""
        results = []
        
        try:
            # Read input CSV
            with open(input_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames.copy()
                
                # Add new columns if not present
                if type_column not in fieldnames:
                    fieldnames.append(type_column)
                if confidence_column not in fieldnames:
                    fieldnames.append(confidence_column)
                
                # Process each row
                for i, row in enumerate(reader, 1):
                    url = row.get(url_column, '').strip()
                    
                    if url:
                        logger.info(f"\nProcessing {i}: {url}")
                        
                        # Get site type
                        site_type, confidence, metadata = self.get_site_type(url)
                        
                        row[type_column] = site_type
                        row[confidence_column] = f"{confidence:.1f}%"
                        
                        # Add small delay to be respectful
                        time.sleep(0.2)  # Reduced delay since no API calls
                    else:
                        row[type_column] = ''
                        row[confidence_column] = ''
                    
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
    
    def analyze_single_url(self, url: str) -> None:
        """Analyze and print detailed information for a single URL."""
        site_type, confidence, metadata = self.get_site_type(url)
        
        print(f"\nSite Analysis for: {url}")
        print("=" * 50)
        print(f"Site Type: {site_type}")
        print(f"Confidence: {confidence:.1f}%")
        print(f"\nDomain: {metadata.get('domain', 'N/A')}")
        
        if metadata.get('domain_matches'):
            print("\nDomain Analysis:")
            for category, score in metadata['domain_matches']:
                print(f"  - {category}: {score:.1f}%")


def main():
    """Main function to run the site type detector."""
    # Configuration
    INPUT_FILE = '2.csv'
    OUTPUT_FILE = '3.csv'
    URL_COLUMN = 'url'
    TYPE_COLUMN = 'site_type'
    CONFIDENCE_COLUMN = 'type_confidence'
    
    print(f"Starting CKAN Site Type Detector...")
    print(f"Input file: {INPUT_FILE}")
    print(f"Output file: {OUTPUT_FILE}")
    
    # Check if input file exists
    import os
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: Input file '{INPUT_FILE}' not found!")
        return
    
    try:
        # Create detector instance
        detector = CKANSiteTypeDetector()
        
        # Process the CSV file
        detector.process_csv(INPUT_FILE, OUTPUT_FILE, URL_COLUMN, 
                           TYPE_COLUMN, CONFIDENCE_COLUMN)
        
        print(f"\nProcessing completed successfully!")
        print(f"Results saved to: {OUTPUT_FILE}")
        
    except Exception as e:
        print(f"\nERROR during processing: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
    