import pandas as pd
import requests
import time
import logging
import warnings
from urllib.parse import quote

# Suppress warnings
warnings.filterwarnings("ignore")

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LocationGeocoder:
    def __init__(self, delay=1):
        self.delay = delay
        self.session = self._create_session()
        self.stats = {
            'total_processed': 0,
            'coordinates_found': 0,
            'continents_skipped': 0,
            'not_found': 0,
            'errors': 0
        }
        
        # List of continents and regions to skip
        self.continents_to_skip = {
            'africa', 'antarctica', 'asia', 'europe', 'north america', 
            'south america', 'oceania', 'australia', 'latin america',
            'latin america & caribbean', 'sub-saharan africa', 
            'asia-pacific', 'north africa & middle east', 'central asia',
            'global', 'uncertain', 'global / uncertain', 'worldwide',
            'international', 'multinational', 'regional'
        }

    def _create_session(self):
        """Create a requests session with proper headers."""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'LocationGeocoder/1.0 (Educational/Research Purpose)'
        })
        return session

    def is_continent_or_region(self, location):
        """Check if the location is a continent or large region to skip."""
        if not location or pd.isna(location):
            return True
        
        location_lower = str(location).lower().strip()
        
        # Check if it's in our skip list
        if location_lower in self.continents_to_skip:
            return True
        
        # Check if it contains continent/region keywords
        skip_keywords = ['continent', 'region', 'global', 'worldwide', 'international']
        for keyword in skip_keywords:
            if keyword in location_lower:
                return True
        
        return False

    def geocode_with_nominatim(self, location):
        """Geocode using OpenStreetMap Nominatim API (free)."""
        try:
            base_url = "https://nominatim.openstreetmap.org/search"
            params = {
                'q': location,
                'format': 'json',
                'limit': 1,
                'addressdetails': 1
            }
            
            url = f"{base_url}?q={quote(location)}&format=json&limit=1&addressdetails=1"
            logger.debug(f"Geocoding with Nominatim: {location}")
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data and len(data) > 0:
                result = data[0]
                lat = float(result['lat'])
                lon = float(result['lon'])
                
                # Format to match your requirement (5 decimal places)
                lat_formatted = f"{lat:.5f}"
                lon_formatted = f"{lon:.5f}"
                
                logger.info(f"Found coordinates for '{location}': {lat_formatted}, {lon_formatted}")
                return lat_formatted, lon_formatted, 'nominatim'
            
            return None, None, 'not_found_nominatim'
            
        except Exception as e:
            logger.warning(f"Nominatim geocoding failed for '{location}': {str(e)}")
            return None, None, 'error_nominatim'

    def geocode_with_photon(self, location):
        """Geocode using Photon API (alternative free service)."""
        try:
            base_url = "https://photon.komoot.io/api/"
            params = {
                'q': location,
                'limit': 1
            }
            
            logger.debug(f"Geocoding with Photon: {location}")
            
            response = self.session.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('features') and len(data['features']) > 0:
                feature = data['features'][0]
                coordinates = feature['geometry']['coordinates']
                lon, lat = coordinates  # Photon returns [lon, lat]
                
                # Format to match your requirement
                lat_formatted = f"{lat:.5f}"
                lon_formatted = f"{lon:.5f}"
                
                logger.info(f"Found coordinates for '{location}': {lat_formatted}, {lon_formatted}")
                return lat_formatted, lon_formatted, 'photon'
            
            return None, None, 'not_found_photon'
            
        except Exception as e:
            logger.warning(f"Photon geocoding failed for '{location}': {str(e)}")
            return None, None, 'error_photon'

    def geocode_location(self, location):
        """Geocode a location using multiple services."""
        if not location or pd.isna(location):
            return None, None, 'empty_location'
        
        location = str(location).strip()
        
        # Skip continents and regions
        if self.is_continent_or_region(location):
            logger.info(f"Skipping continent/region: {location}")
            self.stats['continents_skipped'] += 1
            return None, None, 'continent_skipped'
        
        logger.info(f"Geocoding: {location}")
        
        # Try Nominatim first (more reliable for cities)
        lat, lon, source = self.geocode_with_nominatim(location)
        if lat and lon:
            self.stats['coordinates_found'] += 1
            return lat, lon, source
        
        # If Nominatim fails, try Photon
        time.sleep(0.5)  # Small delay between services
        lat, lon, source = self.geocode_with_photon(location)
        if lat and lon:
            self.stats['coordinates_found'] += 1
            return lat, lon, source
        
        # No coordinates found
        logger.warning(f"No coordinates found for: {location}")
        self.stats['not_found'] += 1
        return None, None, 'not_found'

    def process_csv(self, input_file, output_file=None, location_column='location'):
        """Process the CSV file and add latitude/longitude columns."""
        try:
            # Read the CSV file
            logger.info(f"Reading CSV file: {input_file}")
            df = pd.read_csv(input_file)
            
            # Check if location column exists
            if location_column not in df.columns:
                raise ValueError(f"Column '{location_column}' not found in CSV. Available columns: {list(df.columns)}")
            
            # Initialize new columns
            df['latitude'] = ""
            df['longitude'] = ""
            df['geocode_source'] = ""
            
            # Process each location
            total_locations = len(df)
            self.stats['total_processed'] = total_locations
            logger.info(f"Processing {total_locations} locations...")
            
            for index, row in df.iterrows():
                location = row[location_column]
                
                logger.info(f"Processing {index + 1}/{total_locations}: {location}")
                
                # Add delay between requests to be respectful to free APIs
                if index > 0:
                    time.sleep(self.delay)
                
                # Geocode the location
                lat, lon, source = self.geocode_location(location)
                
                if lat and lon:
                    df.at[index, 'latitude'] = lat
                    df.at[index, 'longitude'] = lon
                    df.at[index, 'geocode_source'] = source
                else:
                    df.at[index, 'latitude'] = ""
                    df.at[index, 'longitude'] = ""
                    df.at[index, 'geocode_source'] = source or 'unknown_error'
                
                # Progress update every 10 locations
                if (index + 1) % 10 == 0:
                    self._print_progress_stats()
            
            # Close session
            self.session.close()
            
            # Save results
            if output_file is None:
                output_file = input_file.replace('.csv', '_with_coordinates.csv')
            
            logger.info(f"Saving results to: {output_file}")
            df.to_csv(output_file, index=False)
            
            # Generate final report
            self._generate_final_report()
            
            logger.info(f"Geocoding complete! Results saved to {output_file}")
            return df
            
        except Exception as e:
            logger.error(f"Error processing file: {str(e)}")
            if hasattr(self, 'session'):
                self.session.close()
            raise

    def _print_progress_stats(self):
        """Print current processing statistics."""
        logger.info("=== Progress Stats ===")
        logger.info(f"Coordinates found: {self.stats['coordinates_found']}")
        logger.info(f"Continents skipped: {self.stats['continents_skipped']}")
        logger.info(f"Not found: {self.stats['not_found']}")
        logger.info(f"Errors: {self.stats['errors']}")

    def _generate_final_report(self):
        """Generate and display final processing report."""
        logger.info("\n" + "="*50)
        logger.info("GEOCODING REPORT")
        logger.info("="*50)
        logger.info(f"Total locations processed: {self.stats['total_processed']}")
        logger.info(f"Coordinates found: {self.stats['coordinates_found']}")
        logger.info(f"Continents/regions skipped: {self.stats['continents_skipped']}")
        logger.info(f"Locations not found: {self.stats['not_found']}")
        logger.info(f"Errors encountered: {self.stats['errors']}")
        
        if self.stats['total_processed'] > 0:
            success_rate = (self.stats['coordinates_found'] / (self.stats['total_processed'] - self.stats['continents_skipped'])) * 100
            logger.info(f"Success rate (excluding skipped): {success_rate:.1f}%")
        
        logger.info("="*50)


# Usage example
if __name__ == "__main__":
    # Initialize the geocoder
    geocoder = LocationGeocoder(delay=1)  # 1 second delay between requests
    
    # Specify your input file path here
    input_file = "5.csv"  # Change this to your actual file path
    output_file = "6.csv"  # Optional: specify output file
    
    try:
        # Process the CSV file
        df = geocoder.process_csv(input_file, output_file, location_column='location')
        print(f"\nGeocoding completed!")
        print(f"Results saved to: {output_file}")
        
    except Exception as e:
        print(f"Script failed: {str(e)}")
        print("\nMake sure you have installed the required libraries:")
        print("pip install pandas requests")