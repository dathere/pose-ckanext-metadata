# CKAN Ecosystem Metadata Collection

This repository contains automation scripts for sourcing and cataloging metadata from the CKAN ecosystem, including extensions and instances worldwide. The collected data powers the [CKAN Ecosystem Catalog](https://catalog.civicdataecosystem.org/) 

## Repository Structure

### Extension Metadata Scripts
- `1get_URL.py` - Discovers CKAN extensions on GitHub
- `2refresh.py` - Updates extension metadata 
- `3update_catalog.py` - Synchronizes data with the catalog
- `4uploadDataset.py` - Upload the file to datasets page

### CKAN Instance Data Collection (`sites-data-fetch/`)
- `0.csv` - Base dataset of CKAN instances
- `1-Name-Process.py` - Processes site names and converts titles to link-friendly identifiers.
- `2-CKANActionAPI copy.py` - Fetches data of instances via CKAN Action API
- `3-siteType.py` - Categorizes site types
- `4-Description.py` - Extracts site descriptions
- `5-Use AI To deduct Location copy.py` - Infers geographic locations
- `6-Geocode using OpenStreetMap Nominatim API.py` - Geocodes locations
- `7-tstamp.py` - Adds timestamps to metadata

## Automation

The repository includes a GitHub Actions workflow (`.github/workflows/update-ckan-metadata.yml`) that automatically fetches and updates extension metadata on a scheduled basis.
<img width="2749" height="3840" alt="Untitled diagram _ Mermaid Chart-2025-07-16-114648" src="https://github.com/user-attachments/assets/169fb3ee-4685-4051-9a5e-90f202b32988" />

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure CKAN API key 

3. Run the extension metadata collection:
   ```bash
   python 1get_URL.py
   python 2refresh.py
   python 3update_catalog.py
   ```
