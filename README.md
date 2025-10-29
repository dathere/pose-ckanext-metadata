[![Datapump CKAN Sites Timeseries Metadata](https://github.com/a5dur/pose-ckanext-metadata/actions/workflows/ckan_site_datapump.yml/badge.svg)](https://github.com/a5dur/pose-ckanext-metadata/actions/workflows/ckan_site_datapump.yml)
[![Datapump CKAN Extensions Timeseries Metadata](https://github.com/a5dur/pose-ckanext-metadata/actions/workflows/ckan_extension_datapump.yml/badge.svg)](https://github.com/a5dur/pose-ckanext-metadata/actions/workflows/ckan_extension_datapump.yml)
# CKAN Ecosystem Metadata Pipelines


Data pipeline workflows for continuously cataloging metadata from CKAN instances and extensions worldwide. Powers the [CKAN Ecosystem Catalog](https://catalog.civicdataecosystem.org/) with real-time insights into the open data infrastructure landscape.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![CKAN](https://img.shields.io/badge/CKAN-2.7%2B-orange.svg)](https://ckan.org/)

---
## Pipeline Details

### Extensions Pipeline

**Trigger:** Every Sunday at 02:00 UTC (or manual dispatch)

**Stages:**

1. **Discovery** (`1getURL.py`)
   - Queries CKAN catalog for extension repositories
   - Outputs: `url_list.csv` with GitHub URLs

2. **Metadata Collection** (`2refresh.py`)
   - Fetches GitHub metrics via REST API
   - Metrics: stars, forks, releases, contributors, issues
   - Outputs: `dynamic_metadata_update.csv`

3. **Catalog Sync** (`3updateCatalog.py`)
   - Updates CKAN package metadata
   - Atomic updates with rollback on failure

4. **Time-Series Storage** (`datapump.py`)
   - Appends daily snapshots to datastore
   - Enables historical trend analysis
  


### CKAN Instance Data Collection (`sites-data-fetch/`)
Work in Progress

### Sites Pipeline

**Trigger:** Every Sunday at 03:00 UTC (1 hour after extensions)

**Stages:**

1. **Site Discovery** (`1getSitesURL.py`)
   - Extracts known CKAN instances from catalog
   - Outputs: `site_urls.csv`

2. **Instance Profiling** (`2CKANActionAPI.py`)
   - Queries CKAN Action API (`/api/3/action/status_show`)
   - Fetches: datasets, groups, organizations, version, extensions
   - Concurrent processing: 10 workers, 15s timeout
   - Outputs: `ckan_stats.csv`

3. **Catalog Update** (`3updateSitesCatalog.py`)
   - Syncs instance metadata to catalog

4. **Time-Series Storage** (`datapump.py`)
   - Appends instance snapshots to datastore

---

## Getting Started

### Prerequisites

- Python 3.9+
- CKAN API access with write permissions
- GitHub Personal Access Token (for extensions pipeline)

### Configuration

Set up Github secret variables:

```bash
CKAN_API_KEY="your-ckan-api-key"
GITHUB_TOKEN="your-github-token"  # For extensions pipeline
```

## Automation

### GitHub Actions Workflows

Both pipelines run automatically via GitHub Actions:

- **Extensions**: Sundays at 02:00 UTC
- **Sites**: Sundays at 03:00 UTC (staggered to avoid resource contention)

**Manual Triggering:**
1. Navigate to Actions tab in GitHub
2. Select workflow
3. Click "Run workflow"

**Monitoring:**
- Workflow status badges in README
- Artifact uploads on success (CSV files, 30-day retention)
- Debug artifact uploads on failure (logs, 7-day retention)
- Detailed execution summaries with file metrics

---

## Data Access

### Public Catalog

Browse and download data via the [CKAN Ecosystem Catalog](https://catalog.civicdataecosystem.org/):

- **Extensions Dataset**: `ckan-extensions-metadata`
- **Sites Dataset**: `ckan-sites-metadata`


---

Project managed by

<img width="330" height="60" alt="image" src="https://github.com/user-attachments/assets/43f0b89d-a203-4d87-95b4-b89c78c65f6c" />
<img width="191" height="65" alt="image" src="https://github.com/user-attachments/assets/12b5e242-4ebc-4d39-b217-10a140e2ac15" />
<img width="338" height="40" alt="image" src="https://github.com/user-attachments/assets/393e5560-0a2e-453d-82af-afc4b4351b08" />


Funding provided through the National Science Foundation's Pathways to Enable Open Source Ecosystems (POSE) program.

<img width="99" height="100" alt="image" src="https://github.com/user-attachments/assets/2180f5f7-ef1a-4182-b5a5-e4d35fc8b9a6" />

