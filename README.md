[![Datapump CKAN Sites Timeseries Metadata](https://github.com/dathere/pose-ckanext-metadata/actions/workflows/ckan_site_datapump.yml/badge.svg)](https://github.com/dathere/pose-ckanext-metadata/actions/workflows/ckan_site_datapump.yml)
[![Datapump CKAN Extensions Timeseries Metadata](https://github.com/dathere/pose-ckanext-metadata/actions/workflows/ckan_extension_datapump.yml/badge.svg)](https://github.com/dathere/pose-ckanext-metadata/actions/workflows/ckan_extension_datapump.yml)

# CKAN Ecosystem Metadata Pipelines


Data pipeline workflows for continuously cataloging metadata from CKAN instances and extensions worldwide. Powers the [CKAN Ecosystem Catalog](https://ecosystem.ckan.org/) with real-time insights into the open data infrastructure landscape.

[![CKAN](https://img.shields.io/badge/CKAN-2.10%2B-orange.svg)](https://ckan.org/)

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

<img width="2649" height="2860" alt="image" src="https://github.com/user-attachments/assets/e2232218-8f0f-494e-9b94-a7a3c28bf3b9" />

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
   - Appends instance snapshots to the datastore resource
   - Creates the new resource and pushes to it *before* deleting the old one, so a
     failed push cannot destroy the time series

5. **Archive and Derive** (`derive_csvs.sh`)
   - Archives the crawl as `sites-history/<crawl-date>.csv`
   - Builds the derived CSVs with [qsv](https://github.com/dathere/qsv): `luau` for
     row enrichment, `explode` for plugin rows, `sqlp` for aggregations and window
     functions, `pivotp` for the weekly series, `cat rows` for stacking the archive
   - From one crawl: `ckan_instances_clean.csv`, `ckan_extension_ranking.csv`
   - From the archive: `ckan_weekly_long.csv`, `ckan_version_changes.csv`,
     `ckan_extension_changes.csv`, `ckan_extension_trends.csv`,
     `ckan_extension_series.csv`, `ckan_extension_cohort_series.csv`

6. **Publish Resources** (`upload_derived.py`)
   - Uploads the eight derived CSVs to `ckan-sites-metadata`
   - Updates existing resources in place, so UUIDs, links and views survive

7. **Extension Install Counts** (`patch_instance_counts.py`)
   - Maps the plugins each instance reports back to catalog extension packages
   - Patches `instances_count` with the number of **distinct** instances running it
   - Extensions with no observed install are left untouched, not set to 0

8. **Commit Archive**
   - Commits `sites-history/<crawl-date>.csv` to `main`

Steps 5-8 run under `!cancelled()` and are `continue-on-error`, so a datastore
failure still leaves the crawl archived and the derived CSVs uploaded.

#### Crawl archive (`sites-history/`)

One CSV per crawl, written once and never rewritten. Seeded from the datastore
with 43 crawls covering 2025-07-18 to 2026-08-23. `qsv cat rows sites-history/*.csv`
reconstructs the full history, which is exactly what weekly mode consumes, and the
archive doubles as an off-datastore backup of the time series.

Crawls before 2025-10-29 recorded only dataset, group and organization counts —
`ckan_version` and `extensions` were not collected yet.

---

## Getting Started

### Prerequisites

- Python 3.9+
- [qsv](https://github.com/dathere/qsv) (sites pipeline, derive step — CI installs a pinned release binary)
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

Each workflow runs independently; no `concurrency` group is set, so two different
workflows can run at the same time.

---

## Data Access

### Public Catalog

Browse and download data via the [CKAN Ecosystem Catalog](https://ecosystem.ckan.org/):

- **Extensions Dataset**: [`ckan-extensions-metadata`](https://ecosystem.ckan.org/dataset/ckan-extensions-metadata) — time series of GitHub metrics per extension
- **Sites Dataset**: [`ckan-sites-metadata`](https://ecosystem.ckan.org/dataset/ckan-sites-metadata) — instance time series plus the eight derived analysis CSVs

Extension packages also carry `instances_count`: how many catalogued CKAN
instances were observed running that extension in the most recent crawl.


---

Project managed by

<img width="330" height="60" alt="image" src="https://github.com/user-attachments/assets/43f0b89d-a203-4d87-95b4-b89c78c65f6c" />
<img width="191" height="65" alt="image" src="https://github.com/user-attachments/assets/12b5e242-4ebc-4d39-b217-10a140e2ac15" />
<img width="338" height="40" alt="image" src="https://github.com/user-attachments/assets/393e5560-0a2e-453d-82af-afc4b4351b08" />


Funding provided through the National Science Foundation's Pathways to Enable Open Source Ecosystems (POSE) program.

<img width="99" height="100" alt="image" src="https://github.com/user-attachments/assets/2180f5f7-ef1a-4182-b5a5-e4d35fc8b9a6" />

