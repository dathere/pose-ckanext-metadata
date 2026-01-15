# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data pipeline workflows for cataloging metadata from CKAN instances and extensions worldwide. Powers the [CKAN Ecosystem Catalog](https://ecosystem.ckan.org/) with automated weekly updates.

## Architecture

Two independent pipelines, each with 4-stage workflows:

### Extensions Pipeline (`extensions-workflow/`)
Collects GitHub metrics for CKAN extensions:
1. `1getURL.py` - Query CKAN catalog for extension repos, extract GitHub URLs -> `url_list.csv`
2. `2refresh.py` - Fetch GitHub metrics (stars, forks, releases, contributors, issues) -> `dynamic_metadata_update.csv`
3. `3updateCatalog.py` - Update CKAN package metadata via `package_patch` API
4. `datapump.py` - Append snapshots to datastore for time-series analysis

### Sites Pipeline (`sites-workflow/`)
Collects statistics from live CKAN instances:
1. `1getSitesURL.py` - Extract CKAN instance URLs from catalog -> `site_urls.csv`
2. `2CKANActionAPI.py` - Query CKAN Action API (`status_show`, `package_list`, etc.) with 10 concurrent workers -> `ckan_stats.csv`
3. `3updateSitesCatalog.py` - Update CKAN site metadata
4. `datapump.py` - Append snapshots to datastore

### Data Flow
Both pipelines follow: Discovery -> API Collection -> Catalog Sync -> Datastore Append

All scripts target `https://ecosystem.ckan.org` as the CKAN base URL.

## Running Locally

```bash
# Extensions pipeline
pip install -r requirements.txt
cd extensions-workflow
python 1getURL.py
GITHUB_TOKEN=your-token python 2refresh.py
CKAN_API_KEY=your-key python 3updateCatalog.py
CKAN_API_KEY=your-key python datapump.py

# Sites pipeline
pip install -r sites-workflow/requirements.txt
cd sites-workflow
python 1getSitesURL.py
python 2CKANActionAPI.py
CKAN_API_KEY=your-key python 3updateSitesCatalog.py
CKAN_API_KEY=your-key python datapump.py
```

## Environment Variables

- `GITHUB_TOKEN` - GitHub Personal Access Token (extensions pipeline)
- `CKAN_API_KEY` - CKAN API key with write permissions

## GitHub Actions

- **Extensions**: Sundays 02:00 UTC (`.github/workflows/ckan_extension_datapump.yml`)
- **Sites**: Sundays 03:00 UTC (`.github/workflows/ckan_site_datapump.yml`)

Secrets required: `GH_METADATA_TOKEN`, `CKAN_API_KEY`

## Key Dependencies

- `pandas` - CSV/DataFrame operations
- `requests` - HTTP API calls
- `PyGithub` - GitHub API wrapper (extensions only)
