# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data pipeline workflows for cataloging metadata from CKAN instances and extensions worldwide. Powers the [CKAN Ecosystem Catalog](https://ecosystem.ckan.org/) with automated weekly updates.

## Architecture

Three independent pipelines:

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

### YAML Metadata Pipeline (`yaml-workflow/`)
Fetches `ckan_ecosystem.yaml` from extension repositories and updates catalog metadata:
1. `update_from_yaml.py` - Interactive/CI script that:
   - Accepts extension names, catalog URLs, or `all` to process every extension
   - Fetches extension details from CKAN catalog to find the GitHub URL
   - Downloads and parses `ckan_ecosystem.yaml` from the repo (tries `main` then `master` branch)
   - Maps YAML fields (title, notes, tags, ckan_version, publisher, license, etc.) to CKAN package fields
   - Updates the catalog via `package_patch` API
   - Supports non-interactive mode via stdin for CI (reads piped input, auto-confirms)

### Data Flow
Extensions and Sites pipelines follow: Discovery -> API Collection -> Catalog Sync -> Datastore Append

YAML pipeline follows: Extension Lookup -> GitHub YAML Fetch -> Field Mapping -> Catalog Sync

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

# YAML metadata pipeline
pip install -r requirements.txt
cd yaml-workflow
CKAN_API_KEY=your-key python update_from_yaml.py            # interactive mode
echo "ckanext-spatial" | CKAN_API_KEY=your-key python update_from_yaml.py  # CI mode
echo "all" | CKAN_API_KEY=your-key AUTO_CONFIRM=true python update_from_yaml.py  # all extensions

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
- `AUTO_CONFIRM` - Set to `true` to skip interactive confirmation prompts (yaml pipeline)

## GitHub Actions

- **Extensions**: Sundays 02:00 UTC (`.github/workflows/ckan_extension_datapump.yml`)
- **Sites**: Sundays 03:00 UTC (`.github/workflows/ckan_site_datapump.yml`)

Secrets required: `GH_METADATA_TOKEN`, `CKAN_API_KEY`

## Key Dependencies

- `pandas` - CSV/DataFrame operations
- `cloudscraper` - HTTP API calls with Cloudflare bypass
- `PyGithub` - GitHub API wrapper (extensions only)
- `PyYAML` - YAML parsing (yaml pipeline)
