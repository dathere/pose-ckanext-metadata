#!/usr/bin/env python3
"""
update_is_featured.py

Sets `is_featured` on a single dataset on the CKAN Ecosystem Catalog.
Designed to be called from GitHub Actions — reads all inputs from
environment variables, no interactive prompts.

Environment variables:
    CKAN_API_KEY       Admin API key (from GitHub secret)
    DATASET_URL        Full dataset URL, e.g.
                         https://ecosystem.ckan.org/site/cork-city-council-open-data
    IS_FEATURED        TRUE or FALSE
"""

import os
import sys
import requests
from urllib.parse import urlparse

BASE_URL = "https://ecosystem.ckan.org"
VALID_TYPES = ("extension", "site")


def extract_slug(url: str) -> tuple[str, str]:
    parsed = urlparse(url.strip())
    parts = [p for p in parsed.path.strip("/").split("/") if p]

    if len(parts) < 2 or parts[0] not in VALID_TYPES:
        raise ValueError(
            f"Invalid URL. Expected: {BASE_URL}/<extension|site>/<dataset-name>\n"
            f"Got: {url}"
        )

    return parts[0], parts[1]


def fetch_dataset(session: requests.Session, api_key: str, name: str) -> dict:
    resp = session.get(
        f"{BASE_URL}/api/3/action/package_show",
        headers={"Authorization": api_key},
        params={"id": name},
        timeout=30,
    )
    result = resp.json()
    if not result.get("success"):
        raise RuntimeError(f"Could not fetch dataset '{name}': {result.get('error')}")
    return result["result"]


def set_featured(session: requests.Session, api_key: str, dataset_id: str, value: str) -> dict:
    resp = session.post(
        f"{BASE_URL}/api/3/action/package_patch",
        headers={"Authorization": api_key, "Content-Type": "application/json"},
        json={"id": dataset_id, "is_featured": value},
        timeout=30,
    )
    result = resp.json()
    if not result.get("success"):
        raise RuntimeError(f"Update failed: {result.get('error')}")
    return result["result"]


def main():
    # Read from environment
    api_key     = os.environ.get("CKAN_API_KEY", "").strip()
    dataset_url = os.environ.get("DATASET_URL", "").strip()
    is_featured = os.environ.get("IS_FEATURED", "").strip().upper()

    # Validate
    errors = []
    if not api_key:
        errors.append("CKAN_API_KEY is not set")
    if not dataset_url:
        errors.append("DATASET_URL is not set")
    if is_featured not in ("TRUE", "FALSE"):
        errors.append(f"IS_FEATURED must be TRUE or FALSE, got: '{is_featured}'")

    if errors:
        for e in errors:
            print(f"::error::{e}")
        sys.exit(1)

    # Parse URL
    try:
        dataset_type, dataset_name = extract_slug(dataset_url)
    except ValueError as e:
        print(f"::error::{e}")
        sys.exit(1)

    session = requests.Session()

    # Fetch dataset
    print(f"Fetching dataset '{dataset_name}'…")
    try:
        dataset = fetch_dataset(session, api_key, dataset_name)
    except RuntimeError as e:
        print(f"::error::{e}")
        sys.exit(1)

    title   = dataset.get("title", dataset_name)
    current = dataset.get("is_featured", "(not set)")

    print(f"  Title              : {title}")
    print(f"  Type               : {dataset_type}")
    print(f"  Name               : {dataset_name}")
    print(f"  Current is_featured: {current}")
    print(f"  Setting is_featured: {is_featured}")

    # Update
    try:
        updated   = set_featured(session, api_key, dataset["id"], is_featured)
        new_value = updated.get("is_featured", "?")
        print(f"\n✔ Done. is_featured is now: {new_value}")
        print(f"  {BASE_URL}/{dataset_type}/{dataset_name}")
    except RuntimeError as e:
        print(f"::error::{e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
