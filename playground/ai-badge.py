#!/usr/bin/env python3
"""
add_ai_badge.py

Prepends an AI Generated badge to the detailed_info field of all datasets
listed in a CSV file (column: url).

Badge added:
    ![AI Generated](https://img.shields.io/badge/ℹ️-AI%20Generated%20Description-green?style=for-the-badge)

Skips datasets that already have the badge to make the script safely re-runnable.

Usage:
    python add_ai_badge.py

Inputs (prompted at runtime):
    - Path to CSV file containing a 'url' column
    - CKAN Admin API key
"""

import csv
import sys
import time
import getpass
import requests
from urllib.parse import urlparse

BASE_URL = "https://ecosystem.ckan.org"

AI_BADGE = "![AI Generated](https://img.shields.io/badge/ℹ️-AI%20Generated%20Description-green?style=for-the-badge)"

SESSION_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "x-cf-bypass": "ckan-ecosystem-bypass-2026",
}

RATE_LIMIT_DELAY = 1  # seconds between every API call


# ── Helpers ───────────────────────────────────────────────────────────────────

def extract_name(url: str) -> str:
    """Extract dataset slug from URL e.g. https://ecosystem.ckan.org/extension/ckanext-dcat → ckanext-dcat"""
    parts = [p for p in urlparse(url.strip()).path.strip("/").split("/") if p]
    if len(parts) < 2:
        raise ValueError(f"Cannot parse dataset name from URL: {url}")
    return parts[1]


def api_get(session: requests.Session, url: str, params: dict = None) -> dict:
    time.sleep(RATE_LIMIT_DELAY)
    resp = session.get(url, params=params, timeout=30)

    if "Just a moment" in resp.text:
        raise RuntimeError("Blocked by Cloudflare. Check the x-cf-bypass header value.")
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status_code} — {resp.text[:200]}")

    result = resp.json()
    if not result.get("success"):
        raise RuntimeError(f"API error: {result.get('error')}")

    return result


def api_post(session: requests.Session, url: str, api_key: str, payload: dict) -> dict:
    time.sleep(RATE_LIMIT_DELAY)
    resp = session.post(
        url,
        headers={"Authorization": api_key, "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )

    if "Just a moment" in resp.text:
        raise RuntimeError("Blocked by Cloudflare. Check the x-cf-bypass header value.")
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status_code} — {resp.text[:200]}")

    result = resp.json()
    if not result.get("success"):
        raise RuntimeError(f"API error: {result.get('error')}")

    return result


def load_urls_from_csv(filepath: str) -> list[str]:
    """Read the 'url' column from the CSV file."""
    urls = []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "url" not in reader.fieldnames:
            raise ValueError(f"CSV file must have a 'url' column. Found: {reader.fieldnames}")
        for row in reader:
            url = row["url"].strip()
            if url:
                urls.append(url)
    return urls


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  CKAN Ecosystem — Add AI Generated Badge")
    print("=" * 60)

    # 1. CSV file path
    csv_path = "ai-datasets-url.csv" 

    try:
        urls = load_urls_from_csv(csv_path)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}")
        sys.exit(1)

    print(f"Loaded {len(urls)} dataset URL(s) from CSV.")

    # 2. API key
    api_key = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJZdlVuZU9hcEhhblFyVFF1YXNhbVQxTklHRU51eGZPekx6RVMwZnRrbjY4aDBxQ19uYUU4QmFhNlQ5M2xGNTZoZGU4VG52RmxVSmFKcVZZZyIsImlhdCI6MTc3Mjk5ODIyNX0.gf4_kl8bt39upYFBoyzzAWKBXvhu_dL9FInON7AHOMw"


    # 3. Confirm
    print(f"\nWill prepend AI badge to detailed_info for {len(urls)} dataset(s).")
    print(f"Rate limit: {RATE_LIMIT_DELAY}s between every API call.")
    estimated = (len(urls) * RATE_LIMIT_DELAY * 2) // 60  # fetch + patch per dataset
    print(f"Estimated time: ~{estimated} minutes\n")

    confirm = input("Proceed? [y/N]: ").strip().lower()
    if confirm not in ("y", "yes"):
        print("Aborted.")
        sys.exit(0)

    session = requests.Session()
    session.headers.update(SESSION_HEADERS)

    total        = len(urls)
    success_count = 0
    skipped_count = 0
    failed        = []

    print()

    for i, url in enumerate(urls, start=1):
        try:
            name = extract_name(url)
        except ValueError as e:
            print(f"  [{i}/{total}] ✗ Skipping bad URL: {e}")
            failed.append(url)
            continue

        print(f"  [{i}/{total}] {name}…", end=" ", flush=True)

        # Fetch current detailed_info
        try:
            result  = api_get(session, f"{BASE_URL}/api/3/action/package_show", params={"id": name})
            dataset = result["result"]
        except RuntimeError as e:
            print(f"✗ fetch failed: {e}")
            failed.append(url)
            continue

        current = dataset.get("detailed_info", "") or ""

        # Skip if badge already present
        if AI_BADGE in current:
            print("⏭  already has badge, skipping.")
            skipped_count += 1
            continue

        # Prepend badge
        updated = f"{AI_BADGE}\n\n{current}" if current else AI_BADGE

        # Patch dataset
        try:
            api_post(
                session,
                f"{BASE_URL}/api/3/action/package_patch",
                api_key,
                {"id": dataset["id"], "detailed_info": updated},
            )
            print("✔")
            success_count += 1
        except RuntimeError as e:
            print(f"✗ patch failed: {e}")
            failed.append(url)

    # Summary
    print()
    print("=" * 60)
    print(f"  ✔ Updated : {success_count}")
    print(f"  ⏭ Skipped : {skipped_count}  (badge already present)")
    print(f"  ✗ Failed  : {len(failed)}")
    print("=" * 60)

    if failed:
        print("\nFailed URLs:")
        for u in failed:
            print(f"  - {u}")


if __name__ == "__main__":
    main()