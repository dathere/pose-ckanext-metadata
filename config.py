#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Shared configuration for CKAN metadata pipelines.
"""


import os

# Cloudflare WAF bypass token (injected via env, never hardcode)
CF_BYPASS_TOKEN = os.environ.get("CF_BYPASS_TOKEN")
if not CF_BYPASS_TOKEN:
    raise RuntimeError("CF_BYPASS_TOKEN env var is not set")

# User agent for all HTTP requests
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# CKAN base URL
CKAN_BASE_URL = 'https://ecosystem.ckan.org'

# Common headers for all requests to ecosystem.ckan.org (WAF bypass + browser-like)
SESSION_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "x-cf-bypass": CF_BYPASS_TOKEN,
}
