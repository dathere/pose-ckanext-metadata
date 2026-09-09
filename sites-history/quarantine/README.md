# Quarantined crawls

Archives that were written before `check_crawl.sh` existed and that must not
reach `qsv cat rows sites-history/*.csv`. They are kept, not deleted: each one
is the evidence of a failure mode worth not repeating.

| File | What went wrong |
|---|---|
| `2026-09-06.csv` | 8 rows and no `name` column — step 1 produced a degraded `site_urls.csv` and step 5 archived the result anyway. Because `cat rows` stacks positionally, this one file made the weekly derive exit 1 on every subsequent run, and `continue-on-error: true` meant nobody was told. The weekly resources on the catalog silently stopped updating after 2026-08-30. |
