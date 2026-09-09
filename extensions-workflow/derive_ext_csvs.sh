#!/usr/bin/env bash
# Build the derived CKAN extensions CSVs from a crawl dump, using qsv.
# The sites counterpart is sites-workflow/derive_csvs.sh; this mirrors it.
#
#   derive_ext_csvs.sh snapshot dynamic_metadata_update.csv out/   one crawl
#   derive_ext_csvs.sh weekly   history.csv                  out/  N crawls stacked
#
# snapshot -> ckan_ext_repos_clean.csv
# weekly   -> the same clean file plus ckan_ext_weekly_long.csv,
#             ckan_ext_growth.csv, ckan_ext_release_cadence.csv,
#             ckan_ext_maintenance.csv, ckan_ext_crawl_quality.csv
set -euo pipefail

MODE=${1:?usage: derive_ext_csvs.sh <snapshot|weekly> <input.csv> [outdir]}
INPUT=${2:?usage: derive_ext_csvs.sh <snapshot|weekly> <input.csv> [outdir]}
OUT=${3:-.}
HERE=$(cd "$(dirname "$0")" && pwd)

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$OUT"

# Row level: week, owner, repo, days_since_release, maintenance, has_releases, contributor_band
qsv luau map week,owner,repo,days_since_release,maintenance,has_releases,contributor_band \
    "$HERE/qsv/ext_enrich.luau" "$INPUT" > "$TMP/enriched.csv"

REPO_COLS=week,repository_name,owner,repo,url,stars,forks_count,open_issues,contributors_count,contributor_band,total_releases,latest_release,release_date,days_since_release,maintenance,has_releases,discussions

qsv select "$REPO_COLS" "$TMP/enriched.csv" > "$TMP/clean.csv"

if [ "$MODE" = snapshot ]; then
    cp "$TMP/clean.csv" "$OUT/ckan_ext_repos_clean.csv"
    LAST_WEEK=$(qsv sqlp "$TMP/enriched.csv" "SELECT MAX(week) FROM _t_1" | qsv behead)
    echo "snapshot: $(qsv count "$TMP/enriched.csv") repos at $LAST_WEEK, $(qsv sqlp "$TMP/enriched.csv" "SELECT COUNT(*) FROM _t_1 WHERE has_releases = 'true'" | qsv behead) with a release"
    exit 0
fi

cp "$TMP/clean.csv" "$OUT/ckan_ext_weekly_long.csv"

# Crawl quality, the extensions counterpart of the sites pipeline's gate. A
# crawl that half-failed still lands in the datastore: 2025-10-21 recorded 6
# repositories. Left ungated, a short week reads as hundreds of extensions
# vanishing and reappearing. Extensions have no "did the portal answer" signal,
# so the measure is crawl size — against the MEDIAN crawl, not the largest. The
# catalog genuinely grew from ~1041 to 1455 on 2026-08-30, and measuring
# against the peak would retroactively condemn every crawl before it.
MIN_CRAWL_PCT=${MIN_CRAWL_PCT:-50}
qsv sqlp "$TMP/enriched.csv" "
WITH per AS (SELECT week, COUNT(*) AS repos FROM _t_1 GROUP BY week),
     mid AS (SELECT MEDIAN(repos) AS typical FROM per)
SELECT p.week, p.repos, CAST(m.typical AS INTEGER) AS typical,
       ROUND(100.0 * p.repos / m.typical, 1) AS pct,
       100.0 * p.repos / m.typical >= $MIN_CRAWL_PCT AS included
FROM per p CROSS JOIN mid m ORDER BY p.week" > "$OUT/ckan_ext_crawl_quality.csv"

# `|| true`: qsv search exits 1 on no matches, which under `set -e` would abort
# with no explanation instead of reaching the message below.
GOOD_WEEKS=$(qsv search -s included '^true$' "$OUT/ckan_ext_crawl_quality.csv" \
    | qsv select week | qsv behead | sed "s/.*/'&'/" | paste -sd, - || true)
if [ -z "$GOOD_WEEKS" ]; then
    echo "✗ no crawl reaches ${MIN_CRAWL_PCT}% of the median; nothing to derive" >&2
    exit 1
fi
qsv sqlp "$TMP/enriched.csv" \
    "SELECT * FROM _t_1 WHERE week IN ($GOOD_WEEKS)" > "$TMP/enriched_ok.csv"
mv "$TMP/enriched_ok.csv" "$TMP/enriched.csv"

FIRST_WEEK=$(qsv sqlp "$TMP/enriched.csv" "SELECT MIN(week) FROM _t_1" | qsv behead)
LAST_WEEK=$(qsv sqlp "$TMP/enriched.csv" "SELECT MAX(week) FROM _t_1" | qsv behead)
NWEEKS=$(qsv sqlp "$TMP/enriched.csv" "SELECT COUNT(DISTINCT week) FROM _t_1" | qsv behead)
ALLWEEKS=$(qsv count "$OUT/ckan_ext_crawl_quality.csv")

qsv sqlp "$TMP/clean.csv" "SELECT * FROM _t_1 WHERE week = '$LAST_WEEK'" \
    > "$OUT/ckan_ext_repos_clean.csv"

# The cohort is the same idea as the sites pipeline's: measure change only on
# repos seen in EVERY week of a trailing window. A repo that drops out of one
# crawl (2026-08-16 returned 1041 of 1455) would otherwise look like it lost
# all its stars and gained them back.
COHORT_WEEKS=${COHORT_WEEKS:-8}
WINDOW_WEEKS=$(( NWEEKS < COHORT_WEEKS ? NWEEKS : COHORT_WEEKS ))
WINDOW_START=$(qsv sqlp "$TMP/enriched.csv" \
    "SELECT DISTINCT week FROM _t_1 ORDER BY week DESC LIMIT $WINDOW_WEEKS" | qsv behead | tail -1)

# Growth over the window, on the cohort only. delta columns are the headline:
# "is this project gaining attention", separate from "how big is it".
qsv sqlp "$TMP/enriched.csv" "
WITH cohort AS (SELECT repository_name FROM _t_1 WHERE week >= '$WINDOW_START'
                GROUP BY repository_name HAVING COUNT(DISTINCT week) = $WINDOW_WEEKS)
SELECT e.repository_name,
       MAX(CASE WHEN e.week = '$LAST_WEEK'    THEN e.stars END)              AS stars,
       MAX(CASE WHEN e.week = '$LAST_WEEK'    THEN e.forks_count END)        AS forks,
       MAX(CASE WHEN e.week = '$LAST_WEEK'    THEN e.contributors_count END) AS contributors,
       MAX(CASE WHEN e.week = '$LAST_WEEK'    THEN e.stars END)
     - MAX(CASE WHEN e.week = '$WINDOW_START' THEN e.stars END)              AS d_stars,
       MAX(CASE WHEN e.week = '$LAST_WEEK'    THEN e.forks_count END)
     - MAX(CASE WHEN e.week = '$WINDOW_START' THEN e.forks_count END)        AS d_forks,
       MAX(CASE WHEN e.week = '$LAST_WEEK'    THEN e.contributors_count END)
     - MAX(CASE WHEN e.week = '$WINDOW_START' THEN e.contributors_count END) AS d_contributors,
       MAX(CASE WHEN e.week = '$LAST_WEEK'    THEN e.total_releases END)
     - MAX(CASE WHEN e.week = '$WINDOW_START' THEN e.total_releases END)     AS d_releases
FROM _t_1 e JOIN cohort c ON c.repository_name = e.repository_name
WHERE e.week >= '$WINDOW_START'
GROUP BY e.repository_name ORDER BY d_stars DESC, stars DESC" > "$OUT/ckan_ext_growth.csv"

# Release activity. The crawl records total_releases and the latest release
# date, never the individual releases, so the honest cadence measure is how
# many releases appeared while we were watching — not a median gap we cannot
# see. weeks_observed makes the denominator explicit.
qsv sqlp "$TMP/enriched.csv" "
SELECT repository_name,
       MAX(CASE WHEN week = '$LAST_WEEK' THEN total_releases END)     AS total_releases,
       MAX(total_releases) - MIN(total_releases)                      AS releases_observed,
       COUNT(DISTINCT week)                                           AS weeks_observed,
       MAX(CASE WHEN week = '$LAST_WEEK' THEN days_since_release END) AS days_since_release,
       MAX(CASE WHEN week = '$LAST_WEEK' THEN maintenance END)        AS maintenance
FROM _t_1 GROUP BY repository_name
ORDER BY releases_observed DESC, total_releases DESC, repository_name" \
    > "$OUT/ckan_ext_release_cadence.csv"

# How the ecosystem's maintenance profile moves week to week.
# Two CTEs rather than a window over an aggregate: polars rejects the latter
# with "CSV format does not support nested data".
qsv sqlp "$TMP/enriched.csv" "
WITH per AS (SELECT week, maintenance, COUNT(*) AS count
             FROM _t_1 GROUP BY week, maintenance),
     tot AS (SELECT week, SUM(count) AS total FROM per GROUP BY week)
SELECT p.week, p.maintenance, p.count, ROUND(100.0 * p.count / t.total, 1) AS pct
FROM per p JOIN tot t ON t.week = p.week
ORDER BY p.week, p.count DESC" \
    > "$OUT/ckan_ext_maintenance.csv"

echo "weekly: $NWEEKS of $ALLWEEKS crawls usable, $FIRST_WEEK..$LAST_WEEK, $(qsv sqlp "$TMP/enriched.csv" "SELECT COUNT(DISTINCT repository_name) FROM _t_1" | qsv behead) repos"
echo "  excluded (under ${MIN_CRAWL_PCT}% of the median crawl): $(qsv sqlp "$OUT/ckan_ext_crawl_quality.csv" "SELECT week FROM _t_1 WHERE included = false ORDER BY week" | qsv behead | paste -sd' ' -)"
echo "  cohort window: $WINDOW_WEEKS weeks from $WINDOW_START, $(qsv count "$OUT/ckan_ext_growth.csv") repos in cohort"
echo "  $(qsv count "$OUT/ckan_ext_repos_clean.csv") repos in the latest crawl"
