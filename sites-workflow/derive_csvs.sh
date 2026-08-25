#!/usr/bin/env bash
# Build the derived CKAN sites CSVs from a crawl dump, using qsv.
#
#   derive_csvs.sh snapshot ckan_stats.csv out/   one crawl
#   derive_csvs.sh weekly   history.csv    out/   N crawls stacked (qsv cat rows)
#
# snapshot -> ckan_instances_clean.csv, ckan_extension_ranking.csv
# weekly   -> the same ranking plus ckan_weekly_long.csv, ckan_version_changes.csv,
#             ckan_extension_changes.csv, ckan_extension_trends.csv,
#             ckan_extension_series.csv, ckan_extension_cohort_series.csv
set -euo pipefail

MODE=${1:?usage: derive_csvs.sh <snapshot|weekly> <input.csv> [outdir]}
INPUT=${2:?usage: derive_csvs.sh <snapshot|weekly> <input.csv> [outdir]}
OUT=${3:-.}
HERE=$(cd "$(dirname "$0")" && pwd)

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$OUT"

# Row level: week, host, branch, status, vkey, ext (pipe-delimited), num_extensions, reachable
qsv luau map week,host,branch,status,vkey,ext,num_extensions,reachable \
    "$HERE/qsv/enrich.luau" "$INPUT" > "$TMP/enriched.csv"

# One row per (week, instance, plugin). The regex keeps rows that reported at
# least one plugin; the rest have nothing to explode.
qsv search -s ext '.' "$TMP/enriched.csv" \
    | qsv select week,name,ext \
    | qsv rename week,instance,name \
    | qsv explode name '|' > "$TMP/plugins.csv"

FIRST_WEEK=$(qsv sqlp "$TMP/enriched.csv" "SELECT MIN(week) FROM _t_1" | qsv behead)
LAST_WEEK=$(qsv sqlp "$TMP/enriched.csv" "SELECT MAX(week) FROM _t_1" | qsv behead)
NWEEKS=$(qsv sqlp "$TMP/enriched.csv" "SELECT COUNT(DISTINCT week) FROM _t_1" | qsv behead)
# Denominator for pct: instances that reported a plugin list in the last week,
# not all instances. "Of portals that told us, how many run this."
REPORTING=$(qsv sqlp "$TMP/enriched.csv" \
    "SELECT COUNT(*) FROM _t_1 WHERE ext <> '' AND week = '$LAST_WEEK'" | qsv behead)

qsv sqlp "$TMP/plugins.csv" "
    SELECT name, COUNT(*) AS count, ROUND(100.0 * COUNT(*) / $REPORTING, 1) AS pct
    FROM _t_1 WHERE week = '$LAST_WEEK'
    GROUP BY name ORDER BY count DESC, name" \
  | qsv luau map core,family "$HERE/qsv/plugin_meta.luau" > "$OUT/ckan_extension_ranking.csv"

INSTANCE_COLS=week,name,url,host,num_datasets,num_groups,num_organizations,ckan_version,branch,status,num_extensions,ext,reachable
INSTANCE_NAMES=week,name,url,host,num_datasets,num_groups,num_organizations,ckan_version,branch,status,num_extensions,extensions,reachable

if [ "$MODE" = snapshot ]; then
    qsv select "$INSTANCE_COLS" "$TMP/enriched.csv" | qsv rename "$INSTANCE_NAMES" \
        > "$OUT/ckan_instances_clean.csv"
    echo "snapshot: $(qsv count "$TMP/enriched.csv") instances, $REPORTING reporting plugins, $(qsv count "$OUT/ckan_extension_ranking.csv") distinct plugins"
    exit 0
fi

qsv select "$INSTANCE_COLS" "$TMP/enriched.csv" | qsv rename "$INSTANCE_NAMES" \
    > "$OUT/ckan_weekly_long.csv"

# Version changes. Comparing each reported version against the previous REPORTED
# one (WHERE ckan_version <> '') keeps a week of downtime from looking like a
# downgrade and back. vkey is zero-padded, so string order is version order.
qsv sqlp "$TMP/enriched.csv" "
WITH v AS (
    SELECT name, week, ckan_version, vkey, branch,
           LAG(ckan_version) OVER (PARTITION BY name ORDER BY week) AS prev_version,
           LAG(vkey)         OVER (PARTITION BY name ORDER BY week) AS prev_key,
           LAG(branch)       OVER (PARTITION BY name ORDER BY week) AS prev_branch
    FROM _t_1 WHERE ckan_version <> ''
)
SELECT name, week, prev_version AS \"from\", ckan_version AS \"to\",
       CASE WHEN vkey > prev_key THEN 'upgrade'
            WHEN vkey < prev_key THEN 'downgrade'
            ELSE 'change' END AS kind,
       prev_branch <> branch AS major
FROM v WHERE prev_version IS NOT NULL AND prev_version <> ckan_version
ORDER BY week, name" > "$OUT/ckan_version_changes.csv"

# Plugin add/remove events, again against the previous week the instance
# actually reported a list.
qsv sqlp "$TMP/plugins.csv" "$TMP/enriched.csv" "
WITH reported AS (SELECT DISTINCT name, week FROM _t_2 WHERE ext <> ''),
     prev AS (SELECT name, week,
                     LAG(week) OVER (PARTITION BY name ORDER BY week) AS prev_week
              FROM reported),
     added AS (
        SELECT p.name AS instance, p.week, c.name AS ext, 'a' AS kind
        FROM prev p
        JOIN _t_1 c ON c.instance = p.name AND c.week = p.week
        LEFT JOIN _t_1 o ON o.instance = p.name AND o.week = p.prev_week AND o.name = c.name
        WHERE p.prev_week IS NOT NULL AND o.name IS NULL),
     removed AS (
        SELECT p.name AS instance, p.week, o.name AS ext, 'r' AS kind
        FROM prev p
        JOIN _t_1 o ON o.instance = p.name AND o.week = p.prev_week
        LEFT JOIN _t_1 c ON c.instance = p.name AND c.week = p.week AND c.name = o.name
        WHERE p.prev_week IS NOT NULL AND c.name IS NULL),
     changes AS (SELECT * FROM added UNION ALL SELECT * FROM removed)
SELECT week, instance AS name,
       COALESCE(STRING_AGG(CASE WHEN kind = 'a' THEN ext END, '|'), '') AS added,
       COALESCE(STRING_AGG(CASE WHEN kind = 'r' THEN ext END, '|'), '') AS removed
FROM (SELECT * FROM changes ORDER BY ext)
GROUP BY week, instance ORDER BY week, name" > "$OUT/ckan_extension_changes.csv"

# Adoption. Raw weekly counts move with crawl reachability, so a plugin can
# "gain 30 installs" purely because more portals answered. delta is measured on
# the cohort — instances that reported a plugin list in EVERY week — which makes
# a change in the number a real install or uninstall.
qsv sqlp "$TMP/plugins.csv" "$TMP/enriched.csv" "
WITH cohort AS (SELECT name FROM _t_2 WHERE ext <> '' GROUP BY name
                HAVING COUNT(DISTINCT week) = $NWEEKS)
SELECT p.name,
       SUM(CASE WHEN p.week = '$LAST_WEEK' THEN 1 ELSE 0 END) AS count,
       ROUND(100.0 * SUM(CASE WHEN p.week = '$LAST_WEEK' THEN 1 ELSE 0 END) / $REPORTING, 1) AS pct,
       SUM(CASE WHEN p.week = '$LAST_WEEK' AND c.name IS NOT NULL THEN 1 ELSE 0 END)
     - SUM(CASE WHEN p.week = '$FIRST_WEEK' AND c.name IS NOT NULL THEN 1 ELSE 0 END) AS delta
FROM _t_1 p LEFT JOIN cohort c ON c.name = p.instance
GROUP BY p.name ORDER BY count DESC, name" \
  | qsv luau map core,family "$HERE/qsv/plugin_meta.luau" > "$OUT/ckan_extension_trends.csv"

# One column per crawl date, rather than the w1..wN of the old layout: the dates
# are the useful labels and they survive a missing week.
qsv pivotp week --index name --values instance --agg len "$TMP/plugins.csv" \
    > "$OUT/ckan_extension_series.csv"

qsv sqlp "$TMP/plugins.csv" "$TMP/enriched.csv" "
WITH cohort AS (SELECT name FROM _t_2 WHERE ext <> '' GROUP BY name
                HAVING COUNT(DISTINCT week) = $NWEEKS)
SELECT p.week, p.instance, p.name
FROM _t_1 p JOIN cohort c ON c.name = p.instance" > "$TMP/cohort_plugins.csv"

qsv pivotp week --index name --values instance --agg len "$TMP/cohort_plugins.csv" \
    > "$OUT/ckan_extension_cohort_series.csv"

echo "weekly: $NWEEKS crawls $FIRST_WEEK..$LAST_WEEK, $(qsv sqlp "$TMP/enriched.csv" "SELECT COUNT(DISTINCT name) FROM _t_1" | qsv behead) instances"
echo "  $(qsv count "$OUT/ckan_version_changes.csv") version changes, $(qsv count "$OUT/ckan_extension_changes.csv") plugin change events, $(qsv count "$OUT/ckan_extension_trends.csv") distinct plugins"
