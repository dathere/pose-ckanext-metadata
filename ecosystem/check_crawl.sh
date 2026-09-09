#!/usr/bin/env bash
# Reject a crawl dump that is malformed or suspiciously short, BEFORE it is
# archived.
#
# A bad archive is worse than a missing one. `qsv cat rows` stacks the history
# positionally, so one file with a different column set does not degrade the
# result — it kills the whole weekly build. That is exactly what
# sites-history/2026-09-06.csv did: 8 rows, no `name` column, archived by a step
# whose only guard was `[ -f ... ]`. Because the archive step is
# continue-on-error, every weekly derived CSV then went stale in silence.
#
#   check_crawl.sh <crawl.csv> <expected-header> [history-dir] [min-pct]
#
# Exits non-zero with a reason on the first failure, so a caller can simply
# `check_crawl.sh ... || exit 1` before copying anything into the archive.

set -euo pipefail

CSV=${1:?usage: check_crawl.sh <crawl.csv> <expected-header> [history-dir] [min-pct]}
EXPECTED=${2:?usage: check_crawl.sh <crawl.csv> <expected-header> [history-dir] [min-pct]}
HISTORY=${3:-}
MIN_PCT=${4:-60}

if [ ! -f "$CSV" ]; then
    echo "✗ $CSV not found"
    exit 1
fi

# --trim so a CRLF dump (the crawler started emitting them on 2026-08-30) is not
# reported as a different schema than an LF one.
HEADER=$(qsv headers --just-names --trim "$CSV" | paste -sd, -)
if [ "$HEADER" != "$EXPECTED" ]; then
    echo "✗ $CSV header mismatch"
    echo "  expected: $EXPECTED"
    echo "  got:      $HEADER"
    exit 1
fi

ROWS=$(qsv count "$CSV")
if [ "$ROWS" -lt 1 ]; then
    echo "✗ $CSV has no data rows"
    exit 1
fi

# Compare against the newest archived crawl rather than a hardcoded floor, so
# the threshold tracks a catalog that grows (or shrinks) over time.
if [ -n "$HISTORY" ] && compgen -G "$HISTORY/*.csv" > /dev/null; then
    PREV_FILE=$(find "$HISTORY" -maxdepth 1 -name '*.csv' | sort | tail -1)
    PREV=$(qsv count "$PREV_FILE")
    FLOOR=$(( PREV * MIN_PCT / 100 ))
    if [ "$ROWS" -lt "$FLOOR" ]; then
        echo "✗ $CSV has $ROWS rows, under $MIN_PCT% of $(basename "$PREV_FILE") ($PREV rows, floor $FLOOR)"
        exit 1
    fi
    echo "✓ $CSV: $ROWS rows (prev $PREV, floor $FLOOR), header OK"
else
    echo "✓ $CSV: $ROWS rows, header OK"
fi
