#!/usr/bin/env bash
# Self-check for check_crawl.sh. Run it directly: ./test_check_crawl.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
CHECK="$HERE/check_crawl.sh"
HEADER='tstamp,name,url,num_datasets,num_groups,num_organizations,ckan_version,extensions'

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/history"

row() { echo "2026-09-07,site-$1,https://s$1.example,10,1,1,2.11.5,\"[\"\"pages\"\"]\""; }

make_csv() {  # make_csv <path> <n-rows> [header]
    { echo "${3:-$HEADER}"; for i in $(seq 1 "$2"); do row "$i"; done; } > "$1"
}

pass=0 fail=0
assert() {  # assert <expect-ok|expect-fail> <label> <args...>
    local want=$1 label=$2; shift 2
    if "$CHECK" "$@" > /dev/null 2>&1; then got=ok; else got=fail; fi
    if [ "$got" = "$want" ]; then
        pass=$((pass + 1)); echo "  ok   $label"
    else
        fail=$((fail + 1)); echo "  FAIL $label (wanted $want, got $got)"
    fi
}

make_csv "$TMP/history/2026-08-30.csv" 100
make_csv "$TMP/good.csv" 100
make_csv "$TMP/short.csv" 10
make_csv "$TMP/slightly-short.csv" 70
make_csv "$TMP/empty.csv" 0
# The real 2026-09-06 failure: no `name` column.
make_csv "$TMP/noname.csv" 8 'tstamp,url,num_datasets,num_groups,num_organizations,ckan_version,extensions'
# CRLF must not read as a schema change.
make_csv "$TMP/crlf.csv" 100
perl -pi -e 's/\n/\r\n/' "$TMP/crlf.csv"

echo "check_crawl.sh"
assert ok   "full crawl passes"                    "$TMP/good.csv"   "$HEADER" "$TMP/history"
assert ok   "70% of previous passes (floor 60%)"   "$TMP/slightly-short.csv" "$HEADER" "$TMP/history"
assert ok   "CRLF header is not a schema change"   "$TMP/crlf.csv"   "$HEADER" "$TMP/history"
assert ok   "no history dir means no floor"        "$TMP/short.csv"  "$HEADER"
assert fail "10% of previous is rejected"          "$TMP/short.csv"  "$HEADER" "$TMP/history"
assert fail "header without name is rejected"      "$TMP/noname.csv" "$HEADER" "$TMP/history"
assert fail "header-only file is rejected"         "$TMP/empty.csv"  "$HEADER" "$TMP/history"
assert fail "missing file is rejected"             "$TMP/nope.csv"   "$HEADER" "$TMP/history"

echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
