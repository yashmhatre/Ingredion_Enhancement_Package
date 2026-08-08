#!/usr/bin/env bash
# Per-environment audit for #231 — the CHANGELOG 0.5.0 `table` -> `table_name`
# migration on `_ingestion_audit`.
#
# READ-ONLY. This script issues SELECTs against information_schema and COUNT(*)
# against `_ingestion_audit`. It writes nothing, in any environment. That is
# deliberate: the audit is Tier 0 under docs/agent_governance.md and needs no
# sign-off, while the UPDATE it informs is Tier 2/3 and does. Keep them separate
# — do not add the UPDATE to this script.
#
# Why an audit before the backfill at all: docs/roadmap.md Phase 0 records that
# `dev` was 38 commits ahead of `main`, so production was running pre-0.5.0 code.
# An environment still on the old code needs the upgrade before it needs the
# backfill, and running the UPDATE there would be a no-op that reads as success.
#
# Usage:
#   scripts/audit_backfill_status.sh -p <profile> [-w <warehouse_id>]
#
#   -p  Databricks CLI profile (required)
#   -w  SQL warehouse ID. If omitted, the first RUNNING warehouse is used;
#       failing that, the first warehouse listed.
#
# Exit codes: 0 = audit completed (regardless of findings), 1 = could not run.

set -uo pipefail

PROFILE=""
WAREHOUSE=""

while getopts "p:w:h" opt; do
  case "$opt" in
    p) PROFILE="$OPTARG" ;;
    w) WAREHOUSE="$OPTARG" ;;
    h) sed -n '2,26p' "$0"; exit 0 ;;
    *) echo "Usage: $0 -p <profile> [-w <warehouse_id>]" >&2; exit 1 ;;
  esac
done

if [[ -z "$PROFILE" ]]; then
  echo "ERROR: -p <profile> is required." >&2
  echo "Available profiles:" >&2
  grep '^\[' ~/.databrickscfg 2>/dev/null | tr -d '[]' | sed 's/^/  /' >&2
  exit 1
fi

CATALOG="ingredion_en"
SCHEMAS=("ingredion_dev" "ingredion_stg" "ingredion_prd")
AUDIT_TABLE="_ingestion_audit"

# --- auth check -------------------------------------------------------------
if ! databricks current-user me -p "$PROFILE" --output json >/dev/null 2>&1; then
  echo "ERROR: profile '$PROFILE' is not authenticated." >&2
  echo "  Run: databricks auth login --profile $PROFILE" >&2
  exit 1
fi
echo "Authenticated as: $(databricks current-user me -p "$PROFILE" --output json | grep -o '"userName":"[^"]*"' | cut -d'"' -f4)"

# --- warehouse --------------------------------------------------------------
if [[ -z "$WAREHOUSE" ]]; then
  WH_JSON="$(databricks warehouses list -p "$PROFILE" --output json 2>/dev/null)"
  WAREHOUSE="$(echo "$WH_JSON" | python -c "
import json,sys
try: whs = json.load(sys.stdin)
except Exception: sys.exit(0)
if not isinstance(whs, list) or not whs: sys.exit(0)
running = [w for w in whs if w.get('state') == 'RUNNING']
print((running or whs)[0].get('id',''))
" 2>/dev/null)"
  if [[ -z "$WAREHOUSE" ]]; then
    echo "ERROR: no SQL warehouse found. Pass one explicitly with -w <warehouse_id>." >&2
    echo "  List them: databricks warehouses list -p $PROFILE" >&2
    exit 1
  fi
  echo "Using warehouse: $WAREHOUSE (auto-selected)"
else
  echo "Using warehouse: $WAREHOUSE"
fi

# --- helper: run a statement, print rows as TSV -----------------------------
run_sql() {
  local stmt="$1"
  local payload
  payload="$(python -c "
import json,sys
print(json.dumps({
  'statement': sys.argv[1],
  'warehouse_id': sys.argv[2],
  'wait_timeout': '50s',
  'on_wait_timeout': 'CANCEL',
}))" "$stmt" "$WAREHOUSE")"

  echo "$payload" \
    | databricks api post /api/2.0/sql/statements -p "$PROFILE" --json @- 2>/dev/null \
    | python -c "
import json,sys
try: r = json.load(sys.stdin)
except Exception:
    print('ERR\tcould not parse API response'); sys.exit(0)
st = (r.get('status') or {}).get('state')
if st != 'SUCCEEDED':
    msg = ((r.get('status') or {}).get('error') or {}).get('message', st or 'unknown error')
    print('ERR\t' + str(msg).replace(chr(10), ' ')[:200]); sys.exit(0)
for row in ((r.get('result') or {}).get('data_array') or []):
    print('\t'.join('' if c is None else str(c) for c in row))
"
}

# --- audit ------------------------------------------------------------------
echo
echo "#231 — _ingestion_audit backfill audit"
echo "Catalog: $CATALOG   Table: $AUDIT_TABLE   $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "READ-ONLY — nothing below writes."
echo
printf '%-16s %-34s %-12s %-12s %s\n' "SCHEMA" "STATE" "NEEDS_BF" "POST_UPG" "TOTAL"
printf '%-16s %-34s %-12s %-12s %s\n' "----------------" "----------------------------------" "------------" "------------" "--------"

ACTION_NEEDED=()

for SCHEMA in "${SCHEMAS[@]}"; do
  # Which of the two columns exist? Drives everything below.
  COLS="$(run_sql "SELECT column_name FROM ${CATALOG}.information_schema.columns WHERE table_schema='${SCHEMA}' AND table_name='${AUDIT_TABLE}' AND column_name IN ('table','table_name')")"

  if [[ "$COLS" == ERR* ]]; then
    printf '%-16s %-34s %-12s %-12s %s\n' "$SCHEMA" "UNREADABLE — see note below" "-" "-" "-"
    echo "    ${COLS#ERR	}" >&2
    continue
  fi

  HAS_OLD=0; HAS_NEW=0
  grep -qx "table"      <<<"$COLS" && HAS_OLD=1
  grep -qx "table_name" <<<"$COLS" && HAS_NEW=1

  if (( HAS_OLD == 0 && HAS_NEW == 0 )); then
    printf '%-16s %-34s %-12s %-12s %s\n' "$SCHEMA" "table absent — not deployed here" "-" "-" "-"
    continue
  fi

  if (( HAS_NEW == 0 )); then
    printf '%-16s %-34s %-12s %-12s %s\n' "$SCHEMA" "PRE-0.5.0 — upgrade before backfill" "n/a" "0" "?"
    ACTION_NEEDED+=("$SCHEMA: running pre-0.5.0 code — deploy the upgrade first, then re-audit")
    continue
  fi

  if (( HAS_OLD == 0 )); then
    # Only table_name: either a clean install or the old column was dropped.
    ROW="$(run_sql "SELECT 0, count(*), count(*) FROM ${CATALOG}.${SCHEMA}.${AUDIT_TABLE}")"
    [[ "$ROW" == ERR* ]] && ROW="?	?	?"
    IFS=$'\t' read -r NEEDS POST TOTAL <<<"$ROW"
    printf '%-16s %-34s %-12s %-12s %s\n' "$SCHEMA" "CLEAN — no legacy column" "0" "$POST" "$TOTAL"
    continue
  fi

  # Both columns present — the case #231 exists for.
  ROW="$(run_sql "SELECT count_if(table_name IS NULL), count_if(\`table\` IS NULL), count(*) FROM ${CATALOG}.${SCHEMA}.${AUDIT_TABLE}")"
  if [[ "$ROW" == ERR* ]]; then
    printf '%-16s %-34s %-12s %-12s %s\n' "$SCHEMA" "UNREADABLE — see note below" "-" "-" "-"
    echo "    ${ROW#ERR	}" >&2
    continue
  fi
  IFS=$'\t' read -r NEEDS POST TOTAL <<<"$ROW"

  if [[ "${NEEDS:-0}" == "0" ]]; then
    STATE="MIGRATED — legacy column still present"
  else
    STATE="** BACKFILL REQUIRED **"
    ACTION_NEEDED+=("$SCHEMA: $NEEDS of $TOTAL audit rows have table_name IS NULL")
  fi
  printf '%-16s %-34s %-12s %-12s %s\n' "$SCHEMA" "$STATE" "${NEEDS:-?}" "${POST:-?}" "${TOTAL:-?}"
done

# --- summary ----------------------------------------------------------------
echo
if (( ${#ACTION_NEEDED[@]} == 0 )); then
  echo "RESULT: no environment needs the backfill."
else
  echo "RESULT: action needed —"
  for a in "${ACTION_NEEDED[@]}"; do echo "  - $a"; done
  echo
  echo "The UPDATE is Tier 2/3 against staging or prod and needs the Project Lead's"
  echo "named sign-off (#231). Record the pre-image Delta version first:"
  echo "    DESCRIBE HISTORY ${CATALOG}.<schema>.${AUDIT_TABLE};"
fi
echo
echo "Paste this table into #231. Columns:"
echo "  NEEDS_BF  rows with table_name IS NULL  — written before the upgrade"
echo "  POST_UPG  rows with \`table\` IS NULL     — written after the upgrade"
