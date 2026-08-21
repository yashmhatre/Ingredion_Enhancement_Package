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
# Transport note: this calls the Statement Execution REST API with curl and a
# bearer token from `databricks auth token`, NOT `databricks api post`. CLI
# v1.9.0 returns "Not Found" for /api/2.0/sql/statements — the endpoint is not
# routed by that subcommand. Verified 2026-08-08.
#
# Usage:
#   scripts/audit_backfill_status.sh -p <profile> [-w <warehouse_id>]
#
#   -p  Databricks CLI profile (required)
#   -w  SQL warehouse ID. If omitted, the first RUNNING warehouse is used;
#       failing that, the first warehouse listed.
#
# Exit codes: 0 = every schema classified, 1 = could not run,
#             2 = ran but at least one schema was UNREADABLE (result incomplete).

set -uo pipefail

PROFILE=""
WAREHOUSE=""

while getopts "p:w:h" opt; do
  case "$opt" in
    p) PROFILE="$OPTARG" ;;
    w) WAREHOUSE="$OPTARG" ;;
    h) sed -n '2,30p' "$0"; exit 0 ;;
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

# --- auth -------------------------------------------------------------------
ME="$(databricks current-user me -p "$PROFILE" --output json 2>/dev/null \
      | python -c "import json,sys; print(json.load(sys.stdin).get('userName',''))" 2>/dev/null)"
if [[ -z "$ME" ]]; then
  echo "ERROR: profile '$PROFILE' is not authenticated." >&2
  echo "  Run: databricks auth login --profile $PROFILE" >&2
  exit 1
fi

HOST="$(databricks auth env -p "$PROFILE" 2>/dev/null \
        | python -c "import json,sys; print(json.load(sys.stdin).get('env',{}).get('DATABRICKS_HOST',''))" 2>/dev/null)"
[[ -z "$HOST" ]] && HOST="$(awk -v p="[$PROFILE]" '$0==p{f=1;next} /^\[/{f=0} f&&/^host/{sub(/^host[ \t]*=[ \t]*/,""); gsub(/\r/,""); print; exit}' ~/.databrickscfg)"
if [[ -z "$HOST" ]]; then
  echo "ERROR: could not determine the workspace host for profile '$PROFILE'." >&2
  exit 1
fi

TOKEN="$(databricks auth token -p "$PROFILE" 2>/dev/null \
         | python -c "import json,sys; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null)"
if [[ -z "$TOKEN" ]]; then
  echo "ERROR: could not obtain an access token for profile '$PROFILE'." >&2
  echo "  Run: databricks auth login --profile $PROFILE" >&2
  exit 1
fi

echo "Authenticated as: $ME"
echo "Workspace:        $HOST"

# --- warehouse --------------------------------------------------------------
if [[ -z "$WAREHOUSE" ]]; then
  WAREHOUSE="$(databricks warehouses list -p "$PROFILE" --output json 2>/dev/null | python -c "
import json,sys
try: whs = json.load(sys.stdin)
except Exception: sys.exit(0)
if not isinstance(whs, list) or not whs: sys.exit(0)
running = [w for w in whs if w.get('state') == 'RUNNING']
print((running or whs)[0].get('id',''))
" 2>/dev/null)"
  if [[ -z "$WAREHOUSE" ]]; then
    echo "ERROR: no SQL warehouse found. Pass one explicitly with -w <warehouse_id>." >&2
    exit 1
  fi
  echo "Warehouse:        $WAREHOUSE (auto-selected)"
else
  echo "Warehouse:        $WAREHOUSE"
fi

# --- helper: run a statement, emit rows as TSV, or 'ERR<TAB>message' --------
run_sql() {
  local stmt="$1" payload resp
  payload="$(python -c "
import json,sys
print(json.dumps({'statement': sys.argv[1], 'warehouse_id': sys.argv[2],
                  'wait_timeout': '50s', 'on_wait_timeout': 'CANCEL'}))" "$stmt" "$WAREHOUSE")"

  resp="$(curl -s -m 120 -X POST "$HOST/api/2.0/sql/statements" \
            -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
            -d "$payload" 2>/dev/null)"

  printf '%s' "$resp" | python -c "
import json,sys
raw = sys.stdin.read()
if not raw.strip():
    print('ERR\tempty response from the statements API'); sys.exit(0)
try: r = json.loads(raw)
except Exception:
    print('ERR\tunparseable response: ' + raw[:160].replace(chr(10),' ')); sys.exit(0)
st = (r.get('status') or {}).get('state')
if st != 'SUCCEEDED':
    msg = ((r.get('status') or {}).get('error') or {}).get('message') or r.get('message') or st or 'unknown error'
    print('ERR\t' + str(msg).replace(chr(10),' ')[:220]); sys.exit(0)
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
printf '%-16s %-38s %10s %10s %8s\n' "SCHEMA" "STATE" "NEEDS_BF" "POST_UPG" "TOTAL"
printf '%-16s %-38s %10s %10s %8s\n' "----------------" "--------------------------------------" "----------" "----------" "--------"

ACTION_NEEDED=(); UNREADABLE=(); ERRORS=()

for SCHEMA in "${SCHEMAS[@]}"; do
  COLS="$(run_sql "SELECT column_name FROM ${CATALOG}.information_schema.columns WHERE table_schema='${SCHEMA}' AND table_name='${AUDIT_TABLE}' AND column_name IN ('table','table_name')")"

  if [[ "$COLS" == ERR* ]]; then
    printf '%-16s %-38s %10s %10s %8s\n' "$SCHEMA" "UNREADABLE" "?" "?" "?"
    UNREADABLE+=("$SCHEMA"); ERRORS+=("$SCHEMA: ${COLS#ERR$'\t'}")
    continue
  fi

  HAS_OLD=0; HAS_NEW=0
  grep -qx "table"      <<<"$COLS" && HAS_OLD=1
  grep -qx "table_name" <<<"$COLS" && HAS_NEW=1

  if (( HAS_OLD == 0 && HAS_NEW == 0 )); then
    printf '%-16s %-38s %10s %10s %8s\n' "$SCHEMA" "not deployed — no _ingestion_audit" "-" "-" "-"
    continue
  fi

  if (( HAS_NEW == 0 )); then
    printf '%-16s %-38s %10s %10s %8s\n' "$SCHEMA" "PRE-0.5.0 — upgrade before backfill" "n/a" "0" "?"
    ACTION_NEEDED+=("$SCHEMA: pre-0.5.0 code — deploy the upgrade first, then re-audit")
    continue
  fi

  if (( HAS_OLD == 0 )); then
    ROW="$(run_sql "SELECT 0, count(*), count(*) FROM ${CATALOG}.${SCHEMA}.${AUDIT_TABLE}")"
    if [[ "$ROW" == ERR* ]]; then
      printf '%-16s %-38s %10s %10s %8s\n' "$SCHEMA" "UNREADABLE (count)" "?" "?" "?"
      UNREADABLE+=("$SCHEMA"); ERRORS+=("$SCHEMA: ${ROW#ERR$'\t'}")
      continue
    fi
    IFS=$'\t' read -r NEEDS POST TOTAL <<<"$ROW"
    printf '%-16s %-38s %10s %10s %8s\n' "$SCHEMA" "CLEAN — no legacy column" "0" "${POST:-?}" "${TOTAL:-?}"
    continue
  fi

  # Both columns present — the case #231 exists for.
  ROW="$(run_sql "SELECT count_if(table_name IS NULL), count_if(\`table\` IS NULL), count(*) FROM ${CATALOG}.${SCHEMA}.${AUDIT_TABLE}")"
  if [[ "$ROW" == ERR* ]]; then
    printf '%-16s %-38s %10s %10s %8s\n' "$SCHEMA" "UNREADABLE (count)" "?" "?" "?"
    UNREADABLE+=("$SCHEMA"); ERRORS+=("$SCHEMA: ${ROW#ERR$'\t'}")
    continue
  fi
  IFS=$'\t' read -r NEEDS POST TOTAL <<<"$ROW"

  if [[ "${NEEDS:-0}" == "0" ]]; then
    STATE="MIGRATED — legacy column still present"
  else
    STATE="** BACKFILL REQUIRED **"
    ACTION_NEEDED+=("$SCHEMA: $NEEDS of $TOTAL audit rows have table_name IS NULL")
  fi
  printf '%-16s %-38s %10s %10s %8s\n' "$SCHEMA" "$STATE" "${NEEDS:-?}" "${POST:-?}" "${TOTAL:-?}"
done

# --- summary ----------------------------------------------------------------
# An unreadable schema is NOT a clean schema. Never report "no backfill needed"
# unless every schema was actually classified — that is the exact failure this
# issue exists to prevent, one level up.
echo
if (( ${#ERRORS[@]} > 0 )); then
  echo "Errors:"
  for e in "${ERRORS[@]}"; do echo "  - $e"; done
  echo
fi

if (( ${#UNREADABLE[@]} > 0 )); then
  echo "RESULT: INCOMPLETE — ${#UNREADABLE[@]} of ${#SCHEMAS[@]} schemas could not be read (${UNREADABLE[*]})."
  echo "        No conclusion about those environments. Do NOT record this as a clean audit."
  (( ${#ACTION_NEEDED[@]} > 0 )) && { echo; echo "Of the schemas that were readable:"; for a in "${ACTION_NEEDED[@]}"; do echo "  - $a"; done; }
  EXIT=2
elif (( ${#ACTION_NEEDED[@]} == 0 )); then
  echo "RESULT: all ${#SCHEMAS[@]} schemas classified — none needs the backfill."
  EXIT=0
else
  echo "RESULT: action needed —"
  for a in "${ACTION_NEEDED[@]}"; do echo "  - $a"; done
  echo
  echo "The UPDATE is Tier 2/3 against staging or prod and needs the Project Lead's"
  echo "named sign-off (#231). Record the pre-image Delta version first:"
  echo "    DESCRIBE HISTORY ${CATALOG}.<schema>.${AUDIT_TABLE};"
  EXIT=0
fi

echo
echo "Columns:  NEEDS_BF = rows with table_name IS NULL (written before the upgrade)"
echo "          POST_UPG = rows with \`table\` IS NULL     (written after the upgrade)"
exit "$EXIT"
