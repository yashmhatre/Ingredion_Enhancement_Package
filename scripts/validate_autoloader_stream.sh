#!/usr/bin/env bash
# Auto Loader stream validation harness (#248).
#
# Drops files into a dev Volume in waves and submits one BOUNDED run between
# waves. Deliberately not a sustained stream: nothing in this workspace runs
# unattended, and bounded runs make every restart an observation point.
# See bronze_layer/docs/testing_autoloader_stream.md for the procedure and
# for what each wave is supposed to prove.
#
# Usage:
#   scripts/validate_autoloader_stream.sh wave-a     # 3 well-formed files
#   scripts/validate_autoloader_stream.sh wave-b     # 2 more files
#   scripts/validate_autoloader_stream.sh wave-drift # 1 file, new column
#   scripts/validate_autoloader_stream.sh wave-jsonl # 1 .jsonl file (#146 guard)
#   scripts/validate_autoloader_stream.sh run        # one bounded availableNow run
#   scripts/validate_autoloader_stream.sh teardown   # remove the dropped files
#
# DEV ONLY. Every path below is under ingredion_dev's scratch area.
set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-bronze-json-loader-dev}"
BASE="dbfs:/Volumes/ingredion_en/ingredion_dev/ext-ingredion-dev/pytest_scratch/al248"
SRC="$BASE/incoming"
CHK="$BASE/_checkpoint"
SCH="$BASE/_schema"
CATALOG="ingredion_en"
SCHEMA="ingredion_dev"
TABLE="al248_stream_bronze"

# Volume paths as the notebook wants them (no dbfs: prefix).
vol() { echo "${1#dbfs:}"; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

drop() {  # drop <filename> <content>
  printf '%s\n' "$2" > "$tmp/$1"
  databricks fs cp "$tmp/$1" "$SRC/$1" -p "$PROFILE" --overwrite
  echo "dropped $1"
}

case "${1:-}" in
  wave-a)
    drop a1.json '{"order_id":"A-1","customer_id":"C-1","amount":10}'
    drop a2.json '{"order_id":"A-2","customer_id":"C-2","amount":20}'
    drop a3.json '{"order_id":"A-3","customer_id":"C-3","amount":30}'
    ;;
  wave-b)
    drop b1.json '{"order_id":"B-1","customer_id":"C-4","amount":40}'
    drop b2.json '{"order_id":"B-2","customer_id":"C-5","amount":50}'
    ;;
  wave-drift)
    # New top-level column. With schemaEvolutionMode=addNewColumns Auto Loader
    # is documented to FAIL the stream on first sight and accept it on the
    # next start - so this wave expects a failure, then a success.
    drop d1.json '{"order_id":"D-1","customer_id":"C-6","amount":60,"currency":"USD"}'
    ;;
  wave-jsonl)
    # Two records, one per line. multiline defaults to the config value; this
    # is the shape the #146 truncation guard exists for.
    printf '%s\n%s\n' \
      '{"order_id":"J-1","customer_id":"C-7","amount":70}' \
      '{"order_id":"J-2","customer_id":"C-8","amount":80}' > "$tmp/j1.jsonl"
    databricks fs cp "$tmp/j1.jsonl" "$SRC/j1.jsonl" -p "$PROFILE" --overwrite
    echo "dropped j1.jsonl"
    ;;
  run)
    # One-off submit: persists no job resource and schedules nothing.
    # Requires `databricks bundle deploy -t dev` to have synced the notebook.
    summary="$(databricks bundle summary -t dev -o json -p "$PROFILE")"
    root="$(printf '%s' "$summary" | python -c 'import json,sys; print(json.load(sys.stdin)["workspace"]["file_path"])')"
    artifacts="$(printf '%s' "$summary" | python -c 'import json,sys; print(json.load(sys.stdin)["workspace"]["artifact_path"])')"
    # The wheel is NOT at bronze_layer/dist in the workspace - the bundle
    # rewrites `../dist/*.whl` to an absolute artifacts/.internal path at
    # deploy time. Resolve it rather than guessing, and fail loudly if the
    # bundle has not been deployed: a wrong path there produces a run that
    # fails on import, which reads like a streaming bug and is not one.
    wheel="$(MSYS_NO_PATHCONV=1 databricks workspace list "${artifacts#/Workspace}/.internal" -p "$PROFILE" 2>/dev/null | awk '/\.whl$/ {print $NF}' | head -1)"
    if [ -z "$wheel" ]; then
      echo "No wheel under ${artifacts}/.internal - run: databricks bundle deploy -t dev" >&2
      exit 1
    fi
    # `workspace list` reports paths without the /Workspace prefix; the
    # deployed job resources use it. Normalise so both agree.
    case "$wheel" in /Workspace/*) ;; *) wheel="/Workspace${wheel}" ;; esac
    echo "notebook: ${root}/bronze_layer/notebooks/run_ingestion"
    echo "wheel:    ${wheel}"
    databricks jobs submit -p "$PROFILE" --no-wait --json "$(cat <<JSON
{
  "run_name": "al248-autoloader-validation",
  "environments": [{"environment_key": "default",
                    "spec": {"client": "3", "dependencies": ["${wheel}"]}}],
  "tasks": [{
    "task_key": "stream_once",
    "environment_key": "default",
    "timeout_seconds": 1800,
    "notebook_task": {
      "notebook_path": "${root}/bronze_layer/notebooks/run_ingestion",
      "base_parameters": {
        "source_path": "$(vol "$SRC")/",
        "catalog": "${CATALOG}",
        "schema_name": "${SCHEMA}",
        "table": "${TABLE}",
        "ingestion_mode": "streaming",
        "checkpoint_location": "$(vol "$CHK")",
        "schema_location": "$(vol "$SCH")"
      }
    }
  }]
}
JSON
)"
    ;;
  teardown)
    databricks fs rm "$SRC" -p "$PROFILE" --recursive
    databricks fs rm "$CHK" -p "$PROFILE" --recursive
    databricks fs rm "$SCH" -p "$PROFILE" --recursive
    echo "removed dropped files, checkpoint and schema store"
    echo "NOTE: ${CATALOG}.${SCHEMA}.${TABLE} is left in place - DROP TABLE is Tier 2."
    ;;
  *)
    sed -n '2,20p' "$0"
    exit 1
    ;;
esac
