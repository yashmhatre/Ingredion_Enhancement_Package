#!/usr/bin/env bash
# scripts/bootstrap_agents.sh
#
# Fetches this project's real agent definitions (prompts, workflows,
# configs) from the private ingredion-agent-config repo into .claude/agents/
# locally. .claude/agents/*.md (other than README.md) is gitignored — this
# script is the only thing that populates it. Never commit its output.
#
# Requires: git, and a credential with read access to the private repo.
#   Local dev : a personal GitHub PAT with access to yashmhatre/ingredion-agent-config,
#               exported as AGENT_CONFIG_TOKEN (or `gh auth login` with access to it).
#   CI        : a CI-scoped deploy key or fine-grained PAT with read-only access
#               to that one repo, injected as the AGENT_CONFIG_TOKEN secret.
#
# Never hardcode a token here or anywhere in this repo. This script only
# ever reads AGENT_CONFIG_TOKEN from the environment.

set -euo pipefail

REPO_OWNER="yashmhatre"
REPO_NAME="ingredion-agent-config"
LOCKFILE="agents.lock"
DEST_DIR=".claude/agents"

if [[ ! -f "$LOCKFILE" ]]; then
  echo "error: $LOCKFILE not found — nothing to pin the fetch to." >&2
  exit 1
fi

# agents.lock format: one line, e.g. `version=v1.0.0`
VERSION="$(grep -E '^version=' "$LOCKFILE" | cut -d= -f2)"
if [[ -z "$VERSION" ]]; then
  echo "error: could not read a version= line from $LOCKFILE." >&2
  exit 1
fi

if [[ -z "${AGENT_CONFIG_TOKEN:-}" ]]; then
  echo "error: AGENT_CONFIG_TOKEN is not set. This must be a credential scoped" >&2
  echo "       to read-only access on ${REPO_OWNER}/${REPO_NAME} — never a broad PAT." >&2
  exit 1
fi

echo "Fetching agent configs ${VERSION} from ${REPO_OWNER}/${REPO_NAME}..."

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

git -c http.extraheader="AUTHORIZATION: bearer ${AGENT_CONFIG_TOKEN}" \
  clone --depth 1 --branch "$VERSION" \
  "https://github.com/${REPO_OWNER}/${REPO_NAME}.git" "$TMP_DIR" >/dev/null

mkdir -p "$DEST_DIR"
# Wipe everything except this bootstrap script's own explanatory README,
# so a stale agent file from a previous version never lingers.
find "$DEST_DIR" -maxdepth 1 -type f ! -name 'README.md' -delete
cp "$TMP_DIR"/agents/*.md "$DEST_DIR"/

echo "Done. $(ls "$DEST_DIR"/*.md | grep -v README.md | wc -l | tr -d ' ') agent file(s) written to ${DEST_DIR}/ (gitignored)."
