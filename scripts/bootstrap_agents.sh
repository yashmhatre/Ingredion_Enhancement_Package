#!/usr/bin/env bash
# scripts/bootstrap_agents.sh
#
# Fetches this project's real agent definitions (prompts, workflows,
# configs) from the private ingredion-agent-config repo into .claude/agents/
# locally. .claude/agents/*.md (other than README.md) is gitignored — this
# script is the only thing that populates it. Never commit its output.
#
# Requires: git, and a GitHub token with read access to the private repo,
# in AGENT_CONFIG_TOKEN. This script reads that variable and nothing else:
# there is no `gh auth login` fallback, and an SSH deploy key will not work
# (the fetch below is HTTPS with an Authorization header, not SSH).
#   Local dev : a fine-grained PAT scoped to yashmhatre/ingredion-agent-config
#               with Contents: read-only, exported as AGENT_CONFIG_TOKEN.
#   CI        : the same kind of fine-grained PAT, injected from the
#               AGENT_CONFIG_TOKEN secret.
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

# GitHub's git-over-HTTPS endpoint accepts `bearer` only for GitHub App
# installation tokens (ghs_*). A personal access token — fine-grained or
# classic — must be sent as Basic auth, username `x-access-token`.
AUTH_B64="$(printf 'x-access-token:%s' "${AGENT_CONFIG_TOKEN}" | base64 | tr -d '\n')"

git -c http.extraheader="AUTHORIZATION: basic ${AUTH_B64}" \
  clone --depth 1 --branch "$VERSION" \
  "https://github.com/${REPO_OWNER}/${REPO_NAME}.git" "$TMP_DIR" >/dev/null

mkdir -p "$DEST_DIR"
# Wipe everything except this bootstrap script's own explanatory README,
# so a stale agent file from a previous version never lingers.
find "$DEST_DIR" -maxdepth 1 -type f ! -name 'README.md' -delete
cp "$TMP_DIR"/agents/*.md "$DEST_DIR"/

echo "Done. $(ls "$DEST_DIR"/*.md | grep -v README.md | wc -l | tr -d ' ') agent file(s) written to ${DEST_DIR}/ (gitignored)."
