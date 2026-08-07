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

# agents.lock format: a version= line and a sha= line, e.g.
#   version=v1.0.0
#   sha=521ef53e23ee7d831e9686e2b26f3245298c5da3
VERSION="$(grep -E '^version=' "$LOCKFILE" | cut -d= -f2 || true)"
if [[ -z "$VERSION" ]]; then
  echo "error: could not read a version= line from $LOCKFILE." >&2
  exit 1
fi

# A tag is still just a name, and a name can be force-moved. `sha=` pins the
# commit that name resolved to when it was reviewed, so a moved tag is caught
# below instead of being installed silently. GitHub tag-protection rulesets
# are unavailable on this private repo's plan, which makes this check the only
# thing standing between a moved tag and every developer's .claude/agents/.
EXPECTED_SHA="$(grep -E '^sha=' "$LOCKFILE" | cut -d= -f2 || true)"
if [[ -z "$EXPECTED_SHA" ]]; then
  echo "error: could not read a sha= line from $LOCKFILE." >&2
  echo "       Every version= must be pinned to the commit it resolved to." >&2
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

# Fetch the release by its *tag* ref, explicitly.
#
# `clone --branch "$VERSION"` accepts a branch of that name just as happily
# as a tag, and a branch is mutable: someone pushing to a `v1.2.0` branch
# would silently change the agent definitions this script installs, with no
# agents.lock bump, no PR in this repo, and nothing in any audit trail.
# That defeats the one property agents.lock exists to provide.
#
# Naming refs/tags/ explicitly makes that impossible. A branch can never
# satisfy this refspec, and if the tag is missing the fetch fails loudly
# rather than falling back to something that merely shares its name.
git init -q "$TMP_DIR"
if ! git -C "$TMP_DIR" -c http.extraheader="AUTHORIZATION: basic ${AUTH_B64}"   fetch -q --depth 1   "https://github.com/${REPO_OWNER}/${REPO_NAME}.git"   "refs/tags/${VERSION}:refs/tags/${VERSION}" 2>/dev/null; then
  echo "error: no tag ${VERSION} in ${REPO_OWNER}/${REPO_NAME}." >&2
  echo "       Releases are pinned by tag, not by branch - a branch of the" >&2
  echo "       same name will not be used. Tag the release in the private" >&2
  echo "       repo first, then re-run." >&2
  exit 1
fi
git -C "$TMP_DIR" checkout -q "refs/tags/${VERSION}"

ACTUAL_SHA="$(git -C "$TMP_DIR" rev-parse "refs/tags/${VERSION}^{commit}")"
if [[ "$ACTUAL_SHA" != "$EXPECTED_SHA" ]]; then
  echo "error: tag ${VERSION} resolves to ${ACTUAL_SHA}," >&2
  echo "       but ${LOCKFILE} pins ${EXPECTED_SHA}." >&2
  echo "       The tag has been moved since it was pinned. Refusing to install." >&2
  echo "       If the move was intentional, update sha= in ${LOCKFILE} via PR." >&2
  exit 1
fi

mkdir -p "$DEST_DIR"
# Wipe everything except this bootstrap script's own explanatory README,
# so a stale agent file from a previous version never lingers.
find "$DEST_DIR" -maxdepth 1 -type f ! -name 'README.md' -delete
cp "$TMP_DIR"/agents/*.md "$DEST_DIR"/

echo "Done. $(ls "$DEST_DIR"/*.md | grep -v README.md | wc -l | tr -d ' ') agent file(s) written to ${DEST_DIR}/ (gitignored)."
