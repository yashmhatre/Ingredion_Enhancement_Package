# Keeping agent files private — architecture options and recommendation

This repo is public. The AI agent definitions in `.claude/agents/` — and
anything with real prompt content, workflow logic, or internal design
rationale baked into them — are proprietary and should never be readable in
this repo's git history, including on unmerged branches (a public repo
exposes every branch and every past commit, not just what's on `main`).
This doc compares options for solving that, and states which one this
project uses and why.

**The mindset shift that matters more than any single tool:** don't treat
this as "how do I hide files in git." Treat agent prompts/configs the same
way you'd treat any other sensitive runtime configuration — a versioned,
access-controlled artifact with its own release process, distributed to
whoever needs it (a developer's laptop, a CI runner, a production job) via
least-privilege, short-lived credentials, not as source code that happens to
need obscuring. That framing is what actually buys you secure runtime
loading, real versioning, and CI/CD support — a git trick alone doesn't.

## Options compared

### 1. Encrypted-in-repo (`git-crypt` / SOPS + a KMS key)

Keep the files in this same repo, but encrypted at rest; only holders of the
decryption key see plaintext, including in git history.

- **Pros:** Single repo, single PR history — no cross-repo sync problem.
  Lowest friction to adopt incrementally (`git-crypt init`, mark paths in
  `.gitattributes`, done). Works with existing CI by handing the runner a
  decryption key as a secret.
- **Cons:** The public repo still contains ciphertext forever. If the key is
  ever compromised or the encryption scheme is ever weakened, the *entire
  history* is retroactively exposed — you're carrying that risk
  indefinitely, not just today. Binary/encrypted-blob diffs make PR review
  of prompt changes effectively impossible (reviewers see "file changed,"
  not what changed). Most security reviews still flag "proprietary IP
  touching a public remote at all" as a finding, encrypted or not. Rejected
  for this project.

### 2. A dedicated private Git repository (submodule, subtree, or plain deploy-key clone)

Proprietary content lives in its own private repo. Consumers (local dev, CI)
clone it separately using a deploy key or scoped PAT.

- **Pros:** Familiar workflow — same PRs, same review culture, same branch
  protection this team already uses. Real diff history and blame. Versioning
  via ordinary git tags/branches.
- **Cons:** Git submodules are notoriously easy to get wrong (detached HEAD,
  forgetting to bump the pointer commit, "works on my machine" drift).
  Every consumer needs *git-level* credentials (an SSH deploy key or a PAT
  with repo scope) — a broader blast radius than a credential scoped to
  "read this one config bundle," since a leaked git credential can read the
  private repo's full history and any PR discussion in it, not just the
  current release.

### 3. A private package registry (GitHub Packages, Azure Artifacts, JFrog Artifactory)

Proprietary content is packaged as a versioned artifact (a wheel, an npm
package, or a generic package) and consumed with standard package-manager
tooling.

- **Pros:** Real semantic versioning and dependency pinning
  (`agent-configs==1.4.0`), which every consumer already knows how to work
  with. Easy to pin different versions per environment. Straightforward to
  layer provenance/signing on top later.
- **Cons:** Markdown prompts and YAML workflow configs aren't natural
  package payloads — they work as data files bundled into a package, but
  it's a slightly awkward fit for content that isn't code. Extra registry
  infrastructure to stand up and keep access-controlled (a private registry
  that's misconfigured to public is the same failure mode as a public repo).

### 4. A secrets manager (Azure Key Vault, Databricks secret scopes, HashiCorp Vault) holding the content directly

Store each agent file's content as a secret value, fetched at runtime.

- **Pros:** Centralized RBAC and audit logging out of the box — every read
  is logged, access is per-secret/per-scope. Encryption at rest and in
  transit by default. Fits directly into the identity model this project
  already uses (the per-target service principals in `databricks.yml`).
  Rotation is built in.
- **Cons:** Secrets managers are sized and designed for small secrets (API
  keys, connection strings), not a structured set of multi-KB prompt files —
  Key Vault secrets cap around 25 KB, and a secret store isn't built to be
  browsed, diffed, or organized as a file tree. Workable as a *thin
  credential layer* (see the recommendation below) but a poor fit as the
  actual content store.

### 5. Recommended — private Git repo (authoring) + private object storage (runtime distribution)

Split the two jobs the other options conflate into one tool. A **private Git
repository** (`ingredion-agent-config`) is the source of truth: agent files
live there as real markdown/YAML, get real PRs, real diffs, real review, and
a release is just a git tag. A CI job on that private repo, triggered on
tag, publishes the tagged bundle to **private, access-controlled object
storage** (an Azure Blob/ADLS Gen2 container, since this project already has
an Azure/Unity Catalog footprint) under a version-prefixed path
(`/releases/v1.2.0/agents/...`). Everything that needs to *consume* the
agents — a developer's laptop, a CI runner testing the public repo, a
production job — pulls from that storage location with a narrowly scoped,
short-lived credential (a SAS token or the same per-environment service
principal pattern `databricks.yml` already uses for
`run_as_service_principal`), never a git credential.

The public repo's only footprint is a **lockfile** (`agents.lock`) pinning
the exact version/commit hash to fetch, and a small bootstrap script that
does the fetching. No proprietary content — encrypted or not — ever enters
the public repo's history.

- **Pros:** Real PR review and diff history for prompt changes (private
  repo). Runtime credentials are scoped to "read this one published bundle,"
  not "read the whole private repo's history" — smaller blast radius than
  option 2 if a credential leaks. True versioning via git tags upstream and
  version-prefixed paths downstream — both a human-readable release history
  and a stable fetch target. Fits this project's existing Azure identity
  model instead of introducing a new one. Works identically for local dev
  (a developer's own scoped credential), CI (a CI-only service principal or
  federated credential), and production (the job's existing service
  principal, granted read on one more path).
- **Cons:** More moving parts than any single option above — a private repo
  *and* a storage account *and* a small CI job *and* a bootstrap script.
  You're building and owning that plumbing rather than getting it free from
  a single tool. Not worth it for a two-person side project; worth it the
  moment more than one environment (dev laptop, CI, prod) needs the same
  versioned content with different trust levels.

## Comparison at a glance

| | In public repo history? | Real diff/PR review | Versioning | Runtime credential scope | New infra required |
| --- | --- | --- | --- | --- | --- |
| 1. Encrypted-in-repo | Yes (ciphertext) | Poor (binary diffs) | Git tags | Decryption key = full history | None |
| 2. Private git repo alone | No | Good | Git tags | Full private-repo git access | One private repo |
| 3. Private package registry | No | Moderate (package diffs) | Strong (semver) | Registry read token | Registry |
| 4. Secrets manager alone | No | None (not a diffable store) | Manual/versioned secret names | Per-secret RBAC | Secrets manager |
| **5. Private repo + private storage (recommended)** | **No** | **Good (private repo)** | **Strong (tags + version paths)** | **Scoped to one published bundle** | **Private repo + storage account + small CI job** |

## What this repo actually does today

The full recommended shape above (private repo + a published, storage-backed
release) is the target state. Today, as a first concrete step, this repo
implements the private-repo half: `ingredion-agent-config` is live, holds
all 8 agent role definitions, and `agents.lock` pins a version of it
(`v1.0.0`) that `scripts/bootstrap_agents.sh` fetches directly via a scoped
GitHub credential — the same shape as option 2 above, deliberately, as the
fastest path to getting proprietary content out of this public repo's
history. Layering the private-storage publish step in front of it (so
runtime credentials never touch git directly) is the next hardening step,
not yet built. Update this section when it is.

## Recommendation for this project

**Option 5,** built out incrementally starting from option 2 (see above).
This project already has the two pieces the full version needs: a
GitHub-based review culture (reuse for the private repo) and an Azure
footprint with a per-environment service-principal pattern already proven
out in `databricks.yml` (reuse for runtime credentials — don't invent a
second identity model). The only new things to stand up are one private
repo (done) and one storage container (not yet), both of which this team
already knows how to operate.

Bumping `agents.lock` to a new tag is the update/versioning mechanism — an
ordinary, reviewable one-line PR diff in this repo, with the actual content
change reviewed separately in the private repo's own PR.

This gets every property asked for: private (never in this repo's git
history), securely loaded (a scoped credential, not a broad one), supports
local dev and CI/CD identically (same bootstrap script, different
credentials), versioned explicitly (`agents.lock` + git tags), and there's
nothing to accidentally expose by forgetting to `.gitignore` a file, because
the real content is never local to this repo's working tree at all — only
ever fetched into a gitignored path.
