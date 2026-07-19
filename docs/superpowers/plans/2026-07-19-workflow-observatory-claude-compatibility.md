# Workflow Observatory Claude Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `subagent-driven-development` to execute this plan task by task, with a fresh implementer and a fresh reviewer for every numbered task.

**Goal:** Ship deterministic `0.2.0-alpha.1` artifacts for one Workflow Observatory plugin root with separate native Codex and Claude adapters, one shared observation core, private Claude parent-marker propagation, preserved Codex behavior, and explicit real-runtime non-validation.

**Architecture:** Keep `wiki_observations.py` as the shared domain/storage core. Thin Codex and Claude CLI entry points explicitly select the only two valid surfaces and their platform homes. Native manifests and skills remain separate. Claude's single `SubagentStart` hook reads a private session binding and only injects the bounded parent marker; it never decides eligibility or observes prompt/transcript content.

**Tech Stack:** Python 3.11+, `unittest`, JSON/YAML metadata, Codex and Claude native validators, deterministic ZIP packaging, Git, GitHub CLI.

## Execution contract

- Work only in the isolated clone created for this feature. Do not edit the live LLM Wiki checkout, user stores, globally installed skills, or locally installed plugins.
- Start and finish the parent workflow observation outside plugin data. Every worker prompt must contain the unchanged marker `Observation managed by parent run obs-20260719-085555-b3c175; do not start a child observation.`
- Run every test or validator process under `caffeinate`.
- For each behavioral change: write the focused test, run it and capture the expected failure, make the smallest implementation change, then rerun the focused and broader suites.
- Keep platform boundaries explicit. Allowed `agent_surface` values are exactly `codex` and `claude`; there is no auto-detection or cross-surface fallback.
- Preserve existing Codex CLI arguments, default home, record bytes, normalized reports, skill behavior, frozen forward/lifecycle manifests, and native marketplace behavior.
- Claude defaults to `~/.claude/workflow-observatory`. `WORKFLOW_OBSERVATORY_HOME` remains an absolute override. Incompatible external LLM Wiki adapters fail closed without portable fallback, relabeling, or session binding.
- Observation records stay outside `${CLAUDE_PLUGIN_DATA}`. Disposable session bindings may use plugin data, but contain only schema version, run ID, and bounded lifecycle state.
- Claude hooks contain only `SubagentStart`. Do not add prompt, stop, session-end, transcript, or message hooks. Hook failure is non-blocking and enters documented skill-only degradation.
- Preserve current atomic record transitions without claiming future CAS, lease, retention, export, deletion, or migration work.
- Alternatives B and C remain documentation only; do not add placeholder manifests, directories, generators, code paths, or tests for them.
- The public alpha statement must remain exact and adjacent to compatibility claims:
  - `Claude support is alpha and has not yet been validated in a real Claude Code runtime.`
  - `If you validate it, find a compatibility issue, or implement a fix, please submit a pull request with sanitized reproduction evidence.`
- Do not push or publish until all deterministic source and clean-room gates pass, every task review is resolved, a final broad review approves the exact release commit, remote readback succeeds, and the downloaded asset checksum is verified.

---

### Task 1: Repair source-checkout packaging without weakening the allowlist

**Files:**

- Modify: `plugins/workflow-observer/tests/test_package_archive.py`
- Modify: `evidence/scripts/package_workflow_observatory.py`

**Step 1: Write the failing checkout-metadata regression**

Add a focused package test that copies the representative marketplace, creates `.git/HEAD` and another nested `.git` file, and proves normal archive construction should succeed. In the same test class, add or retain a distinct case proving an unrelated root file such as `.unexpected` is rejected.

```python
def test_repository_git_metadata_is_ignored_but_other_root_files_are_rejected(self):
    git_dir = self.marketplace_root / ".git" / "refs" / "heads"
    git_dir.mkdir(parents=True)
    (self.marketplace_root / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
    (git_dir / "main").write_text("deadbeef\n")
    self.run_packager(expect_success=True)

    (self.marketplace_root / ".unexpected").write_text("must still fail\n")
    self.run_packager(expect_success=False)
```

Use the repository's existing fixture helpers and assertions rather than introducing a second packaging harness.

**Step 2: Run RED**

```bash
caffeinate -dimsu -- python3 -m unittest plugins.workflow-observer.tests.test_package_archive
```

Expected: the new `.git/HEAD` case fails because `_marketplace_files()` reports it as an unexpected marketplace file, while the unrelated-file rejection remains green.

**Step 3: Exclude only root Git metadata**

In `_marketplace_files()`, compute the path relative to the marketplace root before inspecting or admitting it. Skip only paths whose first relative component is exactly `.git`.

```python
relative = path.relative_to(MARKETPLACE_ROOT)
if relative.parts and relative.parts[0] == ".git":
    continue
```

Do not ignore arbitrary dot-directories, change the public allowlist, relax symlink checks, or suppress unexpected plugin files.

**Step 4: Run GREEN and non-regression suites**

```bash
caffeinate -dimsu -- python3 -m unittest plugins.workflow-observer.tests.test_package_archive
caffeinate -dimsu -- python3 -m unittest discover -s plugins/workflow-observer/tests -p 'test_*.py'
caffeinate -dimsu -- python3 -m unittest discover -s evidence/tests -p 'test_*.py'
```

Verify package completeness, personal-path rejection, symlink rejection, SHA inventory, and byte reproducibility tests still pass.

**Step 5: Commit the prerequisite separately**

```bash
git add plugins/workflow-observer/tests/test_package_archive.py evidence/scripts/package_workflow_observatory.py
git commit -m "fix: ignore git metadata when packaging checkouts"
```

---

### Task 2: Add the shared Claude surface and isolated platform homes

**Files:**

- Modify: `plugins/workflow-observer/scripts/wiki_observations.py`
- Modify: `plugins/workflow-observer/scripts/wiki_observer_cli.py`
- Create: `plugins/workflow-observer/scripts/claude_workflow_observer_cli.py`
- Modify: `plugins/workflow-observer/scripts/store_config.py`
- Modify: `plugins/workflow-observer/tests/test_wiki_observations.py`
- Modify: `plugins/workflow-observer/tests/test_wiki_observer_cli.py`
- Modify: `plugins/workflow-observer/tests/test_store_config.py`
- Create or modify: `plugins/workflow-observer/tests/test_claude_workflow_observer_cli.py`
- Modify only if required by its canonical synchronization contract: `plugins/workflow-observer/core_source/**`

**Step 1: Write RED tests for the exact surface set**

Add core round-trip tests for Claude starts, finishes, reports, and integrity; retain Codex fixtures and byte assertions; reject every unknown label.

```python
for surface in ("codex", "claude"):
    record = valid_record(agent_surface=surface)
    self.assertEqual(validate_record(record), [])

for surface in ("", "Claude", "codex-cli", "other"):
    self.assertValidationError(agent_surface=surface)
```

Add paired isolated-home CLI tests: direct `wiki_observer_cli.py` writes only to a test Codex home, while `claude_workflow_observer_cli.py` writes only to a test Claude home and persists `agent_surface: claude`. Test explicit absolute overrides and rejection of relative overrides.

Add a fake Codex-only LLM Wiki CLI. A Claude delegated start must fail, create neither a portable record nor a Claude session binding, and never produce a Codex-labeled substitute.

**Step 2: Run focused RED tests**

```bash
caffeinate -dimsu -- python3 -m unittest \
  plugins.workflow-observer.tests.test_wiki_observations \
  plugins.workflow-observer.tests.test_wiki_observer_cli \
  plugins.workflow-observer.tests.test_store_config \
  plugins.workflow-observer.tests.test_claude_workflow_observer_cli
```

Expected: Claude surface validation and/or wrapper import/default-home assertions fail while existing Codex cases remain green.

**Step 3: Parameterize the shared CLI without changing Codex defaults**

Change the shared parser and main entry point to accept caller-selected defaults while preserving the direct entry point:

```python
def build_parser(*, agent_surface: str = "codex") -> argparse.ArgumentParser:
    ...

def main(
    argv: Sequence[str] | None = None,
    *,
    agent_surface: str = "codex",
    default_home: Path | None = None,
) -> int:
    ...
```

The parser exposes only the caller-selected surface; the wrapper must not let a user relabel Claude work as Codex or vice versa. Update shared validation to the exact set `{"codex", "claude"}`.

Create a thin Claude wrapper:

```python
from pathlib import Path
from wiki_observer_cli import main as shared_main

if __name__ == "__main__":
    raise SystemExit(
        shared_main(agent_surface="claude", default_home=Path.home() / ".claude" / "workflow-observatory")
    )
```

Keep the existing direct CLI's default `~/.codex/workflow-observatory`. Preserve config schema version 1 and explicit `portable`/`llmwiki` selection. Do not fall back between stores or surfaces.

**Step 4: Run focused GREEN, canonical synchronization, and full suites**

```bash
caffeinate -dimsu -- python3 -m unittest \
  plugins.workflow-observer.tests.test_wiki_observations \
  plugins.workflow-observer.tests.test_wiki_observer_cli \
  plugins.workflow-observer.tests.test_store_config \
  plugins.workflow-observer.tests.test_claude_workflow_observer_cli
caffeinate -dimsu -- python3 -m unittest discover -s plugins/workflow-observer/tests -p 'test_*.py'
caffeinate -dimsu -- python3 -m unittest discover -s evidence/tests -p 'test_*.py'
```

If the repository maintains canonical source copies, run their existing sync/check command and update only the required generated/canonical files.

**Step 5: Commit**

```bash
git add plugins/workflow-observer/scripts plugins/workflow-observer/tests plugins/workflow-observer/core_source
git commit -m "feat: add shared Claude observation surface"
```

---

### Task 3: Implement private Claude session bindings and the narrow hook

**Files:**

- Create: `plugins/workflow-observer/scripts/claude_session_bindings.py`
- Create: `plugins/workflow-observer/scripts/claude_hook.py`
- Create: `plugins/workflow-observer/adapters/claude/hooks/hooks.json`
- Create: `plugins/workflow-observer/tests/test_claude_session_bindings.py`
- Create: `plugins/workflow-observer/tests/test_claude_hook.py`
- Modify: `plugins/workflow-observer/tests/test_claude_workflow_observer_cli.py`
- Modify: `plugins/workflow-observer/scripts/claude_workflow_observer_cli.py`

**Step 1: Write binding RED tests**

Test `bind`, `lookup`, and `unbind` using a temporary `${CLAUDE_PLUGIN_DATA}`. Assert the session ID is represented only by a SHA-256 filename, bindings serialize only `schema_version`, `run_id`, and `state`, directories are 0700, regular files/locks are 0600, and raw session IDs never appear in filenames or file bytes.

Add adversarial cases for symlinked directories/files, non-regular files, permissive modes, identity changes, concurrent binding, unique temporary files, failed replacement, and directory sync. Lifecycle tests must prove:

- rejected start creates no binding;
- successful start binds the returned run;
- successful finish unbinds;
- rejected finish retains the binding;
- replacement binds the new run before superseding the old run.

**Step 2: Write hook RED tests with sensitive fixtures**

Pass a `SubagentStart` JSON fixture that includes sentinel prompt-like text, `cwd`, `transcript_path`, `agent_transcript_path`, messages, and tool data. With a valid binding, stdout must be exactly the platform JSON/context representation containing:

```text
Observation managed by parent run <run-id>; do not start a child observation.
```

No output, error, binding, temporary file, or observation may contain any sentinel. Missing, invalid, symlinked, or permissive binding state emits no marker and returns a non-blocking bounded result. Non-`SubagentStart` events emit no marker.

**Step 3: Run RED**

```bash
caffeinate -dimsu -- python3 -m unittest \
  plugins.workflow-observer.tests.test_claude_session_bindings \
  plugins.workflow-observer.tests.test_claude_hook \
  plugins.workflow-observer.tests.test_claude_workflow_observer_cli
```

Expected: imports or binding/hook contract assertions fail because the implementation does not exist.

**Step 4: Implement fail-closed binding primitives**

Hash the opaque session ID before deriving any path:

```python
digest = hashlib.sha256(session_id.encode("utf-8")).hexdigest()
binding_path = bindings_dir / f"{digest}.json"
```

Use private directories (0700), files and locks (0600), `lstat`/identity validation, `flock` where available, `O_CREAT | O_EXCL` unique temporary files, flush plus file `fsync`, atomic `os.replace`, and directory `fsync`. Reject symlinks, non-regular objects, permissive modes, invalid schemas, invalid run IDs, and unexpected fields. Keep errors bounded and free of raw paths/session IDs.

Wire the Claude wrapper so a successful start binds `${CLAUDE_SESSION_ID}` under `${CLAUDE_PLUGIN_DATA}`, successful finish removes the binding, rejected finish preserves it, and replacement ordering follows the specification. If either variable is unavailable, observation remains usable but hook assistance degrades with a bounded disclosure.

**Step 5: Implement only the `SubagentStart` hook**

`claude_hook.py` reads stdin JSON, accesses only the event name and opaque session ID needed for lookup, ignores all other fields, and emits Claude's documented `additionalContext` response only for a valid active binding. `hooks.json` declares only `SubagentStart`, with a static `python3` command using `${CLAUDE_PLUGIN_ROOT}` and `${CLAUDE_PLUGIN_DATA}`. Document Windows as provisional skill-only degradation; do not add an unproven launcher.

**Step 6: Run GREEN and broad suites**

```bash
caffeinate -dimsu -- python3 -m unittest \
  plugins.workflow-observer.tests.test_claude_session_bindings \
  plugins.workflow-observer.tests.test_claude_hook \
  plugins.workflow-observer.tests.test_claude_workflow_observer_cli
caffeinate -dimsu -- python3 -m unittest discover -s plugins/workflow-observer/tests -p 'test_*.py'
caffeinate -dimsu -- python3 -m unittest discover -s evidence/tests -p 'test_*.py'
```

**Step 7: Commit**

```bash
git add plugins/workflow-observer/adapters/claude/hooks plugins/workflow-observer/scripts plugins/workflow-observer/tests
git commit -m "feat: propagate Claude observation markers privately"
```

---

### Task 4: Add separate native Codex and Claude plugin adapters

**Files:**

- Move: `plugins/workflow-observer/skills/*` → `plugins/workflow-observer/adapters/codex/skills/*`
- Create: `plugins/workflow-observer/adapters/claude/skills/workflow-observer/SKILL.md`
- Create: `plugins/workflow-observer/adapters/claude/skills/workflow-telemetry/SKILL.md`
- Create: `plugins/workflow-observer/adapters/claude/skills/workflow-learning/SKILL.md`
- Create: `plugins/workflow-observer/adapters/claude/skills/workflow-improving/SKILL.md`
- Modify: `plugins/workflow-observer/.codex-plugin/plugin.json`
- Create: `plugins/workflow-observer/.claude-plugin/plugin.json`
- Create: `.claude-plugin/marketplace.json`
- Modify: `.agents/plugins/marketplace.json` only if its explicit Codex skill path is required
- Create or modify: `plugins/workflow-observer/tests/test_plugin_manifest.py`
- Create: `plugins/workflow-observer/tests/test_claude_plugin_manifest.py`
- Create: `plugins/workflow-observer/tests/test_claude_skill_contracts.py`
- Create: `evidence/claude-runtime-evals/*.json`
- Modify: packaging inventory tests required by the moves

**Step 1: Move the Codex skills without editing their behavior**

Use `git mv` for all four existing Codex skill trees. Update the Codex manifest's custom skill path to `adapters/codex/skills/`. Add tests proving the moved files retain their approved substantive bytes/digests and the Codex manifest declares no Claude hooks or Claude skill path.

**Step 2: Write native metadata and discovery RED tests**

Assert:

- Codex metadata resolves only `adapters/codex/skills/`;
- Claude metadata resolves only `adapters/claude/skills/` and `adapters/claude/hooks/hooks.json`;
- no plugin-root `skills/` remains;
- the two marketplace files use their own native schemas and resolve the same self-contained plugin root;
- Claude hooks declare exactly `SubagentStart`;
- package inventory includes both native metadata trees and both adapter trees.

Run the focused tests and capture the expected missing-Claude-artifact failures.

```bash
caffeinate -dimsu -- python3 -m unittest \
  plugins.workflow-observer.tests.test_plugin_manifest \
  plugins.workflow-observer.tests.test_claude_plugin_manifest
```

**Step 3: Write Claude skill RED contract tests**

For each of the four skills, assert deterministic frontmatter and required platform-specific contracts. In particular:

- commands resolve through `${CLAUDE_PLUGIN_ROOT}` and the Claude CLI;
- Claude starts always use `agent_surface: claude` and the Claude home;
- binding/unbinding uses `${CLAUDE_SESSION_ID}` and `${CLAUDE_PLUGIN_DATA}`;
- every worker prompt carries the unchanged parent marker;
- hooks are best-effort and skill-only degradation is explicit;
- automatic observer exclusions match Codex's approved trigger boundary;
- learning is read-only;
- improvement requires an explicit request, evidence, proposal, and fresh approval;
- incompatible LLM Wiki adapters fail closed;
- no skill says deterministic validation proves real Claude runtime compatibility.

```bash
caffeinate -dimsu -- python3 -m unittest plugins.workflow-observer.tests.test_claude_skill_contracts
```

Expected: missing Claude skill files and contracts fail.

**Step 4: Implement the native Claude artifacts**

Create a Claude `.claude-plugin/plugin.json` and root `.claude-plugin/marketplace.json` using only documented Claude fields and relative paths. Write separate Claude skills aligned to the approved Codex semantics, but with Claude commands, variables, default home, hook degradation, and exact alpha disclaimer. Do not branch inside shared skill prose or reuse one platform manifest as the other's validator input.

Add pending fresh-session evaluation definitions for eligibility, exclusions, lifecycle statuses, one-run subagents, hooks enabled/disabled/error, privacy, LLM Wiki failure, install/reload/update/resume, and uninstall store preservation. Definitions record only version, OS, scope, source/ref, case ID, sanitized result, and SHA-256. Mark every real-runtime result pending; no deterministic test may convert it to passed.

**Step 5: Run GREEN, both native validators, and artifact evaluation**

```bash
caffeinate -dimsu -- python3 -m unittest \
  plugins.workflow-observer.tests.test_plugin_manifest \
  plugins.workflow-observer.tests.test_claude_plugin_manifest \
  plugins.workflow-observer.tests.test_claude_skill_contracts
caffeinate -dimsu -- python3 /Users/vincent/.codex/skills/.system/plugin-creator/scripts/validate_plugin.py plugins/workflow-observer
caffeinate -dimsu -- claude plugin validate --strict plugins/workflow-observer
```

Have a fresh non-Claude reviewer inspect the skill artifacts and deterministic eval definitions. Record this accurately as cross-agent artifact review, never as Claude Code runtime execution.

Then run full plugin and evidence suites under `caffeinate`.

**Step 6: Commit**

```bash
git add .agents .claude-plugin plugins/workflow-observer evidence/claude-runtime-evals
git commit -m "feat: add native Claude plugin adapter"
```

---

### Task 5: Document, package, independently review, and publish the alpha

**Files:**

- Modify: `README.md`
- Modify: `plugins/workflow-observer/README.md`
- Modify: roadmap and release acceptance files discovered by `rg`
- Create: `docs/claude-runtime-evaluation.md`
- Modify: `evidence/scripts/package_workflow_observatory.py`
- Modify: `plugins/workflow-observer/tests/test_package_archive.py`
- Modify: evidence hygiene/integrity tests required by the new artifacts
- Include: approved design and this plan in the release inventory

**Step 1: Write public-doc and archive RED tests**

Add contract tests proving the root and plugin READMEs, release acceptance, and generated archive all contain both exact alpha sentences. Assert public claims separate:

- Codex non-regression evidence actually executed;
- Claude deterministic validators/fixtures actually executed;
- Claude runtime `NOT YET VALIDATED`.

Assert the archive includes both native metadata trees, both skill adapters, the single hook, shared core, deterministic tests/evidence, pending runtime eval definitions, approved design, implementation plan, and SHA-256 mapping. Assert it excludes `.git`, runtime bindings, stores, configs, credentials, caches, temporary payloads, observations, and author-specific absolute paths.

Run focused RED and capture missing documentation/inventory failures.

**Step 2: Write the public alpha documentation**

Update installation and architecture docs for separate Codex/Claude native surfaces. Document Claude full mode versus skill-only degraded mode, provisional Windows behavior, supported validator baseline `2.1.153` as unverified, store preservation on uninstall, disposable bindings, manual store handling only, LLM Wiki fail-closed behavior, contribution hygiene, and pending real-runtime evaluation procedure.

Document Alternatives B and C only as rejected/fallback architecture notes. Do not create their implementation shells.

Use the exact adjacent disclaimer:

```text
Claude support is alpha and has not yet been validated in a real Claude Code runtime.
If you validate it, find a compatibility issue, or implement a fix, please submit a pull request with sanitized reproduction evidence.
```

**Step 3: Make source verification green**

```bash
caffeinate -dimsu -- python3 -m unittest discover -s plugins/workflow-observer/tests -p 'test_*.py'
caffeinate -dimsu -- python3 -m unittest discover -s evidence/tests -p 'test_*.py'
caffeinate -dimsu -- python3 /Users/vincent/.codex/skills/.system/plugin-creator/scripts/validate_plugin.py plugins/workflow-observer
caffeinate -dimsu -- claude plugin validate --strict plugins/workflow-observer
```

Run repository integrity, evidence hygiene, personal-path, manifest freeze, source synchronization, and archive inventory commands discovered in the existing README/tests. Save exact counts and tool versions for the release note.

**Step 4: Build twice and verify byte reproducibility**

```bash
mkdir -p /private/tmp/workflow-observatory-release-a /private/tmp/workflow-observatory-release-b
caffeinate -dimsu -- python3 evidence/scripts/package_workflow_observatory.py --output-dir /private/tmp/workflow-observatory-release-a
caffeinate -dimsu -- python3 evidence/scripts/package_workflow_observatory.py --output-dir /private/tmp/workflow-observatory-release-b
cmp /private/tmp/workflow-observatory-release-a/workflow-observatory-0.2.0-alpha.1.zip \
    /private/tmp/workflow-observatory-release-b/workflow-observatory-0.2.0-alpha.1.zip
shasum -a 256 /private/tmp/workflow-observatory-release-a/workflow-observatory-0.2.0-alpha.1.zip
```

Use the packager's actual command interface and produced name if it differs; change its version through the repository's canonical version source, not by renaming an archive afterward.

**Step 5: Verify a clean extraction independently**

Extract archive A to a new empty temporary directory. From that extraction, run:

- both native validators against the extracted plugin;
- all packaged plugin and evidence tests under `caffeinate`;
- packaged integrity, hygiene, personal-path, and inventory checks;
- another two packaged rebuilds and `cmp` against one another and the source artifact;
- a tree scan proving excluded runtime/private files are absent.

No command may rely on the source checkout, an installed plugin, or a user store. Report validator success as deterministic metadata validation only.

**Step 6: Prepare the release commit and receive independent review**

```bash
git add README.md plugins/workflow-observer/README.md docs evidence plugins/workflow-observer/tests evidence/scripts
git commit -m "release: prepare Claude compatibility alpha"
```

Run placeholder scans (for unfinished implementation text), `git diff --check`, status checks, and the complete source gate again on the exact commit. Request a fresh broad reviewer to compare all commits with the approved design and implementation plan, inspect privacy/lifecycle/platform boundaries, and independently assess the clean-room evidence. Resolve every blocking finding with RED/GREEN regression evidence and repeat review until approved.

**Step 7: Push and create the authorized GitHub pre-release**

Only after the final reviewer approves and the exact commit is clean:

```bash
git push origin main
git tag -a v0.2.0-alpha.1 -m "Workflow Observatory 0.2.0-alpha.1"
git push origin v0.2.0-alpha.1
gh release create v0.2.0-alpha.1 \
  /private/tmp/workflow-observatory-release-a/workflow-observatory-0.2.0-alpha.1.zip \
  --prerelease \
  --title "Workflow Observatory 0.2.0-alpha.1" \
  --notes-file /private/tmp/workflow-observatory-release-notes.md
```

The release title/notes must say alpha/unverified and include the exact two-sentence disclaimer. Do not describe the pending Claude runtime suite as executed.

**Step 8: Verify remote state and the downloaded asset**

```bash
git ls-remote --heads --tags origin main v0.2.0-alpha.1
gh release view v0.2.0-alpha.1 --json url,isPrerelease,tagName,targetCommitish,assets,body
mkdir -p /private/tmp/workflow-observatory-release-download
gh release download v0.2.0-alpha.1 --dir /private/tmp/workflow-observatory-release-download
shasum -a 256 /private/tmp/workflow-observatory-release-download/workflow-observatory-0.2.0-alpha.1.zip
cmp /private/tmp/workflow-observatory-release-a/workflow-observatory-0.2.0-alpha.1.zip \
    /private/tmp/workflow-observatory-release-download/workflow-observatory-0.2.0-alpha.1.zip
```

Run the archive verifier on the downloaded asset. Confirm the release is a pre-release, the tag targets the reviewed commit, and the remote `main` matches it.

**Step 9: Close the parent observation and report**

Finish `obs-20260719-085555-b3c175` exactly once after remote readback. Report:

- plan path and all commit IDs;
- exact source and clean-room commands/counts;
- reviewer verdict and any resolved findings;
- GitHub release URL, tag, target commit, archive SHA-256, downloaded SHA-256, and comparison result;
- the explicit limitation that real Claude Code runtime behavior was not validated;
- the sanitized PR invitation;
- observation lifecycle result and any bounded follow-up.
