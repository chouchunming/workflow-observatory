# Parallel Evaluation MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `subagent-driven-development`
> or `executing-plans` task-by-task. Do not combine commits or begin a later task
> before the current task's focused RED/GREEN gate passes.

**Goal:** Build a trustworthy four-lane evaluator for the frozen 20 forward and
8 lifecycle cases while preserving exact inputs, isolated model configuration,
non-authoritative worker evidence, and one coordinator-only atomic result pair.

**Architecture:** A repository-keyed writer lease protects every serial and
parallel result writer. The coordinator resolves one explicit transport config,
creates an auth-only isolated Codex bootstrap, captures a verified read-only
package outside the repository, plans three eight-case exec lanes plus one
four-case app-server lane, and supervises workers through fsynced progress/ACK
barriers. Workers stage writable per-case marketplace installs, write immutable
attempt/case/shard seals, and cannot persist results. A serial validation gate
returns a capability required by ordered aggregation and paired persistence.

**Tech Stack:** Python 3 standard library, existing `unittest` suites, current
Codex CLI/app-server protocols, and the existing deterministic packager.

## Global constraints

- Frozen base: `2f617fea833e583af9cae87308cfde2e620fcd82`.
- Forward manifest SHA-256:
  `f3bd3b758e5fff43ed3bc50359d3799c111174a6bc8a225208b6c9989b7358a2`.
- Lifecycle manifest SHA-256:
  `d3f91c1359b4087ed5d336fb079f020eed3c42e132360b5d5ca684518a411e8b`.
- Preserve every manifest byte, prompt, expectation, ID, order, current result
  row schema, historical result artifact, and `SHA256SUMS.json`.
- Preserve the existing 20-minute exec, 10-minute app-server, and 5-minute gate
  limits. Transport remains derived only from turn count: 24 exec, 4 app-server.
- All evaluation roots, captured inputs, homes, fixtures, stores, audit files,
  progress, and seals stay in a mode-`0700` root outside the repository.
- Never use production LLMWiki as a fixture or observation store.
- Worker evidence never contains prompts, final text, raw commands/output,
  credentials, payloads, or absolute subject paths.
- Diagnostic/discovery shards and reports are always `authoritative: false`;
  only a sealed `formal` epoch can claim one consumed commit capability.
- No real-model formal epoch before independent review of implementation,
  deterministic gates, diagnostic, and discovery evidence.
- Observation managed by parent run `obs-20260719-123724-ab35fd`; do not start a
  child observation.

## Review disposition

Every review item was verified against the current tree; none is disputed.

| Finding | Current-code evidence | Required plan correction |
|---|---|---|
| C1 | `AppServer.start_turn` puts the prompt in `request("turn/start", ...)`; `request` sends before waiting. | Mark model-start immediately before `_send`, and test an accepted request with a dropped response. |
| C2 | `run_suite` calls `persist_result_pair` directly; no lease exists. | Require the same repository-keyed lease/capability for legacy serial and parallel writers. |
| C3 | `build_codex_config_overrides` omits model/reasoning; both commands read Codex config, while isolated homes were unspecified. | Resolve one exact config, apply it explicitly to both transports, and bootstrap only auth into isolated homes. |
| I1 | No worker protocol exists; the prior draft only named JSONL and a token option. | Define durable sequenced progress, ACK barriers, bounded usage, and launch-ceiling semantics. |
| I2 | One `attempts` directory conflicted with immutable repeated names. | Use `attempts/01` and `attempts/02` with exact scan rules. |
| I3 | In-memory aggregation arguments could not reopen shards/stores or prove gate order. | Return a validation capability that aggregation and persistence require. |
| I4 | `git status` cannot see forbidden edits committed after the frozen base. | Add a base-to-HEAD allowlist plus frozen byte/AST comparisons, also run on clean trees. |
| I5 | Fake-process unit tests did not cross the real coordinator/worker boundary. | Add a no-model coordinator subprocess plus four real worker subprocesses over all 28 keys. |
| I6 | Captured execution preceded packager inclusion of new worker modules. | Move minimal package evidence inclusion before coordinator execution. |
| I7 | RED used `crash_at` absent from the declared seal interface. | Declare one `FaultInjector` callback; production passes `None`. |
| I8 | `copytree` preserves read-only modes, then `_run_case` rewrites/chmods the copied CLI. | Stage a writable mode-normalized per-case install and test `0444` capture input. |
| I9 | Two commits combined independent failure domains. | Split into 14 bounded RED/GREEN commits below. |
| I10 | Portable Python/macOS cannot atomically remove a directory name only if it still has one expected device/inode; a same-UID namespace writer can replace a checked quarantine before `rmdir`. | Split descriptor-relative credential scrubbing from name-based Codex-home teardown. Workers retain an empty owned tombstone; only the quiescent coordinator may remove that verified tombstone. |
| I11 | After `close(fd)` raises, Darwin/Python provides no generation token that distinguishes the old open-file description from a same-number/same-inode reuse. | Make every close a one-shot capability retirement. Never inspect/retry a retired integer; poison and exit the worker on an indeterminate close. |
| I12 | Record-writer temp/parent descriptors and setup failures can bypass owner close state, while an exact re-raised exception alone does not tell the worker/factory that exit is mandatory. | Apply one-shot retirement to every Task 4 descriptor and mark exact exception leaves with a recursive indeterminate-close predicate; poison the factory before propagation. |
| M1 | Global fail-closed wording conflicted with allowing an unset legacy role. | Every supported caller sets a coordinator/worker role; unset and unknown fail closed. |
| M2 | Evaluator/marketplace component hashes lacked member/canonicalization rules. | Define exact inventory member sets and canonical JSON bytes below. |

The focused re-review's 1 Critical, 5 Important, and 1 Minor findings were also
verified; none is disputed. They are closed below by sealed run kind plus a
single-use formal capability, a fixed per-UID lock namespace, a pinned Codex
binary, disjoint frozen-boundary algorithms, complete public types/entry points,
production case-driver wiring, and the corrected Task 6 RED. The ten findings
marked resolved by that re-review remain unchanged.

The Task 8 plan re-review's three contract gaps are closed below without adding
a production caller: resume receives and selects exact canonical manifest rows,
progress names each seal hash explicitly with per-message truth, and shard
terminal wire fields and `CaseKey` encoding are closed schemas.

## Files and ownership

| Path | Responsibility |
|---|---|
| `evidence/scripts/workflow_eval_sharding.py` | Pure schemas, planner, fingerprints, paths, seals, progress/ACK, retry/resume, writer lease, validated-epoch capability, aggregation, coordinator state machine. |
| `evidence/scripts/run_observing_workflows_eval_worker.py` | Worker CLI, writable staging, isolated runtime/auth home, lane execution, progress barriers, sealed output. |
| `evidence/scripts/run_observing_workflows_task9_eval.py` | Explicit transport config, conservative start telemetry, process groups, token usage, typed failures, lease-required serial persistence. |
| `evidence/scripts/check_parallel_eval_frozen_boundary.py` | Frozen-base allowlist and byte/AST boundary checker for Git refs or two extracted trees. |
| `evidence/scripts/package_workflow_observatory.py` | Early inclusion of new evaluator sources and test drivers in captured evidence. |
| `evidence/tests/test_workflow_eval_sharding.py` | Pure focused tests for all new schemas and coordinator rules. |
| `evidence/tests/test_observing_workflows_task9_eval.py` | Existing evaluator transport/config/lease regressions. |
| `evidence/tests/test_parallel_eval_frozen_boundary.py` | Base allowlist and forbidden-change tests. |
| `evidence/tests/run_parallel_eval_no_model_worker.py` | Test-only wrapper injecting the no-model case driver into the real worker. |
| `evidence/tests/run_parallel_eval_no_model_coordinator.py` | Test-only real coordinator process using four real worker subprocesses. |
| `evidence/tests/test_parallel_eval_no_model_integration.py` | 28-case real-process integration assertions. |
| `plugins/workflow-observer/tests/run_marketplace_eval.py` | Opt-in public coordinator CLI; legacy modes preserved with the shared lease. |
| `plugins/workflow-observer/tests/test_parallel_eval_runner.py` | Plugin-local CLI/protocol/authority tests. |

Marketplace runner/tests/docs changed under `plugins/` must be byte-identical to
their source copies under
`evidence/marketplace/workflow-observatory/plugins/`. Frozen manifests under
both trees are never rewritten.

## Exact contracts

### 1. Frozen boundary and component fingerprints

`check_parallel_eval_frozen_boundary.py` exposes:

```python
FROZEN_BASE = "2f617fea833e583af9cae87308cfde2e620fcd82"
ALLOWED_IMPLEMENTATION_PATHS: frozenset[str]
FROZEN_BYTE_PATHS: tuple[str, ...]
FROZEN_AST_BINDINGS: dict[str, tuple[str, ...]]

def compare_git_range(repository: Path, base: str, head: str) -> list[str]: ...
def compare_trees(base_tree: Path, head_tree: Path) -> list[str]: ...
def main(argv: Sequence[str] | None = None) -> int: ...
```

`ALLOWED_IMPLEMENTATION_PATHS` is exactly this literal set; no glob grants
broad write access:

```text
README.md
ROADMAP.md
TODO.md
docs/parallel-evaluation-mvp-implementation-plan.md
docs/parallel-evaluation-plan.md
evidence/scripts/check_parallel_eval_frozen_boundary.py
evidence/scripts/package_workflow_observatory.py
evidence/scripts/run_observing_workflows_eval_worker.py
evidence/scripts/run_observing_workflows_task9_eval.py
evidence/scripts/workflow_eval_sharding.py
evidence/tests/run_parallel_eval_no_model_coordinator.py
evidence/tests/run_parallel_eval_no_model_worker.py
evidence/tests/test_observing_workflows_task9_eval.py
evidence/tests/test_parallel_eval_frozen_boundary.py
evidence/tests/test_parallel_eval_no_model_integration.py
evidence/tests/test_workflow_eval_sharding.py
plugins/workflow-observer/tests/run_marketplace_eval.py
plugins/workflow-observer/tests/test_eval_runner_hygiene.py
plugins/workflow-observer/tests/test_package_archive.py
plugins/workflow-observer/tests/test_parallel_eval_runner.py
evidence/marketplace/workflow-observatory/README.md
evidence/marketplace/workflow-observatory/ROADMAP.md
evidence/marketplace/workflow-observatory/TODO.md
evidence/marketplace/workflow-observatory/docs/parallel-evaluation-mvp-implementation-plan.md
evidence/marketplace/workflow-observatory/docs/parallel-evaluation-plan.md
evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/run_marketplace_eval.py
evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/test_eval_runner_hygiene.py
evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/test_package_archive.py
evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/test_parallel_eval_runner.py
```

The checker builds `base_paths`, `head_paths`, and their complete union. Every
added, deleted, or byte-changed path must be in `ALLOWED_IMPLEMENTATION_PATHS`;
otherwise it fails. Independently, `FROZEN_BYTE_PATHS` hard-codes the three copies of
`observing_workflows_cases.json`, the three copies of
`observing_workflows_lifecycle_cases.json`, `SHA256SUMS.json`, and
`evidence/tests/run_observing_workflows_eval.py`. The implementation asserts
`ALLOWED_IMPLEMENTATION_PATHS.isdisjoint(FROZEN_BYTE_PATHS)`, byte-compares every
explicit frozen path, and byte-compares every non-allowlisted base path. It also
rejects every new path whose basename is
`observing_workflows_forward.json`,
`observing_workflows_lifecycle_forward.json`, or
`observing_workflows_results_commit.json`, or whose path contains
`.observing_workflows_result_generations/`. `FROZEN_AST_BINDINGS` compares the
base and HEAD literal values of `RESULT_SCHEMAS`, both manifest field sets,
manifest hashes/IDs, and the three timeout constants even when their containing
evaluator file is otherwise allowed to change. The exact bindings are
`DECISION_MANIFEST_FIELDS`, `LIFECYCLE_MANIFEST_FIELDS`, and `RESULT_SCHEMAS` in
`evidence/tests/run_observing_workflows_eval.py`; and
`EXEC_TURN_TIMEOUT_SECONDS`, `APP_SERVER_TURN_TIMEOUT_SECONDS`,
`GATE_TIMEOUT_SECONDS`, `FROZEN_MANIFEST_HASHES`, and `FROZEN_MANIFEST_IDS` in
`evidence/scripts/run_observing_workflows_task9_eval.py`.

The six literal manifest paths are the forward/lifecycle pair under each of
`evidence/tests/skill_evals/`,
`plugins/workflow-observer/tests/skill_evals/`, and
`evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/skill_evals/`;
discovery is used only to assert that no seventh matching tracked path appears.

```python
RunKind = Literal["diagnostic", "discovery", "formal"]

@dataclass(frozen=True)
class InputFingerprints:
    schema_version: int
    epoch_id: str
    run_kind: RunKind
    archive_sha256: str
    marketplace_sha256: str
    evaluator_sha256: str
    transport_config_sha256: str
    forward_manifest_sha256: str
    lifecycle_manifest_sha256: str

@dataclass(frozen=True)
class EpochPlan:
    schema_version: int
    epoch_id: str
    run_kind: RunKind
    fingerprints: InputFingerprints
    assignments: tuple["CaseAssignment", ...]
```

`epoch_id` is the SHA-256 of canonical ASCII JSON containing `run_kind`, all
fingerprint fields except `epoch_id`, and every ordered assignment field. Plan,
fingerprints, progress, attempt/case/shard seals, validation capability, and
commit capability must carry the same kind. Any mismatch fails closed.

Component hashes use the verified archive inventory after normalization:

```python
def component_digest(entries: Sequence[tuple[str, str]]) -> str:
    payload = json.dumps(
        sorted(entries), ensure_ascii=True, separators=(",", ":")
    ).encode("ascii")
    return hashlib.sha256(payload).hexdigest()
```

- `archive_sha256`: SHA-256 of final verified ZIP bytes.
- `marketplace_sha256`: `component_digest((member, packaged_sha256), ...)` for
  every inventory `marketplace_files` entry.
- `evaluator_sha256`: the same digest for inventory rows whose exact origins are
  `wiki_cli.py`, `wiki_observations.py`,
  `scripts/run_observing_workflows_task9_eval.py`,
  `scripts/run_observing_workflows_eval_worker.py`,
  `scripts/workflow_eval_sharding.py`,
  `tests/observing_workflows_eval_harness.py`, and
  `tests/run_observing_workflows_eval.py`; each tuple is its normalized archive
  member path and `packaged_sha256`.
- manifest hashes: SHA-256 of the two extracted manifest byte strings, checked
  against the frozen values above.
- `transport_config_sha256`: SHA-256 of the canonical public config JSON below.
- Authentication bytes/path/hash are never stored in fingerprints or seals.

### 2. Resolved transport configuration and auth bootstrap

```python
@dataclass(frozen=True)
class ResolvedTransportConfig:
    schema_version: int
    codex_version: str
    codex_executable_path: str
    codex_executable_sha256: str
    codex_executable_device: int
    codex_executable_inode: int
    codex_executable_size: int
    model: str
    model_reasoning_effort: str
    approval_policy: Literal["never"]
    sandbox_mode: Literal["workspace-write"]
    network_access: Literal[False]
    web_search: Literal["disabled"]
    multi_agent: Literal[True]
    exec_timeout_seconds: Literal[1200]
    app_server_timeout_seconds: Literal[600]
    gate_timeout_seconds: Literal[300]

def resolve_transport_config(
    *, codex_executable: Path, source_codex_home: Path,
    requested_model: str | None, requested_reasoning_effort: str | None,
) -> ResolvedTransportConfig: ...
def transport_config_bytes(config: ResolvedTransportConfig) -> bytes: ...
def verify_codex_executable(config: ResolvedTransportConfig) -> Path: ...
```

```python
CleanupState = Literal["active", "scrubbing", "tombstoned"]
DescriptorCloseState = Literal["owned", "closing", "closed", "indeterminate"]

def is_indeterminate_descriptor_close(error: BaseException) -> bool: ...

@dataclass(frozen=True)
class BootstrapOwnership:
    schema_version: Literal[1]
    epoch_id: str
    run_kind: RunKind
    bootstrap_device: int
    bootstrap_inode: int

@dataclass
class InstalledAuthBootstrap:
    path: Path
    ownership: BootstrapOwnership
    descriptor: int
    state: CleanupState
    descriptor_close_state: DescriptorCloseState
    descriptor_close_error: BaseException | None

@dataclass(frozen=True)
class BootstrapTombstoneReceipt:
    schema_version: Literal[1]
    epoch_id: str
    run_kind: RunKind
    ownership_sha256: str
    bootstrap_device: int
    bootstrap_inode: int
    scrubbed: Literal[True]
    empty: Literal[True]
    canonical_binding: Literal["expected", "missing", "replaced"]
    producer: Literal["coordinator", "coordinator-recovery"]

@dataclass(frozen=True)
class CaseAuthOwnership:
    schema_version: Literal[1]
    epoch_id: str
    run_kind: RunKind
    case: CaseKey
    case_root_device: int
    case_root_inode: int
    codex_home_device: int
    codex_home_inode: int

@dataclass
class InstalledCaseAuth:
    ownership: CaseAuthOwnership
    descriptor: int
    state: CleanupState
    descriptor_close_state: DescriptorCloseState
    descriptor_close_error: BaseException | None

@dataclass(frozen=True)
class TombstoneReceipt:
    schema_version: Literal[1]
    epoch_id: str
    run_kind: RunKind
    case: CaseKey
    ownership_sha256: str
    case_root_device: int
    case_root_inode: int
    codex_home_device: int
    codex_home_inode: int
    scrubbed: Literal[True]
    empty: Literal[True]
    canonical_binding: Literal["expected", "missing", "replaced"]
    producer: Literal["worker", "coordinator-recovery"]

@dataclass(frozen=True)
class TeardownReceipt:
    schema_version: Literal[1]
    epoch_id: str
    run_kind: RunKind
    tombstone_receipts: tuple[tuple[CaseKey, str], ...]
    bootstrap_tombstone_receipt_sha256: str
    codex_homes_absent: Literal[True]
    bootstrap_absent: Literal[True]

def prepare_auth_bootstrap(
    *, source_codex_home: Path, coordinator_root: Path, plan: EpochPlan,
) -> InstalledAuthBootstrap: ...

def install_case_auth(
    *, bootstrap: Path, plan: EpochPlan, assignment: CaseAssignment,
    paths: CasePaths,
) -> InstalledCaseAuth: ...
def cleanup_case_auth(
    *, installed: InstalledCaseAuth, paths: CasePaths,
) -> TombstoneReceipt: ...
def recover_case_auth_cleanup(
    *, plan: EpochPlan, assignment: CaseAssignment, paths: CasePaths,
    lease: RunCoordinatorLease, authority: QuiescentRunAuthority,
) -> TombstoneReceipt: ...
def teardown_case_auth(
    *, paths: CasePaths, receipt: TombstoneReceipt,
    lease: RunCoordinatorLease, authority: QuiescentRunAuthority,
) -> None: ...
def cleanup_auth_bootstrap(
    *, installed: InstalledAuthBootstrap, lease: RunCoordinatorLease,
    authority: QuiescentRunAuthority,
) -> BootstrapTombstoneReceipt: ...
def recover_auth_bootstrap_cleanup(
    *, plan: EpochPlan, coordinator_root: Path, lease: RunCoordinatorLease,
    authority: QuiescentRunAuthority,
) -> BootstrapTombstoneReceipt: ...
def teardown_auth_bootstrap(
    *, coordinator_root: Path, receipt: BootstrapTombstoneReceipt,
    lease: RunCoordinatorLease, authority: QuiescentRunAuthority,
) -> None: ...
def write_teardown_receipt(
    *, plan: EpochPlan, run_root: Path,
    tombstones: Sequence[tuple[CaseKey, TombstoneReceipt]],
    bootstrap: BootstrapTombstoneReceipt, lease: RunCoordinatorLease,
    authority: QuiescentRunAuthority,
) -> TeardownReceipt: ...
```

Resolution reads `model` and `model_reasoning_effort` once from explicit CLI
values or the source `config.toml`, resolves the executable symlink to one
strict absolute path, opens it without following a final symlink, requires a
regular file, and records `codex --version`, device/inode/size, and descriptor
hash. It then freezes canonical ASCII JSON with sorted keys and compact
separators.
Both transports receive explicit `-c model=...`,
`-c model_reasoning_effort=...`, approval, sandbox, web, and multi-agent values.
Exec also uses `--ignore-user-config --strict-config`; app-server uses
`--strict-config` with an isolated home containing no `config.toml`. Ambient
config changes after resolution cannot change execution.

`CaseRuntime` carries this exact `ResolvedTransportConfig`. Immediately before
every exec or app-server `Popen`, `verify_codex_executable` reopens the canonical
path without following symlinks and rechecks regular-file type,
device/inode/size, and SHA-256. The command executes that absolute path, never
the string `"codex"` or an ambient `PATH` lookup. Any replacement fails before
process start.

The supported MVP auth path is `$SOURCE_CODEX_HOME/auth.json`, consistent with
`codex exec --help` stating that `--ignore-user-config` still uses
`CODEX_HOME` for auth. The coordinator copies only that regular, non-symlinked
mode-`0600` file into the canonical mode-`0700`
`run_root/coordinator/auth-bootstrap/` outside the repository. Before copying
auth bytes, it opens that directory, atomically commits
`coordinator/cleanup/bootstrap-ownership.json`, and retains the descriptor.
After coordinator-owned processes are quiescent, it scrubs through that
descriptor, verifies the same empty inode, commits
`bootstrap-tombstone.json`, then removes only the verified empty bootstrap
tombstone. Resume without a live descriptor recovers only from the durable
ownership record and exact identity. Workers copy bootstrap auth into one case,
never log/hash/seal it, and retain an open descriptor for the newly created case
Codex home. Missing or unsafe auth fails preflight before workers launch.

Each case has `cleanup/ownership.json` and `cleanup/tombstone.json` below its
case root. Case setup creates the mode-`0700` Codex-home, opens it, atomically
writes and fsyncs the commit-last ownership record, and only then copies the
auth bytes through the retained descriptor. The ownership record contains no
path outside the run root and no auth bytes/hash. This makes an interrupted
case recoverable before a tombstone receipt exists. A receipt is canonical
sorted compact ASCII JSON, mode `0600`, temp-file plus fsync plus atomic replace
plus parent-directory fsync; readers reject extra/missing fields, symlinks,
wrong modes, stale epoch/run/case identity, duplicate records, or hash mismatch.

Credential scrubbing and namespace teardown are separate transactions. After
the case process group is reaped and all readers are joined, the worker removes
every child of the retained Codex-home descriptor within the depth/entry
bounds, verifies the same descriptor identity and an empty directory, and
leaves that directory as a mode-`0700` tombstone. It never renames or removes
the top-level Codex-home or a quarantine name. Cleanup state is explicit and
retryable: `active -> scrubbing -> tombstoned`; a pre-scrub or partial-scrub
failure returns to `active` while retaining the descriptor, and a verified
empty expected inode reaches `tombstoned` even if the canonical name was
separately replaced. Original case/setup exceptions stay first in any
`ExceptionGroup`, followed by the classified cleanup failure. A factory abort
attempts each independently owned descriptor exactly once without deleting
names, and retains at most one Codex-home descriptor per active case. Successful
scrub fsyncs the verified-empty directory, atomically writes
`cleanup/tombstone.json`, retires the descriptor capability through one close
attempt, and returns that same durable receipt on repeated cleanup calls.
`canonical_binding != "expected"` records
that the expected inode was scrubbed but is an integrity failure; it never
authorizes deleting the missing or replacement name.

Descriptor close is an irreversible capability-retirement boundary, distinct
from scrub retry. Immediately before `os.close(fd)`, code copies the integer to
a stack local, sets the owner `descriptor=-1`, and changes
`descriptor_close_state` from `owned` to `closing`. If close returns, state is
`closed`. If any `BaseException` escapes, state is `indeterminate`, the exact
exception is retained/re-raised, and that integer is never inspected, retried,
or passed to later cleanup/factory code. Darwin/Python exposes no generation
token that could distinguish an old open-file description from an asynchronously
reused same-number/same-inode descriptor. A close exception therefore poisons
the current descriptor-owning process, whether worker or coordinator; process
exit is the only unconditional reclamation boundary.
The possible original-FD leak is bounded to descriptors already held at that
terminal failure and is safer than risking closure of an unrelated descriptor.
The exact escaping exception leaf is marked in memory as an indeterminate close
without wrapping or changing its identity. `is_indeterminate_descriptor_close`
recurses through `ExceptionGroup`/`BaseExceptionGroup`, so factories and the
later worker state machine can distinguish mandatory process exit from an
ordinary setup/cleanup failure while preserving the original leaves.

Retained-descriptor retry is valid only before the first close attempt. A
partial scrub failure restores `active/owned` and remains retryable. After a
durable tombstone, close failure leaves cleanup `tombstoned/indeterminate`;
idempotent cleanup verifies unchanged receipt bytes and re-reports the stored
close error without scrubbing or making a syscall on the retired integer.
Factory close marks itself terminal before close attempts, processes every
independent owner once in deterministic case order, groups exact errors, and a
repeat makes no close syscalls while re-reporting indeterminate errors.
Any marked indeterminate close escaping runtime setup or `cleanup_case` poisons
the factory before propagation, retires every other already-owned case
descriptor once, stores the exact ordered error tree, and makes every later
`__call__` fail before filesystem/model work. The public `poisoned` property and
recursive predicate are the Task 6 process-exit signal; catching the original
exception cannot make the factory usable again.

Bootstrap/case setup uses the same one-shot rule for every local descriptor.
Each mutable slot is invalidated before its single close call; finalization
continues through all other independent slots in fixed role order. If setup had
prepared a result descriptor but any primary or temporary-close error exists,
the result descriptor is also retired exactly once and no Installed object is
returned. Error groups preserve the primary first, then exact close exceptions
in role order, using `BaseExceptionGroup` when required. Any indeterminate close
is terminal for that process; it must not start another case, open recovery
files, remove a tombstone, or write teardown state before process exit.
The rule includes temporary and parent-directory descriptors inside canonical
record writers. A record helper invalidates each descriptor slot before one
close attempt, continues closing other already-owned slots, never unlinks or
replaces after an indeterminate close, and preserves primary-first deterministic
errors. No Task 4 helper may use a raw close-in-finally path outside this model.

This worker guarantee assumes the owned case process group is quiescent while
the descriptor tree is scrubbed. Portable Python cannot make recursive child
removal or final `rmdir` safe against an arbitrary hostile same-UID process that
continues mutating the namespace. If quiescence cannot be proved, cleanup fails
closed and retains state; it does not claim credential removal.

Quiescence means all coordinator-owned workers, process groups, and readers are
stopped. The repository writer lease serializes supported evaluator writers;
it does not detect or exclude an arbitrary hostile same-UID process, which is
outside the MVP security boundary. Observed identity/namespace interference
still fails closed.

Only the coordinator, while holding the exclusive per-run coordinator lease
and after proving that quiescence, may reopen and verify each expected
Codex-home tombstone, revalidate its current device/inode immediately before
`rmdir`, and remove only that empty tombstone. Within the cooperative-writer
boundary, an identity mismatch observed before `rmdir` fails closed. The MVP
neither detects nor prevents an uncooperative same-UID mutation after that
check. It proves every
`CasePaths.codex_home` absent while retaining `CasePaths.root` and all attempts,
stores, audits, outputs, and seals for validation. It then deletes the auth
bootstrap through the same ownership/scrub/tombstone protocol and atomically
writes `coordinator/teardown.json` binding all case and bootstrap tombstone
receipt hashes; only that receipt permits final epoch validation. The retained
case evidence root follows normal
post-validation retention policy. Stronger UID/container isolation is outside
this MVP.

Cancellation handles cases without a tombstone receipt: after owned process
quiescence, the coordinator reads the durable ownership record, reopens the
canonical Codex-home without following symlinks, requires the recorded
device/inode, performs the same bounded descriptor-relative scrub, and writes a
`producer="coordinator-recovery"` tombstone receipt before teardown. Missing,
unreadable, or mismatched ownership fails closed; it does not delete an
unverified name or claim cleanup. An interrupted bootstrap follows the same
recovery rule using `bootstrap-ownership.json`; teardown requires its verified
bootstrap tombstone receipt too.

An indeterminate close in coordinator setup, cancellation recovery, case
tombstone cleanup, or bootstrap cleanup ends that coordinator immediately after
it has attempted the other already-owned descriptors once. It does not remove
further names or write `teardown.json`. After process exit releases all OS
descriptors and the run lease, a fresh coordinator may acquire the lease and
enter a cleanup-only resume state; it must never restart model/case launches for
that poisoned run. Ownership without a tombstone resumes bounded scrub and
receipt commit. An already-durable tombstone is verified byte-for-byte and is
not rescrubbed; the fresh coordinator removes only its verified canonical empty
directory, or proves it already absent. Only after all case/bootstrap receipts
and absences are revalidated may this fresh process commit `teardown.json` and
finish the failed/validated transition. A second indeterminate close repeats
the same exit-and-fresh-process boundary, never an in-process FD retry.

### 3. Planner, case layout, and writable staging

```python
EvalMode = Literal["forward", "lifecycle"]
LaneName = Literal["E1", "E2", "E3", "APP"]

@dataclass(frozen=True, order=True)
class CaseKey:
    mode: EvalMode
    ordinal: int
    case_id: str

@dataclass(frozen=True)
class CaseAssignment:
    key: CaseKey
    lane: LaneName
    route: Literal["exec", "app-server"]
    manifest_sha256: str

@dataclass(frozen=True)
class AttemptPaths:
    root: Path
    start: Path
    terminal: Path

@dataclass(frozen=True)
class CasePaths:
    root: Path
    cleanup: Path
    attempts: Path
    staging: Path
    workspace: Path
    store: Path
    audit: Path
    payload: Path
    output: Path
    home: Path
    codex_home: Path
    tmp: Path
    config: Path
    cache: Path
    sealed: Path

def build_epoch_plan(
    *, run_kind: RunKind, manifests: dict[EvalMode, list[dict[str, object]]],
    fingerprints: InputFingerprints,
) -> EpochPlan: ...
def paths_for_case(run_root: Path, assignment: CaseAssignment) -> CasePaths: ...
def paths_for_attempt(case: CasePaths, attempt: Literal[1, 2]) -> AttemptPaths: ...
def scan_attempts(
    case: CasePaths, *, plan: EpochPlan, manifest_case: dict[str, object],
) -> tuple[AttemptRecord, ...]: ...
def stage_marketplace_for_case(
    *, read_only_snapshot: Path, destination: Path,
) -> Path: ...
```

The exact E1/E2/E3/APP mapping remains the approved 8/8/8/4 table in
`docs/parallel-evaluation-plan.md`. Plan rows remain in canonical manifest
order: every forward row in file order followed by every lifecycle row in file
order. `CaseKey.ordinal` is one-based within its mode. An assignment selects
only `manifests[assignment.key.mode][assignment.key.ordinal - 1]` and requires
that exact row's `id` to equal `assignment.key.case_id`; implementations never
search by ID or reorder rows. Workers filter their lane without reordering.
Case roots are
`cases/<mode>-<ordinal:02d>-<id>/`, so same-ID cross-mode cases cannot collide.
`CasePaths.workspace` is the actual Git fixture at
`<case-root>/workspace/<case-id>`, not merely its parent. Production creates and
validates only `<case-root>/workspace` before evaluator entry and requires the
final `<case-id>` child to be absent. Captured `_run_case` receives an optional
keyword-only `workspace_parent`; its default preserves the legacy serial
layout. In the explicit branch the production driver prevalidates
`destination == paths.root`, `workspace_parent == paths.workspace.parent`, and
the absent final child, then calls
`_run_case(destination=paths.root, workspace_parent=paths.workspace.parent)`.
`_run_case` requires that explicit parent to equal
`destination / "workspace"` exactly and to be canonical, private, and
non-symlinked; the unchanged frozen fixture builder must return exactly
`workspace_parent / case_id`. Only after that fixture exists may the runtime
factory be called. Its adapter receives `case_root=destination` and rechecks
`case_root == paths.root` plus the exact returned workspace before runtime setup
or transport. Rejected explicit-branch inputs leave no gate-registry residue.
Neither an adapter nor the driver may rename a gate, create a gate symlink, or
mutate `_GATE_ROOTS` directly; only the unchanged frozen fixture builder and the
public `release_gate` operation may mutate that registry.

`scan_attempts` accepts the exact canonical manifest row selected for its one
assignment. Before trusting any start, terminal, case, or tombstone reference,
it validates that row against the mode-specific frozen manifest schema,
requires its ID to equal the assignment inferred from the canonical case path,
and requires every stored `manifest_case_sha256` to equal SHA-256 of
`canonical_config_bytes(manifest_case)`. A matching whole-manifest
`manifest_sha256`, plan epoch, and case ID do not substitute for this per-case
binding. A noncanonical, wrong-ID, or wrong-digest row invalidates the scan; a
caller-selected wrong ordinal is rejected by `plan_resume` before scanning.
Neither condition is ever treated as reusable evidence.

Attempts are exclusively created as `attempts/01/start.json`,
`attempts/01/terminal.json`, and, only after an allowed pre-model retry,
`attempts/02/start.json` and `attempts/02/terminal.json`. Scanning rejects other
names, gaps, duplicates, symlinks, attempt 2 without a valid attempt-1 terminal,
or a start without a terminal during resume.

Captured archive files are `0444` and directories `0555`. Each case stages a
fresh copy: directories `0700`, ordinary files `0600`, and only declared CLI/
script executables `0700`. Copying rejects symlinks and special files. Wrapper
rewrites occur only in staging; snapshot bytes and modes remain unchanged.

### 4. Transport lifecycle, conservative model start, and token usage

```python
@dataclass(frozen=True)
class TokenUsage:
    input_tokens: int
    cached_input_tokens: int
    output_tokens: int
    reasoning_output_tokens: int
    total_tokens: int

CaseEvent = Literal["process-started", "model-started", "process-stopped"]
CaseEventSink = Callable[[CaseEvent, int | None, int | None], None]

def build_codex_config_overrides(
    config: ResolvedTransportConfig, environment: dict[str, str],
    disabled_skill_paths: tuple[Path, ...],
) -> tuple[str, ...]: ...
def execute_case_transport(
    case: dict, workspace: Path, runtime: CaseRuntime, wiki_root: Path,
    after_first_turn: Callable[[], None] | None = None,
    event_sink: CaseEventSink | None = None,
) -> CaseExecution: ...
def stop_process_group(
    process: subprocess.Popen, *, readers: Sequence[threading.Thread],
    terminate_timeout: float = 5.0, kill_timeout: float = 5.0,
) -> None: ...
```

Exec marks `model-started` immediately before `communicate(input=prompt, ...)`.
App-server marks it immediately before `_send` writes the `turn/start` request,
not after the response. A send or response ambiguity is therefore model-started
and never pre-model retryable. `turn/steer` does not create a second attempt.

Exec normalizes `turn.completed.usage`: `input_tokens` and `output_tokens` are
required; optional cached/reasoning values default to zero; `total_tokens` is
provider total when present, otherwise input plus output. App-server uses the
latest matching `thread/tokenUsage/updated.params.tokenUsage.total` with exact
camel-case fields. Missing, negative, inconsistent, or overflowing usage is a
typed protocol failure. Token values are counts only; MVP does not estimate
currency without a frozen price table.

Both transports use new process sessions. Cleanup always terminate/wait,
kill/re-wait if needed, join readers, and prove `killpg(pgid, 0)` fails before
`process-stopped`.
Production events always carry positive integer PID and PGID values. Event-sink
failure is terminal but never bypasses process-group cleanup or reader joins.
For the conservative model boundary, internal `model_started` truth changes
before invoking the `model-started` sink; if that sink raises, the attempt is
still post-start and cannot be retried. The prompt or `turn/start` request is
sent only after the sink returns successfully. `process-stopped` is emitted
only after group absence and all reader joins have been proved.

### 5. One authority for every result writer

```python
CoordinatorRole = Literal["serial-coordinator", "parallel-coordinator"]
LOCK_ROOT_PARENT = Path("/var/tmp")

class RunCoordinatorLease:
    @classmethod
    def acquire(
        cls, *, run_root: Path, epoch_id: str, run_kind: RunKind,
    ) -> "RunCoordinatorLease": ...
    @property
    def active(self) -> bool: ...
    def close(self) -> None: ...

def result_writer_lock_path(repository_root: Path) -> Path: ...

class ResultWriterLease:
    @classmethod
    def acquire(
        cls, repository_root: Path, role: CoordinatorRole,
        run_kind: Literal["formal"],
        run_lease: RunCoordinatorLease | None = None,
    ) -> "ResultWriterLease": ...
    def authority(self) -> "ResultWriterAuthority": ...
    def close(self) -> None: ...

class ResultWriterAuthority:
    @property
    def repository_key(self) -> str: ...
    @property
    def role(self) -> CoordinatorRole: ...
    @property
    def run_kind(self) -> Literal["formal"]: ...
    @property
    def consumed(self) -> bool: ...

def run_suite(
    evaluator_root: Path, *, repository_root: Path,
    manifest_paths: dict[str, Path] | None = None,
    result_destinations: dict[str, Path] | None = None,
    runtime_factory=None, coordinator_role: str | None = None,
) -> tuple[list[dict], list[dict]]: ...

def persist_result_pair(
    destinations: dict[str, Path], results: dict[str, list[dict]],
    manifests: dict[str, list[dict]], *, authority: ResultWriterAuthority,
    crash_at: str | None = None,
) -> Path: ...
```

Task 7 owns the concrete `RunCoordinatorLease` primitive needed to construct a
real nominal witness before any parallel result-writer acquisition. Task 12
consumes this primitive; it still owns production parallel wiring, the
coordinator loop and state machine, worker launch/cancel, resume/recovery,
quiescent authority, and teardown. Moving the primitive earlier does not grant
Task 7 permission to implement any of those Task 12 behaviors.

`RunCoordinatorLease.acquire` accepts only an already-absolute canonical
`run_root`. Before creation the supplied path must equal its
`resolve(strict=False)` form; after secure initialization it must equal
`resolve(strict=True)`. The exact lock is
`run_root/coordinator/coordinator.lock`. Its existing canonical parent must be
opened and retained as an anchored directory descriptor. From that anchor,
`run_root` and then `coordinator` are created/opened only with dirfd-relative
operations, `O_DIRECTORY | O_NOFOLLOW`, and immediate `lstat`/`fstat`
owner/type/mode/device/inode reconciliation. The existing parent anchor is
trusted only when it is a non-symlink directory whose owner is either the
effective UID or UID 0, and which is either not group/other-writable or sticky.
Thus an effective-UID-owned mode-`0777` non-sticky parent is rejected too. This
accepts the canonical root-owned sticky directory behind macOS `/var/tmp`
without trusting a writable non-sticky anchor. Only the two managed directory
entries—not `/`, `/private`, `/tmp`, `/private/var/tmp`, or other ancestors—must
be effective-UID-owned non-symlink directories in mode `0700`. A missing managed
entry is created mode `0700`, reopened no-follow relative to its verified parent,
and reconciled before descent. The lock is created/opened only relative to the
retained coordinator dirfd with `O_NOFOLLOW`, reconciled between `lstat` and `fstat`,
must be an owned regular file in mode `0600`, and is acquired with nonblocking
`flock(LOCK_EX | LOCK_NB)`. The capability retains the managed directory and
lock descriptors needed to revalidate this exact chain. A swapped name,
descriptor/name identity mismatch, or unsafe pre-existing entry fails closed;
no entry is chmod-repaired, replaced, unlinked, or removed during the lease
lifecycle.

The nominal run capability privately binds its creating PID, canonical run
root, `epoch_id`, `run_kind`, descriptor, and the descriptor's device/inode.
While owned, `active`, any child-writer authorization, and `close` revalidate
the owner PID, open descriptor identity, and process-local registry entry. A
clean owner close makes `active` return `False`; every other call on that
closed capability is reuse and is rejected. A post-fork call, descriptor
identity change, second process-local acquisition, or mismatched epoch/run kind
fails closed. The process-local registry rejects acquiring a run lease while
any result-writer lease is active, rejects serial acquisition while the process
owns a run lease, enforces run-lease before parallel-writer order, permits at
most one active `ResultWriterLease` child, rejects
`RunCoordinatorLease.close()` while that child is active, and requires
writer-before-run release. Parallel writer acquisition requires
`type(run_lease) is RunCoordinatorLease` plus this private nominal liveness and
ordering check; serial acquisition requires exactly `run_lease=None`. No
boolean, protocol, subclass, or duck-typed substitute is accepted.

The POSIX MVP lock root is exactly
`/var/tmp/workflow-observatory-result-locks-uid-<os.getuid()>/`, with root mode
`0700` and retained lock mode `0600`. Acquisition anchors an opened `/var/tmp`
dirfd under the same trusted-parent rule above, creates/opens the fixed per-UID
root only relative to that anchor with `O_DIRECTORY | O_NOFOLLOW`, then
creates/opens the retained lock only relative to the verified root dirfd with
`O_NOFOLLOW`. Every managed name is reconciled
between no-follow `lstat` and opened `fstat` owner/type/mode/device/inode before
use, and the root/lock descriptors remain anchored through lease close. Unsafe
pre-existing or swapped entries fail; the implementation never chmod-repairs,
replaces, unlinks, or removes the result-lock root or retained lock file during
a lease lifecycle. The file is
`<sha256(os.fsencode(canonical_repository_root))>.lock`. The caller must supply
the absolute, strict-resolved, supported Git worktree top-level reported by
`git rev-parse --show-toplevel`; a subdirectory, alias, symlink spelling, bare
repository, missing repository, or different top-level is rejected rather
than silently keyed to another path. It never
consults `tempfile`, `TMPDIR`, `HOME`, `CODEX_HOME`, XDG variables, case roots,
or caller-selected lock roots; unsupported/nonconforming systems fail closed.
Every result destination must be a canonical non-symlink path contained in the
same bound repository before any result path is opened.

Writer acquisition also opens and retains the exact Git top-level's canonical
parent dirfd and the repository dirfd itself, reconciles the repository name,
descriptor, owner, type, device, and inode, and binds both to the lease. After
that acquisition, authorization and persistence never reopen the repository by
absolute path. A repository-name replacement therefore fails before a result
path is opened, while the retained repository descriptor prevents redirection
into the replacement even across a race at the cooperative-writer boundary.

The authority accepts only `type(destinations) is dict` with the exact two keys
and exact Path values. Before consumption it copies that plain mapping once and
performs lexical checks only: absolute paths, normalized components with no
`.`/`..`, lexical containment under the bound repository, file basenames, and
one exact shared parent. This phase performs no `resolve`, `stat`, `lstat`,
`open`, `mkdir`, Git invocation, or other filesystem operation. It stores the
private frozen copy and irreversibly consumes itself before the first result
filesystem operation; any later rejection leaves it consumed. No later read
consults the caller's mapping.

After consumption, result-parent descent and creation are dirfd-relative from
the retained repository descriptor, no-follow at every component, and the
opened destination-parent identity remains retained through commit, pointer
re-resolution, rehash/rescore, committed readback, and exact-delta
verification. Persistence and every authoritative verification step use only
the retained repository/result-parent/generation descriptors and relative
names. They must not call the pathname-based `resolve_committed_result_pair`,
reopen the repository, rescan an absolute result parent, or treat a later
pathname lookup as authority.

Task 7 updates every currently supported writer: `run_suite`, its direct script
`main`, and the marketplace legacy/default formal path all set the explicit
`serial-coordinator` role, pass a separate exact Git `repository_root`, and use
`run_suite`'s one lease. `evaluator_root` continues to locate evaluator inputs
and is never used as the writer key. A standalone extracted marketplace that
has no supported Git top-level may still run non-persisting diagnostic or
discovery modes, but its formal/default writer fails closed. Task 7 implements
and tests the `parallel-coordinator` acquisition contract with a real live
`RunCoordinatorLease`, but Task 12 remains responsible for adding the first
production parallel caller. Unset, `worker`, or unknown roles and every
non-`formal` run kind fail before opening a result path. Diagnostic/discovery
cannot acquire writer authority.

Formal parallel holds the lease from baseline capture through post-commit
readback; parallel acquisition requires its already-live
`RunCoordinatorLease`; serial
acquisition requires `run_lease=None`. This makes the two-lease order
machine-enforced rather than a caller convention.
Serial acquires it before its final production check and holds it through
persist, pointer resolution, rehash/rescore, and exact-delta verification.
`persist_result_pair` requires the live authority capability, so a worker or
unleased caller cannot reach atomic pointer replacement. Each top-level writer
acquires exactly once and passes the capability down: direct evaluator `main`
and marketplace serial/formal both use `run_suite`'s lease; the Task 12 parallel
coordinator will own its lease directly. No path may nest or substitute a
second lease.

`ResultWriterLease` and `ResultWriterAuthority` are private nominal
capabilities bound to the creating PID, live lock descriptor and captured
device/inode, the retained result-root dirfd and its captured device/inode,
exact canonical repository and repository key, formal run kind, role, and
process-local registry. Every liveness/consume/close check reconciles the
anchored descriptors with their current no-follow names and rejects a swap.
`authority()` issues exactly once and only while the lease is live.
`persist_result_pair` accepts only
`type(authority) is ResultWriterAuthority`, validates the module-private
provenance and all bindings, and atomically consumes it before opening or
writing any destination. Reuse, post-fork use, a released/replaced descriptor,
wrong repository, or fabricated/subclassed authority fails before a result
write. The writer lease remains held after authority consumption and is not
closed until pointer resolution, result rehash/rescore, committed readback, and
exact-delta verification finish. Task 7 changes only the writer authority and
existing persistence path; its legacy test-only `crash_at` behavior remains
unchanged. The separate typed seal `FaultInjector` in Exact Contract 6 remains
a Task 8 deliverable and does not replace result-pair fault semantics.

Every run/writer lease descriptor, every acquisition-rollback descriptor, and
every authoritative persistence descriptor is placed in a one-shot descriptor
slot immediately after open. This includes retained repository/result-parent
descriptors, generation directories, staged/temp files, result-generation
reads, pointer reads, and readback/rescore descriptors. Clean release retires
all non-lock descriptors first and the lock descriptor last; it never calls
`LOCK_UN`. Each integer is invalidated before its sole `os.close` call, all
independently owned slots are attempted in deterministic order, and exact close
failures are preserved or grouped. An indeterminate close receives the existing
marker, poisons the lease-owning process, rejects every later acquire/use, and
makes repeated close perform zero syscalls; process exit is the only
unconditional reclamation boundary. When a protected body and close both fail,
the body remains the first error and exact close failures are appended in slot
order. Role, run-kind, and coordinator-role validation requires exact
`type(value) is str` fixed literals before any equality or membership check, so
equality-spoofing objects fail before filesystem work.

`OutcomeClass` is defined once here because Task 8 stores it and Tasks 9, 10,
and 12 consume the same closed vocabulary:

```python
OutcomeClass = Literal[
    "success", "semantic", "model", "pre-model-infrastructure", "cleanup",
    "production-mutation", "manifest-mutation", "timeout", "protocol",
    "post-start-transport", "surviving-process", "coordinator-crash",
]
```

### 6. Seals and fault injection

Task 8 is a pure read/write/storage boundary. It defines record schemas,
immutable publishers, readers, and the tombstone parser moved from the worker;
it does not run a case or model. Task 9 consumes hashes in progress records,
Task 10 consumes attempt readers, Task 11 performs semantic aggregation, and
Task 12 is the first production caller of these writers. Production therefore
passes `fault_injector=None` only in Task 12. Task 8 makes no progress, ACK,
retry, launch-ceiling, aggregation-capability, or coordinator change.

The four established public artifact names remain unchanged:
`case-result.json`, `case-evidence.json`, `case-commit.json`, and
`shard-commit.json`. Attempt records remain `start.json` and `terminal.json`.
The case files live under `CasePaths.sealed`; the shard file lives at
`<worker-root>/sealed/shard-commit.json` so later progress/ACK directories do
not weaken its exact inventory.

```python
CaseSealStatus = Literal["success", "failed"]

FaultPoint = Literal[
    "after-result-replace", "after-evidence-replace", "before-case-commit",
    "after-case-commit", "before-shard-commit", "after-shard-commit",
]
FaultInjector = Callable[[FaultPoint], None]

MAX_ATTEMPT_START_BYTES = 4 * 1024
MAX_ATTEMPT_TERMINAL_BYTES = 8 * 1024
MAX_CASE_RESULT_BYTES = 64 * 1024
MAX_CASE_EVIDENCE_BYTES = 16 * 1024
MAX_CASE_COMMIT_BYTES = 8 * 1024
MAX_SHARD_COMMIT_BYTES = 64 * 1024
MAX_SEAL_COUNTER = 1_000_000
MAX_SEAL_ELAPSED_MILLISECONDS = 3_600_000
MAX_SEAL_FAILURE_CHARS = 2**63 - 1
MAX_SEAL_FAILURE_TYPE_CHARS = 128

SHARD_TERMINAL_FIELDS = frozenset({
    "case", "status", "classification", "attempt_terminal_sha256",
    "case_commit_sha256", "tombstone_receipt_sha256", "failure",
})

@dataclass(frozen=True)
class FailureSummary:
    classification: OutcomeClass
    type: str
    chars: int
    sha256: str

@dataclass(frozen=True)
class VerifiedTombstoneReceipt:
    receipt: TombstoneReceipt
    sha256: str

@dataclass(frozen=True)
class AttemptSeal:
    start: dict[str, object]
    terminal: dict[str, object]
    start_sha256: str
    terminal_sha256: str

@dataclass(frozen=True)
class CaseSeal:
    result: dict[str, object] | None
    evidence: dict[str, object]
    commit: dict[str, object]
    result_sha256: str | None
    evidence_sha256: str
    commit_sha256: str
    tombstone_receipt_sha256: str | None

@dataclass(frozen=True)
class ShardTerminal:
    key: CaseKey
    run_kind: RunKind
    status: CaseSealStatus
    classification: OutcomeClass
    attempt_terminal_sha256: str
    case_commit_sha256: str | None
    tombstone_receipt_sha256: str | None
    failure: FailureSummary | None

@dataclass(frozen=True)
class ShardSeal:
    status: CaseSealStatus
    terminals: tuple[ShardTerminal, ...]
    commit_sha256: str

def read_verified_tombstone_receipt(
    *, plan: EpochPlan, assignment: CaseAssignment, paths: CasePaths,
) -> VerifiedTombstoneReceipt: ...
def read_tombstone_receipt(
    *, plan: EpochPlan, assignment: CaseAssignment, paths: CasePaths,
) -> TombstoneReceipt: ...
def write_attempt_start(
    *, plan: EpochPlan, paths: CasePaths, assignment: CaseAssignment,
    attempt: Literal[1, 2], manifest_case: dict[str, object],
) -> Path: ...
def read_attempt_start(
    *, plan: EpochPlan, paths: CasePaths, assignment: CaseAssignment,
    attempt: Literal[1, 2], manifest_case: dict[str, object],
) -> tuple[dict[str, object], str]: ...
def write_attempt_terminal(
    *, plan: EpochPlan, paths: CasePaths, assignment: CaseAssignment,
    attempt: Literal[1, 2], manifest_case: dict[str, object],
    status: CaseSealStatus, classification: OutcomeClass,
    model_started: bool, cleanup_passed: bool,
    usage: dict[str, object] | None,
    failure: dict[str, object] | None,
) -> Path: ...
def read_attempt_seal(
    *, plan: EpochPlan, paths: CasePaths, assignment: CaseAssignment,
    attempt: Literal[1, 2], manifest_case: dict[str, object],
) -> AttemptSeal: ...
def seal_case(
    *, plan: EpochPlan, paths: CasePaths, assignment: CaseAssignment,
    attempt: Literal[1, 2], result: dict[str, object] | None,
    evidence: dict[str, object], manifest_case: dict[str, object],
    fault_injector: FaultInjector | None = None,
) -> Path: ...
def read_case_seal(
    *, plan: EpochPlan, paths: CasePaths, assignment: CaseAssignment,
    manifest_case: dict[str, object],
) -> CaseSeal: ...
def seal_shard(
    *, worker_root: Path, plan: EpochPlan, lane: LaneName,
    terminals: Sequence[ShardTerminal],
    manifests: dict[EvalMode, list[dict[str, object]]],
    case_paths: dict[CaseKey, CasePaths],
    fault_injector: FaultInjector | None = None,
) -> Path: ...
def read_shard_seal(
    *, worker_root: Path, plan: EpochPlan, lane: LaneName,
    manifests: dict[EvalMode, list[dict[str, object]]],
    case_paths: dict[CaseKey, CasePaths],
) -> ShardSeal: ...
```

Schema scalar/container arguments require exact builtin types: no subclasses,
equality-spoofing objects, unknown keyword, or alternate `crash_at` interface;
the injector is exactly `None` or callable. All stored JSON is canonical compact
sorted-key ASCII with no newline. Hashes are lowercase 64-character hexadecimal
SHA-256 values. `schema_version` is exactly integer `1`; `epoch_id`, `run_kind`,
case, lane, route, attempt, whole-manifest `manifest_sha256`, and canonical
`manifest_case_sha256` must match the supplied plan and assignment. A serialized
case has exactly `mode`, `ordinal`, and `case_id`. The manifest-case hash is
SHA-256 of `canonical_config_bytes(manifest_case)`; the whole-manifest hash
remains the assignment's frozen manifest hash.

The exact attempt-start field set is:

```text
schema_version, epoch_id, run_kind, case, lane, route, attempt,
manifest_sha256, manifest_case_sha256
```

The exact attempt-terminal field set is:

```text
schema_version, epoch_id, run_kind, case, lane, route, attempt,
manifest_sha256, manifest_case_sha256, start_sha256, status, classification,
model_started, cleanup_passed, usage, failure, tombstone_receipt_sha256
```

`write_attempt_terminal` reopens `start.json` and computes `start_sha256`; the
caller cannot provide it. `usage` is either JSON `null` or has exactly
`input_tokens`, `cached_input_tokens`, `output_tokens`,
`reasoning_output_tokens`, and `total_tokens`, each an exact integer in
`0..2**63-1`. Cached input does not exceed input, reasoning output does not
exceed output, and total equals input plus output without overflow. Task 8 does
not implement Task 9's cumulative launch ceiling.
`failure` is JSON `null` for success and otherwise is the exact serialized
`FailureSummary`. Its `classification` equals the terminal classification,
`type` matches `^[A-Za-z_][A-Za-z0-9_.]{0,127}$`, `chars` is an exact integer
in `0..MAX_SEAL_FAILURE_CHARS`, and only the original text's SHA-256 is stored.
There is no message field. A successful terminal requires classification
`success`, `model_started=true`, `cleanup_passed=true`, non-null usage, null
failure, and a verified tombstone receipt hash. A failed terminal requires a
non-success classification and non-null failure; usage may be null, and its
tombstone hash may be null only when cleanup did not pass.

`read_verified_tombstone_receipt` is the canonical parser and durable-byte hash
boundary moved from the worker into sharding. It retains the cleanup-directory
descriptor, performs the existing exact ownership/epoch/run/case/schema checks,
and returns the parsed receipt plus SHA-256 of the canonical bytes it actually
read. `read_tombstone_receipt` is the compatibility projection returning only
that verified receipt; the worker imports it rather than retaining a duplicate
parser. Attempt-terminal and case writers call the verified form. When cleanup
passed they require the receipt; when cleanup failed they bind a valid existing
receipt or store null only if the canonical receipt is absent.

The caller-supplied `evidence` has exactly these fields and no identity or hash
fields:

```text
status, classification, model_started, elapsed_milliseconds, usage, failure,
store_record_count, store_invalidated_count, audit_event_count,
payload_file_count, output_file_count, process_cleanup_passed,
credential_cleanup_passed
```

The stored case-evidence field set is exactly:

```text
schema_version, epoch_id, run_kind, case, lane, route, attempt,
manifest_sha256, manifest_case_sha256, archive_sha256, marketplace_sha256,
evaluator_sha256, transport_config_sha256, status, classification,
model_started, elapsed_milliseconds, usage, failure, store_record_count,
store_invalidated_count, audit_event_count, payload_file_count,
output_file_count, process_cleanup_passed, credential_cleanup_passed,
attempt_start_sha256, attempt_terminal_sha256, result_sha256,
tombstone_receipt_sha256
```

The four frozen component hashes come only from `plan.fingerprints`. Attempt
and result hashes are recomputed from canonical durable bytes. Timing is an
exact integer in `0..MAX_SEAL_ELAPSED_MILLISECONDS`; the five counters are
exact integers in `0..MAX_SEAL_COUNTER`; payload/output counts are measured
after their cleanup checks. Cleanup fields are exact booleans.
Usage and failure use the same exact nested schemas as the attempt terminal and
must equal its corresponding values. Evidence status, classification,
model-started truth, and the conjunction of its two cleanup booleans must also
equal the terminal record. No evidence schema has a field capable of carrying
prompts, final text, raw commands/output, credentials, payload contents, or a
path. Extra fields at any nesting depth are rejected.

The exact case-commit field set is:

```text
schema_version, epoch_id, run_kind, case, lane, route, attempt, status,
manifest_sha256, manifest_case_sha256, result_file, result_sha256,
evidence_file, evidence_sha256, attempt_start_sha256,
attempt_terminal_sha256, tombstone_receipt_sha256
```

`evidence_file` is always `case-evidence.json`; `result_file` is
`case-result.json` exactly when a result exists, otherwise both result fields
are null. `seal_case` reopens the canonical
`CasePaths.cleanup/tombstone.json` through the tombstone validator moved from
the worker, validates ownership/epoch/run/case and hashes its exact durable
canonical bytes. The caller never supplies a receipt or receipt hash. If both
cleanup booleans are true, a receipt and result are required. Success further
requires an expected canonical binding, classification `success`, a successful
attempt terminal, and null failure. A non-expected receipt can seal only a
failed case. A failed case requires a non-success classification and sanitized
failure; result and receipt may be null only when at least one cleanup boolean
is false. When cleanup failed but a valid receipt already exists, it is still
reopened and bound rather than discarded.

Task 8 owns strict deep structural result validation in sharding without a
production import from the frozen test evaluator. Forward rows have exactly
`id`, `decisions`, `record_checkpoints`, `run_count`, `draft_count`, and
`final_statuses`; lifecycle rows have exactly `id`, `record_checkpoints`,
`run_count`, `draft_count`, `final_statuses`, `failure_disclosed`, and
`selected_command`. Nested observed decisions have exactly `after_turn`,
`triggered`, `task_type`, and `workflow_variant`; checkpoints have exactly
`after_turn` and `records`; normalized records have exactly `role`, `status`,
`start_mode`, and `superseded_by_role`. The result ID is a nonempty exact string
equal to the assigned case ID. `decisions` is a list; `after_turn` is an exact
integer (not bool), `triggered` is bool, a triggered taxonomy contains two
nonempty strings, and an untriggered taxonomy contains two nulls.
`record_checkpoints` is a list whose `after_turn` is an exact integer and whose
`records` is a list. Every record role matches `run-[1-9][0-9]*`, status and
start mode are nonempty strings, and the superseding role is null or matches
that same pattern. Run/draft counts are exact nonnegative integers and final
statuses are lists of nonempty strings. A command-selection lifecycle row has
null checkpoints, counts, statuses, failure disclosure, and a nonempty selected
command. An executable lifecycle row has the validated store fields, boolean
failure disclosure, and null selected command. Tests compare every duplicated
field set to the frozen
`RESULT_SCHEMAS`, `OBSERVED_DECISION_FIELDS`, `CHECKPOINT_FIELDS`, and
`NORMALIZED_RECORD_FIELDS`. Manifest-case structure is also checked against the
frozen `DECISION_MANIFEST_FIELDS` or `LIFECYCLE_MANIFEST_FIELDS`, including its
deep field/type/null rules, before hashing. Task 11 remains the sole owner of
expectation matching, scoring, store reopening, and full semantic acceptance.

The exact shard-commit field set is:

```text
schema_version, epoch_id, run_kind, lane, status, terminals
```

Each nested terminal has exactly `SHARD_TERMINAL_FIELDS` on the wire:

```text
case, status, classification, attempt_terminal_sha256, case_commit_sha256,
tombstone_receipt_sha256, failure
```

The dataclass attribute `key` encodes only as wire key `case`; `key` is never a
wire field. Internal `run_kind` is validation context inherited from the
top-level shard commit and is not duplicated in a nested terminal. `case` is
the exact serialized `CaseKey` object with exactly `mode`, `ordinal`, and
`case_id`. The decoder performs the inverse mapping and rejects an extra `key`
or `run_kind`, an omitted `case`, or any extra/missing nested field.
A successful nested terminal has status/classification `success` and null
failure. A failed nested terminal has status `failed`, a non-success
classification, and an exact sanitized `FailureSummary` whose classification
matches. Each terminal's
`attempt_terminal_sha256` is always non-null and must reopen to the same case,
attempt status, classification, failure, run kind, and epoch. Because the
terminal does not duplicate an attempt number, the hash must identify exactly
one valid terminal under attempt `01` or `02`; zero or multiple matches fail
closed. A successful full lane has every assigned case in canonical plan order,
all statuses successful, and non-null verified case-commit and tombstone hashes.
A failed shard is the unique nonempty canonical lane prefix ending at its first
and only failed terminal: preceding terminals are successful, the last is
failed, and no later terminal is stored. The failed terminal's case/tombstone
hashes may be null under the case rules; every non-null hash is reopened and
verified. Duplicate, reordered, skipped, post-failure, cross-lane, stale, or
empty terminal sequences are rejected.

Record publication is immutable and no-clobber. Each existing mode-`0700`
record directory is retained through a no-follow descriptor; a missing record
directory is created mode `0700` only descriptor-relative to the already
validated canonical case or worker root, reopened no-follow, and reconciled
before use. Each mode-`0600` temp file is opened relative to it, placed in a
one-shot descriptor slot immediately, written, fsynced, and atomically
published with a same-directory no-clobber hard-link before the temp name is
removed and the directory is fsynced again. A platform/filesystem without the
required link semantics fails closed. No final is overwritten, chmod-repaired,
or unlinked. Before a new case seal, either its exact expected final inventory
is empty or a complete existing seal must pass `read_case_seal` and be
byte-identical, in which case the call is idempotent. A proper subset, extra
name, differing existing bytes, link collision, or crash temp is an integrity
failure and is never healed. Attempt and shard records use the same exact-byte
idempotence and collision rule. `read_attempt_start` accepts exactly start-only
or the completed start/terminal inventory; `read_attempt_seal` requires exactly
both. `write_attempt_terminal` only advances the start-only state or verifies a
byte-identical complete state. `read_case_seal` derives its exact two-file or
three-file inventory from the commit's nullable result fields; the shard sealed
directory contains exactly its one legacy commit file.

Attempt roots are exactly `CasePaths.attempts/01` or `/02`; every created path
component is effective-UID-owned mode `0700`, non-symlinked, and reconciled
between its retained parent/name and opened descriptor. Case seal paths must
equal `paths_for_case(derived_run_root, assignment)`. A shard root is the
canonical mode-`0700` `run_root/workers/<lane>` for E1/E2/E3 or
`run_root/app-server` for APP, with that same run root derived from and shared
by every supplied case path. Writers reject inconsistent roots before opening a
temp file. Each writer checks its differentiated canonical-byte cap before the
first record write; readers enforce the same cap from retained descriptor
metadata and the actual byte count.

Readers use retained parent descriptors and relative no-follow opens; reconcile
name/fd type, owner, mode, device, inode, size, and unchanged read metadata;
then enforce the differentiated size cap, exact inventory, canonical ASCII
bytes, deep schema, plan identity, and all referenced hashes. Symlinks, special
files, wrong modes, truncation, partial/extra artifacts, duplicates, stale
epoch/run/manifest/component hashes, tampering, and replacement fail closed.
The result file remains the frozen result row with no added metadata and is
trusted only through a valid commit-last case record.

The historical fault-point names retain `replace` for compatibility even
though immutable records use no-clobber publication. A callback runs at these
exact durable boundaries:

- `after-result-replace`: after result publish, temp removal, both required
  directory fsyncs, and committed readback; skipped when result is null.
- `after-evidence-replace`: the same durable boundary for evidence.
- `before-case-commit`: after result/evidence rehash and immediately before any
  case-commit temp is opened.
- `after-case-commit`: after durable case-commit publication and readback.
- `before-shard-commit`: after every referenced attempt/case/receipt reopens and
  immediately before any shard temp is opened.
- `after-shard-commit`: after durable shard publication and readback.

A callback exception is re-raised. Before-commit faults may leave immutable
result/evidence files, but no reader can expose a case seal without a valid
commit. An after-commit fault leaves a readable committed seal. An exact
idempotent replay returns the already-validated path without invoking the fault
callback again. A fault is not called when an earlier stage failed. Every new
parent/temp/read descriptor
inherits Task 7's one-shot capability retirement: invalidate before the sole
close call, preserve primary-first deterministic errors, poison the process on
an indeterminate close, and perform no later publish, unlink, read, retry, or
cleanup syscall on the retired integer. Process exit is the only unconditional
reclamation boundary.

### 7. Durable progress, ACK, and launch-cost semantics

```python
ProgressType = Literal[
    "lane-ready", "case-started", "case-terminal", "shard-terminal",
    "worker-stopped",
]
AckDecision = Literal["continue", "stop-launches", "abort"]
MAX_PROGRESS_BYTES = 4096
MAX_PROGRESS_STRING_CHARS = 256
MAX_TOKEN_COUNT = 2**63 - 1

PROGRESS_FIELDS = {
    "schema_version", "epoch_id", "run_kind", "lane", "seq", "type", "case", "attempt",
    "status", "classification", "model_started", "usage",
    "attempt_terminal_sha256", "case_commit_sha256", "shard_commit_sha256",
    "tombstone_receipt_sha256",
}
ACK_FIELDS = {
    "schema_version", "epoch_id", "run_kind", "lane", "seq", "message_sha256", "decision",
}

def write_progress(worker_root: Path, message: ProgressMessage) -> Path: ...
def read_progress(path: Path, expected_lane: LaneName, expected_seq: int) -> ProgressMessage: ...
def write_ack(worker_root: Path, message: ProgressMessage, decision: AckDecision) -> Path: ...
def wait_for_ack(worker_root: Path, message: ProgressMessage, timeout: float) -> Ack: ...
```

Each lane writes canonical `progress/<seq:06d>.json` by fsync/replace/fsync,
then emits one content-free stdout wake-up `{lane,seq,sha256}`. The coordinator
reopens the durable file, validates exact fields/size/hash/sequence, checkpoints
production, updates cumulative tokens for `case-terminal`, and writes
`acks/<seq:06d>.json`. The worker waits for ACK before model start and after each
terminal before launching the next case. Every progress field is present; values
that do not apply to that message type are JSON `null`. Case IDs must come from
the plan, free strings are capped at `MAX_PROGRESS_STRING_CHARS`, and each token
field is an integer in `0..MAX_TOKEN_COUNT`. Lost stdout is recovered by polling;
identical duplicates are idempotent; different duplicates, gaps, reordered
durable records, truncation, oversize, unknown fields, or unsafe strings abort.

The per-type nullability and meaning are exact:

| `type` | `case` / `attempt` | status/classification/model/usage | attempt hash | case hash | shard hash | tombstone hash |
|---|---|---|---|---|---|---|
| `lane-ready` | both null | all null | null | null | null | null |
| `case-started` | both required | all null | null | null | null | null |
| `case-terminal` | both required | copied exactly from the durable attempt terminal | required | success: required; failed: optional | null | required iff cleanup passed; otherwise optional |
| `shard-terminal` | both null | status copied from shard; classification/model/usage null | null | null | required | null |
| `worker-stopped` | both null | all null | null | null | null | null |

`status` is exactly `success` or `failed` when present. Every non-null digest is
lowercase 64-character SHA-256. A `case-terminal` always reopens the referenced
durable attempt terminal and requires the same case, attempt, status,
classification, model-started value, usage, epoch, run kind, lane, and manifest
bindings. A successful case-terminal additionally requires and reopens the
case commit and expected canonical tombstone receipt, and all three hashes must
agree with that seal. A failed case-terminal may omit its case-commit hash under
Task 8's pre-commit failure rules. Its tombstone hash is required when the
referenced attempt terminal says `cleanup_passed=true`; when cleanup failed, a
valid existing receipt is still bound and only an absent canonical receipt may
produce null. Any present case/receipt hash is reopened and verified. A
`shard-terminal` reopens the shard commit named by `shard_commit_sha256`, and
its status must match; no case-, attempt-, or receipt-level digest may be
smuggled into that message.

`case-terminal` decision truth is exact:

| Terminal condition | attempt hash | case hash | tombstone hash | Decision |
|---|---|---|---|---|
| ordinary success with expected tombstone | required | required | required | `continue` or token-ceiling `stop-launches` |
| evaluated failure after successful scrub | required | committed hash or null | required | `abort` |
| cleanup failure before receipt commit | required | committed hash or null | null | `abort` and coordinator recovery |
| cleanup failure with a valid durable receipt | required | committed hash or null | required | `abort` and coordinator recovery |
| integrity receipt with missing/replaced canonical binding | required | committed hash or null | required | `abort`, never success |
| worker exit before terminal | no record | no record | no record | coordinator detects exit, recovers cleanup, and aborts |

A successful terminal with any required hash null is invalid. A failed terminal
binds whatever was already durably committed but is never resumable as success.

`ParallelOptions.max_total_tokens` is an optional launch ceiling, not a
guaranteed spend cap and not a monetary estimate. The coordinator sums ACKed
case `total_tokens`. When the sum reaches/exceeds the ceiling, it ACKs
`stop-launches`; in-flight cases finish and may overshoot. If planned cases
remain, the epoch is incomplete and cannot persist. With `None`, all 28 launches
remain required. Usage is sealed for audit but never includes model text.

### 8. Retry/resume and aggregation capability

```python
def decide_retry(
    *, classification: OutcomeClass, attempt: int, model_started: bool,
    cleanup_passed: bool, fingerprints_unchanged: bool,
) -> RetryDecision: ...
def plan_resume(
    *, plan: EpochPlan, run_root: Path, current_fingerprints: InputFingerprints,
    manifests: dict[EvalMode, list[dict[str, object]]],
) -> ResumePlan: ...

class ValidatedEpoch:
    @property
    def run_kind(self) -> RunKind: ...
    @property
    def epoch_id(self) -> str: ...
    @property
    def teardown_receipt_sha256(self) -> str: ...
    def claim_formal_commit(self) -> "FormalCommitCapability": ...

class FormalCommitCapability:
    @property
    def epoch_id(self) -> str: ...
    @property
    def run_kind(self) -> Literal["formal"]: ...
    @property
    def consumed(self) -> bool: ...

def validate_epoch_for_aggregation(
    *, plan: EpochPlan, run_root: Path, snapshot_root: Path,
    manifests: dict[str, list[dict]], shard_paths: dict[LaneName, Path],
    case_paths: dict[CaseKey, CasePaths], integrity_runner: Callable[..., dict],
    guard: CoordinatorGuard, current_fingerprints: InputFingerprints,
    teardown_receipt: Path,
) -> ValidatedEpoch: ...
def aggregate_committed_cases(validated: ValidatedEpoch) -> Aggregate: ...
def persist_validated_epoch(
    commit: FormalCommitCapability, *, authority: ResultWriterAuthority,
    destinations: dict[str, Path], guard: CoordinatorGuard,
) -> Path: ...
```

Retry remains one proved pre-model infrastructure retry only. Semantic/model
failure invalidates; cleanup, mutation, timeout, protocol, post-start transport,
or surviving process aborts. Same-epoch sealed successes may resume. A start
without terminal is ambiguous and invalidates. Formal acceptance requires
exactly one model-started attempt per case.

`plan_resume` accepts exactly the `forward` and `lifecycle` manifest lists,
revalidates their whole-manifest hashes and frozen deep schemas, and walks
`plan.assignments` in its existing canonical order. It first requires
`current_fingerprints` to equal the plan's complete fingerprints. For each
assignment it selects only the row at the assignment's one-based ordinal,
verifies the row ID, and passes that exact row to
`scan_attempts(paths_for_case(run_root, assignment), plan=plan,
manifest_case=manifest_case)`. `ResumePlan.reusable`, `pending`, and `invalid`
are stable subsequences of `plan.assignments`; no filesystem discovery, ID
search, path sort, or caller order may change them. Each attempt record's
`manifest_case_sha256` must equal the digest of that exact selected row even
when the plan epoch and whole-manifest hashes still match. Any row/digest
mismatch places the case in `invalid` and prevents model launch or sealed-result
reuse.

`validate_epoch_for_aggregation` receives every shard/case path and callable it
needs. In strict order it revalidates capture/config/manifests, four shard
commits, 28 case commits, attempt truth, 24/4 routes, every store and invalidated
count, payload/output/audit/Codex-state/process cleanup, all existing schemas and
semantic validators, then the commit-last teardown receipt against all 28
tombstone receipt hashes, absent Codex-home paths/bootstrap, and still-readable
case evidence roots. It finally revalidates frozen order, production baseline, and exact allowed
result delta. It returns a module-created capability containing `run_kind`,
validated ordered rows, teardown receipt hash, and expected hashes. Aggregation accepts discovery or
formal capabilities for non-authoritative reporting. `claim_formal_commit`
atomically changes private state from `validated` to `issued`, rejects
diagnostic/discovery and repeat claims, and returns a module-authenticated token.
`persist_validated_epoch` verifies `formal`, atomically consumes that token
before opening a destination or calling `persist_result_pair`, and rejects a
second use before the paired writer. Thus a green discovery epoch and a reused
formal capability cannot publish.

### 9. Complete worker/coordinator types and entry points

`RunCoordinatorLease` and `ResultWriterAuthority` are defined once in Exact
Contract 5 because Task 7 implements those nominal prerequisites.
`OutcomeClass` and `ShardTerminal` are defined once before/in Exact Contract 6
because Task 8 persists them. The types below consume them without
redeclaration.

```python
CoordinatorPhase = Literal[
    "preflight", "running", "cancelling", "tearing-down", "validating", "validated",
    "commit-ready", "committed", "failed",
]

@dataclass(frozen=True)
class AttemptRecord:
    key: CaseKey
    run_kind: RunKind
    attempt: Literal[1, 2]
    classification: OutcomeClass
    model_started: bool
    cleanup_passed: bool
    start_sha256: str
    terminal_sha256: str
    usage: TokenUsage | None

@dataclass(frozen=True)
class ProgressMessage:
    schema_version: int
    epoch_id: str
    run_kind: RunKind
    lane: LaneName
    seq: int
    type: ProgressType
    case: CaseKey | None
    attempt: int | None
    status: CaseSealStatus | None
    classification: OutcomeClass | None
    model_started: bool | None
    usage: TokenUsage | None
    attempt_terminal_sha256: str | None
    case_commit_sha256: str | None
    shard_commit_sha256: str | None
    tombstone_receipt_sha256: str | None

@dataclass(frozen=True)
class Ack:
    schema_version: int
    epoch_id: str
    run_kind: RunKind
    lane: LaneName
    seq: int
    message_sha256: str
    decision: AckDecision

@dataclass(frozen=True)
class RetryDecision:
    retry: bool
    next_attempt: Literal[2] | None
    action: Literal["reuse", "invalidate", "abort"]
    reason: str

@dataclass(frozen=True)
class ResumePlan:
    run_kind: RunKind
    reusable: tuple[CaseKey, ...]
    pending: tuple[CaseKey, ...]
    invalid: tuple[CaseKey, ...]

@dataclass(frozen=True)
class Aggregate:
    run_kind: RunKind
    forward_rows: tuple[dict[str, object], ...]
    lifecycle_rows: tuple[dict[str, object], ...]
    evidence_sha256: str

@dataclass(frozen=True)
class ProductionSnapshot:
    fingerprint: str
    entries: tuple[tuple[str, str], ...]

class CoordinatorGuard:
    @classmethod
    def capture(cls, repository_root: Path) -> "CoordinatorGuard": ...
    @property
    def baseline(self) -> ProductionSnapshot: ...
    def checkpoint(self, reason: str) -> ProductionSnapshot: ...
    def verify_exact_result_delta(
        self, expected: dict[str, str], reason: str,
    ) -> ProductionSnapshot: ...

class QuiescentRunAuthority:
    @property
    def epoch_id(self) -> str: ...
    @property
    def run_kind(self) -> RunKind: ...
    @property
    def consumed(self) -> bool: ...

class CoordinatorStateMachine:
    @classmethod
    def create(
        cls, plan: EpochPlan, options: "ParallelOptions", guard: CoordinatorGuard,
    ) -> "CoordinatorStateMachine": ...
    @property
    def phase(self) -> CoordinatorPhase: ...
    def register_worker(self, lane: LaneName, process: subprocess.Popen) -> None: ...
    def accept_progress(self, message: ProgressMessage) -> AckDecision: ...
    def cancel(self, reason: str) -> None: ...
    def workers_stopped(self) -> QuiescentRunAuthority: ...
    def begin_teardown(self) -> None: ...
    def mark_torn_down(self, receipt: TeardownReceipt) -> None: ...
    def begin_validation(self) -> None: ...
    def mark_validated(self, validated: ValidatedEpoch) -> None: ...
    def mark_commit_ready(self, commit: FormalCommitCapability) -> None: ...
    def mark_committed(self) -> None: ...

@dataclass(frozen=True)
class ParallelOptions:
    run_kind: RunKind
    run_root: Path
    source_codex_home: Path
    codex_executable: Path
    requested_model: str | None = None
    requested_reasoning_effort: str | None = None
    resume_run_root: Path | None = None
    max_total_tokens: int | None = None

@dataclass(frozen=True)
class RuntimePayloadAudit:
    root: Path
    payload_dir: Path
    log_path: Path
    wrapper_path: Path

@dataclass(frozen=True)
class CaseRuntime:
    store_root: Path
    audit: RuntimePayloadAudit
    environment: dict[str, str]
    writable_roots: tuple[Path, ...]
    transport_config: ResolvedTransportConfig
    selected_command: str
    disabled_skill_paths: tuple[Path, ...]
    integrity_command: tuple[str, ...] | None
    audited_wrapper_path: Path | None
    audited_wrapper_content: str | None

@dataclass(frozen=True)
class CaseExecution:
    terminal_status: str
    final_text: str
    command_executions: tuple[str, ...]
    observation_command_diagnostics: tuple[dict[str, object], ...]
    usage: TokenUsage

@dataclass(frozen=True)
class DrivenCase:
    result: dict[str, object]
    execution: CaseExecution

class CaseTransport(Protocol):
    def __call__(
        self, case: dict[str, object], workspace: Path, runtime: CaseRuntime,
        wiki_root: Path, after_first_turn: Callable[[], None] | None = None,
        event_sink: CaseEventSink | None = None,
    ) -> CaseExecution: ...

def _run_case(
    case: dict[str, object], destination: Path, lifecycle: bool,
    runtime_factory: Callable[..., CaseRuntime] | None = None, *,
    workspace_parent: Path | None = None,
    transport_runner: CaseTransport = execute_case_transport,
    event_sink: CaseEventSink | None = None,
    execution_sink: Callable[[CaseExecution], None] | None = None,
) -> dict[str, object]: ...

class RuntimeFactory(Protocol):
    @property
    def poisoned(self) -> bool: ...
    def __call__(
        self, *, assignment: CaseAssignment, manifest_case: dict[str, object],
        paths: CasePaths, transport_config: ResolvedTransportConfig,
    ) -> CaseRuntime: ...
    def cleanup_case(self, paths: CasePaths) -> TombstoneReceipt: ...
    def close(self) -> None: ...

class CaseDriver(Protocol):
    def __call__(
        self, *, assignment: CaseAssignment, manifest_case: dict[str, object],
        paths: CasePaths, runtime_factory: RuntimeFactory,
        event_sink: CaseEventSink,
    ) -> DrivenCase: ...

@dataclass(frozen=True)
class WorkerDependencies:
    runtime_factory: RuntimeFactory
    case_driver: CaseDriver

WorkerCommandFactory = Callable[
    [LaneName, EpochPlan, ParallelOptions, Path], Sequence[str]
]

@dataclass(frozen=True)
class CoordinatorDependencies:
    worker_command_factory: WorkerCommandFactory
    integrity_runner: Callable[..., dict[str, object]]

@dataclass(frozen=True)
class ParallelRunResult:
    run_kind: RunKind
    run_root: Path
    status: Literal["diagnostic", "validated", "committed", "failed"]
    validated: ValidatedEpoch | None

def build_production_case_driver(
    *, snapshot_root: Path, transport_config: ResolvedTransportConfig,
    transport_runner: CaseTransport | None = None,
) -> CaseDriver: ...
def build_production_runtime_factory(
    *, snapshot_root: Path, transport_config: ResolvedTransportConfig,
    plan: EpochPlan,
) -> RuntimeFactory: ...
def production_worker_dependencies(
    *, snapshot_root: Path, transport_config: ResolvedTransportConfig,
    plan: EpochPlan,
) -> WorkerDependencies: ...
def production_coordinator_dependencies(
    *, snapshot_root: Path,
) -> CoordinatorDependencies: ...
def run_worker(
    *, lane: LaneName, plan: EpochPlan, run_root: Path, snapshot_root: Path,
    dependencies: WorkerDependencies | None = None,
) -> Path: ...
def worker_main(argv: Sequence[str] | None = None) -> int: ...
def run_parallel_evaluation(
    *, repository_root: Path, manifests: dict[EvalMode, list[dict[str, object]]],
    result_destinations: dict[EvalMode, Path] | None, options: ParallelOptions,
    dependencies: CoordinatorDependencies | None = None,
) -> ParallelRunResult: ...
```

Every diagnostic, discovery, and formal run acquires exactly one
`RunCoordinatorLease` before any resume scan or mutable run-root operation and
holds it through worker shutdown,
tombstone/bootstrap teardown, and validated/failed terminal state. It is a
mode-`0600`, no-follow, owner-checked `flock` at
`run_root/coordinator/coordinator.lock`, bound to the sealed epoch and run kind;
for a new run, `acquire` itself is the sole secure initializer of the run root,
coordinator directory, and lock before returning the live capability;
it serializes supported original/resume coordinators for that run. Formal runs
then acquire the repository-keyed `ResultWriterLease`; no path may acquire the
two leases in reverse or nest a second instance. Release is always reverse
order: result-writer lease first, per-run lease last. Diagnostic/discovery never
acquire the result-writer lease. Neither lease claims to exclude arbitrary
hostile same-UID processes.

`CoordinatorGuard.capture` takes the one immutable production baseline;
`checkpoint` recomputes and requires exact equality at every named barrier;
`verify_exact_result_delta` is used only after authority-held persistence.
Invalid phase transitions fail closed. `cancel` sends stop decisions, terminates
all groups, joins readers, then enters teardown; cancellation reaches `failed`
only after verified tombstone/bootstrap cleanup or a classified teardown
failure. `workers_stopped()` issues one module-authenticated
`QuiescentRunAuthority`; recovery and removal operations require it plus the
live per-run lease, and commit of the teardown receipt consumes it exactly once.
`begin_validation` requires the commit-last `TeardownReceipt` already recorded
by `mark_torn_down`.

With `dependencies=None`, `run_worker` calls
`production_worker_dependencies`: it imports the verified captured evaluator,
uses the writable staged marketplace runtime factory, and invokes its existing
`_run_case`, extended with keyword-only `workspace_parent`, `transport_runner`,
event, and execution sinks. `workspace_parent=None` preserves legacy serial
behavior; production passes the validated canonical parent whose frozen builder
must return exact `CasePaths.workspace`. `transport_runner` defaults to
`execute_case_transport`. `build_production_case_driver` uses that default in
production; a focused deterministic test may
inject a transport stub at this below-evaluator boundary while retaining real
fixture/runtime/evaluator setup. Only the test-only Task 13 wrapper passes a
replacement `WorkerDependencies.case_driver`. `worker_main` exposes no driver,
module, command, or environment override, and the production coordinator uses
the captured worker command/integrity functions when dependencies are absent.
`run_worker` reopens the canonical transport-config JSON sealed under the run
root and checks its plan hash before building those dependencies. Diagnostic and
discovery require `result_destinations=None`; formal requires both destinations
and the formal commit/authority path above.

## RED-first implementation sequence

Every task below ends in one bounded local commit.

### Task 1: Planner and exact fingerprint algorithms

**Files:** create `workflow_eval_sharding.py` and
`test_workflow_eval_sharding.py`.

First RED: `PlannerTests.test_frozen_plan_has_exact_8_8_8_4_coverage` imports the
missing module, loads the frozen manifests, and asserts 28 unique `CaseKey`s,
24/4 routes, exact lane membership, canonical lane-relative order, and different
epoch IDs for otherwise identical discovery/formal plans.

```bash
cd evidence
python3 -m unittest tests.test_workflow_eval_sharding.PlannerTests.test_frozen_plan_has_exact_8_8_8_4_coverage -v
```

Expected RED: module missing. GREEN adds only planner/run-kind dataclasses,
mapping, canonical config bytes, and the exact inventory-member component digest.

Commit: `test(eval): define parallel plan identity`.

### Task 2: Frozen-base allowlist checker

**Files:** create `check_parallel_eval_frozen_boundary.py` and
`test_parallel_eval_frozen_boundary.py`.

First RED: create temp base/head trees and assert an allowed evaluator edit
passes, while an unallowlisted base-file edit and each explicit frozen path edit
fail with the exact path.

```bash
cd evidence
python3 -m unittest tests.test_parallel_eval_frozen_boundary.FrozenBoundaryTests.test_allowlist_and_frozen_contract_are_disjoint -v
```

Expected RED: checker missing. GREEN implements exact allowed paths, all six
manifest byte checks, `SHA256SUMS.json`/schema byte checks, AST binding
comparisons, forbidden result-name patterns, Git-range mode, and two-tree mode.
It asserts the allowlist/frozen set are disjoint. Run this checker after every
later commit.

Commit: `test(eval): enforce frozen base boundary`.

### Task 3: Resolved transport config and isolated auth

**Files:** modify `run_observing_workflows_task9_eval.py` and its test; extend
`workflow_eval_sharding.py` tests.

First RED: resolve a temp `config.toml`, build exec/app-server commands, mutate
the ambient config and `PATH`, and assert both commands still use the sealed
absolute executable and explicit model/reasoning. Replace that executable after
resolution and assert both transports fail before `Popen`.

```bash
cd evidence
python3 -m unittest tests.test_observing_workflows_task9_eval.Task9EvalRunnerTests.test_resolved_config_binds_both_transports -v
```

Expected RED: current overrides omit model/reasoning and lack a resolved config.
GREEN adds config resolution, `--strict-config`, exec `--ignore-user-config`,
canonical executable path/identity, pre-launch rehash, auth-only bootstrap with
descriptor/mode checks, no secret hashing/logging, and fake-auth tests for
missing/symlink/unsafe files.

Commit: `feat(eval): bind isolated transport config`.

### Task 4: Collision-free paths and writable runtime staging

**Files:** modify `workflow_eval_sharding.py`; create
`run_observing_workflows_eval_worker.py`; extend sharding tests.

First RED: pass a `0444` snapshot containing forward/lifecycle same-ID cases,
stage both, and assert disjoint roots, writable staged wrappers, unchanged
snapshot hashes, and exact `attempts/01`/`02` paths. A second RED calls
`build_production_runtime_factory` and proves it constructs the captured staged
marketplace runtime rather than the read-only tree.

```bash
cd evidence
python3 -m unittest tests.test_workflow_eval_sharding.RuntimeIsolationTests.test_read_only_capture_stages_disjoint_writable_cases -v
python3 -m unittest tests.test_workflow_eval_sharding.RuntimeIsolationTests.test_production_runtime_factory_uses_writable_stage -v
```

Expected RED: worker/path/staging/runtime-factory interfaces missing. GREEN adds
case and attempt paths, safe mode-normalized staging, isolated environment
roots, auth install, retained-descriptor ownership, the explicit
`active/scrubbing/tombstoned` scrub transaction, the production runtime factory,
and scan rejection for gaps/duplicates/partials. REDs must cover canonical
replacement, moved-directory identity, child/depth/entry failure followed by a
successful retry, setup failure plus cleanup failure ordering, descriptor
closure, the one-descriptor-per-case bound, and proof that no top-level
Codex-home/quarantine name is removed by the worker. A successful
`cleanup_case` writes and returns an idempotent durable tombstone receipt for
later coordinator teardown rather than deleting the directory. A commit-last
ownership record must exist before auth bytes are installed so a crashed worker
without a tombstone receipt is recoverable. Receipt tests reject stale identity,
wrong epoch/run/case, symlinks, wrong modes, unknown fields, truncation, and
hash mismatch; repeated cleanup returns byte-identical receipt bytes. Bootstrap
creation uses the same ownership-before-secret ordering and retained descriptor;
its final scrub/removal remains Task 12 after process quiescence. Close-boundary
REDs force `close()` to real-close and immediately reuse the same integer for a
new open of the same directory before raising, and separately raise before a
real close. For `RuntimeError`, `KeyboardInterrupt`, `EBADF`, and post-close
`EIO`, assertions require `descriptor=-1`, `indeterminate`, exact error identity,
one close call on the retired number, live replacement preservation, no later
syscall from idempotent cleanup/factory close, and process-terminal poisoning.
Setup REDs inject every descriptor role, prove all independent roles are each
attempted once, no result is returned after finalization failure, and verify
primary-first deterministic `ExceptionGroup`/`BaseExceptionGroup` leaves.
Record-writer REDs force real-close plus same-directory same-number reuse for
both temp and parent descriptors and prove one close call, replacement survival,
no post-indeterminate unlink/replace, and exact ordered errors. Factory REDs
inject a marked close failure through setup and cleanup, catch it, then prove
`poisoned=True`, every other owned descriptor retired once, and a later
`__call__` performs zero filesystem/model work.

Commit: `feat(eval): stage isolated worker runtimes`.

### Task 5: Early captured-package boundary

**Files:** modify packager and both byte-identical package tests.

First RED: assert `default_evidence` and a built archive contain
`workflow_eval_sharding.py` and `run_observing_workflows_eval_worker.py`.

```bash
python3 -m unittest discover -s plugins/workflow-observer/tests -p 'test_package_archive.py' -v
```

Expected RED: new modules absent from package evidence. GREEN adds only the
minimum sources required by captured workers and proves two identical builds.
This task precedes all coordinator spawning.

Commit: `build(eval): capture parallel worker sources`.

### Task 6: Process groups and conservative model-start telemetry

**Files:** modify
`evidence/scripts/run_observing_workflows_task9_eval.py`,
`evidence/scripts/run_observing_workflows_eval_worker.py`,
`evidence/scripts/workflow_eval_sharding.py`,
`evidence/tests/test_observing_workflows_task9_eval.py`, and
`evidence/tests/test_workflow_eval_sharding.py`.

First RED calls current `execute_case_transport(..., event_sink=...)` and fails
because current code has no event-sink parameter. After adding minimal callback
plumbing, a second RED has fake app-server accept `turn/start` bytes but drop the
response; it asserts `model_started=True` and post-start/abort classification.
A third RED builds the production case driver with a transport stub and proves
it still enters captured `_run_case` plus the staged marketplace runtime.

```bash
cd evidence
python3 -m unittest tests.test_observing_workflows_task9_eval.Task9EvalRunnerTests.test_execute_transport_accepts_event_sink -v
python3 -m unittest tests.test_observing_workflows_task9_eval.Task9EvalRunnerTests.test_app_server_dropped_start_response_is_model_started -v
python3 -m unittest tests.test_workflow_eval_sharding.RuntimeIsolationTests.test_production_driver_uses_captured_evaluator_with_transport_stub -v
```

Expected first RED: missing callback interface; expected second RED: callback is
not yet emitted conservatively before `_send`. GREEN adds the before-send event,
typed transport/protocol/model failures,
process sessions, group cleanup, reader joins, and exec/app token normalization.
It also adds explicit below-boundary transport injection to captured `_run_case`
and wires the default production driver. Separate REDs cover surviving child,
dropped response, malformed/missing usage, and no pre-model retry after any
prompt-send ambiguity. Worker credential scrubbing cannot begin until process
group cleanup and reader joins are proved; a surviving process retains the
active cleanup descriptor/state and fails the case closed. Any indeterminate
descriptor close is likewise terminal: stop launches, emit only already-durable
sanitized failure state if possible, and permit no next case, cleanup retry, or
recovery open in that process. Task 6 exposes and propagates the exact terminal
API
`worker_exit_required(error: BaseException, factory: RuntimeFactory) -> bool`.
It returns true for the factory `poisoned` property or a marked leaf found by
`is_indeterminate_descriptor_close(error)`, including leaves nested in a
`BaseExceptionGroup`. Task 12 catches and uses this API in `run_worker` to exit
the worker process so the OS reclaims the retired capability. Task 6 does not
add generic retry, attempt persistence, or result persistence.

Workspace REDs require the production transport cwd, staged skills/runtime,
and frozen evaluator fixture to be the same exact `CasePaths.workspace`. They
also revise the earlier Task 4 path regression to expect
`<case-root>/workspace/<case-id>` and prove that the runtime factory is not
called until the frozen builder has created that fixture. Explicit-branch REDs
cover the exact `destination`, exact parent, non-canonical/out-of-case or
symlinked `workspace_parent`, a pre-existing final fixture child, unchanged
legacy behavior when `workspace_parent` is omitted, and zero gate residue for
every rejection. The driver passes `destination=paths.root` and the adapter
must receive `case_root=paths.root`; no adapter/direct `_GATE_ROOTS` mutation is
allowed. Event REDs require positive PID/PGID on every production event,
terminal sink failure with cleanup still performed, conservative internal
model-start truth before the sink, no send when that sink raises, and
`process-stopped` only after the entire process group is absent and all readers
have joined.

Commit: `feat(eval): harden transport attempt boundaries`.

### Task 7: Shared writer lease and fail-closed authority

**Files:** extend sharding, evaluator, sharding/evaluator tests, and the two
byte-identical marketplace runner copies plus their focused caller tests. Do
not add the Task 12 coordinator loop or any Task 8+ seal/fault behavior.

**Task-order correction:** Task 7 first implements only the concrete nominal
`RunCoordinatorLease` primitive frozen in Exact Contract 5. This is the minimum
dependency needed by the parallel writer contract and was formerly implicit in
Task 12. Task 12 consumes it and retains every higher-level parallel behavior.
Review this plan-only correction before writing production GREEN code.

First RED: start two real Python processes against one temp Git repo with
different `TMPDIR`, `HOME`, and `CODEX_HOME`; process A holds the parallel lease
through a deterministic barrier injected after atomic pointer replacement but
before committed readback. Process B must invoke the supported legacy serial
`run_suite` entry point—not manually acquire a writer lease—and must contend on
the fixed repository lock and fail before its persistence-entry marker or
destination exists. Release process A, prove its readback completes, then start
the same supported serial entry point again and prove it reacquires the
retained lock and persists successfully. Process A must construct a real
`RunCoordinatorLease`; a boolean, fake, subclass, protocol, or duck-typed object
is not a valid RED fixture.

```bash
cd evidence
python3 -m unittest tests.test_workflow_eval_sharding.WriterLeaseTests.test_serial_and_parallel_writers_contend_across_processes -v
```

Expected RED before the task-order correction is GREEN: the exact live
`RunCoordinatorLease` witness is absent. Add focused REDs before their
corresponding production increments for:

```bash
cd evidence
python3 -m unittest tests.test_workflow_eval_sharding.WriterLeaseTests.test_run_lease_is_nominal_pid_bound_nonreentrant_and_writer_closes_first -v
python3 -m unittest tests.test_workflow_eval_sharding.WriterLeaseTests.test_trusted_parent_policy_accepts_sticky_and_rejects_writable_nonsticky -v
python3 -m unittest tests.test_workflow_eval_sharding.WriterLeaseTests.test_fixed_result_lock_root_rejects_unsafe_entries_and_repository_aliases -v
python3 -m unittest tests.test_workflow_eval_sharding.WriterLeaseTests.test_parallel_writer_requires_exact_live_run_lease -v
python3 -m unittest tests.test_workflow_eval_sharding.WriterLeaseTests.test_authority_issues_and_consumes_once_before_any_destination_open -v
python3 -m unittest tests.test_workflow_eval_sharding.WriterLeaseTests.test_lease_close_failures_are_one_shot_and_poison_process -v
python3 -m unittest tests.test_workflow_eval_sharding.WriterLeaseTests.test_authoritative_persistence_descriptors_are_one_shot_and_lock_closes_last -v
python3 -m unittest tests.test_observing_workflows_task9_eval.Task9EvalRunnerTests.test_run_suite_holds_one_serial_lease_through_readback_and_delta_check -v
python3 -m unittest tests.test_observing_workflows_task9_eval.Task9EvalRunnerTests.test_unset_worker_unknown_and_nonformal_roles_fail_before_result_paths -v
python3 -m unittest tests.test_observing_workflows_task9_eval.Task9EvalRunnerTests.test_persist_freezes_exact_destination_mapping_before_authority_consumption -v
python3 -m unittest tests.test_observing_workflows_task9_eval.Task9EvalRunnerTests.test_consumed_authority_rejects_repository_swap_before_result_open -v
python3 -m unittest tests.test_observing_workflows_task9_eval.Task9EvalRunnerTests.test_destination_parent_replacement_before_and_after_retention_is_not_authoritative -v
```

These REDs include a root-owned sticky parent, equality-spoofing values before
filesystem work, a post-consume caller-mapping switch, repository-name
replacement before result-parent open, and close-then-raise same-number FD reuse
for every lease/rollback descriptor. They require zero redirected result writes,
lock-last release, exact close-error identity/order, process poison, and zero
syscalls on repeated close.

Additional retained-parent REDs replace the destination parent both immediately
before descriptor-relative open and after its descriptor is retained. Both
cases require no write in the replacement or an external directory; the first
must fail on repository/parent identity, while the second must keep all reads
and writes bound to the retained original and reject any observed name mismatch.
The authority remains consumed. Close-injection REDs cover every lease,
rollback, generation, staging, pointer-read, generation-read, and committed
readback slot, simulate close-then-raise followed by same-number FD reuse, prove
the replacement FD is never closed, prove lock close is attempted last, and
require process poison after an indeterminate close.

The pre-existing direct `persist_result_pair(..., crash_at=...)` tests are not
grandfathered around the new authority boundary. Each invocation in
`test_result_commit_pointer_hides_all_precommit_crashes`,
`test_result_store_rejects_symlink_roots_and_pointer`, and any other supported
direct persistence test creates a fresh temporary Git top-level, acquires one
real `serial-coordinator` `ResultWriterLease` with `run_lease=None`, issues one
fresh authority, and keeps the lease live through the test's committed
readback. The original fault points, expected pointer visibility, and cleanup
assertions remain unchanged; no authority or consumed lease is reused between
fault scenarios.

GREEN implements the exact `RunCoordinatorLease` acquire/active/close
primitive and its process-local order registry, but no coordinator loop. It
then implements the fixed validated per-UID `/var/tmp` result lock, canonical
exact Git-root keying, destination containment, formal-only nominal
`ResultWriterLease`/`ResultWriterAuthority`, one authority issuance, and atomic
consumption before any destination open/write. The currently supported
`run_suite`, direct evaluator `main`, marketplace legacy/default formal path,
and both marketplace runner copies pass the explicit `serial-coordinator` role
without nested acquisition. Task 7 exposes and tests parallel acquisition only
through a real already-live run lease; Task 12 adds the production parallel
caller. Serial holds its one lease across final production guard/check,
commit, pointer resolution, rehash/rescore, committed readback, and exact-delta
verification. Existing fault scenarios, fault names, and `crash_at` semantics
remain unchanged, while every test setup and direct persistence call gains its
own fresh real serial lease and one-shot authority as required above. Task 7
does not implement the future `FaultInjector` interface.

Commit: `feat(eval): serialize all result writers`.

### Task 8: Crash-consistent attempt/case/shard seals

**Files:** extend sharding and sharding tests; modify the worker only to remove
its duplicate tombstone parser/reader and import the moved implementation from
sharding. Do not add a production seal caller in this task.

Implement the REDs below in order. Run each named test alone and observe the
contract-specific failure before adding its corresponding production behavior.
Do not batch later REDs ahead of the current GREEN:

1. `test_fault_after_evidence_never_exposes_case_commit` fixes the exact
   `seal_case(..., fault_injector=...)` signature, verifies that Python rejects
   undeclared `crash_at`, injects `after-evidence-replace`, and proves raw
   result/evidence may be durable while `read_case_seal` rejects the missing
   commit.
2. `test_attempt_records_have_exact_canonical_schema_and_caps` fixes the
   attempt-start/terminal field sets, start-hash binding, mode `0600`, canonical
   ASCII bytes, nullable usage/receipt rules, differentiated byte caps, and
   exact-byte idempotence.
3. `test_seal_schema_constants_match_frozen_result_contract` compares every
   duplicated deep result field set to the frozen evaluator constants without
   creating a production import cycle;
   `test_shard_terminal_schema_constant_and_wire_mapping_are_exact` freezes
   `SHARD_TERMINAL_FIELDS`, proves dataclass `key` encodes only as exact wire
   `case={mode,ordinal,case_id}`, keeps internal `run_kind` out of the nested
   wire record, and rejects missing/extra fields or mismatched
   status/classification/failure;
   `test_seal_evidence_has_closed_privacy_schema` rejects every extra
   top-level or nested evidence/failure/usage field and every path/text-shaped
   payload.
4. `test_case_seal_binds_fingerprints_attempt_result_and_expected_tombstone`
   proves a success stores both manifest hashes, all four frozen component
   hashes, recomputed attempt/result/receipt hashes, bounded counters/timing,
   and only the legacy three filenames.
5. `test_failed_case_nullable_artifacts_require_failed_cleanup` covers failed
   cases with and without result/receipt, rejects either null when both cleanup
   booleans are true, and rejects success or a success classification for a
   missing/replaced canonical tombstone binding.
6. `test_shard_seal_accepts_only_full_success_or_unique_failed_prefix` proves
   every terminal binds an attempt terminal, full success requires all case and
   receipt hashes, and a failure is exactly the canonical prefix ending at the
   first failed case with nullable hashes only under the case rules.
7. `test_seal_readers_reject_partial_extra_stale_and_unsafe_inputs` mutates one
   dimension at a time: partial/extra inventory, truncation, noncanonical JSON,
   wrong mode, oversize, stale epoch/run/case/lane/route/manifest/component
   identity, hash mismatch, symlink, FIFO/socket/special file, and referenced
   record replacement. Every reader fails closed.
8. `test_immutable_seal_publication_never_overwrites_collision` proves empty
   publication, byte-identical complete idempotence, rejection of a proper
   subset or differing existing final, atomic no-clobber link collision, no
   chmod repair, and no partial-state healing.
9. `test_all_seal_fault_points_have_exact_durable_visibility` injects all six
   names. Result/evidence points expose no case commit; before-commit exposes
   only rehashed dependencies; after-case/shard commit remains readable while
   re-raising the injected exception; null-result cases skip the result point.
10. `test_seal_descriptor_failures_retire_once_and_poison_process` injects every
    parent/temp/read descriptor role, including close-then-raise with immediate
    same-number reuse. It proves immediate slot ownership, one close attempt,
    replacement survival, primary-first error order, no post-indeterminate
    publish/unlink/read/retry, and process poison before a later seal call.

```bash
cd evidence
python3 -m unittest tests.test_workflow_eval_sharding.SealTests.test_fault_after_evidence_never_exposes_case_commit -v
```

Expected first RED: seals/fault interface missing. Each GREEN is limited to the
smallest Exact Contract 6 increment needed by its current RED. After all
ordered cycles, run the complete `SealTests`, then the Task 3--7 focused
regressions and the deterministic gates below. Request an independent Task 8
implementation review before Task 9 begins.

Commit: `feat(eval): seal worker evidence`.

### Task 9: Durable progress, ACK, and token ceiling

**Files:** extend sharding, worker, and focused tests.

First RED: write a case-terminal message, drop stdout wake-up, poll the durable
file, ACK it, and prove the worker cannot begin its next case before ACK.

```bash
cd evidence
python3 -m unittest tests.test_workflow_eval_sharding.ProgressProtocolTests.test_lost_wakeup_recovers_and_ack_blocks_next_launch -v
python3 -m unittest tests.test_workflow_eval_sharding.ProgressProtocolTests.test_progress_types_have_exact_seal_hash_truth -v
```

Expected RED: protocol absent. GREEN implements exact schemas, `4096`-byte cap,
sealed run kind, sequence/hash rules, polling, fsynced ACK, bounded usage,
cumulative launch ceiling, and tests for duplicate/lost/truncated/reordered/
oversized/prompt-bearing messages. A table-driven RED serializes all five
`ProgressType` values and proves every field is always present, unrelated hash
fields are null, `case-terminal` always names its durable attempt terminal,
successful cases require case-commit plus tombstone hashes, failed cases obey
Task 8's nullable commit and cleanup-dependent receipt rules, and
`shard-terminal` names only its shard commit. Further REDs cover ordinary
success, evaluated failure, cleanup failure with and without a receipt,
non-expected-binding receipt, forged/cross-case attempt, case, shard, and
tombstone hashes, and worker exit before terminal against the exact truth table
above. Monetary cost remains explicitly out of scope.

Commit: `feat(eval): add durable worker acknowledgements`.

### Task 10: Typed retry and exact resume

**Files:** extend sharding, worker, and focused tests.

First RED: create valid attempt 1 pre-model terminal plus attempt 2 success and
assert one model-started attempt is accepted; then remove attempt-1 terminal and
assert resume invalidates. A second RED leaves the plan epoch and
whole-manifest hashes unchanged but coherently substitutes another valid row's
`manifest_case_sha256` through an attempt seal; exact ordinal-row selection must
still place that case in `ResumePlan.invalid`.

```bash
cd evidence
python3 -m unittest tests.test_workflow_eval_sharding.RetryResumeTests.test_two_attempt_layout_requires_proved_pre_model_first_attempt -v
python3 -m unittest tests.test_workflow_eval_sharding.RetryResumeTests.test_resume_rejects_forged_case_digest_with_unchanged_manifest_identity -v
```

Expected RED: exact scanner/decision absent. GREEN implements typed table,
the manifest-taking `plan_resume` signature, canonical forward-then-lifecycle
assignment order, exact one-based ordinal row selection, per-case digest
validation in `scan_attempts`, same-epoch seal reuse, no gaps, ambiguous-start
invalidation, and one model-started formal rule.

Commit: `feat(eval): enforce attempt retry truth`.

### Task 11: Validated-epoch aggregation capability

**Files:** extend sharding and focused tests.

First RED: call aggregation with a plain dict and assert rejection; omit one case
path and assert validation cannot return a capability; then prove an all-green
discovery epoch cannot claim/persist and the second use of one formal commit
capability fails before a spy `persist_result_pair` is entered.

```bash
cd evidence
python3 -m unittest tests.test_workflow_eval_sharding.AggregationGateTests.test_unvalidated_rows_cannot_aggregate_or_persist -v
python3 -m unittest tests.test_workflow_eval_sharding.AggregationGateTests.test_discovery_and_reused_formal_capability_cannot_persist -v
```

Expected RED: capability/gate absent. GREEN implements the exact serial order,
reopened integrity/cleanup, 28 unique seals, 24/4 routes, stable 20+8 order, all
existing validators, run-kind equality, formal-only one-shot commit capability,
and capability-required persistence/readback/delta checks. Validation requires
one coordinator teardown receipt proving every expected Codex-home tombstone
and the auth bootstrap absent while all case evidence roots remain readable; a
worker tombstone receipt alone is not a validated cleanup result.

Commit: `feat(eval): require validated epoch capability`.

### Task 12: Coordinator launcher and cancellation state machine

**Files:** extend sharding, worker, and focused tests only.

Task 12 consumes the exact `RunCoordinatorLease` primitive completed in Task 7
and must not reimplement, widen, subclass, or weaken its nominal checks. This
task owns the first production parallel acquisition sequence:
`RunCoordinatorLease` first, then `ResultWriterLease`, with reverse release.
It also owns the coordinator loop/state machine, worker launch and
cancellation, resume/recovery, quiescent authority, and teardown.

First RED: fake four lane processes deliver out-of-order durable terminals; assert
the coordinator ACKs only after a production checkpoint and cancels all process
groups before returning on a malformed lane record. A second RED cancels one
case after ownership commit but before tombstone commit and proves coordinator
recovery scrubs it, removes only the verified Codex-home, preserves the case
evidence root, and commits teardown before reaching `failed`. Further REDs
reject same-run coordinator contention, teardown without live lease/quiescent
authority, reverse two-lease acquisition, and interrupted bootstrap cleanup
recovered from its durable ownership record. A final RED commits a bootstrap
tombstone, injects an indeterminate close, proves the coordinator exits without
further opens/removals/teardown writes, then starts a fresh coordinator that
reacquires the lease, verifies the existing receipt, completes teardown, and
never launches a worker/model.

```bash
cd evidence
python3 -m unittest tests.test_workflow_eval_sharding.CoordinatorStateTests.test_terminal_checkpoint_precedes_ack_and_failure_cancels_all -v
python3 -m unittest tests.test_workflow_eval_sharding.CoordinatorStateTests.test_cancel_recovers_active_case_before_failed -v
python3 -m unittest tests.test_workflow_eval_sharding.CoordinatorStateTests.test_teardown_requires_ordered_leases_and_quiescent_authority -v
python3 -m unittest tests.test_workflow_eval_sharding.CoordinatorStateTests.test_interrupted_bootstrap_cleanup_recovers_before_teardown -v
python3 -m unittest tests.test_workflow_eval_sharding.CoordinatorStateTests.test_indeterminate_close_requires_fresh_cleanup_only_coordinator -v
```

Expected RED: state machine absent. GREEN adds captured worker launcher (package
boundary already GREEN), uses the already-reviewed Task 7 `flock` capability,
stop-launch markers, ACK/cost
decisions, exact public options/dependency/guard/state-machine entry points,
cancellation, guard checkpoints, and no persistence interface in worker
argv/environment. After every worker process group and reader is joined, the
same per-run lease holder consumes the worker tombstone receipts, or recovers a
missing receipt from the commit-last ownership record, then removes only the
verified empty Codex-home tombstones under the quiescent namespace. It proves
all `CasePaths.codex_home` paths absent while case evidence remains readable,
then removes the auth bootstrap and commits the teardown receipt. Any identity
mismatch, surviving process, unknown case root, or observed namespace mutation
aborts before validation and persistence and retains the run root for
inspection without deleting an unverified name. Indeterminate close poisons the
current coordinator; only a fresh lease holder after process exit may continue
the cleanup-only ownership/receipt transition described above.

Commit: `feat(eval): supervise parallel lanes`.

### Task 13: Real-process, no-model 28-case integration

**Files:** create the two test-only runners and integration test; add them to
package evidence.

First RED: spawn the test coordinator process and assert it starts four distinct
worker PIDs and returns 28 canonical sealed keys without invoking a sentinel
`codex` executable.

```bash
cd evidence
python3 -m unittest tests.test_parallel_eval_no_model_integration.ParallelNoModelIntegrationTests.test_real_processes_cover_all_28_cases -v
```

Expected RED: test drivers absent. GREEN uses dependency injection only: the
coordinator selects the captured test worker entrypoint; the worker injects a
schema-valid no-model case driver into real `run_worker`. It proves 8/8/8/4
lane order, 28 distinct environments, progress/ACK, seals, cleanup, process
joins, aggregation capability, and one atomic pair from a formal-kind no-model
epoch in a temp fixture repo. A discovery-kind twin proves zero writer calls.
It also proves worker tombstones exist before coordinator teardown, all 28
Codex-home tombstones are absent afterward, all 28 case evidence roots remain
readable during validation, bootstrap removal and the teardown receipt precede
validated capability creation, coordinator recovery handles an active case
with only an ownership record, and an injected namespace collision fails closed
without persistence or deletion of the replacement.
Production CLI exposes no test-driver flag.
The test-only wrapper is the only caller that replaces `WorkerDependencies`;
the separate Task 4 test already proves the production driver with a transport
stub below the evaluator boundary.

Commit: `test(eval): cross real parallel process boundary`.

### Task 14: Marketplace CLI, mirrors, docs, and final package gates

**Files:** modify both runner copies, create both parallel-runner tests, update
hygiene/package tests, and update named README/ROADMAP/TODO/plan copies.

First RED: `--help` lacks `--parallel {diagnostic,discovery,formal}` and
`--resume-run-root`. The legacy/default formal path's shared serial authority is
already a Task 7 regression and must be GREEN before Task 14 starts; Task 14
does not reacquire or replace that ownership.

```bash
python3 -m unittest discover -s plugins/workflow-observer/tests -p 'test_parallel_eval_runner.py' -v
```

Expected RED: options absent. GREEN adds opt-in CLI only, preserves current
preflight/diagnostic/sweep/default behavior, routes the new parallel formal path
through Task 12's ordered run/writer leases while retaining Task 7's existing
serial/default lease, maps the CLI mode into sealed `RunKind`, claims a commit
only for `formal`, asserts mirror byte parity, and documents that discovery is
non-authoritative. Do not claim 28/28 before a protected formal epoch.

Commit: `feat(eval): expose reviewed parallel coordinator`.

## Deterministic gates

### Focused worktree gate after every task

Run the task's named test, then:

```bash
cd evidence
python3 -m unittest tests.test_workflow_eval_sharding -v
python3 -m unittest tests.test_parallel_eval_frozen_boundary -v
python3 scripts/check_parallel_eval_frozen_boundary.py --base 2f617fea833e583af9cae87308cfde2e620fcd82 --head HEAD
cd ..
git diff --check 2f617fea833e583af9cae87308cfde2e620fcd82..HEAD
```

Expected: PASS and only exact allowlisted paths changed.

### Frozen hashes

Run `sha256sum` over all six tracked manifest copies returned by:

```bash
git ls-files '*observing_workflows_cases.json' '*observing_workflows_lifecycle_cases.json'
```

Expected: each forward copy has the frozen forward hash and each lifecycle copy
has the frozen lifecycle hash. The boundary checker separately proves base-byte
identity for schemas, `SHA256SUMS.json`, and historical result patterns.

### Real-process no-model gate

```bash
cd evidence
python3 -m unittest tests.test_parallel_eval_no_model_integration -v
```

Expected: one coordinator process, four worker PIDs, 28 sealed cases in 8/8/8/4
lanes, zero sentinel Codex invocations, no discovery writer call, and one
formal-kind temp-fixture atomic pair.

### Clean base-to-HEAD archive gate

```bash
gate_root=$(mktemp -d /private/tmp/workflow-observatory-parallel-gate.XXXXXX)
mkdir -m 700 "$gate_root/base" "$gate_root/head"
git archive 2f617fea833e583af9cae87308cfde2e620fcd82 | tar -x -C "$gate_root/base"
git archive HEAD | tar -x -C "$gate_root/head"
python3 "$gate_root/head/evidence/scripts/check_parallel_eval_frozen_boundary.py" trees --base-tree "$gate_root/base" --head-tree "$gate_root/head"
python3 -m unittest discover -s "$gate_root/head/plugins/workflow-observer/tests" -p 'test_*.py'
cd "$gate_root/head/evidence"
python3 -m unittest discover -s tests -p 'test_*.py'
```

Expected: boundary PASS, baseline 76 plugin tests/one expected skip plus new
tests, and baseline 268 evidence tests plus new tests. This clean archive gate
avoids the diagnosed linked-worktree `.git` fixture difference.

### Reproducible package and official validators

Build the same gate version in two separate clean HEAD extractions, verify both,
`cmp` the ZIP bytes, and compare SHA-256. Then run the existing plugin validator
and all four skill validators from the already provisioned validator environment.
If that environment is absent, report unavailable; do not install.

### Model-bearing rollout

1. Run one fixed parallel diagnostic for `forward/3 reviewed-refactor`; it is
   non-authoritative and cannot persist.
2. Run one all-28 parallel discovery sweep; retain sanitized progress/seals and
   prove production unchanged.
3. Obtain independent re-review of code, deterministic output, diagnostic, and
   discovery. Fix findings test-first and repeat affected gates.
4. Only after explicit approval run one protected formal epoch. It must claim
   and consume exactly one formal commit capability. Acceptance
   requires 28 unique successes, exactly one model-started attempt per case,
   valid usage, all stores/cleanup revalidated, one authority-held persistence,
   pointer re-resolution/rescore, and no unexpected production delta.

## Final self-review

- [ ] The first 14 findings remain closed; all 7 focused re-review findings and
  both final Task 4 cleanup findings map to an exact interface and RED without
  reopening the other ten.
- [ ] App-server start is conservative before request send.
- [ ] Every paired writer holds the same repository-keyed lease and capability.
- [ ] Both transports use the same resolved config hash; isolated homes contain
  only copied auth and no ambient config.
- [ ] Both transports execute the sealed canonical Codex path after an immediate
  identity/hash recheck; ambient `PATH` cannot select a binary.
- [ ] The writer lock uses the fixed validated per-UID `/var/tmp` namespace and
  contends across different temp/home environments.
- [ ] Progress is durable, sequenced, ACK-gated, bounded, and token-accounted.
- [ ] Every progress type has exact nullable attempt/case/shard/receipt hashes;
  successful cases bind all three required durable records.
- [ ] Attempt paths are collision-free; resume selects the exact canonical
  ordinal row and rejects a forged per-case digest even under the same plan.
- [ ] Attempt, case, and shard seals use the exact closed schemas, differentiated
  caps, immutable no-clobber publication, canonical-prefix failure truth, and
  commit-last readers frozen in Exact Contract 6.
- [ ] Read-only capture is never rewritten; staging is writable per case.
- [ ] Worker cleanup scrubs the retained expected Codex-home descriptor and
  leaves a tombstone; it never performs identity-racy top-level name deletion.
- [ ] Durable ownership and tombstone receipts make success, retry, crash, and
  cancellation cleanup verifiable across worker-process exit; bootstrap
  credentials use the same pattern.
- [ ] Every descriptor close is one-shot capability retirement; an ambiguous
  close never probes/retries the integer, poisons its worker/coordinator process,
  and relies on process exit plus fresh cleanup-only resume rather than risking
  an unrelated reused descriptor.
- [ ] Record temp/parent descriptors follow the same rule, exact exception leaves
  carry the recursive indeterminate-close marker, and a poisoned runtime factory
  rejects every later case before filesystem or model work.
- [ ] Only a quiescent authority-held coordinator teardown removes verified
  Codex-home tombstones; validation proves those homes and auth bootstrap absent
  while case evidence roots remain readable.
- [ ] The per-run lease is always acquired before the formal result-writer
  lease and released after it; teardown requires a live per-run lease plus the
  single-use quiescent-run authority.
- [ ] Run kind is part of epoch identity/progress/seals/validation; discovery
  cannot claim, and formal persistence consumes one unrepeatable capability.
- [ ] Aggregation/persistence require the validated-epoch/formal capabilities.
- [ ] Base-to-HEAD and clean-tree gates protect every frozen boundary.
- [ ] Allowlist and explicit frozen-byte sets are disjoint; allowed evaluator
  edits pass while nonallowlisted/frozen edits fail.
- [ ] Every referenced worker/coordinator type and state transition has an exact
  contract, and the default driver reaches captured `_run_case` plus staged
  marketplace setup with transport injection only below that boundary.
- [ ] Actual coordinator/four-worker subprocesses cover all 28 keys without a
  model before discovery.
- [ ] Each task is independently reviewable and revertible.
- [ ] No real-model formal run occurs before independent re-review.
