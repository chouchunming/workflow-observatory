# Workflow Observatory Concurrency and Operability Foundation v0.3 Design

Date: 2026-08-11
Status: Approved design direction; written specification pending review
Review mode: Architecture, then adversarial

## Section status

- Product boundary and milestone decomposition: Approved in design discussion
- Versioned schema and derived migration model: Approved in design discussion
- Same-machine writer safety and maintenance transactions: Approved in design
  discussion
- Immutable health evidence: Approved in design discussion
- Retention, export, logical delete, restore, and purge: Approved in design
  discussion
- Value-based tail sampling: Approved in design discussion
- Cross-platform verification and release claims: Approved in design discussion
- Rollout, responsibilities, and interruption checkpoints: Approved in design
  discussion
- Written specification as a whole: Pending review
- Production implementation: Not authorized by this document

This specification defines one umbrella milestone with five sequential,
independently reviewable implementation phases. The repository maintainer/user
is the approval authority. Until that person explicitly approves this written
specification, it authorizes neither phase-plan creation nor implementation.
Approval of this document authorizes implementation planning only. It does not
authorize code changes, live-store migration, release publication, merge, or
push.

## Purpose

Workflow Observatory v0.2 established privacy-minimized Episodes, canonical
adapter acquisition, deterministic Learning Snapshots, and immutable snapshot
publication. v0.3 makes that local evidence system safe and operable when
multiple cooperative processes use the same store.

The milestone closes this path:

```text
versioned artifact
    -> selected adapter and schema gate
    -> same-machine writer coordination
    -> content-hash compare-and-swap
    -> atomic or recoverable publication
    -> immutable health evidence
    -> deterministic retention, export, delete, and sampling behavior
```

The primary outcome is not a dashboard. It is a set of storage and policy
invariants that prevent silent data loss, ambiguous migrations, unbounded
waiting, misleading sampled analysis, and unverifiable cleanup.

## Milestone decomposition

The umbrella milestone contains five phases in this order:

1. Schema and migration foundation
2. Cross-platform writer safety
3. Immutable health evidence
4. Retention, export, logical delete, restore, and purge
5. Value-based tail sampling

Each phase receives its own bounded implementation plan or a clearly isolated
section of a phase plan, test-driven implementation, adversarial review,
checkpoint commit, and acceptance record. A later phase cannot compensate for
an unverified earlier phase.

Phase 1 defines the health-event schema and enums needed by later components.
Phase 2 emits typed, privacy-bounded health notifications to an injected sink
and tests their exact classification, but does not claim durable health history.
Phase 3 implements the no-recursion immutable sink, makes it the production
default, and reruns the Phase 2 integration cases against persisted events.
Intermediate phase checkpoints are review artifacts, not independently
releasable product versions.

## Existing baseline

v0.2 already provides:

- one observation record per top-level authorized run;
- schema-v1 compatibility and optional Episode v2 machine data;
- per-run POSIX advisory locking in the bundled core;
- descriptor-bound reads and exact source hashing in trust-sensitive paths;
- stable-read manifest A/B checks for Learning Snapshot publication;
- canonical JSON, content-addressed identities, no-clobber snapshot publication,
  and immutable policies;
- fake Portable and LLMWiki test stores;
- deterministic candidates that remain observational and non-authoritative.

These capabilities remain the compatibility baseline. v0.3 does not rewrite
v0.2 observations, Learning Snapshots, release archives, or historical release
evidence.

## Goals

- Give every persisted artifact an explicit, independently versioned schema.
- Define pure, auditable derived migrations without changing historical bytes.
- Prevent lost updates among cooperative writers on one machine.
- Bound lock waiting and fail closed on compare-and-swap conflicts.
- Make cross-file maintenance operations recoverable and externally atomic to
  cooperative readers and writers.
- Record observation-system failures as privacy-minimized immutable facts.
- Define local retention, deterministic export, reversible logical deletion,
  and explicitly confirmed irreversible purge.
- Reduce repeated-success storage only through opt-in, deterministic,
  value-based tail sampling.
- Implement native macOS/Linux and Windows lock backends without overstating
  unverified Windows support.

## Non-goals

- Cross-machine mutual exclusion on Dropbox, SMB, NFS, or another networked
  filesystem
- An always-on watcher, scheduler, daemon, IPC service, or background upload
- Distributed consensus, a remote lock service, or a database migration
- Automatic workflow mutation, Proposal creation, experiment execution, pull
  requests, or causal claims
- `episode-projection@3`, runtime identity capture, runtime compatibility
  policy, or heterogeneous-runtime analysis
- Full prompts, transcripts, tool arguments, tool results, credentials,
  absolute paths, or unrestricted error bodies
- Automatic retention expiry or background purge
- Real-model 20+8 evaluation execution

The real-model 20+8 epoch remains a separately authorized activity. It is not
part of v0.3 implementation or acceptance.

## System invariants

The following rules govern every phase:

1. Historical evidence bytes are immutable unless an explicitly confirmed
   purge removes a logically deleted payload.
2. A legacy artifact never acquires new meaning through an in-place edit or a
   reinterpretation of an existing schema version.
3. Artifact identity, policy identity, migration identity, and code identity
   are content-addressed where they affect a result.
4. One `run_id` represents one Episode even when multiple derived projections
   exist.
5. Cooperative readers never treat a partially applied maintenance transaction
   as authoritative.
6. A lock timeout or CAS mismatch cannot silently overwrite, auto-merge, or
   retry without a fixed bound.
7. Owner metadata is diagnostic; only the native kernel lock establishes
   ownership.
8. Health evidence does not become a recursive dependency of the operation it
   reports.
9. Sampling never removes failures, rework, rollback, rare paths, health
   evidence, or deletion and purge audit records.
10. Export is explicit and local-only. Nothing in v0.3 uploads data.
11. Discovery or targeted diagnostics are not formal acceptance evidence.
12. Unsupported filesystem or platform guarantees fail closed.

## High-level architecture

```text
CLI / skill adapter
    |
    v
Selected store semantics
    |
    +--> schema registry and pure migration policies
    |
    +--> maintenance gate
    |      +--> shared: ordinary reads and single-resource writes
    |      +--> exclusive: cross-file maintenance and recovery
    |
    +--> per-resource lock adapter
    |
    +--> stable read + expected-content-hash CAS
    |
    +--> atomic single-file publication
    |      or recoverable cross-file transaction
    |
    +--> immutable health-event recorder
    |
    +--> deterministic derived reports / exports / sampling analysis
```

The Portable and LLMWiki adapters share the same concurrency, schema, health,
retention, and sampling core. An adapter supplies only its validated storage
layout and reference semantics; it cannot weaken the core invariants.

## Artifact schema registry

### Independent version namespaces

The registry is a canonical, immutable policy artifact. Each entry binds:

```json
{
  "artifact_type": "workflow-observation",
  "schema_version": 3,
  "schema_identity": "workflow-observation@3",
  "reader_contract": "...",
  "writer_contract": "...",
  "migration_policy": "workflow-observation-v2-to-v3@1"
}
```

`artifact_type + schema_version` is the dispatch key. Versions are independent
per artifact type; the plugin release number is not an artifact schema number.

JSON artifacts store both fields directly. New Markdown artifacts also store
`artifact_type` in frontmatter and retain their existing human-facing `type`
field for Wiki and v0.2 compatibility. The registry fixes the only allowed
pairing; for example, a v3 observation uses:

```yaml
type: observation
artifact_type: workflow-observation
schema_version: 3
```

A mismatch between `type` and `artifact_type` is a schema error. Legacy
Markdown artifacts that predate `artifact_type` receive the registry's exact
derived artifact type only after their complete historical shape has been
validated.

The initial v0.3 registry covers at least:

| Artifact type | Existing meaning | v0.3 meaning |
|---|---|---|
| `workflow-observation` | implicit v1 and explicit Episode v2 | explicit v3 lifecycle and sampling envelope |
| `observation-invalidation` | legacy unversioned tombstone | explicit v2 tombstone |
| `learning-snapshot` | v0.2 snapshot schema v1 | schema v2 with sampling-aware missingness and counts |
| `health-event` | none | schema v1 immutable event |
| `sampling-decision` | none | schema v1 canonical embedded subdocument |
| `logical-delete` | none | schema v1 immutable tombstone |
| `restore-record` | none | schema v1 immutable restoration record |
| `purge-plan` | none | schema v1 expiring content-addressed plan |
| `purge-receipt` | none | schema v1 immutable audit receipt |
| `export-manifest` | none | schema v1 deterministic export inventory |
| `maintenance-transaction` | none | schema v1 private recovery manifest |

Observation schema v3 is not `episode-projection@3`. The v0.3 observation
envelope may add sampling and lifecycle fields while canonical learning
projection remains `episode-projection@2` with `runtime_provenance: null`.
Runtime provenance remains deferred.

Existing v1 and v2 readers and explicit writer modes remain available for
compatibility. v0.3 never silently upgrades an existing draft or changes a
caller's explicit schema selection. Sampling and its canonical decision
subdocument require observation schema v3; a v1 or v2 observation is retained
under its historical contract and is not retroactively sampled.

Legacy schema-v1 observations are recognized only by the exact historical
shape with no `schema_version`. Existing `schema_version: 2` retains its
approved Episode v2 meaning. An absent version never becomes a general fallback
for arbitrary records.

Unknown versions, duplicate version fields, ambiguous legacy shapes, unknown
artifact types, or an artifact whose envelope and embedded schema disagree are
Data Trust Gate failures.

### Registry closure

The registry, its referenced schemas, migration mappings, and result-affecting
code are immutable inputs to any normalized projection, export, or analysis.
Their version and SHA-256 identities enter the corresponding manifest or
artifact identity. A label such as `schema-registry@1` cannot be reused with
different bytes.

## Derived migration model

Migration is a read-only projection:

```text
immutable source bytes
    -> validate exact source schema
    -> hash source bytes
    -> apply one pure version-specific migration
    -> validate current canonical projection
    -> bind source hash + migration policy identity
```

It never edits the source artifact and never creates a second Episode sample.
The normalized result records:

- source artifact type and schema version;
- source content SHA-256;
- migration policy version and SHA-256;
- target projection contract and schema;
- availability states that distinguish absent, unsupported, not recorded,
  sampled by policy, and not applicable.

A migration is pure. It cannot read the current clock, environment variables,
Git state, the network, mutable global state, or an unbound file. Repeating it
with the same canonical inputs must produce identical canonical bytes.

No migration may:

- infer missing measurements or runtime identity;
- convert absence to zero;
- copy unrestricted human text into a machine projection;
- collapse conflicting physical records;
- change `run_id`;
- fabricate Decision Events or sampling decisions;
- hide an unsupported schema behind a best-effort parse.

## Same-machine writer safety

### Scope of guarantee

The v0.3 guarantee applies to cooperative Workflow Observatory writers on one
machine using a supported local filesystem. It includes independent Codex,
Claude, shell, and test processes when they use the product API.

The design does not promise distributed locking across machines or correctness
for a remote filesystem whose lock, hard-link, rename, durability, or directory
descriptor semantics are weaker than required. Such a store must fail closed
or be documented as unsupported.

Each platform adapter publishes a supported-filesystem capability matrix and
runs a bounded store preflight for native locking, same-directory atomic
publication, regular-file and directory durability, no-follow traversal, owner
privacy, and stable root identity. A probe is evidence for the tested
capability, not a proof about every future filesystem behavior. An unknown or
failed capability prevents write mode.

Manual edits and legacy tools can bypass every cooperative lock. Stable reads,
CAS checks, and post-write verification detect only changes visible at those
boundaries; they cannot provide atomic compare-and-swap against an
uncooperative pathname writer. v0.3 makes no lost-update guarantee for those
writers and must not describe best-effort detection as protection. A detected
external change fails closed instead of being deliberately overwritten.

### Resource identity

Every mutable resource has a canonical relative identity within the held store
root. A resource lock key is a domain-separated SHA-256 of:

```text
selected adapter semantics identity
+ canonical relative resource identity
```

Lock filenames and public diagnostics contain only the opaque key. Raw paths,
source content, task titles, and user text never appear in lock names.

Equivalent spellings, `.` and `..`, absolute paths, drive-qualified paths,
UNC paths, separators from another platform, NUL, symlink traversal, or case
ambiguity on a case-insensitive store are rejected before lock selection.

### Lock adapter

The interface has two native backends:

- POSIX advisory locking for macOS and Linux;
- native Windows file locking for Windows.

The backend exposes shared and exclusive maintenance leases, exclusive
resource locks, bounded acquisition, verification, and release. Platform
details remain behind this interface; callers cannot mix backends in one
operation.

Lock acquisition uses a monotonic deadline. Infinite waiting is prohibited.
Timeout duration is an explicit bounded policy value. Interruption, timeout,
process exit, or cancellation releases the kernel lock and closes the handle.

Recursive acquisition, lock upgrade, and arbitrary nested lock order are
prohibited. Multi-resource locks are acquired in canonical byte order and
released in reverse order.

### Owner metadata and stale files

The lock file may contain bounded local diagnostic metadata such as an opaque
operation ID, lock kind, acquired-at instant, and owner token. It is not proof
of ownership and is not exported as workflow evidence.

A crashed process releases its kernel lock even if metadata remains. A later
writer may replace stale metadata only after it successfully acquires the
native lock. It must never break or ignore a live lock because a timestamp,
PID, hostname, or owner file appears stale. Recovery records a
`stale-owner-recovered` health event.

### Single-resource write protocol

Ordinary start, finish, invalidate, delete, restore, and equivalent operations
use this order:

```text
acquire shared maintenance gate
    -> acquire exclusive resource lock
    -> securely reread current bytes from held store root
    -> validate state and expected SHA-256
    -> create complete private temporary bytes
    -> fsync temporary
    -> perform no-clobber publication or atomic replacement as allowed
    -> fsync containing directory
    -> securely reread and verify resulting identity
    -> release resource lock
    -> release maintenance gate
```

An operation that creates a resource requires an absent target. An operation
that transitions an existing resource requires the exact expected source hash.
If the state or hash changed, the operation emits `cas-conflict` health evidence
and fails. It does not overwrite, merge, or restart automatically.

### Cross-file maintenance transaction

Export preparation, purge, bulk migration indexes, and other operations that
bind multiple resources use an exclusive maintenance lease. They also acquire
their resource locks in canonical byte order.

Because a filesystem does not generally provide an atomic multi-file replace,
v0.3 uses a recoverable transaction boundary:

```text
exclusive maintenance lease
    -> stable-read all inputs
    -> validate all expected hashes and states
    -> write and fsync all staged outputs
    -> write and fsync content-addressed prepared manifest
    -> begin deterministic apply
    -> verify every post-state
    -> write immutable commit result
    -> remove private staging only after durable result
```

Private transaction state lives beneath one fixed `.transactions/` namespace.
At most one transaction may be in `prepared` or `applying` state because its
creation requires the exclusive maintenance lease. The prepared manifest and
terminal result use canonical bytes, no-clobber publication, file and directory
`fsync`, and content-addressed identities.

Before the first visible mutation, a transaction may publish an immutable
abort result and discard staging. After the first visible mutation, `abort` is
not a valid terminal state: recovery must resume the exact prepared manifest
and cannot improvise a rollback. Every staged byte, before hash, after hash,
deletion target, and operation order is bound to the manifest.

Cooperative readers and ordinary writers acquire the shared maintenance gate
and therefore cannot observe an in-progress apply. After acquiring that gate,
they check the transaction namespace before reading product state. If an
incomplete manifest exists, they release the shared gate, return the exact
transaction identity and `recovery-required`, then stop. v0.3 chooses an
explicit recovery command rather than mutating state during an ordinary read.
The explicit coordinator command acquires the exclusive lease, revalidates the
prepared manifest and staging, and resumes from the first unverified operation.

If an uncooperative writer changed a remaining target, staging is missing or
corrupt, multiple active manifests exist, or durability fails after mutation,
recovery enters a bounded `recovery-blocked` state. It does not overwrite the
conflict, publish success, or reopen shared product access. The exact blocking
reason is returned and a `maintenance-recovery-blocked` health notification is
attempted. Resolution requires an explicit operator action governed by a later
recovery plan; v0.3 never guesses. Thus partial physical bytes may exist inside
a quarantined store after a crash or external race, but no cooperative API
treats them as authoritative partial success.

Private transaction state is not an authoritative artifact. A completed purge
receipt or export manifest is authoritative only after every bound post-state
has been verified.

### Deadlock prevention

- The maintenance gate is always acquired before resource locks.
- A resource lock is never acquired by the health-event recorder.
- Multiple resource locks use one canonical global order.
- Release occurs in reverse order.
- No shared-to-exclusive upgrade is allowed.
- No operation waits indefinitely.
- Recovery uses the same exclusive-lease and lock-order rules.
- Recovery has its own bounded deadline and cannot reopen shared access until
  an exact committed or pre-mutation aborted result is durable.

## Immutable health evidence

### Source of truth

Health facts are append-only events. Counters, rates, reports, and dashboards
are deterministic derived views and are never mutable truth.

Events are stored beneath:

```text
.health/events/YYYY/MM/DD/<event-id>.json
```

The event ID is collision-resistant and the file is published with exclusive,
no-clobber creation. Health events are never sampled and are retained unless a
future, separately approved policy changes that rule.

### Event schema

A v1 event contains only bounded, privacy-minimized fields:

```json
{
  "artifact_type": "health-event",
  "schema_version": 1,
  "event_id": "health-...",
  "occurred_at": "2026-08-11T04:00:00Z",
  "event_type": "cas-conflict",
  "operation_id": "op-...",
  "run_id": null,
  "resource_kind": "observation",
  "resource_key": "sha256-...",
  "evidence": {"attempt": 1},
  "error_class": "state",
  "policy_identity": "writer-safety@1",
  "event_sha256": "..."
}
```

The initial low-cardinality event types are:

- `validation-rejected`
- `schema-mismatch`
- `duplicate-finish`
- `record-dropped`
- `payload-cleanup-failed`
- `lock-contended`
- `lock-timeout`
- `stale-owner-recovered`
- `cas-conflict`
- `maintenance-lease-timeout`
- `maintenance-recovery-blocked`
- `sampling-decision-failed`
- `export-aborted`
- `purge-aborted`

Events must not contain full exception messages, prompts, tool arguments,
credentials, absolute paths, record bodies, unrestricted filenames, hostnames,
or process IDs. `evidence` uses an event-specific exact-key schema of bounded
integers, booleans, enums, or opaque identifiers.

The event is canonical JCS JSON. `event_sha256` is the domain-separated SHA-256
of the exact event object with only `event_sha256` removed. Publication writes
a complete private temporary, fsyncs it, creates the final event path with
no-clobber semantics, fsyncs the directory, and securely rereads the bytes.
Event-ID collision uses a bounded fresh-ID retry and never overwrites.

### Failure isolation

The health recorder does not acquire the primary resource lock or maintenance
lease. It appends to a unique event path so reporting an error cannot deadlock
the failed operation. It may acquire only a dedicated health-manifest gate;
health reports acquire that gate exclusively for a bounded manifest capture,
while appends acquire it in shared mode. No health-report path acquires a
product resource lock, so there is no reverse lock dependency.

The primary operation error remains primary. A successfully recorded event ID
is supplementary diagnostic evidence. If the health recorder also fails, the
caller receives a bounded compound diagnostic. If the primary operation
already committed successfully but its health event fails, the data operation
remains committed and the caller receives `success-with-health-gap`; it must
not claim a complete health history for that operation. If an event-worthy
condition is known before product mutation, its mandatory event append occurs
first and recorder failure aborts the mutation. The recorder never rolls back
committed evidence and does not try to record an event about its own failure.

### Derived reports

A health report fixes an explicit UTC interval and briefly acquires the
dedicated health-manifest gate exclusively to capture one stable, hashed event
inventory. Appends after that capture belong to a later report even if their
`occurred_at` describes an earlier operation. This avoids starvation under
continuous appends without pretending the report includes events that were not
yet durable at capture time. Reports show at least:

- total event count;
- distinct operation count;
- counts by low-cardinality event type;
- missing or unavailable operation/run attribution;
- schema versions and policy identities represented.

The report also records its manifest-capture instant and digest. If a captured
event changes or disappears during readback, the report aborts rather than
returning a partial aggregate.

Repeated failures remain separate events. Aggregation may deduplicate only by
exact event ID, not by similar text.

## Retention, export, delete, restore, and purge

### Default retention

The default policy is:

```text
retain forever on the selected local store
no automatic expiry
no automatic deletion
no automatic export
no upload
```

The policy is explicit and versioned even when it chooses no action. v0.3 has
no scheduler or TTL worker.

### Deterministic local export

Export requires explicit artifact types, an absolute UTC interval, and an
explicit destination outside the selected store. The operation:

1. Acquires the exclusive maintenance lease.
2. Captures a stable source manifest.
3. Validates schema, hashes, references, logical deletion state, and migration
   identity.
4. Builds a deterministic inventory in canonical path order.
5. Produces a content-addressed archive and checksum with fixed metadata.
6. Publishes same-user-only output without clobbering an existing target
   (`0600` on POSIX; an equivalent owner-only ACL on Windows).
7. Rereads and verifies the published bytes.

The export format and platform-independent metadata rules are frozen in the
phase implementation plan. Tool-specific filesystem ordering, current time,
UID/GID, absolute paths, and compression randomness cannot affect archive
identity.

By default, a logically deleted payload is excluded. The archive may include
its minimal tombstone, restore chain, purge receipt, or source-hash provenance
when those audit artifacts are in scope. A normalized migrated view is labeled
as a derived representation and records its source hash and migration identity;
it never masquerades as original evidence.

Export failure produces no authoritative manifest or partial success. v0.3
does not upload or transmit the archive.

### Logical delete

Deletion is an immutable state transition, not an edit to the target. A
logical-delete tombstone contains:

- target artifact type and identity;
- exact target content hash;
- target-scoped lifecycle sequence number;
- predecessor lifecycle record identity and hash, or explicit null for the
  first transition;
- bounded reason code;
- requested-at UTC instant;
- deletion policy identity;
- tombstone identity and digest.

Normal query, learning, sampling, and export exclude an effectively deleted
payload. Audit reads may return its bounded metadata and deletion chain but do
not bypass ordinary content access rules.

### Restore

Restore creates a new immutable record that references one exact deletion
tombstone and target hash. It does not remove or edit the tombstone. Effective
state is derived from the validated ordered delete/restore chain.

The target's lifecycle resource lock serializes chain creation. The first
delete uses sequence 1 with a null predecessor. Every later restore or delete
uses the prior effective lifecycle record's identity and hash and increments
the sequence by exactly one. Gaps, forks, repeats, and a predecessor that is
not the current effective tip fail CAS and do not create a transition.

Conflicting branches, duplicate sequence identities, missing targets, hash
mismatches, or an unknown chain schema fail closed. A later deletion creates a
new tombstone that continues the chain.

### Irreversible purge

Purge is a two-stage operation:

```text
purge plan
    -> user reviews exact identities and hashes
    -> second explicit apply using purge-plan identity
    -> exclusive maintenance transaction
    -> immutable purge receipt
```

The content-addressed plan includes:

- creation and expiry instants;
- exact store identity and schema registry identity;
- every target identity, current hash, and effective deletion tombstone;
- required operation order;
- purge policy identity.

Planning and applying are separate CLI invocations; one command cannot create
and immediately apply its own plan. Apply requires both the unexpired plan
identity and an explicit irreversible-action confirmation that repeats the
same identity. A noninteractive caller must provide both values explicitly;
an interactive prompt cannot be assumed to grant consent to automation.

The plan is bound to the selected store-owner identity using the native local
owner primitive (POSIX user identity or Windows owner SID) without exporting
that raw identity. Apply must run as the same validated store owner. The plan
is one-shot: an existing commit receipt or applied-plan marker rejects replay.
The local UTC clock must be at or after `created_at` and at or before
`expires_at`; a clock that moves before `created_at` fails closed. v0.3 does not
claim resistance to a malicious administrator changing the host clock.

Apply then reacquires the store, lease, and resource locks and revalidates every
bound state. Any drift aborts the whole plan before mutation.

Purge rejects:

- active or restored data;
- drafts;
- data without a valid logical-delete tombstone;
- a target held by an incompatible active operation;
- a changed target, store, schema registry, or deletion chain;
- an expired, reused, or unknown plan.

Once deletion begins, the recoverable maintenance transaction rolls forward
to the exact prepared post-state. The caller receives success only after all
targets are absent and the immutable purge receipt is durable. The receipt
keeps identity, prior hash, time, policy, plan, and outcome—not payload content.

Purge is the only v0.3 operation allowed to remove historical payload bytes.
It cannot remove frozen release evidence shipped in the repository.

Logical deletion is a product-level visibility rule, not filesystem access
control. Neither logical delete nor purge can retract bytes already exported,
backed up, synchronized to another machine, or retained by the underlying
storage system.

## Value-based tail sampling

### Default and policy boundary

Sampling is disabled by default. Enabling it requires an explicit, immutable,
versioned policy. No task type, workflow variant, or generation is sampled
unless its low-cardinality cohort is explicitly allowlisted.

Unallowlisted means rare for v0.3. Rarity is not inferred by an LLM, recent
frequency, wall-clock state, or an unversioned threshold.

“Repeated success” is a policy-curation criterion, not a runtime inference.
Before a cohort is allowlisted, a maintainer must review prior evidence that it
is a sufficiently repetitive success path. The runtime enforces only the exact
immutable allowlist and does not recalculate rarity from current traffic.

### Forced-retain rules

The complete observation is retained when any of these applies:

- status is anything other than success, including failed, partial,
  rolled-back, or superseded;
- `rework_count > 0`;
- defects, test failures, timeouts, cleanup failures, or CAS conflicts exist;
- the user or caller explicitly requests retention;
- the cohort is not explicitly allowlisted;
- task type, workflow variant, workflow generation, or schema is new or
  unavailable;
- the policy marks the path rare;
- sampling policy parsing, decision, or pre-publication payload cleanup fails.

Health events, logical-delete records, restore records, purge plans, purge
receipts, export manifests, and invalidations are never sampled.

### Deterministic decision

For an eligible successful observation, the decision is derived from:

```text
SHA-256(
    domain separator
    + sampling policy identity
    + run_id
    + canonical low-cardinality cohort key
)
```

The policy defines the deterministic threshold or modulus. The result is
independent of processing order, machine, current time, or concurrent load.
Changing the policy produces a different policy identity and an auditable new
decision generation.

### Tail boundary and record shape

The decision occurs only after outcome and forced-retain facts are known, but
before final record publication. Every v0.3 final observation contains one
canonical `sampling-decision@1` subdocument, including decisions to retain:

```json
{
  "artifact_type": "sampling-decision",
  "schema_version": 1,
  "policy_identity": "sampling-policy@1+sha256:...",
  "cohort_key": {"task_type": "...", "workflow_variant": "...", "workflow_generation": "..."},
  "decision": "retain-full",
  "reason": "policy-disabled",
  "decision_hash": "..."
}
```

Allowed decisions are `retain-full` and `retain-minimal`. Reasons are bounded,
versioned enums.

A sampled-out success is not dropped. It becomes a minimal terminal
observation containing:

- run identity and privacy-safe taxonomy;
- start and finish timestamps;
- success status;
- schema and projection identities;
- sampling policy, cohort, decision, reason, and decision hash;
- no human narrative and no unselected execution or quality metrics.

Any Markdown fields required for storage compatibility use deterministic
generated values: the title is derived only from `run_id`, `sources` is empty,
and the minimal record omits `task_ref` and the original human title. Sampling
therefore cannot preserve hidden free text through a nominally minimal record.

The sampling decision is embedded in the same atomically finalized
observation. A second sidecar transaction is prohibited.

### Temporary full payload cleanup

The producer should hold the full completion payload in memory. If a temporary
file is unavoidable, it must be private, bounded, same-user only, and outside
the authoritative store namespace.

For `retain-minimal`, cleanup must succeed before minimal publication. If
cleanup cannot be verified, the system forces `retain-full`, emits
`payload-cleanup-failed`, and does not claim that sensitive bytes were erased.
Unlinking cannot guarantee physical erasure from SSDs, snapshots, journals, or
backups; v0.3 guarantees removal of the plugin-managed live temporary path, not
forensic media sanitization.

If authoritative final-record publication itself fails, finalization fails and
the prior draft remains authoritative. The system must not report either a
minimal or a full retained final record unless that record was durably written
and verified.

### Learning semantics

Minimal successes remain in the outcome denominator. Metrics omitted because
of sampling use `sampled_by_policy`; they are not `not_recorded`, zero,
unsupported, or not applicable.

Every Learning Snapshot that includes sampled Episodes discloses:

- sampling policy identities;
- full retained outcome count;
- sampled-minimal outcome count;
- per-metric `sampled_by_policy_n`;
- cohorts excluded from comparison because policies are incompatible.

Sampling policies are comparison-compatible only when their full
content-addressed identities are equal, unless a later explicit compatibility
policy says otherwise. Mixed identities remain in descriptive outcome counts
but do not share metric aggregates or recurring-pattern inference.

v0.3 does not perform inverse-probability weighting, population estimation, or
causal adjustment. Numeric and quality aggregates use only eligible observed
values and state their exact denominators.

## Platform contract

### macOS and Linux

macOS and Linux are runtime acceptance platforms. Native-process tests must
exercise contention, timeout, crash release, stale metadata recovery, lock
ordering, CAS conflicts, stable reads, transaction recovery, and no-clobber
publication on both platforms before an rc is called verified for them.

### Windows

Windows is a first-class implementation target, not a stub or POSIX emulation.
The backend must implement the same public lock and timeout contract using
native Windows primitives.

The current project does not have an available Windows runtime. Therefore:

- backend-independent behavior and injected native-adapter contracts are tested
  on available platforms;
- Windows-only integration tests are marked `requires Windows` and skip
  elsewhere;
- the skip is reported, not converted into a pass;
- Windows runtime tests remain a roadmap gate.

Before the backend exists, documentation must say it is planned or under
implementation. After the backend is implemented but before a real Windows run
passes, release documentation must use exactly this support level:

```text
Windows backend implemented.
Windows runtime verification pending.
Windows support not yet certified.
```

Lack of Windows hardware does not block v0.3 rc development, but it blocks a
claim of full Windows support.

## Privacy and security boundary

- Content capture remains off by default.
- Lock keys, operation IDs, event IDs, and store identities are opaque.
- Metric labels and report dimensions are fixed, low-cardinality enums.
- Prompts, bodies, absolute paths, credentials, raw tool input/output, full
  exceptions, and unrestricted user text are prohibited from health and
  sampling policy artifacts.
- Export never includes local configuration, keys, live temporary files, lock
  owner metadata, caches, or transaction staging.
- All JSON ingress uses strict duplicate-key, finite-number, valid-Unicode
  parsing and canonical serialization before hashing.
- Trust-sensitive reads are bounded, descriptor-bound, no-follow, and tied to
  one held store-root identity.
- Private directories and files use the strictest supported same-user modes;
  permissive or unsupported storage fails closed.

## Failure behavior

| Failure | Required outcome |
|---|---|
| Unknown or ambiguous schema | Stop; emit `schema-mismatch`; no derived artifact |
| Invalid record or policy | Stop; emit `validation-rejected`; no partial output |
| Resource lock contention within deadline | Continue after acquisition; optionally record bounded contention |
| Resource lock timeout | Stop; emit `lock-timeout`; no write |
| Maintenance lease timeout | Stop; emit `maintenance-lease-timeout`; no transaction |
| CAS mismatch | Stop; emit `cas-conflict`; never overwrite or auto-merge |
| Stale owner metadata after valid lock acquisition | Replace metadata; emit `stale-owner-recovered` |
| Crash before first cross-file mutation | Recover as aborted; no authoritative result |
| Crash after first cross-file mutation | Exclusive recovery rolls forward exact prepared manifest |
| Recovery conflict, corrupt staging, or durability failure | Quarantine store as `recovery-blocked`; no shared product access or overwrite |
| Health recorder failure | Preserve primary error and add bounded recorder diagnostic; no recursion |
| Sampling decision or cleanup failure | Retain full record and emit health evidence |
| Export drift | Abort; no authoritative export manifest |
| Purge drift or expired plan | Abort before mutation; emit `purge-aborted` |
| Unsupported filesystem/platform guarantee | Fail closed with explicit capability error |

## Testing strategy

### Isolation

All development and acceptance tests use temporary fake Portable or fake
LLMWiki roots. No test reads, locks, migrates, samples, exports, deletes, or
purges the user's live store.

Frozen fixtures contain no author-specific paths, credentials, or private
observation content. Platform fixtures produce the same canonical bytes and
hashes when the contract says their semantics are equivalent.

### Test layers

1. Schema and migration unit tests
   - known versions, ambiguous legacy shapes, unknown versions;
   - pure and idempotent migrations;
   - absent values remain distinct from zero;
   - one `run_id` remains one Episode;
   - source, policy, and code identity closure.
2. Real-process writer tests
   - simultaneous start/finish/invalidate/delete operations;
   - bounded contention and timeout;
   - crash release and stale metadata;
   - canonical lock ordering and deadlock probes;
   - CAS conflict and no lost update.
3. Cross-file maintenance tests
   - prepared manifest validation;
   - crash before and after first mutation;
   - deterministic roll-forward;
   - no cooperative reader observes partial state;
   - no partial success is reported.
4. Health evidence tests
   - every initial event type;
   - recorder failure without recursion;
   - deterministic aggregation, distinct-operation counts, and stable-read
     aborts.
5. Retention/export/delete tests
   - forever-local default;
   - deterministic archive reproduction and no-clobber publication;
   - delete/restore chains;
   - purge plan expiry, second confirmation, drift abort, crash recovery, and
     permanent receipt.
6. Sampling tests
   - default off;
   - every forced-retain path;
   - allowlist-only deterministic selection;
   - cleanup failure forces full retention;
   - minimal observation privacy;
   - learning denominator and `sampled_by_policy` behavior.
7. Adapter and package tests
   - Portable/LLMWiki semantic parity;
   - macOS/Linux runtime matrix;
   - injected Windows backend contract and honest native-test skip;
   - packaged schema, policies, tests, approved design, plans, and roadmap.

### Evaluation gates

The deterministic v0.3 suite is expected to remain cheap and fast. Therefore:

> Skip the separate discovery sweep because the complete suite is cheap and
> fast.

Development uses ordinary RED/GREEN cycles and targeted diagnostics. Formal
acceptance is one clean execution over one fixed implementation, frozen
fixtures, frozen policies, frozen expected results, and the complete available
platform matrix.

Formal rules:

- a case failure fails the formal run;
- an integrity, isolation, cleanup, or provenance failure aborts it;
- partial results from different implementation generations cannot be combined;
- rerunning only failed cases is diagnostic, not acceptance;
- exact commands, interpreter versions, skips, source commit, and policy hashes
  are retained in the acceptance artifact;
- Windows skips remain visible and prevent a full-Windows-support claim.

The umbrella acceptance gate proves at minimum:

- schema and migration parity;
- within the frozen cooperative-writer schedule matrix on supported local
  filesystems, no observed lost update, deadlock, or silent CAS overwrite;
- within the frozen crash/fault-injection matrix, no cooperative API accepts an
  authoritative partial multi-file state;
- valid immutable health aggregation;
- deterministic export and valid delete/restore/purge gates;
- deterministic sampling and honest learning denominators;
- complete marketplace packaging;
- no live-store access;
- no real-model evaluation.

The phase plan freezes worker counts, operation interleavings, deadlines,
filesystem capabilities, and crash injection points. Passing this finite
matrix supports the bounded contract; it is not a universal mathematical proof
that no race or deadlock can exist outside the declared model.

## Rollout and responsibilities

### Durable development branch

Development begins from current `origin/main` in a linked worktree whose Git
object database remains in the durable canonical clone. Each independently
reviewable phase is committed after scoped verification.

Task acceptance, review completion, session pause, context compaction, machine
transfer, and worktree cleanup are interruption boundaries. Before crossing
one, the work must be reachable from the durable clone and either an authorized
remote WIP branch or a verified Git bundle on durable storage.

### Roles

- Schema steward: owns registry, migrations, canonical fixtures, and backward
  compatibility.
- Platform/concurrency owner: owns lock adapters, deadlines, CAS, maintenance
  lease, transaction recovery, and platform claims.
- Phase feature owner: owns health, lifecycle, export, or sampling behavior for
  the current bounded phase.
- Adversarial tester: challenges crash, race, symlink, TOCTOU, privacy,
  determinism, and ambiguous-schema boundaries.
- Integrator: verifies phase ancestry, complete suite, package inventory,
  documentation claims, and checkpoint durability.

A reviewer cannot replace green tests with source inspection, and green tests
cannot replace schema, security, or concurrency review.

### Phase gates

Each phase follows:

```text
approved phase plan
    -> RED tests
    -> implementation
    -> targeted verification
    -> full current regression suite
    -> adversarial review
    -> focused fixes and re-verification
    -> checkpoint commit
    -> next-phase authorization
```

macOS/Linux acceptance plus the injected Windows contract may support an rc
candidate. Release, public prerelease creation, merge, or push still requires
separate explicit authorization. Windows remains implemented but uncertified
until a real Windows runner passes.

## Packaging and documentation

The eventual v0.3 source archive includes:

- this approved design and every superseding phase plan;
- schema registry, schemas, migration policies, retention policy, sampling
  policy, health enums, and code artifact manifests;
- unit, concurrency, security, adapter, recovery, and acceptance tests;
- platform support statement and visible Windows verification status;
- README, ROADMAP, TODO, release notes, and deferred work;
- deterministic source inventory and SHA-256 checksums.

The current packager intentionally allowlists the exact approved v0.2 design
and plan paths. While this v0.3 document is a draft, package tests are expected
to reject it as an unknown marketplace file. After written-spec approval, the
first implementation phase must add the exact approved v0.3 document path and
test its inventory; a broad `docs/superpowers/**` wildcard is prohibited.

It excludes:

- live observations and health events;
- local configuration, credentials, caches, lock metadata, staging files, and
  temporary payloads;
- author-specific absolute paths;
- frozen historical release evidence rewritten as if it were current source.

README and ROADMAP must distinguish implemented behavior, runtime-verified
behavior, and deferred work. `Windows backend implemented` cannot be shortened
to `Windows supported` before native verification.

## Rejected alternatives

### SQLite as the v0.3 store

Rejected because it would combine concurrency hardening with a storage-engine
migration, reduce direct human inspection, and broaden adapter, packaging, and
migration risk. The file store remains canonical.

### Always-on coordination daemon

Rejected because it introduces lifecycle, IPC, startup, authentication, and
background-failure concerns that are unnecessary for same-machine cooperative
writers. Native file locks and recoverable manifests are sufficient.

### Timestamp-based stale-lock breaking

Rejected because metadata age cannot prove a kernel lock is dead. The process
must first acquire the native lock.

### Best-effort CAS retry or automatic merge

Rejected because retries can hide conflicts and change the evidence being
finalized. Conflicts are explicit failures requiring a new authorized attempt.

### Mutable health counters

Rejected because concurrent increments create another shared-write authority
and lose event provenance. Counters are derived from immutable events.

### Sampling by a global percentage

Rejected because uniform sampling disproportionately removes rare failures and
decision-changing paths. Only allowlisted repeated successes are eligible.

### Automatic purge after retention expiry

Rejected because v0.3 has no scheduler and irreversible deletion requires a
second explicit confirmation bound to an exact plan.

## Known limitations and deferred work

- Cross-machine synchronization has no correctness guarantee. External changes
  may be detected at stable-read, CAS, or verification boundaries, but v0.3
  specifies no comprehensive detection mechanism or distributed exclusion.
- Media-level secure erase cannot be guaranteed on SSDs, snapshots, journals,
  or backups.
- Windows runtime behavior is not certified until a native runner executes the
  full applicable matrix.
- No background retention, export, purge, watcher, or health collector runs.
- Runtime provenance and heterogeneous-runtime analysis await
  `episode-projection@3`.
- Human narrative annotations, post-hoc evaluator artifacts, improvement
  experiments, trace-context propagation, OpenTelemetry export, and automated
  workflow evolution remain separate designs.

## Acceptance of this design

Written-spec approval freezes the architecture and permits creation of the
five bounded phase implementation plans. It does not itself authorize Task 1
code execution.

Any later change that weakens evidence immutability, introduces background or
network behavior, changes purge confirmation, expands sampling eligibility,
claims distributed locking, redefines a historical schema, or claims verified
Windows support requires renewed design approval.
