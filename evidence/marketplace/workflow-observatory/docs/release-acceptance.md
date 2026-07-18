# 0.1.0 Release Acceptance

## Evaluation boundary

Task 6 was accepted on July 19, 2026 under an explicit composite boundary:

- 26 consecutive protected formal cases passed;
- targeted `complete-eval-override` passed after a fixture-isolation repair;
- targeted `incomplete-eval-override` passed without that repair changing its
  frozen expectation;
- both frozen manifests retained their approved bytes and SHA-256 digests;
- deterministic tests and independent review passed after the repair.

This evidence is accepted as coverage of all 20 forward and 8 lifecycle cases
for this release. It is not a single uninterrupted formal run, did not create an
authoritative atomic result pair, and must never be represented as one. A future
formal evaluator still uses the stricter all-green aggregate publication gate.

## Deterministic and clean-room evidence

The release archive contains the installable marketplace, full plugin-local
test suite, repository evaluator and integration tests, frozen manifests,
approved specifications and implementation plans, the historical Task 6
report, the public backlog, and the parallel-evaluation plan.

`SHA256SUMS.json` is the machine-readable completeness boundary. It maps every
marketplace file and repository evidence source to the packaged member, source
digest, packaged digest, and any deterministic path normalization. Archive
verification rejects missing, duplicate, unsafe, tampered, or unexpected
members.

Final release validation must be run from a clean extraction and includes:

1. archive inventory and SHA-256 verification;
2. marketplace/plugin manifest validation;
3. official validation of all four skills;
4. complete plugin-local tests, including packaging and frozen manifests;
5. relevant packaged repository tests;
6. a second deterministic build whose bytes match the first build.

The pre-publication clean-room gate completed with 76 plugin-local tests
(75 pass and one expected development-source skip), 268 packaged repository
tests passing, the plugin validator passing, and all four skill validators
passing. The extracted package also rebuilt a fresh archive and verified that
archive against its generated inventory. Two independent source builds of the
same release inputs were byte-identical.

Model-bearing evaluation is not required for installation or archive
verification. The original model evidence and every remediation remain in the
packaged Task 6 report.
