# ADR 0018: Fuzz testing for job payload handling and serialization

## Status
Accepted

## Context
Job.Payload accepts arbitrary caller-supplied json.RawMessage, and
every Redis round-trip depends on Job marshaling/unmarshaling
correctly. Table-driven unit tests cover known cases well but can't
systematically explore the space of malformed or unusual input the
way property-based fuzzing can.

## Decision
Add Go-native fuzz tests: FuzzNew_NeverPanicsOnPayload (job.New must
never panic and must always produce a marshalable Job, regardless of
Payload content) and FuzzJobMarshalUnmarshalRoundTrip (a successfully
marshaled Job must always unmarshal back with matching ID/Type).

## Consequences
- Any fuzzer-discovered failure is automatically persisted to
  testdata/fuzz/ and becomes a permanent regression test on every
  future `go test` run, not just during active fuzzing.
- Fuzz tests deliberately assert weaker properties than "input must be
  valid JSON" — Payload validation is a job handler's responsibility,
  not the queue infrastructure's; the fuzz tests check structural
  invariants (non-panic, round-trip fidelity) that must hold
  regardless of payload validity.
- Fuzzing runs on-demand (`make fuzz-job`/`fuzz-queue`), not in CI on
  every push — Go's fuzzer is designed for extended, ideally
  long-running local exploration rather than a bounded CI time budget;
  a scheduled (e.g. nightly) CI fuzz job would be a reasonable future
  addition if this becomes valuable enough to run unattended.
