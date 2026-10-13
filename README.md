# CCE Step SLA Service

The time plane of the CCE system. Picks up the SLA transitions the Matcher Service scheduled — as their
deadlines pass, or as soon as work is recorded on time — and writes the resulting `OVERDUE`, `MISSED`
or `MET` verdict with its deviation. It writes `step_instance.sla_status` — it is the column's only
writer.

It matches no events, enrols no patients and manages no definitions. It **owns no tables** and runs no
migrations. It uses **no Kafka** — it neither consumes nor publishes.

> **Before you run it during an Event Replay: don't.** This service must be stopped while the Matcher
> Service still has an event backlog to process, or it will record `OVERDUE` and `MISSED` against steps
> whose completing event has not been matched yet — verdicts the service will never revise. Why:
> [Architecture — Operational prerequisite](docs/architecture-overview.md#operational-prerequisite--event-replay).
> Runbook: [Deployment Guide](docs/deployment-guide.md#event-replay--sequencing-the-two-services).

**Port** `8092` · **Java** 21 · **Spring Boot** 3.4.2 · **Version** 2.0.0

## Quick Start

This service cannot create its own schema. Bring up the Protocol and Matcher services against `ccedb`
first — in that order — then:

```bash
./gradlew build
./gradlew bootRun

curl -s localhost:8092/actuator/health
```

Starting against an unmigrated database fails at boot on `ddl-auto: validate`, by design. Full
sequence: [Developer Setup](docs/developer-setup.md#quick-start).

## Documentation

| Document | Contents |
|---|---|
| [Architecture & Design](docs/architecture-overview.md) | The fetch-and-apply cycle, the applier's behaviour table, retry and backoff, observability, scaling |
| [API Reference](docs/api-reference.md) | The operational endpoints — there is no application API |
| [Developer Setup](docs/developer-setup.md) | Prerequisites, configuration and tuning, project layout, testing, and the invariants to preserve |
| [Deployment Guide](docs/deployment-guide.md) | Docker and Kubernetes, replica scaling, alerts, troubleshooting |

System-wide context lives in **cce-common-util** and is not restated here:

| For | See |
|---|---|
| Why the services are split, and the SLA handoff contract | `cce-common-util` → [docs/architecture-overview.md](https://github.com/openphc/cce-common-util/blob/release-2.0.0/docs/architecture-overview.md) |
| Schema, columns, enums, table ownership | `cce-common-util` → [docs/data-dictionary.md](https://github.com/openphc/cce-common-util/blob/release-2.0.0/docs/data-dictionary.md) |
| The shared entities, deviation recorder and history writer this service uses | `cce-common-util` → [docs/library-reference.md](https://github.com/openphc/cce-common-util/blob/release-2.0.0/docs/library-reference.md) |
| Status vocabularies and their FHIR provenance | `cce-common-util` → [docs/fhir-conformance.md](https://github.com/openphc/cce-common-util/blob/release-2.0.0/docs/fhir-conformance.md) |

Cross-repository links go to the `openphc` repositories on GitHub, on `release-2.0.0`, so they work
on GitHub as well as locally. The Gradle composite build does assume the repositories are checked
out as siblings.

## How it works

```
@Scheduled poll ──> SlaTransitionEvaluator      drives; holds no transaction
                          │
                          ▼
                  step_sla_state_transition
                  is_processed = false, next_attempt_at <= now
                  (DUE_DATE_REACHED · MISSED_DATE_REACHED · MET_CONDITION_REACHED)
                          │
                          ▼
                  SlaTransitionApplier          one transaction per batch
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
  step_instance      step_instance       deviation
  .sla_status        _history            (breaches only)
```

One table, one loop, every verdict. A deadline row comes round when its threshold falls; the
`MET_CONDITION_REACHED` row is written by Matcher at the completion itself, with `process_by` equal to
that `completed_at`, so it is already due and the verdict lands on the next cycle instead of waiting
for a due date that could be weeks away.

The row lock **is** what reserves the row — no lease table, no heartbeat, no leader election. Every replica can
poll the same table concurrently, and a replica that dies mid-batch releases its rows immediately.

A row is fetched for one reason — its schedule came round — and what it decides is read from the step:
`completed_at` against the row's `process_by` for a breach, against the step's `due_date` for `MET`.
The wall clock never enters into it, so *when* a row is applied cannot change what it decides. Details
in [Architecture §3](docs/architecture-overview.md#3-the-fetch-and-apply-cycle).

## API

None. The only HTTP surface is actuator's health and metrics endpoints — see
[API Reference](docs/api-reference.md). Everything this service writes is driven by its scheduler, never
by a request.

## Testing

```bash
./gradlew test              # 44 tests (42 unit + 2 context-boot tests)
./gradlew build             # tests + coverage gate (0.98 instruction coverage)
./gradlew jacocoTestReport  # build/reports/jacoco/test/html/index.html
```

Concurrent-fetch behaviour depends on real `FOR UPDATE SKIP LOCKED` semantics and must be verified
against PostgreSQL, not H2.
