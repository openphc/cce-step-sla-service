# Developer Setup — Step SLA Service

## Prerequisites

| Requirement | Notes |
|---|---|
| JDK 21 | Gradle toolchain |
| PostgreSQL 16 | shared `ccedb` — **must already contain the schema** (see below) |
| `cce-common-util` | checked out as a sibling directory — wired in as a composite build |

This service **owns no tables and runs no migrations**, so it cannot bring up its own schema. Start
the Protocol Service and then the Matcher Service against an empty `ccedb` first; both apply their
migrations on startup. Starting this service against a schema-less database fails at boot on
`ddl-auto: validate`, which is the intended behaviour.

## Quick start

```bash
# 1. Shared infrastructure
cd ../cce-collector-service && docker compose up -d postgres kafka   # Kafka is for the Matcher Service

# 2. Schema — from the two services that own it, in this order
cd ../cce-protocol-service && ./gradlew bootRun   # creates 4 tables
cd ../cce-matcher-service  && ./gradlew bootRun   # creates 9 tables

# 3. This service
./gradlew build && ./gradlew bootRun

# 4. Verify
curl -s localhost:8092/actuator/health
```

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `SERVER_PORT` | `8092` | |
| `DB_HOST` / `DB_PORT` | `localhost` / `5432` | `5433` for the collector's shared instance |
| `DB_NAME` | `ccedb` | |
| `DB_USERNAME` / `DB_PASSWORD` | `cce_user` / `cce_pass` | needs **no** DDL rights |
| `DB_POOL_SIZE` / `DB_POOL_MIN_IDLE` | `3` / `1` | one connection does the work; see [Architecture §7](architecture-overview.md#7-scaling) |
| `CCE_SLA_POLL_INTERVAL_MS` | `5000` (`15000` under the `prod` profile) | how often to look for due transitions |
| `CCE_SLA_BATCH_SIZE` | `100` | rows fetched per transaction |
| `CCE_SLA_INSTANCE_ID` | `$HOSTNAME` | recorded in `processed_by` |
| `CCE_SLA_MAX_BACKOFF_SECONDS` | `3600` | cap on the `2^attempts` retry backoff |

### Tuning the sweep

`poll-interval-ms` is the steady-state cost, not the drain rate — a backlog is cleared within a single
cycle because batches are drained until one comes back short
([Architecture §3](architecture-overview.md#3-the-fetch-and-apply-cycle)). Lowering it shortens the *detection*
delay for a newly-due transition; it does not make a backlog clear faster.

`batch-size` trades transaction length against round trips. Larger batches hold row locks longer, which
only matters when the Matcher Service is inserting into `step_sla_state_transition` heavily at the same
time.

## Project layout

```
org.openphc.cce.sla
├── service/SlaTransitionEvaluator   @Scheduled driver — polls, loops, holds no transaction
├── service/SlaTransitionApplier     the @Transactional boundary — fetch and apply
├── domain/repository/SlaTransitionFetchRepository   the SKIP LOCKED fetch — every verdict comes through it
└── config/ObservabilityConfig
```

Entities, repositories, `DeviationRecorder` and `StateTransitionHistoryWriter` come from
`cce-common-util`. What this service adds is the fetch queries, the transaction boundary and the
scheduler.

It does not take everything that library contributes. The application class filters common-util's
`intelligence` and `kafka` packages out of component scanning and excludes Kafka auto-configuration:
those beans would otherwise be created regardless, and `IntelligenceActionEvaluator` brings a Kafka
producer with it. `ApplicationContextTest` fails if either exclusion is lost.

`build.gradle` reflects that: it declares no FHIR, JSONLogic or Flyway dependency of its own. The FHIR
layer arrives transitively through `cce-common-util`, and Flyway would be dead weight in a service that
owns no tables. Redeclaring any of them here would only be a second version to keep in step.

The driver/applier split is not stylistic — see
[Architecture §3](architecture-overview.md#why-a-driver-and-an-applier). If you merge them, the
`@Transactional` annotation silently stops taking effect.

## Testing

```bash
./gradlew test              # 44 tests — 42 unit plus two context-boot tests
./gradlew build             # tests + coverage gate
./gradlew jacocoTestReport
```

The coverage gate is **0.98** instruction coverage, excluding `StepSlaServiceApplication`.

`ApplicationContextTest` boots the real context on H2 with Flyway disabled and the poll interval widened so the sweep does not repeat. It is
the only test that exercises the wiring: everything else constructs its subject directly, which leaves a
bean this service needs at runtime but never names in source invisible behind the coverage figure. It
also validates the fetch queries — Spring Data parses every `@Query` at bootstrap, so a typo in the JPQL
fails there rather than on the first poll in production. That matters more here than in the sibling
services, because this one runs `ddl-auto: validate` against a schema it does not own: a mapping it gets
wrong is a failure to start.

There is no integration-test source set. The behaviour that would justify one — concurrent fetches
across replicas — cannot be reproduced against H2, because `FOR UPDATE SKIP LOCKED` semantics are the
thing under test. Verify that against real PostgreSQL.

## Working on the applier

Four invariants to preserve:

1. **Never write `step_status`.** It belongs to the Matcher Service. Column ownership is what keeps the
   two services from overwriting each other —
   [Architecture Overview §4](../../cce-common-util/docs/architecture-overview.md#4-step-status-and-sla-status).
2. **Judge against `completed_at`, never the wall clock.** The row was fetched because its deadline
   passed; the only remaining question is whether the work had happened by then, and the clinical
   occurrence time is the evidence for that.
3. **Write `MET` only from a `MET_CONDITION_REACHED` row, and only over a null.** A deadline row never
   writes it: keeping a threshold is not being on time — a step completed between its two thresholds
   stays `OVERDUE`. `MET` is confirmed as `completed_at <= due_date` on the step itself, not taken on
   the row's word, and `writeSlaStatus` refuses it over any existing judgement. The same forward-only
   rule keeps a retry applying rows out of order from walking `MISSED` back to `OVERDUE`.
4. **Judge mandatory steps only.** Only a step the protocol required has a deadline — to breach or to
   beat — so `applyRow` consumes any row whose step is not `must` before it looks at the row's type,
   writing neither status nor deviation, `MET` included. Matcher no longer schedules such a
   step at all and the Protocol Service rejects a protocol that tries to give one a deadline; the check
   here is what covers the rows written before those rules. Use `RequiredBehavior.isMandatory` rather
   than comparing the string, so this service and the matcher cannot drift on what "optional" means.

All four are asserted by the existing tests; a change that breaks any of them will fail rather than
silently corrupt a step.

When adding a case to the behaviour table, add it to
[Architecture §4](architecture-overview.md#4-what-the-applier-does) as well — that table is the spec,
and a case that exists in code but not there is undiscoverable.
