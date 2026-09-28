# Deployment Guide — Step SLA Service

Deploy **last**. This service creates no tables and validates its JPA mapping at startup, so it will
fail fast against a `ccedb` the other two services have not yet migrated. Ordering rationale:
[Architecture Overview §6](../../cce-common-util/docs/architecture-overview.md#6-deployment-order).

> **Upgrading to the `MET_CONDITION_REACHED` release: this service goes up before the new Matcher.**
> The Matcher's `V3` migration admits a third `transition_type` and the Matcher then starts writing it.
> A Step SLA Service from before this release cannot map that value — its fetch throws on the unknown
> enum constant, the whole batch rolls back, and every batch holding such a row backs off. So:
>
> 1. Roll this service to the new version first. It stops sweeping `step_instance` for `MET`, so
>    on-time completions sit at a null `sla_status` for the length of the gap — nothing is lost.
> 2. Roll the Matcher. `V3` runs at its startup and seeds a `MET_CONDITION_REACHED` row for every step
>    the sweep had not settled, so the gap drains on the next few cycles. `V4` follows it and deletes
>    every row belonging to an optional step — expect `cce.sla.transitions.due` to drop, and expect the
>    drop to be large on a database carrying protocols that gave optional actions a `tolerance-days`.
>
> Reverse the order and the old service jams on rows it cannot read. There is no version in which both
> write `MET`, so there is no double-write to worry about either way.
>
> The same applies to **cce-compliance-service**, the 1.x service this one replaces: it maps the same
> two transition types and would jam identically. It must be stopped before the new Matcher starts
> writing `MET_CONDITION_REACHED` rows — which it should be regardless, since both services writing
> `sla_status` is exactly what the 2.0.0 split ended.

## Requirements

| Component | Requirement |
|---|---|
| JRE | 21 |
| PostgreSQL | 16, database `ccedb` — schema already applied by the Protocol and Matcher services |
| Memory | 1 GB heap is comfortable |

The database user needs **no DDL rights**. If it has them, that is a wider grant than this service
requires.

## Environment variables

Full list with defaults: [Developer Setup](developer-setup.md#configuration). The ones that matter in
production:

| Variable | Default | Notes |
|---|---|---|
| `DB_HOST` / `DB_PORT` | `localhost` / `5432` | |
| `DB_USERNAME` / `DB_PASSWORD` | `cce_user` / `cce_pass` | never leave at the default |
| `DB_POOL_SIZE` | `3` | per replica; size the database's connection limit as replicas × this |
| `CCE_SLA_INSTANCE_ID` | `$HOSTNAME` | **set this per replica** — it lands in `processed_by` |
| `SPRING_PROFILES_ACTIVE` | — | **set to `prod`** — raises the poll interval to 15s |
| `CCE_SLA_POLL_INTERVAL_MS` | `5000`, `15000` under `prod` | overrides either |
| `CCE_SLA_BATCH_SIZE` | `100` | |

`CCE_SLA_INSTANCE_ID` defaults to `$HOSTNAME`, which is already distinct per pod in Kubernetes. Set it
explicitly anywhere hostnames are not unique, or `processed_by` stops being able to identify a
misbehaving replica.

## Docker

**Build from the workspace directory, not from this repository.** This service depends on
`cce-common-util` as a Gradle composite build, and Docker's `COPY` cannot reach outside its build
context:

```bash
cd ..            # the directory containing cce-step-sla-service and cce-common-util
docker build -f cce-step-sla-service/Dockerfile -t cce-step-sla-service:2.0.0 .
```

```bash
docker run -d --name cce-step-sla-service \
  -p 8092:8080 \
  -e DB_HOST=postgres-host -e DB_PORT=5433 \
  -e DB_USERNAME=cce_user -e DB_PASSWORD='<secret>' \
  -e CCE_SLA_INSTANCE_ID=step-sla-1 \
  cce-step-sla-service:2.0.0
```

The image pins `SERVER_PORT=8080` to match its `EXPOSE` and healthcheck; the application's own default
outside Docker is `8092`.

## Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cce-step-sla-service
spec:
  replicas: 2
  selector:
    matchLabels: { app: cce-step-sla-service }
  template:
    metadata:
      labels: { app: cce-step-sla-service }
    spec:
      containers:
        - name: cce-step-sla-service
          image: cce-step-sla-service:2.0.0
          ports: [{ containerPort: 8080 }]
          env:
            - { name: SPRING_PROFILES_ACTIVE, value: prod }
            - { name: SERVER_PORT, value: "8080" }
            - name: CCE_SLA_INSTANCE_ID
              valueFrom: { fieldRef: { fieldPath: metadata.name } }
            - name: DB_HOST
              value: postgres.cce.svc.cluster.local
            - { name: DB_USERNAME, valueFrom: { secretKeyRef: { name: cce-db, key: username } } }
            - { name: DB_PASSWORD, valueFrom: { secretKeyRef: { name: cce-db, key: password } } }
          readinessProbe:
            httpGet: { path: /actuator/health/readiness, port: 8080 }
            initialDelaySeconds: 20
          livenessProbe:
            httpGet: { path: /actuator/health/liveness, port: 8080 }
            initialDelaySeconds: 40
          resources:
            requests: { memory: 1Gi, cpu: 500m }
            limits:   { memory: 2Gi, cpu: "2" }
```

**Multiple replicas are safe and useful.** The fetch-and-apply cycle needs no coordination — no leader
election, no lease, no partition assignment — so an added replica adds throughput directly. This
is unlike the Matcher Service, whose parallelism is bounded by Kafka partitions.

Scale on the `cce.sla.transitions.due` gauge rather than on CPU: this service is
database-bound, and a backlog is visible in that gauge long before it shows up as CPU pressure.

## Kafka

None. This service neither consumes nor produces: no topic, no consumer group, no DLQ. If you find a
consumer group named after this service on the broker, it is a leftover from the pre-split monolith and
can be deleted.

## Event Replay — sequencing the two services

**Event Replay** is any run where the Matcher Service has a backlog of events on `cce.events.inbound`
to work through: events re-published after a fix, a historical backfill during migration, or a long
outage that left its consumer group far behind.

**This service must be stopped for the duration.** Running it against an unmatched backlog makes it
record `OVERDUE` and `MISSED` against steps whose completing event has not been processed yet — and
the service never revises those verdicts. The mechanism, and why
stopping costs nothing, is in
[Architecture — Operational prerequisite](architecture-overview.md#operational-prerequisite--event-replay).

### Procedure

**1. Stop this service.**

```bash
kubectl scale deployment/cce-step-sla-service --replicas=0
```

There is no in-process pause switch — the `@Scheduled` poll has no guard — so scaling to zero (or
stopping the container) is the only way to hold the sweep.

**2. Let the Matcher Service drain.** Watch its consumer group until every partition reads `LAG 0`:

```bash
kafka-consumer-groups.sh --bootstrap-server "$BROKER" \
  --group cce-matcher-service --describe
```

The group commits offsets only after a record has been processed (`enable-auto-commit: false`), so
`LAG 0` means the database writes for those events are committed — not merely that the records were
read. Wait a few minutes at zero before continuing; a late burst is easy to miss.

**3. Start this service.**

```bash
kubectl scale deployment/cce-step-sla-service --replicas=1
```

**4. Watch the drain.** `cce.sla.transitions.due` starts high — every deadline that fell during the
replay is due at once — and should fall towards zero within a few cycles, with
`cce.sla.evaluator.batches.failed` flat. Batches drain within a cycle, so even a large backlog clears
in minutes rather than one batch per poll interval.

### If the sequence was missed

There is no automatic correction, and it is worth being blunt about what that means:

| Already written | Recoverable? |
|---|---|
| `sla_status` = `OVERDUE` / `MISSED` on a step that was on time | Only by a manual data fix — the service will not revise it, since writes are forward-only and `MET` is written only over a null |
| The `OVERDUE` / `MISSED` deviation row | Only by deleting it manually |

This query finds the affected steps — ones whose recorded breach disagrees with the `completed_at`
that arrived afterwards:

```sql
SELECT s.id, s.action_id, s.sla_status, s.completed_at,
       t.transition_type, t.process_by, t.processed_at
FROM step_instance s
JOIN step_sla_state_transition t ON t.step_instance_id = s.id
WHERE s.step_status = 'COMPLETED'
  AND t.is_processed = true
  AND ( (s.sla_status = 'OVERDUE' AND t.transition_type = 'DUE_DATE_REACHED')
     OR (s.sla_status = 'MISSED'  AND t.transition_type = 'MISSED_DATE_REACHED') )
  AND s.completed_at <= t.process_by   -- thresholds are inclusive: at the instant is in time
ORDER BY t.processed_at DESC;
```

Pairing each status with the row that produced it is what keeps the result clean: without it, a step
legitimately `OVERDUE` for completing late would also match, because its missed-date row was kept.

A row here means the verdict was reached before the completion was known. It is not proof of a missed
Event Replay — any backdated event arriving after its deadline produces the same shape — but after a
replay this is the list to work from.

## Health checks & monitoring

| Endpoint | Use |
|---|---|
| `/actuator/health/readiness` | Route traffic — fails while the database is unreachable |
| `/actuator/health/liveness` | Restart decisions |
| `/actuator/prometheus` | Scrape target |

Scrape every 60s rather than the 15s global default. Each scrape evaluates the
`cce.sla.transitions.due` gauge, which is one database query per replica (an index-only scan on
`idx_sslt_due`, about 0.2 ms in steady state), and a backlog signal needs no finer resolution than
that:

```yaml
- job_name: "cce-step-sla-service"
  metrics_path: /actuator/prometheus
  scrape_interval: 60s
  static_configs:
    - targets: ["cce-step-sla-service:8080"]
```

Note what readiness does **not** cover: the scheduler. A pod can report ready while its SLA sweep is
stalled. The metric to alert on is `cce.sla.transitions.due` — see
[Architecture §6](architecture-overview.md#6-observability) for how to read it alongside
`evaluator.cycles` and `batches.failed`.

One sweep now reaches every verdict, `MET` included, so there is a single failure mode to watch rather
than two with different symptoms: a stalled service shows as `transitions.due` rising, whatever kind of
row is piling up behind it.

Suggested alerts:

| Condition | Meaning |
|---|---|
| `cce.sla.transitions.due` rising for > 15 min | the sweep is not keeping up — breaches, on-time completions or both |
| `cce.sla.evaluator.batches.failed` increasing | rows are failing and backing off |
| `cce.sla.evaluator.cycles` flat | the scheduler thread has stopped — liveness will not catch this |

## Backup

This service owns no tables, so there is nothing here to back up. `step_sla_state_transition`
and `deviation` are covered by the Matcher Service's backup.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Startup fails: schema validation error | Deployed out of order — the Protocol and Matcher services must migrate `ccedb` first |
| Every batch rolls back with `No enum constant … MET_CONDITION_REACHED` | An old build of this service against a post-`V3` database. Roll this service forward; see the upgrade note at the top |
| `due` gauge rising, `cycles` incrementing | Sweep running but not keeping up — add replicas or raise `batch-size` |
| `due` rising, `batches.failed` rising | Rows failing and backing off; check the logs for the rolled-back batch |
| `cycles` not incrementing | Scheduler stopped; restart the pod. Liveness will not detect this |
| A completed step is `OVERDUE` / `MISSED` although its `completed_at` beat the threshold | The row was judged before the Matcher Service had matched the completing event — the [Event Replay](#event-replay--sequencing-the-two-services) sequence was not held. Not self-correcting |
| A step's `sla_status` looks wrong for a completed step | This service is its **only** writer — Matcher records `step_status` and `completed_at` and never judges timeliness. Compare `completed_at` against the row's `process_by` ([Architecture §4](architecture-overview.md#4-what-the-applier-does)) |
| A completed step stays at a null `sla_status` | It is optional, or it is a 1.x row with no `due_date`, so nothing schedules a verdict for it: `MET` requires a deadline to have been beaten. Null is terminal here and correct |
| A settled step still has an unprocessed `MISSED_DATE_REACHED` row | Expected, not a stuck row. A row is taken when its own deadline arrives, so a step completed before its missed date keeps that row until the date passes — then it is consumed and records nothing |
| A step completed on or before its due date is still null | Its `MET_CONDITION_REACHED` row has not been applied yet, or was never written — Matcher writes it at completion, and only for a mandatory step with a `due_date` |
