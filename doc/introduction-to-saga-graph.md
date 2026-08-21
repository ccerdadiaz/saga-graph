# Introduction to saga-graph

This document presents a proposal for extending the linear SAGA pattern with
parallel steps, WAL-before-action as a safety net, and a clear separation of
responsibilities for recovery outside the happy path — all without external
dependencies, without a workflow engine, and without a message broker.

It assumes familiarity with the SAGA pattern and its fundamentals. It does
not explain what a saga is or why distributed transactions are hard.

---

## 1. Premises

### WAL-before-action

The Write-Ahead Log is the safety net. Before any action executes, its
compensation is written to the WAL. If the process dies between the WAL write
and the action execution, the compensation entry exists but the action never
ran — nothing to undo. If the process dies after the action executes, the
compensation entry is there and will be found on recovery.

This ordering is not optional. It is the invariant that makes recovery possible.

```
WAL write → action → (success or failure)
     ↑
     if process dies here, nothing happened
                ↑
                if process dies here, WAL has the compensation
```

### `Left` means clean

A service that returns `Left` guarantees its own internal consistency. It may
have attempted something and rolled back internally, or it may have rejected
the request before touching any state. From saga-graph's perspective it does
not matter — `Left` means "nothing happened here that needs undoing."

This is a contract, not a mechanism. saga-graph cannot enforce it. The service
must honor it. If a service returns `Left` after a partial state change, the
system is already inconsistent — no orchestration engine can fix that.

The consequence: saga-graph never compensates a step that returned `Left`.
Only successful steps — those that returned `Right` — can have their effects
undone.

### Zero external dependencies

The `core` module has no external dependencies. No Akka, no ZIO, no Cats, no
logging framework, no configuration library. The engine is pure Scala 3 and
JDK.

Persistence is pluggable — `WalStore` is a trait. The reference implementation
uses SQLite via `store-sqlite`. Your production implementation can use
PostgreSQL, Oracle, or any other store. The engine does not know and does not
care.

Logging is injectable — `SagaLogger` is a trait with a `noOp` default. Pass
your own implementation or use the provided SLF4J bridge in the examples.

---

## 2. Your first saga

A saga is a sequence of steps. Each step has an action and a compensation.
The compensation is registered in the WAL before the action executes.

```scala
import sagagraph.*

val store = InMemoryWalStore()

val result = SagaGraph()
  .step(
    name       = "reserve-seat",
    action     = () => SeatService.reserve("seat-42"),
    compensate = () => SeatService.release("seat-42"),
    ref        = Some("releaseSeat"),
    args       = CompArgs("seatId" -> "seat-42"),
    ttl        = 500.millis  // step times out if no response within 500ms
  )
  .step(
    name       = "charge-card",
    action     = () => PaymentService.charge("card-99", 50),
    compensate = () => PaymentService.refund("card-99", 50),
    ref        = Some("refundCard"),
    args       = CompArgs("cardId" -> "card-99", "amount" -> "50")
  )
  .run(store)
```

If `charge-card` fails, saga-graph compensates `reserve-seat` in LIFO order —
last executed, first compensated. The seat is released. The card is not charged.

If `reserve-seat` fails, there is nothing to compensate — `Left` means clean.

The `ref` and `args` parameters are the compensation registry key and
arguments. They are persisted in the WAL and used by the ZombieHunter for
recovery if the process dies during compensation. They are optional for simple
cases but required for production use.

### Observing the WAL

```scala
val store = SqliteWalStore("./my-saga.db")
// ... run sagas ...

// inspect with sqlite3
// sqlite3 my-saga.db "SELECT step_name, status FROM wal_entries;"
// step_name      status
// reserve-seat   Compensated
// charge-card    Failed
```

---

## 3. Parallel forks

Extending the linear SAGA model to parallel execution introduces new problems.
Some are well-understood. Others may have implications not yet fully mapped.
This is an open frontier where both theoretical and practical contributions
are possible.

### The problem

Consider two steps that can execute concurrently — reserve a seat and reserve
a hotel room for the same trip. They are independent. Running them sequentially
wastes time. But running them in parallel introduces a new failure mode: what
if one succeeds and the other fails?

The naive answer is "compensate the one that succeeded." But compensating
concurrently executing steps requires knowing which ones completed before
the failure was detected — and that knowledge must be durable, not in memory.

### The WAL-before-action invariant in a fork

saga-graph registers **all** fork entries in the WAL **before** executing
**any** action. When the fork starts, every step's compensation is already
persisted. If the process dies mid-fork, the WAL contains all the information
needed for recovery — regardless of which steps completed.

```
WAL: [weapon-entry, uniform-entry]  ← both registered before any action
     ↓                    ↓
     weapon thread         uniform thread
     acquires sword        acquires uniform
                    ↓
              uniform unavailable → Left
                    ↓
         compensate weapon → Right   ← WAL told us weapon succeeded
```

### All-or-nothing semantics

A parallel fork is all-or-nothing. If any branch fails, all successful branches
are compensated. The saga fails as a unit.

```scala
SagaGraph()
  .step(...)
  .parallel(
    SagaGraph.par(
      name       = "acquire-weapon",
      action     = () => SmithyService.acquire(weaponId),
      compensate = () => SmithyService.return_(weaponId),
      ref        = Some("returnWeapon"),
      args       = CompArgs("weaponId" -> weaponId)
    ),
    SagaGraph.par(
      name       = "acquire-uniform",
      action     = () => RagsAndStyleService.acquire(uniformId),
      compensate = () => RagsAndStyleService.return_(uniformId),
      ref        = Some("returnUniform"),
      args       = CompArgs("uniformId" -> uniformId)
    )
  )
  .step(...)
  .run(store)
```

The parallel block is atomic in both directions — it advances as a unit and
compensates as a unit.

---

## 4. Step types

| Type | Semantics | On failure | On timeout | Compensation |
|------|-----------|-----------|------------|--------------|
| `step` — Mandatory | Must succeed | Saga fails, LIFO compensation begins | `Unknown` → saga fails, LIFO begins | Required |
| `parallel` — Fork | All branches must succeed | Saga fails, all successful branches compensated | `Unknown` → fork fails | Required per branch |
| `optional` | May fail | Saga continues | `Unknown` → saga continues | Required |
| `bestEffort` | May fail, no record | Silently ignored | Silently ignored | None |

All step types have a configurable TTL. Defaults: 30 seconds for mandatory
and optional, 5 seconds for bestEffort. BestEffort steps should be
fire-and-forget services — if a BestEffort step regularly needs a meaningful
TTL, use Optional instead.

---

## 5. The WAL — your operational view

The WAL is not just a safety net — it is an audit log and an operational tool.

### Step states

| State | Meaning |
|-------|---------|
| `Registered` | Recorded in WAL, action not yet invoked |
| `Running` | Engine has invoked the action |
| `Done` | Action completed successfully |
| `Failed` | Action failed — service guarantees clean state |
| `Unknown` | No response within TTL — service may or may not have acted |
| `Compensated` | Compensation executed successfully |
| `CompensationFailed` | Compensation failed — ZombieHunter will retry |
| `HumanIntervention` | Compensation policy exhausted — requires human action |

### Saga states

| State | Meaning |
|-------|---------|
| `Running` | Executing steps forward |
| `Compensating` | Compensation in progress or blocked |
| `Completed` | Happy path — all steps done |
| `Compensated` | All steps successfully undone |
| `Failed` | Compensation could not complete — human intervention required |

### Querying the WAL

```sql
-- How many sagas are in each state?
SELECT status, COUNT(*) FROM sagas GROUP BY status;

-- Which steps need attention?
SELECT saga_id, step_name, status
FROM wal_entries
WHERE status IN ('CompensationFailed', 'HumanIntervention', 'Unknown');

-- Execution timeline for a specific saga
SELECT step_name, started_at, finished_at, finished_at - started_at AS duration_ms
FROM wal_entries
WHERE saga_id = '...'
ORDER BY started_at;
```

---

## 6. Engine and ZombieHunter — two independent agents

### The engine's responsibility

The engine executes steps forward and compensates backward. If a step fails
or times out, the engine begins LIFO compensation. If a compensation fails,
the engine **stops immediately** — it does not retry, it does not skip, it
does not continue. The saga stays in `Compensating`. The remaining `Done`
steps are left untouched.

This is a deliberate design decision. The engine is synchronous and simple.
Recovery is the ZombieHunter's job.

### The ZombieHunter's responsibility

The ZombieHunter is an independent background agent. It finds sagas in
`Compensating` state, retries their `CompensationFailed` and `Unknown` steps,
and if successful, continues the LIFO from where the engine stopped —
compensating the remaining `Done` steps in order.

```scala
val registry = CompensationRegistry()
  .register("returnWeapon",  args => SmithyService.return_(...))
  .register("returnUniform", args => RagsAndStyleService.return_(...))

val zh = ZombieHunter(store, registry)
  .withInterval(30.seconds)
  .withThreshold(60.seconds)
  .withMaxAttempts(2)
  .withLogger(myLogger)

val handle = zh.start()
// ... application runs ...
handle.stop()
```

### Retry policy

- Compensation fails → `CompensationFailed`, retried next ZH cycle
- After `maxAttempts` failures → `HumanIntervention`, saga marked `Failed`
- No handler registered for ref → `HumanIntervention` immediately
- `Unknown` steps are compensated idempotently — the service must handle
  duplicate compensation requests safely

### Consequences for service design

**Compensations must be idempotent.** The ZombieHunter may call a compensation
that was already attempted by the engine. The service must handle duplicate
compensation requests safely — returning the same resource twice must be safe.

**Compensations must be finalista** — they must eventually return a definitive
result. A compensation that hangs indefinitely blocks the ZombieHunter cycle.
Services should implement timeouts on their compensation endpoints.

---

## 7. Scarce resources — the goblin army

The goblin army example demonstrates saga-graph under real resource contention.
Five goblins compete for three swords, four uniforms, and two pairs of boots.
All five recruit concurrently.

```bash
sbt "examples/runMain sagagraph.examples.goblin.GoblinArmyDemo"
```

The execution timeline shows parallel forks in action — weapon and uniform
acquired concurrently within each saga, while five sagas compete for the same
pool.

### The happy-happy path

```bash
sbt "examples/runMain sagagraph.examples.goblin.GoblinHappyHappyDemo"
```

Grishnakh (bigfoot, size 15) reserves a sword and a uniform, then fails on
boots — no size 15 available. His compensation returns sword-1 and uniform-1
to the pool. Ugluk (standard size 7) arrives next, picks up exactly those
resources, and completes successfully.

Compensation is not loss — it is reaprovisionamiento. What one saga cannot
use, the next one can.

### TTL and Unknown state

```bash
sbt "examples/runMain sagagraph.examples.goblin.GoblinTimeoutDemo"
```

Grimfang's weapon step times out — the Smithy takes 200ms to respond but
the TTL is 50ms. The step is marked `Unknown`. The engine compensates the
previous steps and leaves the `Unknown` step for the ZombieHunter.
Bolg's weapon step takes 30ms — within the 200ms TTL — and completes normally.

`Unknown` and `Left` trigger the same compensation behavior. The engine does
not distinguish — it compensates either way.

### Volume test

```bash
sbt "examples/runMain sagagraph.examples.goblin.GoblinArmyDemo 200 0.1"
```

200 goblins, 10% induced compensation failures. The ZombieHunter runs
autonomously, retries `CompensationFailed` steps, continues LIFO where the
engine stopped. The WAL correctness analysis at the end shows the distribution
of states.

---

## 8. Adoption in existing systems

saga-graph does not require rewriting your services. It requires one addition:
a compensation endpoint for each action that participates in a saga.

If your service already has a `POST /reservation` endpoint, it needs a
`DELETE /reservation/{id}` — or equivalent — for compensation. That is the
only contract saga-graph imposes on the services it orchestrates.

### The deterministic model

Compensation parameters must be known before the saga starts. This is the
deterministic model — the orchestrator resolves which resources it will use
before executing any action, and registers those IDs in the WAL as compensation
arguments.

```scala
// Before the saga — resolve resources
val weaponId  = SmithyService.getAvailable().head
val uniformId = RagsAndStyleService.getAvailable("S").head

// The saga knows its compensation parameters before any action executes
val equipment = GoblinEquipment(weaponId, uniformId, bootsId = None)
ArmGoblinSaga(goblinName, equipment, store)
```

This covers the majority of real-world cases. The non-deterministic model —
where the service returns a resource ID that is needed for compensation — is
a known limitation and is planned for a future version.

### Wrapping existing services

```scala
// Existing service — unchanged
object SmithyService:
  def acquireWeapon(id: String): Either[Throwable, Weapon] = ...
  def returnWeapon(id: String): Either[Throwable, Unit] = ...

// saga-graph step — thin wrapper
SagaGraph.par(
  name       = s"acquire-weapon",
  action     = () => SmithyService.acquireWeapon(weaponId).map(_ => ()),
  compensate = () => SmithyService.returnWeapon(weaponId),
  ref        = Some("returnWeapon"),
  args       = CompArgs("weaponId" -> weaponId)
)
```

The service knows nothing about saga-graph. The wrapper is the only coupling.

---

## 9. Known boundaries

### The Two Generals problem

When a service does not respond within its configured TTL, saga-graph marks
the step as `Unknown` — the engine does not know if the action was executed
or not. The Two Generals problem guarantees this is unsolvable in the general
case.

saga-graph's response is pragmatic: compensate `Unknown` steps idempotently.
The service must handle duplicate compensation requests safely. This is the
standard at-least-once delivery constraint.

### Compensation idempotency is the caller's responsibility

The ZombieHunter may call a compensation more than once for the same step.
Services must handle duplicate compensation requests safely. This is a standard
at-least-once delivery constraint and must be designed for explicitly.

### What saga-graph does not try to be

saga-graph is not a workflow engine. It does not handle human approval steps,
timers, signals, or long-running processes that span days. It is an
orchestration engine for distributed transactions that complete in milliseconds
to seconds.

saga-graph is not a message broker. It does not publish events or subscribe to
topics. It calls services directly and synchronously. If your architecture
requires choreography via events, saga-graph is not the right tool.

saga-graph is not a distributed system. It runs in a single process. The
`WalStore` is the shared state between the engine and the ZombieHunter — they
can run in different threads but not in different JVMs without a shared
persistent store.
