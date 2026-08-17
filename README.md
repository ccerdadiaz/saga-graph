# saga-graph

The Dark Lord needs his army ready. Equipping a goblin means coordinating
scarce resources across independent services — and when the forge runs cold,
everything that was reserved must find its way back to the pool.

**saga-graph** is a non-linear SAGA orchestration engine for Scala:
WAL-before-action semantics, parallel fork support, pluggable persistence,
and zombie recovery.

*The Dark Lord will be eventually pleased.*

---

## The problem

Distributed transactions are hard. You can't lock resources across services.
You can't roll back what already happened in another process. And when a
service goes silent — not failing, just *silent* — you don't know if it acted
or not.

The SAGA pattern is the industry answer: break the transaction into steps,
define a compensation for each one, and if anything goes wrong, compensate
in reverse order.

**saga-graph** attempts to extend that idea to parallel execution and competition
for real scarce resources — where a failed saga must return what it reserved
so the next one can succeed.

- Compensation is registered in the WAL **before** the action executes
- A failed action means the service guarantees its own clean state — no
  compensation needed
- A process that dies mid-saga leaves a WAL that can be recovered
- Scarce resources reserved by a failed saga are returned to the pool and
  available for the next request

---

## The example

Five goblins. Three swords. Four uniforms. Two pairs of boots.
All five recruited concurrently — they compete for real scarce resources.

Each goblin requires:
- **Mandatory** — measured by Weights & Measures (always available)
- **Parallel** — a short sword from the Smithy + a uniform from Rags & Style
  (both or neither — all-or-nothing semantics)
- **Optional** — boots from the Cobblery (a goblin can fight barefoot)
- **BestEffort** — portrait sent to mother (the postal raven is... unreliable)

```
=== DARK LORD'S ARMY RECRUITMENT — Operation: Ready for Battle ===
2026-08-17 12:55:34.439 [INFO ] [global-219] [88bba1a3] Saga started — Grishnakh
2026-08-17 12:55:34.439 [INFO ] [global-221] [53b233ec] Saga started — Muzgash   ← all five start at the same time
2026-08-17 12:55:34.439 [INFO ] [global-223] [19b3cacf] Saga started — Snaga
  ...
2026-08-17 12:55:34.489 [DEBUG] [global-225] [19b3cacf] Requesting uniform — Snaga  ←┐ same saga
2026-08-17 12:55:34.489 [DEBUG] [global-224] [19b3cacf] Requesting weapon — Snaga   ←┘ two threads
2026-08-17 12:55:34.533 [DEBUG] [global-225] [19b3cacf] Uniform acquired — Snaga: size L
2026-08-17 12:55:34.548 [DEBUG] [global-224] [19b3cacf] Weapon acquired — Snaga: standard short sword
  ...
2026-08-17 12:55:34.755 [INFO ] [global-219] [88bba1a3] Saga failed — Grishnakh — Parallel fork failed in: weapon-Grishnakh, uniform-Grishnakh

=== RECRUITMENT REPORT ===
  Grishnakh: ✗ FAILED — Parallel fork failed in: weapon-Grishnakh, uniform-Grishnakh
  Ugluk:     ✓ ARMED AND READY
  Muzgash:   ✗ FAILED — Parallel fork failed in: weapon-Muzgash
  Lagduf:    ✓ ARMED AND READY
  Snaga:     ✓ ARMED AND READY

=== EXECUTION TIMELINE — saga 19b3cacf (Snaga) ===
  step                      start(ms)  end(ms)  dur(ms)  parallel
  measure-Snaga             0          32       32       no
  uniform-Snaga             33         79       46       YES ←  ┐ same saga
  weapon-Snaga              33         93       60       YES ←  ┘ two threads — confirmed
  boots-Snaga               94         160      66       no
```

Two log streams are produced: saga lifecycle and orchestration to stdout,
remote services to `examples/target/services.log` — completely separate.

Lagduf's uniform was reserved, then returned to the pool when the sword
failed. Another goblin picked it up. The resource was not lost. That is the point.

Software models the real world. A uniform is not a row in a database —
it is a uniform. When Lagduf cannot be fully equipped, that uniform must
go back on the shelf so the next goblin can wear it. Bytes represent things.
Compensation is not a technical detail — it is the system honoring that reality.

---

## The DSL

```scala
SagaGraph()
  .step(
    name       = "measure-goblin",
    action     = () => WeightsAndMeasuresService.measure(name),
    compensate = () => destroyRecords(name),
    ref        = "destroyMeasurements",
    args       = CompArgs("goblin" -> name)
  )
  .parallel(
    SagaGraph.par(
      name       = "acquire-weapon",
      action     = () => SmithyService.acquireWeapon(goblin).map(_ => ()),
      compensate = () => SmithyService.returnWeapon(goblin),
      ref        = "returnWeapon",
      args       = CompArgs("goblin" -> name)
    ),
    SagaGraph.par(
      name       = "acquire-uniform",
      action     = () => RagsAndStyleService.acquireUniform(goblin).map(_ => ()),
      compensate = () => RagsAndStyleService.returnUniform(goblin),
      ref        = "returnUniform",
      args       = CompArgs("goblin" -> name)
    )
  )
  .optional(
    name       = "acquire-boots",
    action     = () => CobbleryService.acquireBoots(goblin).map(_ => ()),
    compensate = () => CobbleryService.returnBoots(goblin),
    ref        = "returnBoots",
    args       = CompArgs("goblin" -> name)
  )
  .bestEffort(
    name   = "portrait-to-mother",
    action = () => PortraitService.sendToMother(goblin)
  )
  .run(store)
```

---

## Step semantics

| Type | On failure | Compensation |
|------|-----------|--------------|
| `step` — Mandatory | Saga fails, compensate all previous steps | Required |
| `parallel` — All-or-nothing fork | Saga fails if any branch fails, compensate all successful branches | Required per branch |
| `optional` | Saga continues | Required |
| `bestEffort` | Silently ignored | None |

**Key invariant:** if a service returns `Left`, it guarantees its own internal
consistency. saga-graph will not attempt to compensate a failed action — only
successful ones that need to be undone.

---

## WAL semantics

The Write-Ahead Log is the safety net:

1. Compensation is persisted **before** the action executes
2. If the process dies mid-saga, the WAL survives
3. The ZombieHunter finds interrupted sagas and re-executes pending compensations
4. Failed actions are marked `ActionFailed` — no compensation attempted
5. Successful compensations are marked `Compensated`
6. Compensations that fail are marked `CompensationFailed` — human intervention
   required in current version

---

## Pluggable persistence

```scala
// In-memory — for tests
val store = InMemoryWalStore()

// SQLite — reference implementation, no server required
val store = SqliteWalStore("./saga.db")

// Your own — implement WalStore
class MyStore extends WalStore:
  def append(sagaId, entry)                    = ...
  def loadPending(sagaId)                      = ...
  def markCompensated(sagaId, stepName)        = ...
  def markCompensationFailed(sagaId, stepName) = ...
  def markActionFailed(sagaId, stepName)       = ...
  def complete(sagaId)                         = ...
  def findZombies(olderThanMs)                 = ...
  def getStatus(sagaId, stepName)              = ...
```

---

## Zombie recovery

```scala
val registry = CompensationRegistry()
  .register("returnWeapon",  args => SmithyService.returnWeapon(...))
  .register("returnUniform", args => RagsAndStyleService.returnUniform(...))

// Find sagas that started but never completed
// and re-execute their pending compensations
ZombieHunter(store, registry).recoverAll(olderThanMs = 60_000L)
```

---

## Run the example

### Local services (in-memory)
```bash
git clone https://github.com/ccerdadiaz/saga-graph.git
cd saga-graph
sbt "examples/runMain sagagraph.examples.goblin.GoblinArmyDemo"
```

### HTTP services (Jetty embedded)
```bash
sbt "examples/runMain sagagraph.examples.goblin.http.GoblinArmyHttpDemo"
```
Services start automatically on ports 8080-8084. While running:
```bash
curl -X POST http://localhost:8081/weapon/acquire \
     -H "Content-Type: application/json" \
     -d '{"name":"Grishnakh","weightKg":67}'
```

## Transport agnostic

The saga engine sees only `() => Either[Throwable, Unit]` — it knows nothing
about HTTP, in-memory objects, or any other transport. Both demos run the same
saga structure. Only the service calls differ.

This is the point of adoption: your existing services, whatever their transport,
can be wrapped in a saga step. The compensation endpoint is the only addition
you need to request from the teams that own those services.

---

## Project structure

```
saga-graph/
├── core/          # Zero external dependencies — the pure engine
│   ├── src/main/  # SagaGraph, SagaEngine, WalStore, ZombieHunter,
│   │              # CompensationRegistry, Domain
│   └── src/test/  # SagaEngineSpec, ZombieHunterSpec
├── store-sqlite/  # Reference WalStore implementation using SQLite
│   ├── src/main/  # SqliteWalStore
│   └── src/test/  # SqliteWalStoreSpec, SagaEngineIntegrationSpec
└── examples/      # Goblin Army — scarce resources and compensation in action
    └── src/main/  # GoblinServices, ArmGoblinSaga, GoblinArmyDemo
```

---

## Roadmap

- temporal dimension per step: `UNKNOWN` state, configurable TTL,
  active reconciliation for silent services
- the Two Generals problem, explicit concessions documented
- Sovereign Compensation: saga_id as the only key that can trigger
  compensation — inspired by Rust ownership semantics
- HTTP embedded — wrap example services in http4s for true microservice demo

### Uncharted territory

Ideas that may never leave the concept stage — or may become the most
interesting parts of the project.

- **saga-viz** — real Gantt diagram from the WAL: parallel branches, timings,
  and compensation flows drawn from what actually happened, not what was planned
- **happy-happy path** — demonstrate a resource returned by saga A being
  consumed by saga B with precise timing, proving compensation is not loss
- **store modularity** — `store-sqlite` and future stores as proper
  installable plugins

---

## License

Copyright 2026 Carlos Cerdá Díaz

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


## Known limitations

### Compensation parameters must be known at design time

The current version assumes that compensation parameters are known when the
saga is defined. This covers the deterministic model — where the orchestrator
knows which resource it will use before executing the action.

The non-deterministic model — where a service returns a resource identifier
that is needed for compensation (e.g. "give me any available sword" → receives
ID "J" → must return "J") — is not fully supported. Compensation with a
runtime-generated ID requires a second WAL write after the action succeeds,
which is planned but not yet implemented.

As a workaround, use BestEffort for steps where the compensation resource
is unknown at design time — accepting that those steps cannot be automatically
compensated.

### Irreversible actions

Actions with no possible inverse (send email, burn log, publish event) should
be modeled as BestEffort steps. saga-graph cannot compensate what cannot be
undone — and neither can anything else.

## Acknowledgements

The goblin army example is inspired by **Aye, Dark Overlord!** (*Sì, Oscuro Signore!*),
a storytelling card game designed by Fabrizio Bonifacio, Massimiliano Enrico, and Chiara Ferlito,
with art by Riccardo Crosa. Originally published by Pendragon Game Studio in 2005.
Winner of Best Original Game at Lucca Comics and Games 2005.

If you enjoyed the Dark Lord's recruitment process, the original game is worth finding.
