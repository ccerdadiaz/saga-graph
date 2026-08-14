package sagagraph.examples.goblin

import sagagraph.*
import sagagraph.CompArgs.given

// ---------------------------------------------------------------------------
// ArmGoblinSaga — equips a single goblin for the Dark Lord's army
//
// Steps:
//   1. Mandatory  — measure the goblin (Weights & Measures)
//   2. Parallel   — acquire weapon (Smithy) + acquire uniform (Rags & Style)
//   3. Optional   — acquire boots (Cobblery) — barefoot is acceptable
//   4. BestEffort — send portrait to mother (Portrait) — always fails
// ---------------------------------------------------------------------------
object ArmGoblinSaga:

  def apply(goblinName: String, store: WalStore): SagaResult =

    // Shared state — populated by step 1, consumed by steps 2+
    var goblin: Goblin = Goblin(goblinName, 0, 0)

    SagaGraph()
      .step(
        name   = s"measure-$goblinName",
        action = () =>
          WeightsAndMeasuresService.measure(goblinName) match
            case Right(g)  => goblin = g; Right(())
            case Left(err) => Left(err),
        compensate = () =>
          println(s"  [Compensation] Destroying measurement records for $goblinName. Never happened.")
          Right(()),
        ref  = "destroyMeasurements",
        args = CompArgs("goblin" -> goblinName)
      )
      .parallel(
        SagaGraph.par(
          name       = s"weapon-$goblinName",
          action     = () => SmithyService.acquireWeapon(goblin).map(_ => ()),
          compensate = () => SmithyService.returnWeapon(goblin),
          ref        = "returnWeapon",
          args       = CompArgs("goblin" -> goblinName)
        ),
        SagaGraph.par(
          name       = s"uniform-$goblinName",
          action     = () => RagsAndStyleService.acquireUniform(goblin).map(_ => ()),
          compensate = () => RagsAndStyleService.returnUniform(goblin),
          ref        = "returnUniform",
          args       = CompArgs("goblin" -> goblinName)
        )
      )
      .optional(
        name       = s"boots-$goblinName",
        action     = () => CobbleryService.acquireBoots(goblin).map(_ => ()),
        compensate = () => CobbleryService.returnBoots(goblin),
        ref        = "returnBoots",
        args       = CompArgs("goblin" -> goblinName)
      )
      .bestEffort(
        name   = s"portrait-$goblinName",
        action = () => PortraitService.sendToMother(goblin)
      )
      .run(store)