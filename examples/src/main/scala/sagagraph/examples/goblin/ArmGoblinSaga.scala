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

    val log = Slf4jLogger("sagagraph.examples.goblin.ArmGoblinSaga")

    // Shared state — populated by step 1, consumed by steps 2+
    var goblin: Goblin = Goblin(goblinName, 0, 0)

    SagaGraph()
      .step(
        name = s"measure-$goblinName",
        action = () =>
          log.debug(s"Requesting measurement — $goblinName")
          WeightsAndMeasuresService.measure(goblinName) match
            case Right(g) =>
              goblin = g
              log.debug(s"Measurement received — $goblinName: ${g.weightKg}kg, ${g.heightCm}cm")
              Right(())
            case Left(err) =>
              log.info(s"Measurement failed — $goblinName: ${err.getMessage}")
              Left(err),
        compensate = () =>
          log.info(s"Compensating — destroying measurement records for $goblinName")
          Right(()),
        ref  = Some("destroyMeasurements"),
        args = CompArgs("goblin" -> goblinName)
      )
      .parallel(
        SagaGraph.par(
          name   = s"weapon-$goblinName",
          action = () =>
            log.debug(s"Requesting weapon — $goblinName")
            SmithyService.acquireWeapon(goblin) match
              case Right(w) =>
                log.debug(s"Weapon acquired — $goblinName: ${w.size} ${w.kind}")
                Right(())
              case Left(err) =>
                log.info(s"Weapon unavailable — $goblinName")
                Left(err),
          compensate = () =>
            log.info(s"Compensating — returning weapon — $goblinName")
            SmithyService.returnWeapon(goblin),
          ref  = Some("returnWeapon"),
          args = CompArgs("goblin" -> goblinName)
        ),
        SagaGraph.par(
          name   = s"uniform-$goblinName",
          action = () =>
            log.debug(s"Requesting uniform — $goblinName")
            RagsAndStyleService.acquireUniform(goblin) match
              case Right(u) =>
                log.debug(s"Uniform acquired — $goblinName: size ${u.size}")
                Right(())
              case Left(err) =>
                log.info(s"Uniform unavailable — $goblinName")
                Left(err),
          compensate = () =>
            log.info(s"Compensating — returning uniform — $goblinName")
            RagsAndStyleService.returnUniform(goblin),
          ref  = Some("returnUniform"),
          args = CompArgs("goblin" -> goblinName)
        )
      )
      .optional(
        name   = s"boots-$goblinName",
        action = () =>
          log.debug(s"Requesting boots — $goblinName")
          CobbleryService.acquireBoots(goblin) match
            case Right(b) =>
              log.debug(s"Boots acquired — $goblinName: size ${b.size}")
              Right(())
            case Left(err) =>
              log.info(s"Boots unavailable — $goblinName — going barefoot")
              Left(err),
        compensate = () =>
          log.info(s"Compensating — returning boots — $goblinName")
          CobbleryService.returnBoots(goblin),
        ref  = Some("returnBoots"),
        args = CompArgs("goblin" -> goblinName)
      )
      .bestEffort(
        name   = s"portrait-$goblinName",
        action = () =>
          log.debug(s"Sending portrait to mother — $goblinName")
          PortraitService.sendToMother(goblin)
      )
      .run(store)