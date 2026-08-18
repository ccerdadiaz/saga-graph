package sagagraph.examples.goblin

import sagagraph.*
import sagagraph.CompArgs.given

// ---------------------------------------------------------------------------
// GoblinEquipment — resolved resources before the saga starts
//
// Resource selection is the caller's responsibility — saga-graph only
// requires that the chosen IDs are known before the saga starts.
// ---------------------------------------------------------------------------
case class GoblinEquipment(
  weaponId:  String,
  uniformId: String,
  bootsId:   Option[String]  // None if no boots available — optional step
)

// ---------------------------------------------------------------------------
// ArmGoblinSaga — equips a single goblin for the Dark Lord's army
//
// Receives pre-resolved resource IDs — compensation parameters are known
// before any action executes. This is the deterministic model.
//
// Steps:
//   1. Mandatory  — measure the goblin (Weights & Measures)
//   2. Parallel   — acquire weapon + acquire uniform (all-or-nothing)
//   3. Optional   — acquire boots (barefoot is acceptable)
//   4. BestEffort — send portrait to mother (always fails)
// ---------------------------------------------------------------------------
object ArmGoblinSaga:

  def apply(goblinName: String, equipment: GoblinEquipment, store: WalStore): SagaResult =

    val log    = Slf4jLogger("sagagraph.examples.goblin.ArmGoblinSaga")
    val sagaId = SagaId.generate()
    var goblin = Goblin(goblinName, 0, 0)

    SagaContext.run(sagaId):

      log.info(s"Saga started — $goblinName [weapon:${equipment.weaponId}, uniform:${equipment.uniformId}, boots:${equipment.bootsId.getOrElse("none")}]")

      val result = SagaGraph()
        .step(
          name   = s"measure-$goblinName",
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
              log.debug(s"Requesting weapon ${equipment.weaponId} — $goblinName")
              SmithyService.acquire(equipment.weaponId) match
                case Right(w) =>
                  log.debug(s"Weapon acquired — $goblinName: ${w.label}")
                  Right(())
                case Left(err) =>
                  log.info(s"Weapon ${equipment.weaponId} unavailable — $goblinName")
                  Left(err),
            compensate = () =>
              log.info(s"Compensating — returning weapon ${equipment.weaponId} — $goblinName")
              SmithyService.return_(equipment.weaponId),
            ref  = Some("returnWeapon"),
            args = CompArgs("weaponId" -> equipment.weaponId)
          ),
          SagaGraph.par(
            name   = s"uniform-$goblinName",
            action = () =>
              log.debug(s"Requesting uniform ${equipment.uniformId} — $goblinName")
              RagsAndStyleService.acquire(equipment.uniformId) match
                case Right(u) =>
                  log.debug(s"Uniform acquired — $goblinName: size ${u.size}")
                  Right(())
                case Left(err) =>
                  log.info(s"Uniform ${equipment.uniformId} unavailable — $goblinName")
                  Left(err),
            compensate = () =>
              log.info(s"Compensating — returning uniform ${equipment.uniformId} — $goblinName")
              RagsAndStyleService.return_(equipment.uniformId),
            ref  = Some("returnUniform"),
            args = CompArgs("uniformId" -> equipment.uniformId)
          )
        )
        .optional(
          name   = s"boots-$goblinName",
          action = () =>
            equipment.bootsId match
              case None =>
                log.debug(s"No boots available for $goblinName — going barefoot")
                Left(OutOfStockException("Cobblery"))
              case Some(id) =>
                log.debug(s"Requesting boots $id — $goblinName")
                CobbleryService.acquire(id) match
                  case Right(b) =>
                    log.debug(s"Boots acquired — $goblinName: size ${b.bootSize}")
                    Right(())
                  case Left(err) =>
                    log.info(s"Boots $id unavailable — $goblinName — going barefoot")
                    Left(err),
          compensate = () =>
            equipment.bootsId match
              case None => Right(())
              case Some(id) =>
                log.info(s"Compensating — returning boots $id — $goblinName")
                CobbleryService.return_(id),
          ref  = equipment.bootsId.map(_ => "returnBoots"),
          args = CompArgs("bootsId" -> equipment.bootsId.getOrElse("none"))
        )
        .bestEffort(
          name   = s"portrait-$goblinName",
          action = () =>
            log.debug(s"Sending portrait to mother — $goblinName")
            PortraitService.sendToMother(goblin)
        )
        .run(store, sagaId = sagaId)

      result match
        case SagaResult.Completed => log.info(s"Saga completed — $goblinName")
        case SagaResult.Failed(e) => log.info(s"Saga failed — $goblinName — ${e.getMessage}")

      result
