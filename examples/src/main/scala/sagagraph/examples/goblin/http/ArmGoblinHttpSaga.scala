package sagagraph.examples.goblin.http

import _root_.sagagraph.{WalStore, SagaResult, SagaId, SagaGraph, SagaContext}
import _root_.sagagraph.CompArgs.given
import _root_.sagagraph.examples.goblin.{Goblin, GoblinEquipment, OutOfStockException, Slf4jLogger}

// ---------------------------------------------------------------------------
// ArmGoblinHttpSaga — equips a single goblin using real HTTP services
//
// Receives pre-resolved resource IDs — compensation parameters are known
// before any action executes. This is the deterministic model.
//
// Identical saga structure to ArmGoblinSaga — only the service calls differ.
// Demonstrates that saga-graph is transport-agnostic.
// ---------------------------------------------------------------------------
object ArmGoblinHttpSaga:

  def apply(goblinName: String, equipment: GoblinEquipment, store: WalStore): SagaResult =

    val log    = Slf4jLogger("sagagraph.examples.goblin.http.ArmGoblinHttpSaga")
    val sagaId = SagaId.generate()
    var goblin = Goblin(goblinName, 0, 0)

    SagaContext.run(sagaId):

      log.info(s"Saga started — $goblinName [weapon:${equipment.weaponId}, uniform:${equipment.uniformId}, boots:${equipment.bootsId.getOrElse("none")}]")

      val result = SagaGraph()
        .step(
          name   = s"measure-$goblinName",
          action = () =>
            log.debug(s"Requesting measurement — $goblinName")
            GoblinHttpClient.measure(goblinName) match
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
          args = _root_.sagagraph.CompArgs("goblin" -> goblinName)
        )
        .parallel(
          SagaGraph.par(
            name   = s"weapon-$goblinName",
            action = () =>
              log.debug(s"Requesting weapon ${equipment.weaponId} — $goblinName")
              GoblinHttpClient.acquireWeapon(equipment.weaponId) match
                case Right(w) =>
                  log.debug(s"Weapon acquired — $goblinName: ${w.label}")
                  Right(())
                case Left(err) =>
                  log.info(s"Weapon ${equipment.weaponId} unavailable — $goblinName")
                  Left(err),
            compensate = () =>
              log.info(s"Compensating — returning weapon ${equipment.weaponId} — $goblinName")
              GoblinHttpClient.returnWeapon(equipment.weaponId),
            ref  = Some("returnWeapon"),
            args = _root_.sagagraph.CompArgs("weaponId" -> equipment.weaponId)
          ),
          SagaGraph.par(
            name   = s"uniform-$goblinName",
            action = () =>
              log.debug(s"Requesting uniform ${equipment.uniformId} — $goblinName")
              GoblinHttpClient.acquireUniform(equipment.uniformId) match
                case Right(u) =>
                  log.debug(s"Uniform acquired — $goblinName: size ${u.size}")
                  Right(())
                case Left(err) =>
                  log.info(s"Uniform ${equipment.uniformId} unavailable — $goblinName")
                  Left(err),
            compensate = () =>
              log.info(s"Compensating — returning uniform ${equipment.uniformId} — $goblinName")
              GoblinHttpClient.returnUniform(equipment.uniformId),
            ref  = Some("returnUniform"),
            args = _root_.sagagraph.CompArgs("uniformId" -> equipment.uniformId)
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
                GoblinHttpClient.acquireBoots(id) match
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
                GoblinHttpClient.returnBoots(id),
          ref  = equipment.bootsId.map(_ => "returnBoots"),
          args = _root_.sagagraph.CompArgs("bootsId" -> equipment.bootsId.getOrElse("none"))
        )
        .bestEffort(
          name   = s"portrait-$goblinName",
          action = () =>
            log.debug(s"Sending portrait to mother — $goblinName")
            GoblinHttpClient.sendPortrait(goblin)
        )
        .run(store, sagaId = sagaId)

      result match
        case SagaResult.Completed => log.info(s"Saga completed — $goblinName")
        case SagaResult.Failed(e) => log.info(s"Saga failed — $goblinName — ${e.getMessage}")

      result
