package sagagraph.examples.goblin.http

import sagagraph.*
import sagagraph.CompArgs.given
import sagagraph.examples.goblin.{Goblin, OutOfStockException}
import sagagraph.examples.goblin.{Slf4jLogger}

// ---------------------------------------------------------------------------
// ArmGoblinHttpSaga — equips a single goblin using real HTTP services
//
// Identical saga structure to ArmGoblinSaga — only the service calls differ.
// The saga engine sees () => Either[Throwable, Unit] in both cases.
// This demonstrates that saga-graph is transport-agnostic.
// ---------------------------------------------------------------------------
object ArmGoblinHttpSaga:

  def apply(goblinName: String, store: WalStore): SagaResult =

    val log    = Slf4jLogger("sagagraph.examples.goblin.http.ArmGoblinHttpSaga")
    val sagaId = SagaId.generate()
    var goblin = Goblin(goblinName, 0, 0)

    SagaContext.run(sagaId):

      log.info(s"Saga started — $goblinName")

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
          args = CompArgs("goblin" -> goblinName)
        )
        .parallel(
          SagaGraph.par(
            name   = s"weapon-$goblinName",
            action = () =>
              log.debug(s"Requesting weapon — $goblinName")
              GoblinHttpClient.acquireWeapon(goblin) match
                case Right(w) =>
                  log.debug(s"Weapon acquired — $goblinName: ${w.size} ${w.kind}")
                  Right(())
                case Left(err) =>
                  log.info(s"Weapon unavailable — $goblinName")
                  Left(err),
            compensate = () =>
              log.info(s"Compensating — returning weapon — $goblinName")
              GoblinHttpClient.returnWeapon(goblin),
            ref  = Some("returnWeapon"),
            args = CompArgs("goblin" -> goblinName)
          ),
          SagaGraph.par(
            name   = s"uniform-$goblinName",
            action = () =>
              log.debug(s"Requesting uniform — $goblinName")
              GoblinHttpClient.acquireUniform(goblin) match
                case Right(u) =>
                  log.debug(s"Uniform acquired — $goblinName: size ${u.size}")
                  Right(())
                case Left(err) =>
                  log.info(s"Uniform unavailable — $goblinName")
                  Left(err),
            compensate = () =>
              log.info(s"Compensating — returning uniform — $goblinName")
              GoblinHttpClient.returnUniform(goblin),
            ref  = Some("returnUniform"),
            args = CompArgs("goblin" -> goblinName)
          )
        )
        .optional(
          name   = s"boots-$goblinName",
          action = () =>
            log.debug(s"Requesting boots — $goblinName")
            GoblinHttpClient.acquireBoots(goblin) match
              case Right(b) =>
                log.debug(s"Boots acquired — $goblinName: size ${b.size}")
                Right(())
              case Left(err) =>
                log.info(s"Boots unavailable — $goblinName — going barefoot")
                Left(err),
          compensate = () =>
            log.info(s"Compensating — returning boots — $goblinName")
            GoblinHttpClient.returnBoots(goblin),
          ref  = Some("returnBoots"),
          args = CompArgs("goblin" -> goblinName)
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