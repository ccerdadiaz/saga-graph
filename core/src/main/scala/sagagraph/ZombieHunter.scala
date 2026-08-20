package sagagraph

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.concurrent.duration.*

// ---------------------------------------------------------------------------
// ZombieHunter — recovers sagas that died before completing compensation
//
// Finds sagas not in terminal state beyond threshold, loads their actionable
// WAL entries (Registered, CompensationFailed), and re-executes compensations
// using the registry.
//
// Retry policy — one retry per CompensationFailed entry:
//   - First failure  → CompensationFailed (will be retried next cycle)
//   - Second failure → HumanIntervention  (compensation policy applied with no result)
//
// Can be used synchronously (recoverAll) or as an autonomous background
// process (start/stop) using a ScheduledExecutorService — zero dependencies.
// ---------------------------------------------------------------------------
class ZombieHunter(
    store: WalStore,
    registry: CompensationRegistry,
    interval: Duration = 60.seconds,
    threshold: Duration = 60.seconds
):

  // -------------------------------------------------------------------------
  // Configuration — fluent API
  // -------------------------------------------------------------------------
  def withInterval(d: Duration): ZombieHunter =
    ZombieHunter(store, registry, d, threshold)
  def withThreshold(d: Duration): ZombieHunter =
    ZombieHunter(store, registry, interval, d)

  // -------------------------------------------------------------------------
  // Synchronous API — recover all zombies once
  // -------------------------------------------------------------------------
  def recoverAll(
      olderThanMs: Long = threshold.toMillis
  ): List[ZombieHunter.Result] =
    store.findZombies(olderThanMs) match
      case Left(err)      => List(ZombieHunter.Result.StoreError(err))
      case Right(zombies) => zombies.map(recover)

  // -------------------------------------------------------------------------
  // Autonomous background process
  // Starts a ScheduledExecutorService that calls recoverAll periodically.
  // Returns a ZombieHunterHandle to stop the process cleanly.
  // -------------------------------------------------------------------------
  def start(): ZombieHunter.Handle =
    val executor = Executors.newSingleThreadScheduledExecutor(r =>
      val t = Thread(r, "zombie-hunter")
      t.setDaemon(true)
      t
    )
    executor.scheduleWithFixedDelay(
      () => recoverAll(),
      interval.toMillis,
      interval.toMillis,
      TimeUnit.MILLISECONDS
    )
    ZombieHunter.Handle(executor)

  // -------------------------------------------------------------------------
  // Recovery logic
  // -------------------------------------------------------------------------
  private def recover(sagaId: SagaId): ZombieHunter.Result =
    store.loadActionable(sagaId) match
      case Left(err) =>
        ZombieHunter.Result.StoreError(err)
      case Right(entries) =>
        val failures = entries.flatMap { entry => compensate(sagaId, entry) }

        if failures.isEmpty then
          store.markSagaCompensated(sagaId)
          ZombieHunter.Result.Recovered(sagaId)
        else if failures.exists(_.humanIntervention) then
          store.markSagaFailed(
            sagaId,
            failures.find(_.humanIntervention).get.cause
          )
          ZombieHunter.Result.HumanInterventionRequired(sagaId, failures)
        else ZombieHunter.Result.PartiallyRecovered(sagaId, failures)

  private def compensate(
      sagaId: SagaId,
      entry: WalEntry
  ): Option[ZombieHunter.CompensationFailure] =
    entry.compensationRef match
      case None =>
        store.markCompensated(sagaId, entry.stepName)
        None

      case Some(ref) =>
        registry.resolve(ref) match
          case None =>
            store.markHumanIntervention(sagaId, entry.stepName)
            Some(
              ZombieHunter.CompensationFailure(
                entry.stepName,
                ref,
                Exception(s"No handler registered for ref '$ref'"),
                humanIntervention = true
              )
            )

          case Some(handler) =>
            handler(entry.compensationArgs) match
              case Right(_) =>
                store.markCompensated(sagaId, entry.stepName)
                None
              case Left(err) =>
                store.getStatus(sagaId, entry.stepName) match
                  case Right(WalEntry.Status.CompensationFailed) =>
                    store.markHumanIntervention(sagaId, entry.stepName)
                    Some(
                      ZombieHunter.CompensationFailure(
                        entry.stepName,
                        ref,
                        err,
                        humanIntervention = true
                      )
                    )
                  case _ =>
                    store.markCompensationFailed(sagaId, entry.stepName)
                    Some(
                      ZombieHunter.CompensationFailure(
                        entry.stepName,
                        ref,
                        err,
                        humanIntervention = false
                      )
                    )

object ZombieHunter:

  case class CompensationFailure(
      stepName: String,
      ref: String,
      cause: Throwable,
      humanIntervention: Boolean = false
  )

  enum Result:
    case Recovered(sagaId: SagaId)
    case PartiallyRecovered(sagaId: SagaId, failures: List[CompensationFailure])
    case HumanInterventionRequired(
        sagaId: SagaId,
        failures: List[CompensationFailure]
    )
    case StoreError(cause: Throwable)

  // -------------------------------------------------------------------------
  // Handle — controls the autonomous background process
  // -------------------------------------------------------------------------
  class Handle(executor: ScheduledExecutorService):

    // Stops after current cycle completes
    def stop(): Unit =
      executor.shutdown()
      executor.awaitTermination(10, TimeUnit.SECONDS)

    // Stops immediately — current cycle may be interrupted
    def stopNow(): Unit =
      executor.shutdownNow()
