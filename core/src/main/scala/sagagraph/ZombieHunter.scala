package sagagraph

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
// ---------------------------------------------------------------------------
class ZombieHunter(store: WalStore, registry: CompensationRegistry):

  def recoverAll(olderThanMs: Long = 60_000L): List[ZombieHunter.Result] =
    store.findZombies(olderThanMs) match
      case Left(err)      => List(ZombieHunter.Result.StoreError(err))
      case Right(zombies) => zombies.map(recover)

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
        // No ref — no compensation defined, mark as compensated and skip
        store.markCompensated(sagaId, entry.stepName)
        None

      case Some(ref) =>
        registry.resolve(ref) match
          case None =>
            // Ref not found in registry — cannot compensate, human intervention required
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
                // Check current status — if already CompensationFailed this is the retry
                store.getStatus(sagaId, entry.stepName) match
                  case Right(WalEntry.Status.CompensationFailed) =>
                    // Second failure — human intervention required
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
                    // First failure — mark CompensationFailed, retry next cycle
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
