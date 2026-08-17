package sagagraph

// ---------------------------------------------------------------------------
// ZombieHunter — recovers sagas that died before completing compensation
//
// Finds Running sagas older than the threshold, loads their Pending WAL
// entries, and re-executes compensations in LIFO order using the registry.
//
// Entries that cannot be resolved or fail compensation are marked
// CompensationFailed — human intervention required in current version.
// ---------------------------------------------------------------------------
class ZombieHunter(store: WalStore, registry: CompensationRegistry):

  def recoverAll(olderThanMs: Long = 60_000L): List[ZombieHunter.Result] =
    store.findZombies(olderThanMs) match
      case Left(err)      => List(ZombieHunter.Result.StoreError(err))
      case Right(zombies) => zombies.map(recover)

  private def recover(sagaId: SagaId): ZombieHunter.Result =
    store.loadPending(sagaId) match
      case Left(err) => ZombieHunter.Result.StoreError(err)
      case Right(entries) =>
        val failures = entries.flatMap { entry =>
          compensate(sagaId, entry)
        }
        store.markCompensated(sagaId)
        if failures.isEmpty then ZombieHunter.Result.Recovered(sagaId)
        else ZombieHunter.Result.PartiallyRecovered(sagaId, failures)

  private def compensate(
      sagaId: SagaId,
      entry: WalEntry
  ): Option[ZombieHunter.CompensationFailure] =
    entry.compensationRef match
      case None =>
        // No ref — no compensation defined, mark as done and skip
        store.markCompensated(sagaId, entry.stepName)
        None

      case Some(ref) =>
        registry.resolve(ref) match
          case None =>
            // Ref not found in registry — cannot compensate
            store.markCompensationFailed(sagaId, entry.stepName)
            Some(
              ZombieHunter.CompensationFailure(
                entry.stepName,
                ref,
                Exception(s"No handler registered for ref '$ref'")
              )
            )

          case Some(handler) =>
            handler(entry.compensationArgs) match
              case Right(_) =>
                store.markCompensated(sagaId, entry.stepName)
                None
              case Left(err) =>
                markFailed(sagaId, entry.stepName)
                Some(ZombieHunter.CompensationFailure(entry.stepName, ref, err))

  private def markFailed(sagaId: SagaId, stepName: String): Unit =
    store.markCompensationFailed(sagaId, stepName)

object ZombieHunter:

  case class CompensationFailure(
      stepName: String,
      ref: String,
      cause: Throwable
  )

  enum Result:
    case Recovered(sagaId: SagaId)
    case PartiallyRecovered(sagaId: SagaId, failures: List[CompensationFailure])
    case StoreError(cause: Throwable)
