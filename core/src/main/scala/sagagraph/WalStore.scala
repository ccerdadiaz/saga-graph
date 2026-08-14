package sagagraph

// ---------------------------------------------------------------------------
// WalStore — durable storage for saga compensation log
//
// The motor writes to the WAL BEFORE executing each action.
// On recovery, the motor reads the WAL and compensates pending steps.
//
// Implementations:
//   - InMemoryWalStore  — for tests
//   - FileWalStore      — for production PoC
// ---------------------------------------------------------------------------
trait WalStore:

  // Append a compensation entry BEFORE the action executes
  def append(sagaId: SagaId, entry: WalEntry): Either[Throwable, Unit]

  // Returns all Pending entries for the saga — order is not guaranteed
  def loadPending(sagaId: SagaId): Either[Throwable, List[WalEntry]]

  // Mark an entry as compensated
  def markCompensated(sagaId: SagaId, stepName: String): Either[Throwable, Unit]

  // Mark saga as completed
  def complete(sagaId: SagaId): Either[Throwable, Unit]

  // Find zombie sagas — started but not completed within threshold
  def findZombies(olderThanMs: Long): Either[Throwable, List[SagaId]]

  // Mark a WAL entry as permanently failed — requires human intervention
  def markCompensationFailed(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, Unit]

  // Mark saga as compensated by zombie recovery
  def markCompensated(sagaId: SagaId): Either[Throwable, Unit]

  // Mark a WAL entry as failed — no compensation needed, service guarantees clean state
  def markActionFailed(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, Unit]

  // Get the current status of a WAL entry
  def getStatus(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, WalEntry.Status]
