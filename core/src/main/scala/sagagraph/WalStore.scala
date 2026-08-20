package sagagraph

// ---------------------------------------------------------------------------
// WalStore — durable storage for saga compensation mechanism.
//
// The motor writes to the WAL BEFORE executing each step.
// On recovery, the motor reads the WAL and compensates necessary steps.
//
// Implementations:
//   - InMemoryWalStore  — for tests
//   - SqliteWalStore    — reference implementation
// ---------------------------------------------------------------------------
trait WalStore:

  //
  // Steps
  //

  // Registers a step (and its compensation) with Registered status
  def append(sagaId: SagaId, entry: WalEntry): Either[Throwable, Unit]

  // Step Running
  def markRunning(sagaId: SagaId, stepName: String): Either[Throwable, Unit]

  // Step Done (successfully completed)
  def markDone(sagaId: SagaId, stepName: String): Either[Throwable, Unit]

  // Step Failed
  def markFailed(sagaId: SagaId, stepName: String): Either[Throwable, Unit]

  // Step Compensated (successfully)
  def markCompensated(sagaId: SagaId, stepName: String): Either[Throwable, Unit]

  // Step's compensation Failed — eventually ZombieHunter will retry
  def markCompensationFailed(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, Unit]

  // Marks the step as requiring human intervention
  // — compensation policy applied with no result
  def markHumanIntervention(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, Unit]

  // Returns the current status of a step
  def getStatus(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, WalEntry.Status]

  // Increments the compensation attempt counter and returns the new count
  // Used by ZombieHunter to enforce the retry policy (max attempts configurable)
  def incrementCompensationAttempts(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, Int]

  // Returns only CompensationFailed entries — actionable by ZombieHunter
  // NOTE: Registered entries are NOT included — they belong to live or
  // not-yet-started sagas. ZombieHunter must never touch Registered steps.
  def loadCompensationFailed(sagaId: SagaId): Either[Throwable, List[WalEntry]]

  // Returns Done entries in LIFO order (insertion order reversed)
  // ZombieHunter uses this to continue compensation after unblocking a step
  def loadDoneInLifoOrder(sagaId: SagaId): Either[Throwable, List[WalEntry]]

  //
  // Sagas
  //

  // Registers a new saga with Running status
  def registerSaga(sagaId: SagaId): Either[Throwable, Unit]

  // Marks the saga as Completed — happy path reached the end
  def markSagaCompleted(sagaId: SagaId): Either[Throwable, Unit]

  // Marks the saga as Compensating — compensation in progress or blocked
  def markSagaCompensating(sagaId: SagaId): Either[Throwable, Unit]

  // Marks the saga as Compensated — all steps successfully undone
  def markSagaCompensated(sagaId: SagaId): Either[Throwable, Unit]

  // Marks the saga as Failed — human intervention required
  def markSagaFailed(sagaId: SagaId, cause: Throwable): Either[Throwable, Unit]

  // Finds zombie sagas — not in terminal state beyond threshold
  // Running or TTL — sagas not in terminal state beyond threshold
  def findZombies(olderThanMs: Long): Either[Throwable, List[SagaId]]
