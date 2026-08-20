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

  // Registers an step (and its compensation) with Registered status
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
  // TODO: Rename or mantain naming
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

  // Returns all actionable entries — Registered, CompensationFailed
  def loadActionable(sagaId: SagaId): Either[Throwable, List[WalEntry]]

  //
  // Sagas
  //

  // Registers a new saga with Running status
  def registerSaga(sagaId: SagaId): Either[Throwable, Unit]

  // Marks the saga as Completed — happy path reached the end
  def markSagaCompleted(sagaId: SagaId): Either[Throwable, Unit]

  // Marks the saga as Compensating — more Done steps should be compensated
  def markSagaCompensating(sagaId: SagaId): Either[Throwable, Unit]

  // Marks the saga as Compensated — all actions successfully undone
  def markSagaCompensated(sagaId: SagaId): Either[Throwable, Unit]

  // Marks the saga as Failed — human intervention required
  def markSagaFailed(sagaId: SagaId, cause: Throwable): Either[Throwable, Unit]

  // Finds zombie sagas — Running but not completed within threshold
  def findZombies(olderThanMs: Long): Either[Throwable, List[SagaId]]
