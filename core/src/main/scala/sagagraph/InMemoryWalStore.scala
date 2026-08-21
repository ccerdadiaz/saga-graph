package sagagraph

import scala.collection.mutable

class InMemoryWalStore extends WalStore:

  private case class StoredEntry(
      entry:     WalEntry,
      @volatile var status:               WalEntry.Status = WalEntry.Status.Registered,
      @volatile var startedAt:            Option[Long]    = None,
      @volatile var finishedAt:           Option[Long]    = None,
      @volatile var compensationAttempts: Int             = 0
  )

  private val store      = mutable.Map.empty[SagaId, mutable.ListBuffer[StoredEntry]]
  private val sagas      = mutable.Map.empty[SagaId, SagaStatus]
  private val timestamps = mutable.Map.empty[SagaId, Long]

  // -------------------------------------------------------------------------
  // Steps
  // -------------------------------------------------------------------------

  def append(sagaId: SagaId, entry: WalEntry): Either[Throwable, Unit] =
    synchronized:
      try
        store.getOrElseUpdate(sagaId, mutable.ListBuffer.empty) += StoredEntry(entry)
        timestamps.getOrElseUpdate(sagaId, System.currentTimeMillis())
        Right(())
      catch case e: Throwable => Left(e)

  def markRunning(sagaId: SagaId, stepName: String): Either[Throwable, Unit] =
    synchronized:
      findEntry(sagaId, stepName) match
        case None => Left(Exception(s"[$sagaId] step '$stepName' not found"))
        case Some(e) =>
          e.status    = WalEntry.Status.Running
          e.startedAt = Some(System.currentTimeMillis())
          Right(())

  def markDone(sagaId: SagaId, stepName: String): Either[Throwable, Unit] =
    synchronized:
      findEntry(sagaId, stepName) match
        case None => Left(Exception(s"[$sagaId] step '$stepName' not found"))
        case Some(e) =>
          e.status     = WalEntry.Status.Done
          e.finishedAt = Some(System.currentTimeMillis())
          Right(())

  def markFailed(sagaId: SagaId, stepName: String): Either[Throwable, Unit] =
    synchronized:
      findEntry(sagaId, stepName) match
        case None => Left(Exception(s"[$sagaId] step '$stepName' not found"))
        case Some(e) =>
          e.status     = WalEntry.Status.Failed
          e.finishedAt = Some(System.currentTimeMillis())
          Right(())

  def markUnknown(sagaId: SagaId, stepName: String): Either[Throwable, Unit] =
    synchronized:
      findEntry(sagaId, stepName) match
        case None    => Left(Exception(s"[$sagaId] step '$stepName' not found"))
        case Some(e) =>
          e.status     = WalEntry.Status.Unknown
          e.finishedAt = Some(System.currentTimeMillis())
          Right(())

  def markCompensated(sagaId: SagaId, stepName: String): Either[Throwable, Unit] =
    synchronized:
      findEntry(sagaId, stepName) match
        case None    => Left(Exception(s"[$sagaId] step '$stepName' not found"))
        case Some(e) => e.status = WalEntry.Status.Compensated; Right(())

  def markCompensationFailed(sagaId: SagaId, stepName: String): Either[Throwable, Unit] =
    synchronized:
      findEntry(sagaId, stepName) match
        case None    => Left(Exception(s"[$sagaId] step '$stepName' not found"))
        case Some(e) => e.status = WalEntry.Status.CompensationFailed; Right(())

  def markHumanIntervention(sagaId: SagaId, stepName: String): Either[Throwable, Unit] =
    synchronized:
      findEntry(sagaId, stepName) match
        case None    => Left(Exception(s"[$sagaId] step '$stepName' not found"))
        case Some(e) => e.status = WalEntry.Status.HumanIntervention; Right(())

  def getStatus(sagaId: SagaId, stepName: String): Either[Throwable, WalEntry.Status] =
    synchronized:
      findEntry(sagaId, stepName) match
        case None    => Left(Exception(s"[$sagaId] step '$stepName' not found"))
        case Some(e) => Right(e.status)

  def incrementCompensationAttempts(sagaId: SagaId, stepName: String): Either[Throwable, Int] =
    synchronized:
      findEntry(sagaId, stepName) match
        case None => Left(Exception(s"[$sagaId] step '$stepName' not found"))
        case Some(e) =>
          e.compensationAttempts += 1
          Right(e.compensationAttempts)

  // Returns CompensationFailed and Unknown entries — actionable by ZombieHunter
  // NOTE: Registered entries are NOT included — they belong to live or
  // not-yet-started sagas. ZombieHunter must never touch Registered steps.
  def loadCompensationFailed(sagaId: SagaId): Either[Throwable, List[WalEntry]] =
    synchronized:
      Right(
        store
          .getOrElse(sagaId, mutable.ListBuffer.empty)
          .filter(e =>
            e.status == WalEntry.Status.CompensationFailed ||
            e.status == WalEntry.Status.Unknown
          )
          .map(_.entry)
          .toList
      )

  // Returns Done entries in LIFO order (reverse insertion order)
  def loadDoneInLifoOrder(sagaId: SagaId): Either[Throwable, List[WalEntry]] =
    synchronized:
      Right(
        store
          .getOrElse(sagaId, mutable.ListBuffer.empty)
          .filter(_.status == WalEntry.Status.Done)
          .map(_.entry)
          .toList
          .reverse  // LIFO — last inserted first
      )

  // -------------------------------------------------------------------------
  // Sagas
  // -------------------------------------------------------------------------

  def registerSaga(sagaId: SagaId): Either[Throwable, Unit] =
    synchronized:
      sagas(sagaId) = SagaStatus.Running
      timestamps.getOrElseUpdate(sagaId, System.currentTimeMillis())
      Right(())

  def markSagaCompleted(sagaId: SagaId): Either[Throwable, Unit] =
    synchronized:
      sagas(sagaId) = SagaStatus.Completed
      Right(())

  def markSagaCompensating(sagaId: SagaId): Either[Throwable, Unit] =
    synchronized:
      sagas(sagaId) = SagaStatus.Compensating
      Right(())

  def markSagaCompensated(sagaId: SagaId): Either[Throwable, Unit] =
    synchronized:
      sagas(sagaId) = SagaStatus.Compensated
      Right(())

  def markSagaFailed(sagaId: SagaId, cause: Throwable): Either[Throwable, Unit] =
    synchronized:
      sagas(sagaId) = SagaStatus.Failed(cause)
      Right(())

  def findZombies(olderThanMs: Long): Either[Throwable, List[SagaId]] =
    synchronized:
      val threshold = System.currentTimeMillis() - olderThanMs
      Right(
        timestamps
          .filter { case (id, ts) =>
            ts < threshold &&
            !sagas.get(id).exists {
              case SagaStatus.Completed   => true
              case SagaStatus.Compensated => true
              case SagaStatus.Failed(_)   => true
              case _                      => false
            }
          }
          .keys
          .toList
      )

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  private def findEntry(sagaId: SagaId, stepName: String): Option[StoredEntry] =
    store.get(sagaId).flatMap(_.find(_.entry.stepName == stepName))
