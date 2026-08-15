package sagagraph

import scala.collection.mutable

class InMemoryWalStore extends WalStore:

  private case class StoredEntry(
      entry: WalEntry,
      @volatile var status: WalEntry.Status = WalEntry.Status.Pending
  )

  private val store = mutable.Map.empty[SagaId, mutable.ListBuffer[StoredEntry]]
  private val completed = mutable.Set.empty[SagaId]
  private val timestamps = mutable.Map.empty[SagaId, Long]

  def append(sagaId: SagaId, entry: WalEntry): Either[Throwable, Unit] =
    synchronized:
      try
        store.getOrElseUpdate(sagaId, mutable.ListBuffer.empty) += StoredEntry(
          entry
        )
        timestamps.getOrElseUpdate(sagaId, System.currentTimeMillis())
        Right(())
      catch case e: Throwable => Left(e)

  def loadPending(sagaId: SagaId): Either[Throwable, List[WalEntry]] =
    synchronized:
      Right(
        store
          .getOrElse(sagaId, mutable.ListBuffer.empty)
          .filter(_.status == WalEntry.Status.Pending)
          .map(_.entry)
          .toList
      )

  def markCompensated(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, Unit] =
    synchronized:
      findEntry(sagaId, stepName) match
        case None    => Left(Exception(s"[$sagaId] step '$stepName' not found"))
        case Some(e) => e.status = WalEntry.Status.Compensated; Right(())

  def markCompensationFailed(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, Unit] =
    synchronized:
      findEntry(sagaId, stepName) match
        case None    => Left(Exception(s"[$sagaId] step '$stepName' not found"))
        case Some(e) => e.status = WalEntry.Status.CompensationFailed; Right(())

  def markActionFailed(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, Unit] =
    synchronized:
      findEntry(sagaId, stepName) match
        case None    => Left(Exception(s"[$sagaId] step '$stepName' not found"))
        case Some(e) => e.status = WalEntry.Status.ActionFailed; Right(())

  def markCompensated(sagaId: SagaId): Either[Throwable, Unit] =
    synchronized:
      completed += sagaId
      Right(())

  def complete(sagaId: SagaId): Either[Throwable, Unit] =
    synchronized:
      completed += sagaId
      Right(())

  def findZombies(olderThanMs: Long): Either[Throwable, List[SagaId]] =
    synchronized:
      val threshold = System.currentTimeMillis() - olderThanMs
      Right(
        timestamps
          .filterNot { case (id, _) => completed.contains(id) }
          .collect { case (id, ts) if ts < threshold => id }
          .toList
      )

  def getStatus(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, WalEntry.Status] =
    synchronized:
      findEntry(sagaId, stepName) match
        case None    => Left(Exception(s"[$sagaId] step '$stepName' not found"))
        case Some(e) => Right(e.status)

  private def findEntry(sagaId: SagaId, stepName: String): Option[StoredEntry] =
    store.get(sagaId).flatMap(_.find(_.entry.stepName == stepName))
