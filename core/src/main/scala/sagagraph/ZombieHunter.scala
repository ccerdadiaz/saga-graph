package sagagraph

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.concurrent.duration.*

// ---------------------------------------------------------------------------
// ZombieHunter — recovers sagas blocked during compensation
//
// Finds sagas in Compensating state, loads their CompensationFailed entries,
// and retries them. If successful, continues the LIFO compensation from where
// the engine stopped. If not, escalates to HumanIntervention after maxAttempts.
//
// ZombieHunter NEVER touches Registered entries — those belong to live or
// not-yet-started sagas.
//
// Retry policy — per step, configurable (default: 2 attempts):
//   - Attempt N < maxAttempts fails → CompensationFailed, retry next cycle
//   - Attempt maxAttempts fails     → HumanIntervention, saga marked Failed
//   - No handler for ref            → HumanIntervention immediately
//
// After unblocking a CompensationFailed step, ZombieHunter continues the
// LIFO compensation by processing all remaining Done entries.
//
// The engine and ZombieHunter are independent — the engine stops on first
// compensation failure, ZombieHunter picks up from that point.
// ---------------------------------------------------------------------------
class ZombieHunter(
    store:       WalStore,
    registry:    CompensationRegistry,
    interval:    Duration   = 60.seconds,
    threshold:   Duration   = 60.seconds,
    maxAttempts: Int        = 2,
    logger:      SagaLogger = SagaLogger.noOp
):

  // -------------------------------------------------------------------------
  // Configuration — fluent API
  // -------------------------------------------------------------------------
  def withInterval(d: Duration):  ZombieHunter = ZombieHunter(store, registry, d, threshold, maxAttempts, logger)
  def withThreshold(d: Duration): ZombieHunter = ZombieHunter(store, registry, interval, d, maxAttempts, logger)
  def withMaxAttempts(n: Int):    ZombieHunter = ZombieHunter(store, registry, interval, threshold, n, logger)
  def withLogger(l: SagaLogger):  ZombieHunter = ZombieHunter(store, registry, interval, threshold, maxAttempts, l)

  // -------------------------------------------------------------------------
  // Synchronous API — recover all zombies once
  // -------------------------------------------------------------------------
  def recoverAll(olderThanMs: Long = threshold.toMillis): List[ZombieHunter.Result] =
    logger.debug(s"[ZombieHunter] Scanning for zombies older than ${olderThanMs}ms")
    store.findZombies(olderThanMs) match
      case Left(err) =>
        logger.warn(s"[ZombieHunter] Store error finding zombies: ${err.getMessage}")
        List(ZombieHunter.Result.StoreError(err))
      case Right(zombies) =>
        if zombies.isEmpty then logger.debug("[ZombieHunter] No zombies found")
        else logger.info(s"[ZombieHunter] Found ${zombies.size} zombie(s)")
        zombies.map(recover)

  // -------------------------------------------------------------------------
  // Autonomous background process
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
  // Recovery logic for a single saga
  // -------------------------------------------------------------------------
  private def recover(sagaId: SagaId): ZombieHunter.Result =
    logger.info(s"[ZombieHunter] Recovering saga ${sagaId.value.take(8)}")
    store.loadCompensationFailed(sagaId) match
      case Left(err) =>
        logger.warn(s"[ZombieHunter] Store error for ${sagaId.value.take(8)}: ${err.getMessage}")
        ZombieHunter.Result.StoreError(err)

      case Right(Nil) =>
        // No CompensationFailed entries — check if Done entries remain
        continueLifo(sagaId)

      case Right(failed) =>
        logger.info(s"[ZombieHunter] Saga ${sagaId.value.take(8)} — ${failed.size} CompensationFailed entries")
        // Retry all CompensationFailed entries in LIFO order — stop on first failure
        var stopped = false
        val failures = scala.collection.mutable.ListBuffer.empty[ZombieHunter.CompensationFailure]
        val iter = failed.iterator
        while iter.hasNext && !stopped do
          val entry = iter.next()
          retryCompensation(sagaId, entry) match
            case None => () // success — continue
            case Some(failure) =>
              failures += failure
              stopped = true

        if failures.exists(_.humanIntervention) then
          store.markSagaFailed(sagaId, failures.find(_.humanIntervention).get.cause)
          logger.warn(s"[ZombieHunter] Saga ${sagaId.value.take(8)} — HumanInterventionRequired")
          ZombieHunter.Result.HumanInterventionRequired(sagaId, failures.toList)
        else if stopped then
          store.markSagaCompensating(sagaId)
          logger.warn(s"[ZombieHunter] Saga ${sagaId.value.take(8)} — PartiallyRecovered, will retry")
          ZombieHunter.Result.PartiallyRecovered(sagaId, failures.toList)
        else
          // All CompensationFailed unblocked — continue LIFO with Done entries
          continueLifo(sagaId)

  // -------------------------------------------------------------------------
  // Continue LIFO compensation with remaining Done entries
  // Called after unblocking a CompensationFailed step
  // -------------------------------------------------------------------------
  private def continueLifo(sagaId: SagaId): ZombieHunter.Result =
    store.loadDoneInLifoOrder(sagaId) match
      case Left(err) =>
        ZombieHunter.Result.StoreError(err)
      case Right(Nil) =>
        // No Done entries remain — saga fully compensated
        store.markSagaCompensated(sagaId)
        logger.info(s"[ZombieHunter] Saga ${sagaId.value.take(8)} — Recovered")
        ZombieHunter.Result.Recovered(sagaId)
      case Right(doneEntries) =>
        logger.info(s"[ZombieHunter] Saga ${sagaId.value.take(8)} — continuing LIFO with ${doneEntries.size} Done entries")
        // Compensate in LIFO order — stop on first failure (same policy as engine)
        var stopped = false
        val iter    = doneEntries.iterator
        while iter.hasNext && !stopped do
          val entry = iter.next()
          compensateDoneEntry(sagaId, entry) match
            case None => () // success — continue
            case Some(failure) =>
              if failure.humanIntervention then
                store.markSagaFailed(sagaId, failure.cause)
                logger.warn(s"[ZombieHunter] Saga ${sagaId.value.take(8)} — HumanInterventionRequired during LIFO")
                stopped = true
              else
                store.markSagaCompensating(sagaId)
                logger.warn(s"[ZombieHunter] Saga ${sagaId.value.take(8)} — LIFO stopped, will retry")
                stopped = true

        if stopped then
          ZombieHunter.Result.PartiallyRecovered(sagaId, List.empty)
        else
          store.markSagaCompensated(sagaId)
          logger.info(s"[ZombieHunter] Saga ${sagaId.value.take(8)} — Recovered")
          ZombieHunter.Result.Recovered(sagaId)

  // -------------------------------------------------------------------------
  // Retry a CompensationFailed entry
  // Returns None on success, Some(failure) on failure
  // -------------------------------------------------------------------------
  private def retryCompensation(
      sagaId: SagaId,
      entry:  WalEntry
  ): Option[ZombieHunter.CompensationFailure] =
    entry.compensationRef match
      case None =>
        store.markCompensated(sagaId, entry.stepName)
        logger.debug(s"[ZombieHunter] '${entry.stepName}' — no ref, marked Compensated")
        None

      case Some(ref) =>
        registry.resolve(ref) match
          case None =>
            store.markHumanIntervention(sagaId, entry.stepName)
            logger.warn(s"[ZombieHunter] '${entry.stepName}' — no handler for '$ref' — HumanIntervention")
            Some(ZombieHunter.CompensationFailure(
              entry.stepName, ref,
              Exception(s"No handler registered for ref '$ref'"),
              humanIntervention = true
            ))

          case Some(handler) =>
            handler(entry.compensationArgs) match
              case Right(_) =>
                store.markCompensated(sagaId, entry.stepName)
                logger.info(s"[ZombieHunter] '${entry.stepName}' — Compensated via '$ref'")
                None
              case Left(err) =>
                store.incrementCompensationAttempts(sagaId, entry.stepName) match
                  case Right(attempts) if attempts >= maxAttempts =>
                    store.markHumanIntervention(sagaId, entry.stepName)
                    logger.warn(s"[ZombieHunter] '${entry.stepName}' — attempt $attempts/$maxAttempts — HumanIntervention")
                    Some(ZombieHunter.CompensationFailure(
                      entry.stepName, ref, err, humanIntervention = true
                    ))
                  case Right(attempts) =>
                    store.markCompensationFailed(sagaId, entry.stepName)
                    logger.warn(s"[ZombieHunter] '${entry.stepName}' — attempt $attempts/$maxAttempts — will retry")
                    Some(ZombieHunter.CompensationFailure(
                      entry.stepName, ref, err, humanIntervention = false
                    ))
                  case Left(storeErr) =>
                    store.markCompensationFailed(sagaId, entry.stepName)
                    logger.warn(s"[ZombieHunter] '${entry.stepName}' — store error: ${storeErr.getMessage}")
                    Some(ZombieHunter.CompensationFailure(
                      entry.stepName, ref, err, humanIntervention = false
                    ))

  // -------------------------------------------------------------------------
  // Compensate a Done entry during LIFO continuation
  // Same logic as retryCompensation but for entries in Done state
  // -------------------------------------------------------------------------
  private def compensateDoneEntry(
      sagaId: SagaId,
      entry:  WalEntry
  ): Option[ZombieHunter.CompensationFailure] =
    entry.compensationRef match
      case None =>
        store.markCompensated(sagaId, entry.stepName)
        None

      case Some(ref) =>
        registry.resolve(ref) match
          case None =>
            store.markHumanIntervention(sagaId, entry.stepName)
            logger.warn(s"[ZombieHunter] '${entry.stepName}' — no handler for '$ref' — HumanIntervention")
            Some(ZombieHunter.CompensationFailure(
              entry.stepName, ref,
              Exception(s"No handler registered for ref '$ref'"),
              humanIntervention = true
            ))

          case Some(handler) =>
            handler(entry.compensationArgs) match
              case Right(_) =>
                store.markCompensated(sagaId, entry.stepName)
                logger.info(s"[ZombieHunter] '${entry.stepName}' — Compensated via '$ref'")
                None
              case Left(err) =>
                store.markCompensationFailed(sagaId, entry.stepName)
                logger.warn(s"[ZombieHunter] '${entry.stepName}' — compensation failed during LIFO — will retry: ${err.getMessage}")
                Some(ZombieHunter.CompensationFailure(
                  entry.stepName, ref, err, humanIntervention = false
                ))


object ZombieHunter:

  case class CompensationFailure(
      stepName:          String,
      ref:               String,
      cause:             Throwable,
      humanIntervention: Boolean = false
  )

  enum Result:
    case Recovered(sagaId: SagaId)
    case PartiallyRecovered(sagaId: SagaId, failures: List[CompensationFailure])
    case HumanInterventionRequired(sagaId: SagaId, failures: List[CompensationFailure])
    case StoreError(cause: Throwable)

  class Handle(executor: ScheduledExecutorService):
    def stop(): Unit =
      executor.shutdown()
      executor.awaitTermination(10, TimeUnit.SECONDS)
    def stopNow(): Unit =
      executor.shutdownNow()
