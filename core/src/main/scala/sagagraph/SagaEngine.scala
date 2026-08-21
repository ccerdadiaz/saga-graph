package sagagraph

import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.*
import scala.util.{Try, Success, Failure}

// ---------------------------------------------------------------------------
// SagaEngine — executes a saga graph, manages WAL and compensation
//
// Responsibilities:
//   1. Register the saga in the WAL with Running status
//   2. Execute each element in order (Single or Parallel)
//   3. Register compensation in WAL BEFORE executing action
//   4. Record Running and Done/Failed/Unknown status for each step
//   5. On failure: compensate in reverse order (LIFO) until all Done
//      steps are Compensated OR until a compensation fails
//   6. Respect step type (Mandatory / Optional / BestEffort)
//   7. Mark saga terminal state:
//      - All compensated → Compensated
//      - Compensation failed → Compensating (ZombieHunter picks it up)
//
// TTL policy:
//   Every step has a TTL. If the action does not respond within the TTL,
//   the step is marked Unknown. Unknown steps are treated as compensation
//   candidates by the ZombieHunter — the service may or may not have acted.
//
// Compensation policy:
//   The engine compensates in LIFO order and STOPS on first failure.
//   It does NOT retry — ZombieHunter is responsible for retrying failed
//   compensations and continuing the LIFO from where the engine stopped.
// ---------------------------------------------------------------------------
class SagaEngine(
    sagaId: SagaId,
    store:  WalStore,
    ec:     ExecutionContext,
    logger: SagaLogger = SagaLogger.noOp
):

  def run(elements: List[SagaElement]): SagaResult =
    SagaContext.run(sagaId):
      store.registerSaga(sagaId)
      execute(elements, wal = List.empty)

  // -------------------------------------------------------------------------
  // Recursive traversal — executes each SagaElement in order
  // Depth is bounded by the number of steps defined at saga construction time
  // -------------------------------------------------------------------------
  private def execute(
      remaining: List[SagaElement],
      wal:       List[WalEntry]
  ): SagaResult =
    remaining match
      case Nil =>
        store.markSagaCompleted(sagaId)
        SagaResult.Completed

      case head :: tail =>
        head match
          case SagaElement.Single(step) =>
            runStep(step, wal) match
              case Right(updatedWal) => execute(tail, updatedWal)
              case Left((error, updatedWal)) =>
                store.markSagaCompensating(sagaId)
                val stopped = compensate(updatedWal)
                if stopped then
                  logger.warn(s"Compensation stopped — ZombieHunter will retry")
                else
                  store.markSagaCompensated(sagaId)
                SagaResult.Failed(error)

          case SagaElement.Parallel(steps) =>
            runParallel(steps, wal) match
              case Right(updatedWal) => execute(tail, updatedWal)
              case Left((error, updatedWal)) =>
                store.markSagaCompensating(sagaId)
                val stopped = compensate(updatedWal)
                if stopped then
                  logger.warn(s"Compensation stopped — ZombieHunter will retry")
                else
                  store.markSagaCompensated(sagaId)
                SagaResult.Failed(error)

  // -------------------------------------------------------------------------
  // Execute a single step — dispatch by type
  // All steps run within their TTL — timeout → Unknown
  // -------------------------------------------------------------------------
  private def runStep(
      step: SagaStep,
      wal:  List[WalEntry]
  ): Either[(Throwable, List[WalEntry]), List[WalEntry]] =
    step match

      case s: BestEffortStep =>
        // No WAL entry — failure and timeout silently ignored
        // BestEffort steps should be fire-and-forget — TTL is a safety net
        runWithTtl(s.run, s.ttl) match
          case _ => Right(wal) // always continue

      case s: OptionalStep =>
        val entry = WalEntry(
          stepName         = s.name,
          compensate       = s.compensate,
          compensationRef  = s.compensationRef,
          compensationArgs = s.compensationArgs
        )
        val updatedWal = entry :: wal
        store.append(sagaId, entry)
        store.markRunning(sagaId, s.name)
        runWithTtl(s.run, s.ttl) match
          case Right(_)              => store.markDone(sagaId, s.name)
          case Left(_: TimeoutError) => store.markUnknown(sagaId, s.name)
          case Left(_)               => store.markFailed(sagaId, s.name)
        Right(updatedWal) // always continue

      case s: MandatoryStep =>
        val entry = WalEntry(
          stepName         = s.name,
          compensate       = s.compensate,
          compensationRef  = s.compensationRef,
          compensationArgs = s.compensationArgs
        )
        val updatedWal = entry :: wal
        store.append(sagaId, entry)
        store.markRunning(sagaId, s.name)
        runWithTtl(s.run, s.ttl) match
          case Right(_) =>
            store.markDone(sagaId, s.name)
            Right(updatedWal)
          case Left(err: TimeoutError) =>
            store.markUnknown(sagaId, s.name)
            logger.warn(s"Step '${s.name}' timed out — marked Unknown, ZombieHunter will compensate")
            Left((err, updatedWal))
          case Left(err) =>
            store.markFailed(sagaId, s.name)
            Left((err, updatedWal))

  // -------------------------------------------------------------------------
  // Execute parallel steps — all-or-nothing
  // ALL compensations registered in WAL before ANY action runs
  // Each branch runs within its own TTL
  // -------------------------------------------------------------------------
  private def runParallel(
      steps: List[MandatoryStep],
      wal:   List[WalEntry]
  ): Either[(Throwable, List[WalEntry]), List[WalEntry]] =

    given ExecutionContext = ec

    val entries = steps.map(s =>
      WalEntry(
        stepName         = s.name,
        compensate       = s.compensate,
        compensationRef  = s.compensationRef,
        compensationArgs = s.compensationArgs
      )
    )
    entries.foreach(store.append(sagaId, _))
    val forkWal = entries.reverse ++ wal

    // Launch all steps concurrently — each within its own TTL
    val futures = steps.map(s =>
      Future {
        // Re-bind sagaId — fork threads do not inherit the parent ScopedValue scope
        SagaContext.run(sagaId):
          store.markRunning(sagaId, s.name)
          val result = s.name -> runWithTtl(s.run, s.ttl)
          result match
            case (_, Right(_))              => store.markDone(sagaId, s.name)
            case (_, Left(_: TimeoutError)) =>
              store.markUnknown(sagaId, s.name)
              logger.warn(s"Fork step '${s.name}' timed out — marked Unknown")
            case (_, Left(_))               => store.markFailed(sagaId, s.name)
          result
      }
    )
    // Fork timeout: max TTL of all branches + 5s buffer
    val forkTimeout = steps.map(_.ttl).maxBy(_.toMillis) + 5.seconds
    val results     = Await.result(Future.sequence(futures), forkTimeout)

    val failures = results.collect { case (name, Left(err)) => name -> err }

    if failures.isEmpty then Right(forkWal)
    else Left((ParallelForkException(failures), forkWal))

  // -------------------------------------------------------------------------
  // Compensate in LIFO order — STOPS on first failure
  //
  // Returns true if compensation stopped (a step failed to compensate)
  // Returns false if all steps were compensated successfully
  //
  // Steps in Failed status are skipped — service guarantees clean state.
  // Steps in Registered status are skipped — action was never executed.
  // Steps in Unknown status ARE compensated — service may have acted.
  // -------------------------------------------------------------------------
  private def compensate(wal: List[WalEntry]): Boolean =
    var stopped = false
    val iter    = wal.iterator
    while iter.hasNext && !stopped do
      val entry = iter.next()
      store.getStatus(sagaId, entry.stepName) match
        case Right(WalEntry.Status.Failed)      => () // skip — service guarantees clean state
        case Right(WalEntry.Status.Registered)  => () // skip — action was never executed
        case Right(WalEntry.Status.Compensated) => () // skip — already compensated
        case _ =>
          // Done and Unknown — compensate idempotently
          entry.compensate() match
            case Right(_) =>
              store.markCompensated(sagaId, entry.stepName)
            case Left(err) =>
              store.markCompensationFailed(sagaId, entry.stepName)
              logger.warn(s"Compensation failed for '${entry.stepName}' — stopping LIFO, ZombieHunter will retry: ${err.getMessage}")
              stopped = true
    stopped

  // -------------------------------------------------------------------------
  // Run an action within its TTL
  // Returns Left(TimeoutError) if the action does not respond in time
  // -------------------------------------------------------------------------
  private def runWithTtl(
      action: () => Either[Throwable, Unit],
      ttl:    Duration
  ): Either[Throwable, Unit] =
    given ExecutionContext = ec
    Try(Await.result(Future(action()), ttl)) match
      case Success(result) => result
      case Failure(_)      => Left(TimeoutError(ttl))

// ---------------------------------------------------------------------------
// Timeout error — step did not respond within its TTL
// ---------------------------------------------------------------------------
case class TimeoutError(ttl: Duration)
    extends Exception(s"Step timed out after ${ttl.toMillis}ms")
