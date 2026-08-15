package sagagraph

import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.*

// ---------------------------------------------------------------------------
// SagaEngine — executes a saga graph, manages WAL and compensation
//
// Responsibilities:
//   1. Execute each element in order (Single or Parallel)
//   2. Register compensation in WAL BEFORE executing action
//   3. Record started_at and finished_at for each action
//   4. On failure: compensate in reverse order (LIFO)
//   5. Respect step type (Mandatory / Optional / BestEffort)
// ---------------------------------------------------------------------------
class SagaEngine(
    sagaId: SagaId,
    store: WalStore,
    ec: ExecutionContext,
    logger: SagaLogger = SagaLogger.noOp
):

  def run(elements: List[SagaElement]): SagaResult =
    execute(elements, wal = List.empty)

  // -------------------------------------------------------------------------
  // Main recursive loop
  // -------------------------------------------------------------------------
  private def execute(
      remaining: List[SagaElement],
      wal: List[WalEntry]
  ): SagaResult =
    remaining match
      case Nil =>
        store.complete(sagaId)
        SagaResult.Completed

      case head :: tail =>
        head match
          case SagaElement.Single(step) =>
            runStep(step, wal) match
              case Right(updatedWal) => execute(tail, updatedWal)
              case Left((error, updatedWal)) =>
                compensate(updatedWal)
                SagaResult.Failed(error)

          case SagaElement.Parallel(steps) =>
            runParallel(steps, wal) match
              case Right(updatedWal) => execute(tail, updatedWal)
              case Left((error, updatedWal)) =>
                compensate(updatedWal)
                SagaResult.Failed(error)

  // -------------------------------------------------------------------------
  // Execute a single step — dispatch by type
  // -------------------------------------------------------------------------
  private def runStep(
      step: SagaStep,
      wal: List[WalEntry]
  ): Either[(Throwable, List[WalEntry]), List[WalEntry]] =
    step match

      case s: BestEffortStep =>
        s.run() // failure silently ignored, no WAL entry
        Right(wal)

      case s: OptionalStep =>
        val entry = WalEntry(
          stepName = s.name,
          compensate = s.compensate,
          compensationRef = s.compensationRef,
          compensationArgs = s.compensationArgs
        )
        val updatedWal = entry :: wal
        store.append(sagaId, entry)
        store.markStarted(sagaId, s.name)
        s.run()
        store.markFinished(sagaId, s.name)
        Right(updatedWal)

      case s: MandatoryStep =>
        val entry = WalEntry(
          stepName = s.name,
          compensate = s.compensate,
          compensationRef = s.compensationRef,
          compensationArgs = s.compensationArgs
        )
        val updatedWal = entry :: wal
        store.append(sagaId, entry)
        store.markStarted(sagaId, s.name)
        val result = s.run()
        store.markFinished(sagaId, s.name)
        result match
          case Right(_)  => Right(updatedWal)
          case Left(err) => Left((err, updatedWal))

  // -------------------------------------------------------------------------
  // Execute parallel steps — all-or-nothing
  // ALL compensations registered in WAL before ANY action runs
  // -------------------------------------------------------------------------
  private def runParallel(
      steps: List[MandatoryStep],
      wal: List[WalEntry]
  ): Either[(Throwable, List[WalEntry]), List[WalEntry]] =

    given ExecutionContext = ec

    val entries = steps.map(s =>
      WalEntry(
        stepName = s.name,
        compensate = s.compensate,
        compensationRef = s.compensationRef,
        compensationArgs = s.compensationArgs
      )
    )
    entries.foreach(store.append(sagaId, _))
    val forkWal = entries.reverse ++ wal

    // Launch all steps concurrently — record timing for parallelism evidence
    val futures = steps.map(s =>
      Future {
        store.markStarted(sagaId, s.name)
        val result = s.name -> s.run()
        store.markFinished(sagaId, s.name)
        result
      }
    )
    val results = Await.result(Future.sequence(futures), 30.seconds)

    // Collect ALL failures — not just the first one
    val failures = results.collect { case (name, Left(err)) => name -> err }

    if failures.isEmpty then Right(forkWal)
    else
      failures.foreach { case (name, _) =>
        store.markActionFailed(sagaId, name)
      }
      Left((ParallelForkException(failures), forkWal))

  // -------------------------------------------------------------------------
  // Compensate in LIFO order — skip ActionFailed entries
  // -------------------------------------------------------------------------
  private def compensate(wal: List[WalEntry]): Unit =
    wal.foreach { entry =>
      store.getStatus(sagaId, entry.stepName) match
        case Left(_) =>
          // Cannot determine status — compensate defensively
          runCompensation(entry)
        case Right(WalEntry.Status.ActionFailed) =>
          // Action failed — service guarantees clean state, nothing to compensate
          ()
        case Right(_) =>
          runCompensation(entry)
    }

  private def runCompensation(entry: WalEntry): Unit =
    entry.compensate() match
      case Right(_) =>
        store.markCompensated(sagaId, entry.stepName)
      case Left(err) =>
        store.markCompensationFailed(sagaId, entry.stepName)
        logger.warn(
          s"Compensation failed for '${entry.stepName}': ${err.getMessage}"
        )
