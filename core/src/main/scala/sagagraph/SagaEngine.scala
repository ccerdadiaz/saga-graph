package sagagraph

// ---------------------------------------------------------------------------
// SagaEngine — executes a saga graph, manages WAL and compensation
//
// Changes from previous version:
//   - Receives SagaId and WalStore — WAL is now durable, not only in-memory
//   - append() called BEFORE each action (WAL invariant preserved)
//   - markCompensated() called after each successful compensation
//   - complete() called when saga finishes successfully
//   - SagaEngine is instantiated per execution, not a static object
// ---------------------------------------------------------------------------
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.*

class SagaEngine(sagaId: SagaId, store: WalStore, ec: ExecutionContext):

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
          s.name,
          s.compensate,
          Some(s.compensationRef),
          Some(s.compensationArgs)
        )
        val updatedWal = entry :: wal
        store.append(sagaId, entry) // WAL before action
        s.run() // failure recorded but saga continues
        Right(updatedWal)

      case s: MandatoryStep =>
        val entry = WalEntry(
          s.name,
          s.compensate,
          Some(s.compensationRef),
          Some(s.compensationArgs)
        )
        val updatedWal = entry :: wal
        store.append(sagaId, entry) // WAL before action — invariant
        s.run() match
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

    given ExecutionContext =
      ec // make ec available implicitly for Future.sequence

    val entries = steps.map(s =>
      WalEntry(
        s.name,
        s.compensate,
        Some(s.compensationRef),
        Some(s.compensationArgs)
      )
    )
    entries.foreach(store.append(sagaId, _))
    val forkWal = entries.reverse ++ wal

    val futures = steps.map(s => Future(s.name -> s.run()))
    val results = Await.result(Future.sequence(futures), 30.seconds)

    val failures = results.collect { case (name, Left(err)) => name -> err }

    if failures.isEmpty then Right(forkWal)
    else
      failures.foreach { case (name, _) =>
        store.markActionFailed(sagaId, name)
      }
      Left((ParallelForkException(failures), forkWal))

  // -------------------------------------------------------------------------
  // Compensate in LIFO order — mark each step after successful compensation
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
        println(
          s"[WARN] Compensation failed for '${entry.stepName}': ${err.getMessage}"
        )
