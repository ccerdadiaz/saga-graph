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
class SagaEngine(sagaId: SagaId, store: WalStore):

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

    val results = steps.map(s => s.name -> s.run())

    results.collectFirst { case (_, Left(err)) => err } match
      case None      => Right(forkWal)
      case Some(err) =>
        // Mark failed actions — no compensation needed for them
        results.foreach {
          case (name, Left(_))  => store.markActionFailed(sagaId, name)
          case (name, Right(_)) => () // succeeded — will be compensated
        }
        Left((err, forkWal))

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
