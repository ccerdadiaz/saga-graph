package sagagraph

// ---------------------------------------------------------------------------
// SagaEngine — executes a saga graph, manages WAL and compensation
//
// Responsibilities:
//   1. Execute each element in order (Single or Parallel)
//   2. Register compensation in WAL BEFORE executing action
//   3. On failure: compensate in reverse order (LIFO)
//   4. Respect step type (Mandatory / Optional / BestEffort)
// ---------------------------------------------------------------------------
object SagaEngine:

  def run(elements: List[SagaElement]): SagaResult =
    execute(elements, wal = List.empty)

  private def execute(
    remaining: List[SagaElement],
    wal:       List[WalEntry]
  ): SagaResult =
    remaining match
      case Nil =>
        SagaResult.Completed

      case head :: tail =>
        head match
          case SagaElement.Single(step) =>
            runStep(step, wal) match
              case Right(updatedWal) =>
                execute(tail, updatedWal)
              case Left((error, updatedWal)) =>
                compensate(updatedWal)
                SagaResult.Failed(error)

          case SagaElement.Parallel(steps) =>
            runParallel(steps, wal) match
              case Right(updatedWal) =>
                execute(tail, updatedWal)
              case Left((error, updatedWal)) =>
                compensate(updatedWal)
                SagaResult.Failed(error)

  // -------------------------------------------------------------------------
  // Execute a single step — dispatch by type
  // -------------------------------------------------------------------------
  private def runStep(
    step: SagaStep,
    wal:  List[WalEntry]
  ): Either[(Throwable, List[WalEntry]), List[WalEntry]] =
    step match
      case s: BestEffortStep =>
        s.run()  // ignore result completely
        Right(wal)

      case s: OptionalStep =>
        val entry      = walEntry(s.name, s.compensate, None, None)
        val updatedWal = entry :: wal
        s.run() // failure recorded in wal but saga continues
        Right(updatedWal)

      case s: MandatoryStep =>
        val entry      = walEntry(s.name, s.compensate,
                           Some(s.compensationRef),
                           Some(s.compensationArgs))
        val updatedWal = entry :: wal
        s.run() match
          case Right(_)  => Right(updatedWal)
          case Left(err) => Left((err, updatedWal))

  // -------------------------------------------------------------------------
  // Execute parallel steps — all-or-nothing
  // All compensations registered before any action runs
  // -------------------------------------------------------------------------
  private def runParallel(
    steps: List[MandatoryStep],
    wal:   List[WalEntry]
  ): Either[(Throwable, List[WalEntry]), List[WalEntry]] =

    // Register ALL compensations in WAL before executing any action
    val forkWal = steps.map(s =>
      walEntry(s.name, s.compensate,
        Some(s.compensationRef),
        Some(s.compensationArgs))
    ) ++ wal

    // Execute all steps — collect results
    val results = steps.map(s => (s.name, s.run()))

    // Check for failures
    results.collectFirst { case (name, Left(err)) => err } match
      case Some(err) => Left((err, forkWal))
      case None      => Right(forkWal)

  // -------------------------------------------------------------------------
  // Compensate in LIFO order
  // -------------------------------------------------------------------------
  private def compensate(wal: List[WalEntry]): Unit =
    wal.foreach(_.compensate())

  // -------------------------------------------------------------------------
  // Build a WAL entry
  // -------------------------------------------------------------------------
  private def walEntry(
    name:    String,
    comp:    () => Either[Throwable, Unit],
    ref:     Option[String],
    args:    Option[String]
  ): WalEntry =
    WalEntry(name, comp, ref, args)