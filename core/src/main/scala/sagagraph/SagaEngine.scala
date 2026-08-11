package sagagraph

// ---------------------------------------------------------------------------
// SagaEngine — executes a saga graph, manages WAL and compensation
//
// Responsibilities:
//   1. Execute each element in order (Single or Parallel)
//   2. Register compensation in WAL BEFORE executing action
//   3. On failure: compensate in reverse order (LIFO)
//   4. Respect step semantics (Mandatory / Optional / BestEffort)
// ---------------------------------------------------------------------------
object SagaEngine:

  def run(elements: List[SagaElement]): SagaResult =
    execute(elements, wal = List.empty)

  private def execute(
    remaining: List[SagaElement],
    wal:       List[SagaLogEntry]
  ): SagaResult =
    remaining match
      case Nil =>
        SagaResult.Completed

      case head :: tail =>
        head match
          case SagaElement.Single(node) =>
            runNode(node, wal) match
              case Right(updatedWal) =>
                execute(tail, updatedWal)
              case Left((error, updatedWal)) =>
                compensate(updatedWal)
                SagaResult.Failed(error)

          case SagaElement.Parallel(fork) =>
            runFork(fork, wal) match
              case Right(updatedWal) =>
                execute(tail, updatedWal)
              case Left((error, updatedWal)) =>
                compensate(updatedWal)
                SagaResult.Failed(error)

  // -------------------------------------------------------------------------
  // Execute a single node — WAL before action
  // -------------------------------------------------------------------------
  private def runNode(
    node: SagaNode,
    wal:  List[SagaLogEntry]
  ): Either[(Throwable, List[SagaLogEntry]), List[SagaLogEntry]] =

    val entry = SagaLogEntry(node.name, node.semantics, node.compensate)

    node.semantics match
      case StepSemantics.BestEffort =>
        node.run()  // ignore result completely
        Right(wal)  // WAL not updated — nothing to compensate

      case StepSemantics.Optional =>
        val updatedWal = entry :: wal  // WAL before action
        node.run() match
          case Right(_)  => Right(updatedWal)
          case Left(err) => Right(updatedWal)  // failure recorded but saga continues

      case StepSemantics.Mandatory =>
        val updatedWal = entry :: wal  // WAL before action
        node.run() match
          case Right(_)  => Right(updatedWal)
          case Left(err) => Left((err, updatedWal))

  // -------------------------------------------------------------------------
  // Execute a parallel fork — all-or-nothing
  // All compensations registered before any action runs
  // -------------------------------------------------------------------------
  private def runFork(
    fork: SagaFork,
    wal:  List[SagaLogEntry]
  ): Either[(Throwable, List[SagaLogEntry]), List[SagaLogEntry]] =

    // Register ALL compensations in WAL before executing any action
    val forkWal = fork.nodes.map(n =>
      SagaLogEntry(n.name, n.semantics, n.compensate)
    ) ++ wal

    // Execute all nodes — collect results
    val results = fork.nodes.map(n => (n, n.run()))

    // Check for failures
    results.collectFirst { case (n, Left(err)) => (n, err) } match
      case Some((_, err)) =>
        Left((err, forkWal))  // any failure → compensate all
      case None =>
        Right(forkWal)        // all succeeded

  // -------------------------------------------------------------------------
  // Compensate in LIFO order
  // -------------------------------------------------------------------------
  private def compensate(wal: List[SagaLogEntry]): Unit =
    wal.foreach: entry =>
      entry.semantics match
        case StepSemantics.BestEffort =>
          ()  // never compensate bestEffort
        case _ =>
          entry.compensate() // ignore compensation errors for now