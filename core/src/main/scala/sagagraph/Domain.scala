package sagagraph

// ---------------------------------------------------------------------------
// Unique identifier for a saga execution
// ---------------------------------------------------------------------------
opaque type SagaId = String
object SagaId:
  def apply(v: String): SagaId = v
  def generate(): SagaId       = java.util.UUID.randomUUID().toString
  extension (s: SagaId) def value: String = s

// ---------------------------------------------------------------------------
// Step semantics — controls compensation behavior on failure
// ---------------------------------------------------------------------------
enum StepSemantics:
  case Mandatory   // failure compensates the entire graph
  case Optional    // failure recorded, saga continues
  case BestEffort  // failure silently ignored

// ---------------------------------------------------------------------------
// A graph node — pre-applied functions, no type parameter exposed
// ---------------------------------------------------------------------------
case class SagaNode(
  name:       String,
  semantics:  StepSemantics,
  run:        () => Either[Throwable, Unit],
  compensate: () => Either[Throwable, Unit]
)

// ---------------------------------------------------------------------------
// Fork — parallel nodes with all-or-nothing semantics
// ---------------------------------------------------------------------------
case class SagaFork(nodes: List[SagaNode])

// ---------------------------------------------------------------------------
// Graph element — either a single node or a parallel fork
// ---------------------------------------------------------------------------
enum SagaElement:
  case Single(node: SagaNode)
  case Parallel(fork: SagaFork)

// ---------------------------------------------------------------------------
// Saga execution status
// ---------------------------------------------------------------------------
enum SagaStatus:
  case Running
  case Completed
  case Compensating
  case Compensated
  case Failed(cause: Throwable)

// ---------------------------------------------------------------------------
// WAL entry — compensation already captured in closure
// ---------------------------------------------------------------------------
case class SagaLogEntry(
  stepName:   String,
  semantics:  StepSemantics,
  compensate: () => Either[Throwable, Unit]
)