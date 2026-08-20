package sagagraph

// ---------------------------------------------------------------------------
// Unique identifier for a saga execution
// ---------------------------------------------------------------------------
opaque type SagaId = String
object SagaId:
  def apply(v: String): SagaId = v
  def generate(): SagaId = java.util.UUID.randomUUID().toString
  extension (s: SagaId) def value: String = s

// ---------------------------------------------------------------------------
// SagaStep hierarchy — each type enforces its own contract at compile time
//
// MandatoryStep — failure compensates the entire graph
// OptionalStep  — failure recorded, saga continues
// BestEffortStep — failure silently ignored, no compensation
// ---------------------------------------------------------------------------
sealed trait SagaStep:
  def name: String
  def run: () => Either[Throwable, Unit]

case class MandatoryStep(
    name: String,
    run: () => Either[Throwable, Unit],
    compensate: () => Either[Throwable, Unit],
    compensationRef: Option[String] = None,
    compensationArgs: Option[String] = None
) extends SagaStep

case class OptionalStep(
    name: String,
    run: () => Either[Throwable, Unit],
    compensate: () => Either[Throwable, Unit],
    compensationRef: Option[String] = None,
    compensationArgs: Option[String] = None
) extends SagaStep

case class BestEffortStep(
    name: String,
    run: () => Either[Throwable, Unit]
) extends SagaStep

// ---------------------------------------------------------------------------
// Graph element — single step or parallel fork
// Note: parallel fork only accepts MandatoryStep — all-or-nothing semantics
// require compensation to be defined for every branch
// ---------------------------------------------------------------------------
enum SagaElement:
  case Single(step: SagaStep)
  case Parallel(steps: List[MandatoryStep])

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
// WAL entry — compensation captured in closure + serializable reference
// ---------------------------------------------------------------------------
case class WalEntry(
    stepName: String,
    compensate: () => Either[Throwable, Unit],
    compensationRef: Option[String],
    compensationArgs: Option[String],
    status: WalEntry.Status = WalEntry.Status.Registered
)

object WalEntry:
  enum Status:
    case Registered // Action recorded, still not started
    case Running // Action started
    case Done // Action finished Ok
    case Failed // Action failed
    case Compensated // Compensation Ok
    case CompensationFailed // Compensation failed, ZH will retry
    case HumanIntervention // Not able to compensate.

case class ParallelForkException(failures: List[(String, Throwable)])
    extends Exception(
      s"Parallel fork failed in: ${failures.map(_._1).mkString(", ")}"
    )
