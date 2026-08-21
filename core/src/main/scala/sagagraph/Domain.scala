package sagagraph

import scala.concurrent.duration.*

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
// MandatoryStep  — failure compensates the entire graph
// OptionalStep   — failure recorded, saga continues
// BestEffortStep — failure silently ignored, no compensation
//
// All steps have a TTL — no infinite waits. If a step does not respond
// within its TTL, it is marked Unknown and treated as a compensation
// candidate by the ZombieHunter.
//
// BestEffort steps should be fire-and-forget services that never block.
// If a BestEffort step requires a meaningful TTL, consider OptionalStep.
// ---------------------------------------------------------------------------
sealed trait SagaStep:
  def name: String
  def run:  () => Either[Throwable, Unit]
  def ttl:  Duration

case class MandatoryStep(
    name:             String,
    run:              () => Either[Throwable, Unit],
    compensate:       () => Either[Throwable, Unit],
    compensationRef:  Option[String]  = None,
    compensationArgs: Option[String]  = None,
    ttl:              Duration        = 30.seconds
) extends SagaStep

case class OptionalStep(
    name:             String,
    run:              () => Either[Throwable, Unit],
    compensate:       () => Either[Throwable, Unit],
    compensationRef:  Option[String]  = None,
    compensationArgs: Option[String]  = None,
    ttl:              Duration        = 30.seconds
) extends SagaStep

case class BestEffortStep(
    name: String,
    run:  () => Either[Throwable, Unit],
    // BestEffort steps should be fire-and-forget — TTL here is a safety net.
    // If this step regularly needs more than a few seconds, use OptionalStep.
    ttl:  Duration = 5.seconds
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
    stepName:         String,
    compensate:       () => Either[Throwable, Unit],
    compensationRef:  Option[String],
    compensationArgs: Option[String],
    status:           WalEntry.Status = WalEntry.Status.Registered
)

object WalEntry:
  enum Status:
    case Registered         // Step recorded, still not started
    case Running            // Step started
    case Done               // Step finished Ok
    case Failed             // Step failed — service guarantees clean state
    case Unknown            // No response within TTL — service may or may not have acted
    case Compensated        // Compensation Ok
    case CompensationFailed // Compensation failed, ZH will retry
    case HumanIntervention  // Compensation policy exhausted — requires human action

case class ParallelForkException(failures: List[(String, Throwable)])
    extends Exception(
      s"Parallel fork failed in: ${failures.map(_._1).mkString(", ")}"
    )
