package sagagraph

// ---------------------------------------------------------------------------
// SagaContext — scoped access to the current SagaId
//
// Uses Java 22+ ScopedValue for automatic propagation across threads,
// including Future fork branches — no ThreadLocal, no MDC, no dependencies.
//
// Requirements: Java 22+
// Note: for Java 17/21 compatibility, InheritableThreadLocal is an alternative
//       with limitations in virtual thread environments.
// ---------------------------------------------------------------------------
object SagaContext:

  private val _sagaId: ScopedValue[SagaId] = ScopedValue.newInstance()

  // Run a block with the given SagaId bound to the current scope
  def run[A](id: SagaId)(block: => A): A =
    ScopedValue.where(_sagaId, id).call(() => block)

  // Access the current SagaId if bound — None outside a saga execution
  def current: Option[SagaId] =
    if _sagaId.isBound then Some(_sagaId.get()) else None
