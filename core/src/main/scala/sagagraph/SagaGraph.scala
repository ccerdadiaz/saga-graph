package sagagraph

// ---------------------------------------------------------------------------
// SagaGraph — fluent builder for saga execution graphs
//
// Usage:
//   SagaGraph()
//     .step("createOrder", action, compensate)
//     .parallel(
//       SagaGraph.par("reserveLogical", action, compensate),
//       SagaGraph.par("reservePhysical", action, compensate)
//     )
//     .step("confirmInstallation", action, compensate)
//     .optional("transferDoc", action, compensate)
//     .bestEffort("notifySms", action, compensate)
//     .run()
// ---------------------------------------------------------------------------
class SagaGraph private (elements: List[SagaElement]):

  def step(
    name:       String,
    action:     () => Either[Throwable, Unit],
    compensate: () => Either[Throwable, Unit]
  ): SagaGraph =
    val node = SagaNode(name, StepSemantics.Mandatory, action, compensate)
    SagaGraph(elements :+ SagaElement.Single(node))

  def optional(
    name:       String,
    action:     () => Either[Throwable, Unit],
    compensate: () => Either[Throwable, Unit]
  ): SagaGraph =
    val node = SagaNode(name, StepSemantics.Optional, action, compensate)
    SagaGraph(elements :+ SagaElement.Single(node))

  def bestEffort(
    name:   String,
    action: () => Either[Throwable, Unit]
  ): SagaGraph =
    // bestEffort has no meaningful compensation — noop
    val node = SagaNode(name, StepSemantics.BestEffort, action, () => Right(()))
    SagaGraph(elements :+ SagaElement.Single(node))

  def parallel(nodes: SagaGraph.ParNode*): SagaGraph =
    val forkNodes = nodes.toList.map(p =>
      SagaNode(p.name, StepSemantics.Mandatory, p.action, p.compensate)
    )
    SagaGraph(elements :+ SagaElement.Parallel(SagaFork(forkNodes)))

  def run(): SagaResult =
    SagaEngine.run(elements)


object SagaGraph:

  def apply(elements: List[SagaElement] = List.empty): SagaGraph =
    new SagaGraph(elements)

  case class ParNode(
    name:       String,
    action:     () => Either[Throwable, Unit],
    compensate: () => Either[Throwable, Unit]
  )

  def par(
    name:       String,
    action:     () => Either[Throwable, Unit],
    compensate: () => Either[Throwable, Unit]
  ): ParNode = ParNode(name, action, compensate)