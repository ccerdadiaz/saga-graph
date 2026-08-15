package sagagraph

import scala.concurrent.ExecutionContext

class SagaGraph private (elements: List[SagaElement]):

  def step(
      name: String,
      action: () => Either[Throwable, Unit],
      compensate: () => Either[Throwable, Unit],
      ref: String = "",
      args: CompArgs = CompArgs.empty
  ): SagaGraph =
    val node = MandatoryStep(
      name = name,
      run = action,
      compensate = compensate,
      compensationRef = ref,
      compensationArgs = args.toJson
    )
    SagaGraph(elements :+ SagaElement.Single(node))

  def optional(
      name: String,
      action: () => Either[Throwable, Unit],
      compensate: () => Either[Throwable, Unit],
      ref: String = "",
      args: CompArgs = CompArgs.empty
  ): SagaGraph =
    val node = OptionalStep(
      name = name,
      run = action,
      compensate = compensate,
      compensationRef = ref,
      compensationArgs = args.toJson
    )
    SagaGraph(elements :+ SagaElement.Single(node))

  def bestEffort(
      name: String,
      action: () => Either[Throwable, Unit]
  ): SagaGraph =
    val node = BestEffortStep(name = name, run = action)
    SagaGraph(elements :+ SagaElement.Single(node))

  def parallel(nodes: SagaGraph.ParNode*): SagaGraph =
    val forkNodes = nodes.toList.map(p =>
      MandatoryStep(
        name = p.name,
        run = p.action,
        compensate = p.compensate,
        compensationRef = p.ref,
        compensationArgs = p.args.toJson
      )
    )
    SagaGraph(elements :+ SagaElement.Parallel(forkNodes))

  def run(
      store: WalStore = InMemoryWalStore(),
      ec: ExecutionContext = ExecutionContext.global
  ): SagaResult =
    val sagaId = SagaId.generate()
    val engine = SagaEngine(sagaId, store, ec)
    engine.run(elements)

object SagaGraph:

  def apply(elements: List[SagaElement] = List.empty): SagaGraph =
    new SagaGraph(elements)

  case class ParNode(
      name: String,
      action: () => Either[Throwable, Unit],
      compensate: () => Either[Throwable, Unit],
      ref: String = "",
      args: CompArgs = CompArgs.empty
  )

  def par(
      name: String,
      action: () => Either[Throwable, Unit],
      compensate: () => Either[Throwable, Unit],
      ref: String = "",
      args: CompArgs = CompArgs.empty
  ): ParNode = ParNode(name, action, compensate, ref, args)
