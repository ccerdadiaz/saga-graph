package sagagraph

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

class SagaGraph private (elements: List[SagaElement]):

  def step(
      name:       String,
      action:     () => Either[Throwable, Unit],
      compensate: () => Either[Throwable, Unit],
      ref:        Option[String] = None,
      args:       CompArgs       = CompArgs.empty,
      ttl:        Duration       = 30.seconds
  ): SagaGraph =
    val node = MandatoryStep(
      name             = name,
      run              = action,
      compensate       = compensate,
      compensationRef  = ref,
      compensationArgs = Some(args.toJson).filter(_ != "{}"),
      ttl              = ttl
    )
    SagaGraph(elements :+ SagaElement.Single(node))

  def optional(
      name:       String,
      action:     () => Either[Throwable, Unit],
      compensate: () => Either[Throwable, Unit],
      ref:        Option[String] = None,
      args:       CompArgs       = CompArgs.empty,
      ttl:        Duration       = 30.seconds
  ): SagaGraph =
    val node = OptionalStep(
      name             = name,
      run              = action,
      compensate       = compensate,
      compensationRef  = ref,
      compensationArgs = Some(args.toJson).filter(_ != "{}"),
      ttl              = ttl
    )
    SagaGraph(elements :+ SagaElement.Single(node))

  def bestEffort(
      name:   String,
      action: () => Either[Throwable, Unit],
      ttl:    Duration = 5.seconds
  ): SagaGraph =
    val node = BestEffortStep(name = name, run = action, ttl = ttl)
    SagaGraph(elements :+ SagaElement.Single(node))

  def parallel(nodes: SagaGraph.ParNode*): SagaGraph =
    val forkNodes = nodes.toList.map(p =>
      MandatoryStep(
        name             = p.name,
        run              = p.action,
        compensate       = p.compensate,
        compensationRef  = p.ref,
        compensationArgs = Some(p.args.toJson).filter(_ != "{}"),
        ttl              = p.ttl
      )
    )
    SagaGraph(elements :+ SagaElement.Parallel(forkNodes))

  def run(
      store:  WalStore        = InMemoryWalStore(),
      ec:     ExecutionContext = ExecutionContext.global,
      logger: SagaLogger      = SagaLogger.noOp,
      sagaId: SagaId          = SagaId.generate()
  ): SagaResult =
    val engine = SagaEngine(sagaId, store, ec, logger)
    engine.run(elements)

object SagaGraph:

  def apply(elements: List[SagaElement] = List.empty): SagaGraph =
    new SagaGraph(elements)

  case class ParNode(
      name:       String,
      action:     () => Either[Throwable, Unit],
      compensate: () => Either[Throwable, Unit],
      ref:        Option[String] = None,
      args:       CompArgs       = CompArgs.empty,
      ttl:        Duration       = 30.seconds
  )

  def par(
      name:       String,
      action:     () => Either[Throwable, Unit],
      compensate: () => Either[Throwable, Unit],
      ref:        Option[String] = None,
      args:       CompArgs       = CompArgs.empty,
      ttl:        Duration       = 30.seconds
  ): ParNode = ParNode(name, action, compensate, ref, args, ttl)
