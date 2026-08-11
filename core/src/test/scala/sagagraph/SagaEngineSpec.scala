package sagagraph

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SagaEngineSpec extends AnyFlatSpec with Matchers:

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------
  def success(
    name:       String,
    executed:   scala.collection.mutable.ListBuffer[String],
    compensated: scala.collection.mutable.ListBuffer[String]
  ): (String, () => Either[Throwable, Unit], () => Either[Throwable, Unit]) =
    (name,
     () => { executed += name; Right(()) },
     () => { compensated += name; Right(()) })

  def failing(
    name:       String,
    executed:   scala.collection.mutable.ListBuffer[String],
    compensated: scala.collection.mutable.ListBuffer[String]
  ): (String, () => Either[Throwable, Unit], () => Either[Throwable, Unit]) =
    (name,
     () => { executed += name; Left(RuntimeException(s"$name failed")) },
     () => { compensated += name; Right(()) })

  // -------------------------------------------------------------------------
  // TEST 1: Happy path
  // -------------------------------------------------------------------------
  "SagaGraph" should "complete successfully when all steps succeed" in:
    val executed    = scala.collection.mutable.ListBuffer.empty[String]
    val compensated = scala.collection.mutable.ListBuffer.empty[String]

    val (n1, a1, c1) = success("step1", executed, compensated)
    val (n2, a2, c2) = success("step2", executed, compensated)
    val (n3, a3, c3) = success("step3", executed, compensated)

    val result = SagaGraph()
      .step(n1, a1, c1)
      .step(n2, a2, c2)
      .step(n3, a3, c3)
      .run()

    result shouldBe SagaResult.Completed
    executed.toList    shouldBe List("step1", "step2", "step3")
    compensated.toList shouldBe List.empty

  // -------------------------------------------------------------------------
  // TEST 2: Failure — LIFO compensation
  // -------------------------------------------------------------------------
  it should "compensate completed steps in LIFO order on failure" in:
    val executed    = scala.collection.mutable.ListBuffer.empty[String]
    val compensated = scala.collection.mutable.ListBuffer.empty[String]

    val (n1, a1, c1) = success("step1", executed, compensated)
    val (n2, a2, c2) = failing("step2", executed, compensated)
    val (n3, a3, c3) = success("step3", executed, compensated)

    val result = SagaGraph()
      .step(n1, a1, c1)
      .step(n2, a2, c2)
      .step(n3, a3, c3)
      .run()

    result shouldBe a[SagaResult.Failed]
    executed.toList    shouldBe List("step1", "step2")
    compensated.toList shouldBe List("step2", "step1")

  // -------------------------------------------------------------------------
  // TEST 3: Optional step fails — saga continues
  // -------------------------------------------------------------------------
  it should "continue after optional step failure" in:
    val executed    = scala.collection.mutable.ListBuffer.empty[String]
    val compensated = scala.collection.mutable.ListBuffer.empty[String]

    val (n1, a1, c1) = success("step1", executed, compensated)
    val (n2, a2, c2) = failing("optional", executed, compensated)
    val (n3, a3, c3) = success("step3", executed, compensated)

    val result = SagaGraph()
      .step(n1, a1, c1)
      .optional(n2, a2, c2)
      .step(n3, a3, c3)
      .run()

    result shouldBe SagaResult.Completed
    executed.toList shouldBe List("step1", "optional", "step3")
    compensated.toList shouldBe List.empty

  // -------------------------------------------------------------------------
  // TEST 4: BestEffort fails — ignored silently
  // -------------------------------------------------------------------------
  it should "ignore bestEffort step failure silently" in:
    val executed    = scala.collection.mutable.ListBuffer.empty[String]
    val compensated = scala.collection.mutable.ListBuffer.empty[String]

    val (n1, a1, c1) = success("step1", executed, compensated)
    val (n2, a2, _)  = failing("sms", executed, compensated)
    val (n3, a3, c3) = success("step3", executed, compensated)

    val result = SagaGraph()
      .step(n1, a1, c1)
      .bestEffort(n2, a2)
      .step(n3, a3, c3)
      .run()

    result shouldBe SagaResult.Completed
    executed.toList    shouldBe List("step1", "sms", "step3")
    compensated.toList shouldBe List.empty

  // -------------------------------------------------------------------------
  // TEST 5: Parallel fork — all succeed
  // -------------------------------------------------------------------------
  it should "execute parallel fork and complete when all succeed" in:
    val executed    = scala.collection.mutable.ListBuffer.empty[String]
    val compensated = scala.collection.mutable.ListBuffer.empty[String]

    val (n1, a1, c1) = success("step1", executed, compensated)
    val (n2, a2, c2) = success("par-A", executed, compensated)
    val (n3, a3, c3) = success("par-B", executed, compensated)
    val (n4, a4, c4) = success("par-C", executed, compensated)
    val (n5, a5, c5) = success("step5", executed, compensated)

    val result = SagaGraph()
      .step(n1, a1, c1)
      .parallel(
        SagaGraph.par(n2, a2, c2),
        SagaGraph.par(n3, a3, c3),
        SagaGraph.par(n4, a4, c4)
      )
      .step(n5, a5, c5)
      .run()

    result shouldBe SagaResult.Completed
    executed should contain("step1")
    executed should contain("par-A")
    executed should contain("par-B")
    executed should contain("par-C")
    executed should contain("step5")
    compensated.toList shouldBe List.empty

  // -------------------------------------------------------------------------
  // TEST 6: Parallel fork — one fails, all compensated
  // -------------------------------------------------------------------------
  it should "compensate all fork nodes and previous steps when fork fails" in:
    val executed    = scala.collection.mutable.ListBuffer.empty[String]
    val compensated = scala.collection.mutable.ListBuffer.empty[String]

    val (n1, a1, c1) = success("step1",  executed, compensated)
    val (n2, a2, c2) = success("par-A",  executed, compensated)
    val (n3, a3, c3) = failing("par-B",  executed, compensated)
    val (n4, a4, c4) = success("par-C",  executed, compensated)
    val (n5, a5, c5) = success("step5",  executed, compensated)

    val result = SagaGraph()
      .step(n1, a1, c1)
      .parallel(
        SagaGraph.par(n2, a2, c2),
        SagaGraph.par(n3, a3, c3),
        SagaGraph.par(n4, a4, c4)
      )
      .step(n5, a5, c5)
      .run()

    result shouldBe a[SagaResult.Failed]
    executed should contain("step1")
    executed should contain("par-B")
    compensated should contain("par-A")
    compensated should contain("par-B")
    compensated should contain("par-C")
    compensated should contain("step1")
    executed shouldNot contain("step5")