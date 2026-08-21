package sagagraph

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration.*
import scala.collection.mutable.ListBuffer

class SagaEngineTimeoutSpec extends AnyFlatSpec with Matchers:

  def store(): InMemoryWalStore = InMemoryWalStore()

  // -------------------------------------------------------------------------
  // TEST 1: MandatoryStep timeout → Unknown → saga fails → compensates previous
  // -------------------------------------------------------------------------
  "SagaEngine TTL" should "mark MandatoryStep as Unknown on timeout and compensate previous steps" in:
    val s           = store()
    val sagaId      = SagaId.generate()
    val compensated = ListBuffer.empty[String]

    val result = SagaGraph()
      .step(
        name       = "step1",
        action     = Sluggard.diligent(),
        compensate = () => { compensated += "step1"; Right(()) }
      )
      .step(
        name       = "slow-step",
        action     = Sluggard.action(500.millis),  // sleeps 500ms
        compensate = () => { compensated += "slow-step"; Right(()) },
        ttl        = 50.millis                      // TTL too short → Unknown
      )
      .run(s, sagaId = sagaId)

    result shouldBe a[SagaResult.Failed]
    s.getStatus(sagaId, "step1").toOption.get    shouldBe WalEntry.Status.Compensated
    // slow-step timed out (Unknown) — engine compensates Unknown defensively
    // by the time we check, it has already transitioned to Compensated
    s.getStatus(sagaId, "slow-step").toOption.get shouldBe WalEntry.Status.Compensated
    compensated should contain("step1")

  // -------------------------------------------------------------------------
  // TEST 2: MandatoryStep timeout → Unknown is compensated defensively by engine
  // -------------------------------------------------------------------------
  it should "compensate Unknown steps defensively — service may have acted" in:
    val s           = store()
    val sagaId      = SagaId.generate()
    val compensated = ListBuffer.empty[String]

    val result = SagaGraph()
      .step(
        name       = "slow-step",
        action     = Sluggard.action(500.millis),
        compensate = () => { compensated += "slow-step"; Right(()) },
        ttl        = 50.millis
      )
      .run(s, sagaId = sagaId)

    result shouldBe a[SagaResult.Failed]
    // Engine compensates Unknown — service may have acted
    compensated should contain("slow-step")

  // -------------------------------------------------------------------------
  // TEST 3: OptionalStep timeout → Unknown → saga continues
  // -------------------------------------------------------------------------
  it should "continue after OptionalStep timeout — saga marks step Unknown" in:
    val s      = store()
    val sagaId = SagaId.generate()

    val result = SagaGraph()
      .optional(
        name       = "slow-optional",
        action     = Sluggard.action(500.millis),
        compensate = () => Right(()),
        ttl        = 50.millis
      )
      .step(
        name       = "next-step",
        action     = Sluggard.diligent(),
        compensate = () => Right(())
      )
      .run(s, sagaId = sagaId)

    result shouldBe SagaResult.Completed
    s.getStatus(sagaId, "slow-optional").toOption.get shouldBe WalEntry.Status.Unknown
    s.getStatus(sagaId, "next-step").toOption.get     shouldBe WalEntry.Status.Done

  // -------------------------------------------------------------------------
  // TEST 4: BestEffortStep timeout → ignored → saga continues
  // -------------------------------------------------------------------------
  it should "ignore BestEffortStep timeout — saga continues silently" in:
    val s      = store()
    val sagaId = SagaId.generate()

    val result = SagaGraph()
      .bestEffort(
        name   = "slow-besteffort",
        action = Sluggard.action(500.millis),
        ttl    = 50.millis
      )
      .step(
        name       = "next-step",
        action     = Sluggard.diligent(),
        compensate = () => Right(())
      )
      .run(s, sagaId = sagaId)

    result shouldBe SagaResult.Completed
    s.getStatus(sagaId, "next-step").toOption.get shouldBe WalEntry.Status.Done

  // -------------------------------------------------------------------------
  // TEST 5: Step within TTL completes normally
  // -------------------------------------------------------------------------
  it should "complete normally when step finishes within TTL" in:
    val s      = store()
    val sagaId = SagaId.generate()

    val result = SagaGraph()
      .step(
        name       = "fast-enough",
        action     = Sluggard.action(50.millis),  // 50ms delay
        compensate = () => Right(()),
        ttl        = 500.millis                    // generous TTL
      )
      .run(s, sagaId = sagaId)

    result shouldBe SagaResult.Completed
    s.getStatus(sagaId, "fast-enough").toOption.get shouldBe WalEntry.Status.Done

  // -------------------------------------------------------------------------
  // TEST 6: Parallel fork — one branch times out → fork fails → all compensate
  // -------------------------------------------------------------------------
  it should "fail parallel fork when one branch times out and compensate successful branches" in:
    val s           = store()
    val compensated = ListBuffer.empty[String]

    val sagaId = SagaId.generate()
    val result = SagaGraph()
      .parallel(
        SagaGraph.par(
          name       = "fast-branch",
          action     = Sluggard.diligent(),
          compensate = () => { compensated += "fast-branch"; Right(()) }
        ),
        SagaGraph.par(
          name       = "slow-branch",
          action     = Sluggard.action(500.millis),
          compensate = () => { compensated += "slow-branch"; Right(()) },
          ttl        = 50.millis
        )
      )
      .run(s, sagaId = sagaId)

    result shouldBe a[SagaResult.Failed]
    compensated should contain("fast-branch")  // Done → compensated
    // slow-branch timed out (Unknown) — engine compensates Unknown defensively
    s.getStatus(sagaId, "fast-branch").toOption.get  shouldBe WalEntry.Status.Compensated
    s.getStatus(sagaId, "slow-branch").toOption.get  shouldBe WalEntry.Status.Compensated

  // -------------------------------------------------------------------------
  // TEST 7: ZombieHunter finds Unknown step and compensates idempotently
  // -------------------------------------------------------------------------
  it should "ZombieHunter compensates Unknown steps found in Compensating saga" in:
    val s           = store()
    val compensated = ListBuffer.empty[String]
    val sagaId      = SagaId.generate()

    // Simulate: saga has an Unknown step — ZH must compensate it
    s.registerSaga(sagaId)
    s.append(sagaId, WalEntry(
      stepName         = "unknown-step",
      compensate       = () => Right(()),
      compensationRef  = Some("rollbackUnknown"),
      compensationArgs = Some("""{"step":"unknown-step"}""")
    ))
    s.markSagaCompensating(sagaId)
    s.markUnknown(sagaId, "unknown-step")
    Thread.sleep(100)

    val registry = CompensationRegistry()
      .register("rollbackUnknown", _ => { compensated += "unknown-step"; Right(()) })

    val results = ZombieHunter(s, registry).recoverAll(50)

    results should have length 1
    results.head shouldBe a[ZombieHunter.Result.Recovered]
    compensated should contain("unknown-step")
    s.getStatus(sagaId, "unknown-step").toOption.get shouldBe WalEntry.Status.Compensated

  // -------------------------------------------------------------------------
  // Pending tests — engine stops on first compensation failure
  // -------------------------------------------------------------------------
  "SagaEngine compensation policy" should "stop on first compensation failure and leave Done steps untouched" in:
    val s           = store()
    val sagaId      = SagaId.generate()
    val compensated = ListBuffer.empty[String]

    val result = SagaGraph()
      .step(
        name       = "step1",
        action     = Sluggard.diligent(),
        compensate = () => { compensated += "step1"; Right(()) }
      )
      .step(
        name       = "step2",
        action     = Sluggard.diligent(),
        compensate = () => Left(Exception("compensation failed"))  // fails
      )
      .step(
        name       = "step3",
        action     = Sluggard.boom(),  // triggers compensation
        compensate = () => { compensated += "step3"; Right(()) }
      )
      .run(s, sagaId = sagaId)

    result shouldBe a[SagaResult.Failed]
    // step3 failed — not compensated (Failed status)
    // step2 compensation failed — stops here
    // step1 — NOT reached — Done, left untouched
    s.getStatus(sagaId, "step2").toOption.get shouldBe WalEntry.Status.CompensationFailed
    s.getStatus(sagaId, "step1").toOption.get shouldBe WalEntry.Status.Done
    compensated shouldNot contain("step1")

  it should "leave saga in Compensating when a compensation fails" in:
    val s      = store()
    val sagaId = SagaId.generate()

    SagaGraph()
      .step(
        name       = "step1",
        action     = Sluggard.diligent(),
        compensate = () => Left(Exception("compensation failed"))
      )
      .step(
        name       = "step2",
        action     = Sluggard.boom(),
        compensate = () => Right(())
      )
      .run(s, sagaId = sagaId)

    // saga should be Compensating — ZombieHunter will pick it up
    Thread.sleep(10)
    s.findZombies(0).toOption.get should not be empty
