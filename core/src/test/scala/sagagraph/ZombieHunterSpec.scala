package sagagraph

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import sagagraph.*

class ZombieHunterSpec extends AnyFlatSpec with Matchers:

  def store(): InMemoryWalStore = InMemoryWalStore()

  def entry(name: String, ref: String) = WalEntry(
    stepName         = name,
    compensate       = () => Right(()),
    compensationRef  = Some(ref),
    compensationArgs = Some(s"""{"step":"$name"}""")
  )

  // -------------------------------------------------------------------------
  // TEST 1: All compensations succeed — saga marked Recovered
  // -------------------------------------------------------------------------
  "ZombieHunter" should "recover a zombie saga when all compensations succeed" in:
    val s      = store()
    val sagaId = SagaId.generate()
    val compensated = scala.collection.mutable.ListBuffer.empty[String]

    s.append(sagaId, entry("step1", "rollbackStep1"))
    s.append(sagaId, entry("step2", "rollbackStep2"))
    Thread.sleep(100)

    val registry = CompensationRegistry()
      .register("rollbackStep1", _ => { compensated += "step1"; Right(()) })
      .register("rollbackStep2", _ => { compensated += "step2"; Right(()) })

    val results = ZombieHunter(s, registry).recoverAll(50)

    results should have length 1
    results.head shouldBe a[ZombieHunter.Result.Recovered]
    compensated should contain("step1")
    compensated should contain("step2")
    s.findZombies(50).toOption.get shouldBe List.empty

  // -------------------------------------------------------------------------
  // TEST 2: One compensation fails — saga PartiallyRecovered
  // -------------------------------------------------------------------------
  it should "report PartiallyRecovered when a compensation handler fails" in:
    val s      = store()
    val sagaId = SagaId.generate()

    s.append(sagaId, entry("step1", "rollbackStep1"))
    s.append(sagaId, entry("step2", "rollbackStep2"))
    Thread.sleep(100)

    val registry = CompensationRegistry()
      .register("rollbackStep1", _ => Right(()))
      .register("rollbackStep2", _ => Left(RuntimeException("service unavailable")))

    val results = ZombieHunter(s, registry).recoverAll(50)

    results should have length 1
    results.head shouldBe a[ZombieHunter.Result.PartiallyRecovered]

  // -------------------------------------------------------------------------
  // TEST 3: Missing handler in registry — HumanInterventionRequired
  // -------------------------------------------------------------------------
  it should "report HumanInterventionRequired when handler is missing from registry" in:
    val s      = store()
    val sagaId = SagaId.generate()

    s.append(sagaId, entry("step1", "unknownRef"))
    Thread.sleep(100)

    val registry = CompensationRegistry() // empty — no handlers registered

    val results = ZombieHunter(s, registry).recoverAll(50)

    results should have length 1
    results.head shouldBe a[ZombieHunter.Result.HumanInterventionRequired]

  // -------------------------------------------------------------------------
  // TEST 4: No zombies — recoverAll returns empty list
  // -------------------------------------------------------------------------
  it should "return empty list when no zombies exist" in:
    val s      = store()
    val sagaId = SagaId.generate()

    s.append(sagaId, entry("step1", "rollbackStep1"))
    s.markSagaCompleted(sagaId)

    val registry = CompensationRegistry()
    val results  = ZombieHunter(s, registry).recoverAll(50)

    results shouldBe List.empty
