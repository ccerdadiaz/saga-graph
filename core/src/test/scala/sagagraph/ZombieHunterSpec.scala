package sagagraph

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ZombieHunterSpec extends AnyFlatSpec with Matchers:

  def store(): InMemoryWalStore = InMemoryWalStore()

  def entry(name: String, ref: String) = WalEntry(
    stepName         = name,
    compensate       = () => Right(()),
    compensationRef  = Some(ref),
    compensationArgs = Some(s"""{"step":"$name"}""")
  )

  // Setup helper — saga with CompensationFailed steps
  def setupWithCompensationFailed(s: InMemoryWalStore, steps: String*): SagaId =
    val sagaId = SagaId.generate()
    s.registerSaga(sagaId)
    steps.foreach(name => s.append(sagaId, entry(name, s"rollback${name.capitalize}")))
    s.markSagaCompensating(sagaId)
    steps.foreach(name => s.markCompensationFailed(sagaId, name))
    sagaId

  // -------------------------------------------------------------------------
  // TEST 1: All compensations succeed — saga marked Recovered
  // -------------------------------------------------------------------------
  "ZombieHunter" should "recover a zombie saga when all compensations succeed" in:
    val s      = store()
    val sagaId = setupWithCompensationFailed(s, "step1", "step2")
    val compensated = scala.collection.mutable.ListBuffer.empty[String]
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
  // TEST 2: Compensation fails on first attempt — saga PartiallyRecovered
  // -------------------------------------------------------------------------
  it should "report PartiallyRecovered when a compensation handler fails first attempt" in:
    val s      = store()
    val sagaId = setupWithCompensationFailed(s, "step1", "step2")
    Thread.sleep(100)

    val registry = CompensationRegistry()
      .register("rollbackStep1", _ => Right(()))
      .register("rollbackStep2", _ => Left(RuntimeException("service unavailable")))

    val results = ZombieHunter(s, registry).recoverAll(50)

    results should have length 1
    results.head shouldBe a[ZombieHunter.Result.PartiallyRecovered]
    s.getStatus(sagaId, "step2").toOption.get shouldBe WalEntry.Status.CompensationFailed

  // -------------------------------------------------------------------------
  // TEST 3: Missing handler — HumanInterventionRequired immediately
  // -------------------------------------------------------------------------
  it should "report HumanInterventionRequired when handler is missing from registry" in:
    val s      = store()
    val sagaId = SagaId.generate()
    s.registerSaga(sagaId)
    s.append(sagaId, entry("step1", "unknownRef"))
    s.markSagaCompensating(sagaId)
    s.markCompensationFailed(sagaId, "step1")
    Thread.sleep(100)

    val results = ZombieHunter(s, CompensationRegistry()).recoverAll(50)

    results should have length 1
    results.head shouldBe a[ZombieHunter.Result.HumanInterventionRequired]

  // -------------------------------------------------------------------------
  // TEST 4: No zombies — recoverAll returns empty list
  // -------------------------------------------------------------------------
  it should "return empty list when no zombies exist" in:
    val s      = store()
    val sagaId = SagaId.generate()
    s.registerSaga(sagaId)
    s.append(sagaId, entry("step1", "rollbackStep1"))
    s.markSagaCompleted(sagaId)

    val results = ZombieHunter(s, CompensationRegistry()).recoverAll(50)
    results shouldBe List.empty

  // -------------------------------------------------------------------------
  // TEST 5: Second attempt escalates to HumanIntervention
  // -------------------------------------------------------------------------
  it should "escalate to HumanIntervention after maxAttempts failures" in:
    val s      = store()
    val sagaId = setupWithCompensationFailed(s, "step1")
    Thread.sleep(100)

    val registry = CompensationRegistry()
      .register("rollbackStep1", _ => Left(RuntimeException("still unavailable")))

    // First attempt — PartiallyRecovered
    val results1 = ZombieHunter(s, registry).recoverAll(50)
    results1.head shouldBe a[ZombieHunter.Result.PartiallyRecovered]
    s.getStatus(sagaId, "step1").toOption.get shouldBe WalEntry.Status.CompensationFailed

    // Second attempt — HumanIntervention
    Thread.sleep(100)
    val results2 = ZombieHunter(s, registry).recoverAll(50)
    results2.head shouldBe a[ZombieHunter.Result.HumanInterventionRequired]
    s.getStatus(sagaId, "step1").toOption.get shouldBe WalEntry.Status.HumanIntervention

  // -------------------------------------------------------------------------
  // TEST 6: ZH unblocks CompensationFailed and continues LIFO with Done entries
  // -------------------------------------------------------------------------
  it should "continue LIFO compensation after unblocking a CompensationFailed step" in:
    val s      = store()
    val sagaId = SagaId.generate()
    val compensated = scala.collection.mutable.ListBuffer.empty[String]

    // Simulate: step1 Done, step2 CompensationFailed, step3 Done (not yet compensated)
    // LIFO order: step3 → step2 → step1
    // Engine stopped at step2 (CompensationFailed), step1 still Done
    s.registerSaga(sagaId)
    s.append(sagaId, entry("step1", "rollbackStep1"))
    s.append(sagaId, entry("step2", "rollbackStep2"))
    s.append(sagaId, entry("step3", "rollbackStep3"))
    s.markSagaCompensating(sagaId)
    s.markDone(sagaId, "step1")    // not yet compensated — engine stopped before reaching it
    s.markCompensationFailed(sagaId, "step2")  // engine stopped here
    s.markCompensated(sagaId, "step3")  // already compensated by engine
    Thread.sleep(100)

    val registry = CompensationRegistry()
      .register("rollbackStep1", _ => { compensated += "step1"; Right(()) })
      .register("rollbackStep2", _ => { compensated += "step2"; Right(()) })
      .register("rollbackStep3", _ => { compensated += "step3"; Right(()) })

    val results = ZombieHunter(s, registry).recoverAll(50)

    results should have length 1
    results.head shouldBe a[ZombieHunter.Result.Recovered]
    compensated should contain("step2")  // unblocked by ZH
    compensated should contain("step1")  // continued LIFO by ZH
    compensated shouldNot contain("step3")  // already Compensated
    s.getStatus(sagaId, "step1").toOption.get shouldBe WalEntry.Status.Compensated
    s.getStatus(sagaId, "step2").toOption.get shouldBe WalEntry.Status.Compensated
    s.findZombies(50).toOption.get shouldBe List.empty
