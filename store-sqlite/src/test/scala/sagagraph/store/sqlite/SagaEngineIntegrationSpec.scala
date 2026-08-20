package sagagraph.store.sqlite

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import java.nio.file.{Files, Paths}
import sagagraph.*

class SagaEngineIntegrationSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterEach:

  val dbPath = "store-sqlite/target/saga-integration.db"

  override def beforeEach(): Unit =
    Files.createDirectories(Paths.get("store-sqlite/target"))
    Files.deleteIfExists(Paths.get(dbPath))

  def store(): SqliteWalStore = SqliteWalStore(dbPath)

  def success(
      name: String,
      executed: scala.collection.mutable.ListBuffer[String],
      compensated: scala.collection.mutable.ListBuffer[String]
  ): (String, () => Either[Throwable, Unit], () => Either[Throwable, Unit]) =
    (name, () => { executed += name; Right(()) }, () => { compensated += name; Right(()) })

  def failing(
      name: String,
      executed: scala.collection.mutable.ListBuffer[String],
      compensated: scala.collection.mutable.ListBuffer[String]
  ): (String, () => Either[Throwable, Unit], () => Either[Throwable, Unit]) =
    (name, () => { executed += name; Left(RuntimeException(s"$name failed")) }, () => { compensated += name; Right(()) })

  // -------------------------------------------------------------------------
  // TEST 1: Happy path — WAL complete, saga marked Completed
  // -------------------------------------------------------------------------
  "SagaEngine with SqliteWalStore" should "mark saga as Completed in WAL after happy path" in:
    val executed    = scala.collection.mutable.ListBuffer.empty[String]
    val compensated = scala.collection.mutable.ListBuffer.empty[String]
    val s = store()

    val (n1, a1, c1) = success("step1", executed, compensated)
    val (n2, a2, c2) = success("step2", executed, compensated)

    val result = SagaGraph()
      .step(n1, a1, c1)
      .step(n2, a2, c2)
      .run(s)

    result shouldBe SagaResult.Completed
    s.loadCompensationFailed(SagaId("")).toOption.get shouldBe List.empty
    s.findZombies(0).toOption.get shouldBe List.empty

  // -------------------------------------------------------------------------
  // TEST 2: Failure — WAL entries marked as Compensated
  // -------------------------------------------------------------------------
  it should "mark entries as Compensated in WAL after failure" in:
    val executed    = scala.collection.mutable.ListBuffer.empty[String]
    val compensated = scala.collection.mutable.ListBuffer.empty[String]
    val s = store()

    val (n1, a1, c1) = success("step1", executed, compensated)
    val (n2, a2, c2) = failing("step2", executed, compensated)

    val result = SagaGraph()
      .step(n1, a1, c1)
      .step(n2, a2, c2)
      .run(s)

    result shouldBe a[SagaResult.Failed]
    s.findZombies(60000).toOption.get shouldBe List.empty

  // -------------------------------------------------------------------------
  // TEST 3: Parallel fork — WAL registers all branches before any action runs
  // -------------------------------------------------------------------------
  it should "register all fork branches in WAL before executing any action" in:
    val executed    = scala.collection.mutable.ListBuffer.empty[String]
    val compensated = scala.collection.mutable.ListBuffer.empty[String]
    val s = store()

    val (n1, a1, c1) = success("step1", executed, compensated)
    val (n2, a2, c2) = success("par-A", executed, compensated)
    val (n3, a3, c3) = failing("par-B", executed, compensated)
    val (n4, a4, c4) = success("par-C", executed, compensated)

    val result = SagaGraph()
      .step(n1, a1, c1)
      .parallel(
        SagaGraph.par(n2, a2, c2),
        SagaGraph.par(n3, a3, c3),
        SagaGraph.par(n4, a4, c4)
      )
      .run(s)

    result shouldBe a[SagaResult.Failed]
    compensated should contain("par-A")
    compensated shouldNot contain("par-B") // failed — service guarantees clean state
    compensated should contain("par-C")
    compensated should contain("step1")

  // -------------------------------------------------------------------------
  // TEST 4: Zombie detection — saga interrupted before completing
  // -------------------------------------------------------------------------
  it should "detect zombie saga when complete() was never called" in:
    val s      = store()
    val sagaId = SagaId.generate()
    val entry = WalEntry(
      stepName         = "orphanStep",
      compensate       = () => Right(()),
      compensationRef  = Some("rollbackOrphan"),
      compensationArgs = Some("""{"id":999}""")
    )

    s.registerSaga(sagaId)
    s.append(sagaId, entry)
    Thread.sleep(100)

    val zombies = s.findZombies(50)
    zombies.isRight shouldBe true
    zombies.toOption.get should contain(sagaId)
