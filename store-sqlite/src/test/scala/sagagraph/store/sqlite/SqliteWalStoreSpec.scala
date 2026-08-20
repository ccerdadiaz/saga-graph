package sagagraph.store.sqlite

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import java.nio.file.{Files, Paths}
import scala.compiletime.uninitialized
import sagagraph.*

class SqliteWalStoreSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterEach:

  val dbPath = "store-sqlite/target/saga-test.db"
  var store: SqliteWalStore = uninitialized

  override def beforeEach(): Unit =
    Files.createDirectories(Paths.get("store-sqlite/target"))
    Files.deleteIfExists(Paths.get(dbPath))
    store = SqliteWalStore(dbPath)

  override def afterEach(): Unit =
    store.close()

  val dummyEntry = WalEntry(
    stepName         = "LogicalReservation",
    compensate       = () => Right(()),
    compensationRef  = Some("deleteReservation"),
    compensationArgs = Some("""{"id":1234}""")
  )

  // -------------------------------------------------------------------------
  // TEST 1: append creates saga and entry in Registered state
  // -------------------------------------------------------------------------
  "SqliteWalStore" should "create saga and Registered entry on append" in:
    val sagaId = SagaId.generate()
    store.registerSaga(sagaId)
    store.append(sagaId, dummyEntry).isRight shouldBe true
    store.getStatus(sagaId, "LogicalReservation").toOption.get shouldBe WalEntry.Status.Registered

  // -------------------------------------------------------------------------
  // TEST 2: markCompensated removes entry from CompensationFailed
  // -------------------------------------------------------------------------
  it should "remove entry from CompensationFailed after markCompensated" in:
    val sagaId = SagaId.generate()
    store.registerSaga(sagaId)
    store.append(sagaId, dummyEntry)
    store.markSagaCompensating(sagaId)
    store.markCompensationFailed(sagaId, "LogicalReservation")
    store.markCompensated(sagaId, "LogicalReservation").isRight shouldBe true
    store.loadCompensationFailed(sagaId).toOption.get shouldBe List.empty

  // -------------------------------------------------------------------------
  // TEST 3: markSagaCompleted marks saga as Completed
  // -------------------------------------------------------------------------
  it should "mark saga as completed" in:
    val sagaId = SagaId.generate()
    store.registerSaga(sagaId)
    store.append(sagaId, dummyEntry)
    store.markSagaCompleted(sagaId).isRight shouldBe true

  // -------------------------------------------------------------------------
  // TEST 4: findZombies detects Running sagas older than threshold
  // -------------------------------------------------------------------------
  it should "detect Running sagas older than threshold as zombies" in:
    val sagaId = SagaId.generate()
    store.registerSaga(sagaId)
    store.append(sagaId, dummyEntry)
    Thread.sleep(100)
    val zombies = store.findZombies(50)
    zombies.isRight shouldBe true
    zombies.toOption.get should contain(sagaId)

  // -------------------------------------------------------------------------
  // TEST 5: findZombies excludes completed sagas
  // -------------------------------------------------------------------------
  it should "not include completed sagas in zombie list" in:
    val sagaId = SagaId.generate()
    store.registerSaga(sagaId)
    store.append(sagaId, dummyEntry)
    store.markSagaCompleted(sagaId)
    Thread.sleep(100)
    store.findZombies(50).toOption.get shouldBe List.empty

  // -------------------------------------------------------------------------
  // TEST 6: loadCompensationFailed returns only CompensationFailed entries
  // -------------------------------------------------------------------------
  it should "return only CompensationFailed entries in loadCompensationFailed" in:
    val sagaId = SagaId.generate()
    val entry2 = dummyEntry.copy(stepName = "PhysicalReservation")
    store.registerSaga(sagaId)
    store.append(sagaId, dummyEntry)
    store.append(sagaId, entry2)
    store.markSagaCompensating(sagaId)
    // Only mark one as CompensationFailed
    store.markCompensationFailed(sagaId, "LogicalReservation")
    val failed = store.loadCompensationFailed(sagaId).toOption.get
    failed.map(_.stepName) shouldBe List("LogicalReservation")
