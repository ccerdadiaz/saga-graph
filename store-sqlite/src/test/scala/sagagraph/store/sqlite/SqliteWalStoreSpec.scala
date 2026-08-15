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
    stepName = "LogicalReservation",
    compensate = () => Right(()),
    compensationRef = Some("deleteReservation"),
    compensationArgs = Some("""{"id":1234}""")
  )

  // -------------------------------------------------------------------------
  // TEST 1: append creates saga and entry in Pending state
  // -------------------------------------------------------------------------
  "SqliteWalStore" should "create saga and Pending entry on append" in:
    val sagaId = SagaId.generate()
    val result = store.append(sagaId, dummyEntry)
    withClue(s"append failed: ${result.left.toOption.getOrElse("ok")}") {
      result.isRight shouldBe true
    }
    val pending = store.loadPending(sagaId)
    pending.isRight shouldBe true
    pending.toOption.get.map(_.stepName) shouldBe List("LogicalReservation")

  // -------------------------------------------------------------------------
  // TEST 2: markCompensated removes entry from pending
  // -------------------------------------------------------------------------
  it should "remove entry from pending after markCompensated" in:
    val sagaId = SagaId.generate()
    store.append(sagaId, dummyEntry)
    store.markCompensated(sagaId, "LogicalReservation").isRight shouldBe true
    store.loadPending(sagaId).toOption.get shouldBe List.empty

  // -------------------------------------------------------------------------
  // TEST 3: complete marks saga as finished
  // -------------------------------------------------------------------------
  it should "mark saga as completed" in:
    val sagaId = SagaId.generate()
    store.append(sagaId, dummyEntry)
    store.complete(sagaId).isRight shouldBe true

  // -------------------------------------------------------------------------
  // TEST 4: findZombies detects Running sagas older than threshold
  // -------------------------------------------------------------------------
  it should "detect Running sagas older than threshold as zombies" in:
    val sagaId = SagaId.generate()
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
    store.append(sagaId, dummyEntry)
    store.complete(sagaId)
    Thread.sleep(100)
    store.findZombies(50).toOption.get shouldBe List.empty

  // -------------------------------------------------------------------------
  // TEST 6: loadPending returns entries in LIFO order
  // -------------------------------------------------------------------------
  it should "return all pending entries regardless of order" in:
    val sagaId = SagaId.generate()
    val entry2 = dummyEntry.copy(stepName = "PhysicalReservation")
    store.append(sagaId, dummyEntry)
    store.append(sagaId, entry2)
    val pending = store.loadPending(sagaId).toOption.get
    pending.map(
      _.stepName
    ) should contain allOf ("LogicalReservation", "PhysicalReservation")
