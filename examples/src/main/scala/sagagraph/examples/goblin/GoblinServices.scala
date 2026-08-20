package sagagraph.examples.goblin

import org.slf4j.LoggerFactory

// ---------------------------------------------------------------------------
// Goblin Army Supply Services — resources are scarce, first come first served
//
// Each service maintains a fixed catalog of resources with availability flags.
// getAvailable() returns available IDs in random order — simulates a
// service-side selection policy. In production the service would apply its
// own business criteria (priority, TTL lease, etc.).
// The caller applies additional business criteria to choose from the list.
//
// These services simulate remote endpoints — in production each would be
// a separate process with its own logging infrastructure.
// Log output goes to examples/target/services.log — not to stdout.
// Random latency simulates real remote service response times.
// ---------------------------------------------------------------------------

private val log = LoggerFactory.getLogger("GoblinServices")

case class OutOfStockException(service: String)
    extends Exception(s"[$service] Out of stock — the Dark Lord is displeased")

case class Goblin(name: String, weightKg: Int, heightCm: Int)
case class Weapon(id: String, label: String)
case class Uniform(id: String, size: String, color: String)
case class Boot(id: String, bootSize: Int)

// ---------------------------------------------------------------------------
// Weights & Measures — always available, no catalog needed
// ---------------------------------------------------------------------------
object WeightsAndMeasuresService:
  def measure(goblinName: String): Either[Throwable, Goblin] =
    latency()
    val weight = 40 + (goblinName.length * 3) % 30
    val height = 120 + (goblinName.length * 7) % 40
    log.info(
      s"[Weights & Measures] $goblinName: ${weight}kg, ${height}cm. Adequate."
    )
    Right(Goblin(goblinName, weight, height))

// ---------------------------------------------------------------------------
// Smithy & Kitchenware — fixed catalog of weapons
// ---------------------------------------------------------------------------
object SmithyService:

  private case class WeaponEntry(
      id: String,
      label: String,
      var available: Boolean = true
  )

  private val catalog = scala.collection.mutable.ListBuffer(
    WeaponEntry("sword-1", "heavy short sword"),
    WeaponEntry("sword-2", "standard short sword"),
    WeaponEntry("sword-3", "heavy short sword")
  )

  // Returns available IDs in random order — simulates service-side selection policy
  def getAvailable(): List[String] =
    synchronized {
      scala.util.Random.shuffle(catalog.filter(_.available).map(_.id)).toList
    }

  def acquire(weaponId: String): Either[Throwable, Weapon] =
    latency()
    synchronized:
      catalog.find(w => w.id == weaponId && w.available) match
        case Some(w) =>
          w.available = false
          log.info(
            s"[Smithy] $weaponId acquired. Available: ${getAvailable().mkString(", ")}."
          )
          Right(Weapon(w.id, w.label))
        case None =>
          log.info(s"[Smithy] $weaponId — not available. The forge is cold.")
          Left(OutOfStockException("Smithy"))

  def return_(weaponId: String): Either[Throwable, Unit] =
    latency()
    synchronized:
      catalog.find(_.id == weaponId).foreach(_.available = true)
      log.info(
        s"[Smithy] $weaponId returned and available for another request. Available: ${getAvailable()
            .mkString(", ")}."
      )
      Right(())

  // Resets all weapons to available — for demo and testing purposes
  def reset(): Unit = synchronized { catalog.foreach(_.available = true) }

  // Resets stock to n weapons — generates additional entries if needed
  def resetWithStock(n: Int): Unit = synchronized:
    catalog.foreach(_.available = false)
    val entries = (1 to n).map(i =>
      WeaponEntry(
        s"sword-$i",
        if i % 2 == 0 then "standard short sword" else "heavy short sword",
        available = true
      )
    )
    catalog.clear()
    catalog.addAll(entries)

// ---------------------------------------------------------------------------
// Rags & Style — fixed catalog of uniforms
// ---------------------------------------------------------------------------
object RagsAndStyleService:

  private case class UniformEntry(
      id: String,
      size: String,
      var available: Boolean = true
  )

  private val catalog = scala.collection.mutable.ListBuffer(
    UniformEntry("uniform-1", "S"),
    UniformEntry("uniform-2", "L"),
    UniformEntry("uniform-3", "S"),
    UniformEntry("uniform-4", "L")
  )

  // Returns available IDs in random order — simulates service-side selection policy
  def getAvailable(size: String): List[String] =
    synchronized {
      scala.util.Random
        .shuffle(
          catalog.filter(u => u.available && u.size == size).map(_.id)
        )
        .toList
    }

  def acquire(uniformId: String): Either[Throwable, Uniform] =
    latency()
    synchronized:
      catalog.find(u => u.id == uniformId && u.available) match
        case Some(u) =>
          u.available = false
          log.info(
            s"[Rags & Style] $uniformId (size ${u.size}) acquired. Available: ${catalog.filter(_.available).map(_.id).mkString(", ")}."
          )
          Right(Uniform(u.id, u.size, "Dark Army Green™"))
        case None =>
          log.info(
            s"[Rags & Style] $uniformId — not available. Naked goblins are undignified."
          )
          Left(OutOfStockException("Rags & Style"))

  def return_(uniformId: String): Either[Throwable, Unit] =
    latency()
    synchronized:
      catalog.find(_.id == uniformId).foreach(_.available = true)
      log.info(
        s"[Rags & Style] $uniformId returned and available for another request."
      )
      Right(())

  // Resets all uniforms to available — for demo and testing purposes
  def reset(): Unit = synchronized { catalog.foreach(_.available = true) }

  // Resets stock to n uniforms — generates additional entries if needed
  def resetWithStock(n: Int): Unit = synchronized:
    catalog.clear()
    val sizes = List("S", "L", "S", "L")
    (1 to n).foreach(i =>
      catalog += UniformEntry(
        s"uniform-$i",
        sizes((i - 1) % 4),
        available = true
      )
    )

// ---------------------------------------------------------------------------
// Cobblery — fixed catalog of boots
// ---------------------------------------------------------------------------
object CobbleryService:

  private case class BootsEntry(
      id: String,
      bootSize: Int,
      var available: Boolean = true
  )

  private val catalog = scala.collection.mutable.ListBuffer(
    BootsEntry("boots-1", 7),
    BootsEntry("boots-2", 8)
  )

  // Returns available IDs in random order — simulates service-side selection policy
  def getAvailable(size: Int): List[String] =
    synchronized {
      scala.util.Random
        .shuffle(
          catalog.filter(b => b.available && b.bootSize == size).map(_.id)
        )
        .toList
    }

  def getAvailableAny(): List[String] =
    synchronized {
      scala.util.Random.shuffle(catalog.filter(_.available).map(_.id)).toList
    }

  def acquire(bootsId: String): Either[Throwable, Boot] =
    latency()
    synchronized:
      catalog.find(b => b.id == bootsId && b.available) match
        case Some(b) =>
          b.available = false
          log.info(
            s"[Cobblery] $bootsId (size ${b.bootSize}) acquired. Available: ${catalog.filter(_.available).map(_.id).mkString(", ")}."
          )
          Right(Boot(b.id, b.bootSize))
        case None =>
          log.info(s"[Cobblery] $bootsId — not available. Barefoot it is.")
          Left(OutOfStockException("Cobblery"))

  def return_(bootsId: String): Either[Throwable, Unit] =
    latency()
    synchronized:
      catalog.find(_.id == bootsId).foreach(_.available = true)
      log.info(
        s"[Cobblery] $bootsId returned and available for another request."
      )
      Right(())

  // Resets all boots to available — for demo and testing purposes
  def reset(): Unit = synchronized { catalog.foreach(_.available = true) }

  // Resets stock to n boots — generates additional entries if needed
  def resetWithStock(n: Int): Unit = synchronized:
    catalog.clear()
    val sizes = List(7, 8, 9, 6)
    (1 to n).foreach(i =>
      catalog += BootsEntry(s"boots-$i", sizes((i - 1) % 4), available = true)
    )

// ---------------------------------------------------------------------------
// Portrait Service — best effort, always unreliable
// ---------------------------------------------------------------------------
object PortraitService:
  def sendToMother(goblin: Goblin): Either[Throwable, Unit] =
    latency()
    log.info(
      s"[Portrait] Attempting to send ${goblin.name}'s portrait to mother..."
    )
    log.warn(s"[Portrait] Postal raven lost. Mother will never know.")
    Left(Exception("Raven not found"))

// ---------------------------------------------------------------------------
// Shared helpers
// latency() — random delay 20-100ms simulating remote service response time
// ---------------------------------------------------------------------------
private def latency(): Unit = Thread.sleep(20 + (math.random() * 80).toLong)
