package sagagraph.examples.goblin

import java.util.concurrent.atomic.AtomicInteger

case class OutOfStockException(service: String)
    extends Exception(s"[$service] Out of stock — the Dark Lord is displeased")

case class Goblin(name: String, weightKg: Int, heightCm: Int)
case class Weapon(kind: String, size: String)
case class Uniform(size: String, color: String)
case class Boot(size: Int)

// ---------------------------------------------------------------------------
// Weights & Measures — always available, no stock limit
// The Dark Lord insists on proper documentation
// ---------------------------------------------------------------------------
object WeightsAndMeasuresService:
  def measure(goblinName: String): Either[Throwable, Goblin] =
    val weight = 40 + (goblinName.length * 3) % 30
    val height = 120 + (goblinName.length * 7) % 40
    println(
      s"  [Weights & Measures] $goblinName: ${weight}kg, ${height}cm. Adequate."
    )
    Right(Goblin(goblinName, weight, height))

// ---------------------------------------------------------------------------
// Smithy & Kitchenware — limited stock of short weapons
// ---------------------------------------------------------------------------
object SmithyService:
  private val stock = AtomicInteger(3)

  def acquireWeapon(goblin: Goblin): Either[Throwable, Weapon] =
    if stock.decrementAndGet() >= 0 then
      val weapon = Weapon(
        "short sword",
        if goblin.weightKg > 55 then "heavy" else "standard"
      )
      println(
        s"  [Smithy] ${goblin.name} equipped with ${weapon.size} ${weapon.kind}. Stock: ${stock.get()} remaining."
      )
      Right(weapon)
    else
      stock.incrementAndGet()
      println(s"  [Smithy] ${goblin.name} — OUT OF STOCK. The forge is cold.")
      Left(OutOfStockException("Smithy"))

  def returnWeapon(goblin: Goblin): Either[Throwable, Unit] =
    val current = stock.incrementAndGet()
    println(
      s"  [Smithy] ${goblin.name}'s short sword returned and available for another request (compensated: full equipment could not be completed). Stock: $current available."
    )
    Right(())

  def reset(initialStock: Int = 3): Unit = stock.set(initialStock)

// ---------------------------------------------------------------------------
// Rags & Style — limited stock of uniforms
// ---------------------------------------------------------------------------
object RagsAndStyleService:
  private val stock = AtomicInteger(4)

  def acquireUniform(goblin: Goblin): Either[Throwable, Uniform] =
    if stock.decrementAndGet() >= 0 then
      val size = if goblin.heightCm > 145 then "L" else "S"
      val uniform = Uniform(size, "Dark Army Green™")
      println(
        s"  [Rags & Style] ${goblin.name} fitted in size $size. Stock: ${stock.get()} remaining."
      )
      Right(uniform)
    else
      stock.incrementAndGet()
      println(
        s"  [Rags & Style] ${goblin.name} — OUT OF STOCK. Naked goblins are undignified."
      )
      Left(OutOfStockException("Rags & Style"))

  def returnUniform(goblin: Goblin): Either[Throwable, Unit] =
    val current = stock.incrementAndGet()
    println(
      s"  [Rags & Style] ${goblin.name}'s uniform returned and available for another request (compensated: full equipment could not be completed). Stock: $current available."
    )
    Right(())

  def reset(initialStock: Int = 4): Unit = stock.set(initialStock)

// ---------------------------------------------------------------------------
// Cobblery — optional, limited stock of boots
// A goblin can fight barefoot. Uncomfortably, but it can.
// ---------------------------------------------------------------------------
object CobbleryService:
  private val stock = AtomicInteger(2)

  def acquireBoots(goblin: Goblin): Either[Throwable, Boot] =
    if stock.decrementAndGet() >= 0 then
      val size = (goblin.weightKg / 10) + 2
      println(
        s"  [Cobblery] ${goblin.name} gets boots size $size. Stock: ${stock.get()} remaining."
      )
      Right(Boot(size))
    else
      stock.incrementAndGet()
      println(s"  [Cobblery] ${goblin.name} — OUT OF STOCK. Barefoot it is.")
      Left(OutOfStockException("Cobblery"))

  def returnBoots(goblin: Goblin): Either[Throwable, Unit] =
    val current = stock.incrementAndGet()
    println(
      s"  [Cobblery] ${goblin.name}'s boots returned and available for another request (compensated: full equipment could not be completed). Stock: $current available."
    )
    Right(())

  def reset(initialStock: Int = 2): Unit = stock.set(initialStock)

// ---------------------------------------------------------------------------
// Portrait Service — best effort, always unreliable
// The Dark Lord's postal system is... suboptimal
// ---------------------------------------------------------------------------
object PortraitService:
  def sendToMother(goblin: Goblin): Either[Throwable, Unit] =
    println(
      s"  [Portrait] Attempting to send ${goblin.name}'s portrait to mother..."
    )
    println(s"  [Portrait] Postal raven lost. Mother will never know.")
    Left(Exception("Raven not found"))
