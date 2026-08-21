package sagagraph.examples.goblin

import sagagraph.*
import sagagraph.store.sqlite.SqliteWalStore

// ---------------------------------------------------------------------------
// GoblinHappyHappyDemo — compensation feeds the next saga
//
// Two goblins, one sword, one uniform, boots only in standard size.
//
// Grishnakh is a bigfoot — size 15. No standard boots fit him.
// His saga fails at boots (mandatory by decree), compensates, and returns
// sword-1 and uniform-1 to the pool.
//
// Ugluk arrives next — standard size 7. He picks up exactly the resources
// Grishnakh returned. His saga completes successfully.
//
// The log shows the chain:
//   Grishnakh compensates → returns sword-1
//   Grishnakh compensates → returns uniform-1
//   Ugluk acquires → sword-1     ← same resource
//   Ugluk acquires → uniform-1   ← same resource
//   Ugluk ARMED AND READY
//
// This is the happy-happy path: compensation is not loss — it is
// reaprovisionamiento. What one goblin cannot use, the next one can.
// ---------------------------------------------------------------------------
object GoblinHappyHappyDemo:

  def main(args: Array[String]): Unit =

    val store = SqliteWalStore("examples/target/goblin-happy-happy.db")

    // Stock: exactly 1 sword, 1 uniform, boots in standard sizes only
    // No size 15 boots — Grishnakh will fail
    SmithyService.resetWithStock(1)           // sword-1 only
    RagsAndStyleService.resetWithStock(1)     // uniform-1 only (size S)
    CobbleryService.resetWithStock(2)         // boots-1 (size 7), boots-2 (size 8)

    println("=== THE HAPPY-HAPPY PATH ===")
    println("Compensation is not loss — it is reaprovisionamiento.")
    println()
    println("Stock:")
    println(s"  Weapons:  ${SmithyService.getAvailable().mkString(", ")}")
    println(s"  Uniforms: ${RagsAndStyleService.getAvailable("S").mkString(", ")}")
    println(s"  Boots:    ${CobbleryService.getAvailableAny().mkString(", ")}")
    println()
    println("=" * 60)

    // -------------------------------------------------------------------------
    // Goblin A — Grishnakh, the bigfoot
    // Size 15 boots — none available → saga fails after acquiring weapon + uniform
    // -------------------------------------------------------------------------
    println("\n--- GOBLIN A: Grishnakh (bigfoot, size 15) ---")

    val equipmentA = GoblinEquipment(
      weaponId  = SmithyService.getAvailable().headOption.getOrElse("none"),
      uniformId = RagsAndStyleService.getAvailable("S").headOption.getOrElse("none"),
      bootsId   = CobbleryService.getAvailable(15).headOption  // size 15 — none available
    )
    println(s"Resolved equipment: weapon=${equipmentA.weaponId}, uniform=${equipmentA.uniformId}, boots=${equipmentA.bootsId.getOrElse("none — no size 15 available")}")

    val resultA = ArmGoblinHappyHappySaga("Grishnakh", equipmentA, store)

    println(s"\nGrishnakh result: ${resultA match
      case SagaResult.Completed => "✓ ARMED AND READY"
      case SagaResult.Failed(e) => s"✗ FAILED — ${e.getMessage}"
    }")

    println(s"\nPool after Grishnakh:")
    println(s"  Weapons:  ${SmithyService.getAvailable().mkString(", ")}")
    println(s"  Uniforms: ${RagsAndStyleService.getAvailable("S").mkString(", ")}")
    println(s"  Boots:    ${CobbleryService.getAvailableAny().mkString(", ")}")

    // -------------------------------------------------------------------------
    // Goblin B — Ugluk, standard size 7
    // Picks up exactly what Grishnakh returned
    // -------------------------------------------------------------------------
    println("\n--- GOBLIN B: Ugluk (standard size 7) ---")

    val equipmentB = GoblinEquipment(
      weaponId  = SmithyService.getAvailable().headOption.getOrElse("none"),
      uniformId = RagsAndStyleService.getAvailable("S").headOption.getOrElse("none"),
      bootsId   = CobbleryService.getAvailable(7).headOption  // size 7 — available
    )
    println(s"Resolved equipment: weapon=${equipmentB.weaponId}, uniform=${equipmentB.uniformId}, boots=${equipmentB.bootsId.getOrElse("none")}")

    val resultB = ArmGoblinHappyHappySaga("Ugluk", equipmentB, store)

    println(s"\nUgluk result: ${resultB match
      case SagaResult.Completed => "✓ ARMED AND READY"
      case SagaResult.Failed(e) => s"✗ FAILED — ${e.getMessage}"
    }")

    // -------------------------------------------------------------------------
    // The point
    // -------------------------------------------------------------------------
    println("\n" + "=" * 60)
    println("=== WHAT JUST HAPPENED ===")
    println()
    println("  Grishnakh reserved sword-1 and uniform-1.")
    println("  Grishnakh could not get size-15 boots — saga failed.")
    println("  Grishnakh's compensation returned sword-1 and uniform-1 to the pool.")
    println()
    println("  Ugluk arrived. The pool had sword-1 and uniform-1.")
    println("  Ugluk acquired sword-1 — the same sword Grishnakh returned.")
    println("  Ugluk acquired uniform-1 — the same uniform Grishnakh returned.")
    println("  Ugluk got size-7 boots. Ugluk is ARMED AND READY.")
    println()
    println("  Compensation is not loss. It is reaprovisionamiento.")
    println("=" * 60)

    println("\n=== EXECUTION TIMELINE ===")
    store.printTimeline()

    store.close()
