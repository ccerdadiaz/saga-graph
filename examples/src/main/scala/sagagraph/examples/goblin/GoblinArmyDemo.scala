package sagagraph.examples.goblin

import sagagraph.*
import sagagraph.store.sqlite.SqliteWalStore
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.*

// ---------------------------------------------------------------------------
// GoblinArmyDemo — arms 5 goblins concurrently with limited resources
//
// Stock: 3 weapons, 4 uniforms, 2 boots
// All sagas run concurrently — they compete for real scarce resources
// ---------------------------------------------------------------------------
object GoblinArmyDemo:

  def main(args: Array[String]): Unit =
    given ExecutionContext = ExecutionContext.global

    val log     = Slf4jLogger("sagagraph.examples.goblin.GoblinArmyDemo")
    val goblins = List("Grishnakh", "Ugluk", "Muzgash", "Lagduf", "Snaga")
    val store   = SqliteWalStore("examples/target/goblin-army.db")

    // ---------------------------------------------------------------------------
    // UI output — belongs to the demo presentation layer, not to business logging
    // ---------------------------------------------------------------------------
    println("=== DARK LORD'S ARMY RECRUITMENT — Operation: Ready for Battle ===")
    println(s"Recruits: ${goblins.mkString(", ")}")
    println(s"Catalog: ${SmithyService.getAvailable().mkString(", ")} | ${RagsAndStyleService.getAvailable("S").mkString(", ")} | ${CobbleryService.getAvailableAny().mkString(", ")}")
    println("=" * 60)

    log.info(s"Recruiting ${goblins.size} goblins")

    // -------------------------------------------------------------------------
    // Pre-saga — resolve resources before starting
    // Resource selection is business responsibility — saga-graph only requires
    // that chosen IDs are known before the saga starts.
    // getAvailable() returns IDs in random order — caller takes head as selection.
    // In production: apply additional business criteria (size, priority, etc.)
    // -------------------------------------------------------------------------
    val futures = goblins.map { name =>
      Future {
        val goblinHeight = 120 + (name.length * 7) % 40
        val goblinWeight = 40 + (name.length * 3) % 30
        val uniformSize  = if goblinHeight > 145 then "L" else "S"
        val bootsSize    = (goblinWeight / 10) + 2

        val equipment = GoblinEquipment(
          weaponId  = SmithyService.getAvailable().headOption.getOrElse("none"),
          uniformId = RagsAndStyleService.getAvailable(uniformSize).headOption.getOrElse("none"),
          bootsId   = CobbleryService.getAvailable(bootsSize).headOption
        )

        name -> ArmGoblinSaga(name, equipment, store)
      }
    }
    val results = Await.result(Future.sequence(futures), 30.seconds)

    // ---------------------------------------------------------------------------
    // UI output — belongs to the demo presentation layer, not to business logging
    // ---------------------------------------------------------------------------
    println("\n" + "=" * 60)
    println("=== RECRUITMENT REPORT ===")
    results.foreach { case (name, result) =>
      val status = result match
        case SagaResult.Completed => "✓ ARMED AND READY"
        case SagaResult.Failed(e) => s"✗ FAILED — ${e.getMessage}"
      println(s"  $name: $status")
    }
    println("=" * 60)
    println("The Dark Lord will be... partially satisfied.")

    println("\n" + "=" * 60)
    println("=== EXECUTION TIMELINE ===")
    store.printTimeline()
    store.close()
