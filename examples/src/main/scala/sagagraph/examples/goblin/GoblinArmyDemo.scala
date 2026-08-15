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

    val log = Slf4jLogger("sagagraph.examples.goblin.GoblinArmyDemo")
    val goblins = List("Grishnakh", "Ugluk", "Muzgash", "Lagduf", "Snaga")
    val store = SqliteWalStore("examples/target/goblin-army.db")

    // ---------------------------------------------------------------------------
    // UI output — belongs to the demo presentation layer, not to business logging
    // ---------------------------------------------------------------------------
    println(
      "=== DARK LORD'S ARMY RECRUITMENT — Operation: Ready for Battle ==="
    )
    println(s"Recruits: ${goblins.mkString(", ")}")
    println(s"Stock: 3 weapons | 4 uniforms | 2 boots")
    println("=" * 60)

    log.info(
      s"Recruiting ${goblins.size} goblins — stock: 3 weapons, 4 uniforms, 2 boots"
    )

    // Launch all goblin sagas concurrently — they compete for real resources
    val futures = goblins.map { name =>
      Future {
        log.info(s"Saga started — $name")
        val result = ArmGoblinSaga(name, store)
        result match
          case SagaResult.Completed => log.info(s"Saga completed — $name")
          case SagaResult.Failed(e) =>
            log.info(s"Saga failed — $name — ${e.getMessage}")
        name -> result
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
