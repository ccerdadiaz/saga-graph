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

    val goblins = List("Grishnakh", "Ugluk", "Muzgash", "Lagduf", "Snaga")
    val store = SqliteWalStore("examples/target/goblin-army.db")

    println(
      "=== DARK LORD'S ARMY RECRUITMENT — Operation: Ready for Battle ==="
    )
    println(s"Recruits: ${goblins.mkString(", ")}")
    println(s"Stock: 3 weapons | 4 uniforms | 2 boots")
    println("=" * 60)

    // Launch all goblin sagas concurrently — they compete for real resources
    val futures = goblins.map { name =>
      Future {
        println(s"  [${System.currentTimeMillis()}] >> Arming $name...")
        name -> ArmGoblinSaga(name, store)
      }
    }
    val results = Await.result(Future.sequence(futures), 30.seconds)

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
