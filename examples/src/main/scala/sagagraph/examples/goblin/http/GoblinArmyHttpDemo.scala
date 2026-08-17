package sagagraph.examples.goblin.http

import sagagraph.*
import sagagraph.store.sqlite.SqliteWalStore
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.*

// ---------------------------------------------------------------------------
// GoblinArmyHttpDemo — arms 5 goblins using real HTTP services
//
// Starts all goblin supply services, runs the sagas concurrently,
// then stops the services. Executable in GitHub Actions with no
// external infrastructure — everything runs in the same JVM process.
//
// Ports: 8080 (W&M), 8081 (Smithy), 8082 (Rags), 8083 (Cobblery), 8084 (Portrait)
// ---------------------------------------------------------------------------
object GoblinArmyHttpDemo:

  def main(args: Array[String]): Unit =
    given ExecutionContext = ExecutionContext.global

    println("=== Starting goblin supply services ===")
    val servers = GoblinHttpServiceRunner.startAll()
    Thread.sleep(500) // give services time to start

    println(
      "=== DARK LORD'S ARMY RECRUITMENT — Operation: Ready for Battle (HTTP) ==="
    )
    println("Recruits: Grishnakh, Ugluk, Muzgash, Lagduf, Snaga")
    println("Stock: 3 weapons | 4 uniforms | 2 boots")
    println("=" * 60)

    val goblins = List("Grishnakh", "Ugluk", "Muzgash", "Lagduf", "Snaga")
    val store = SqliteWalStore("examples/target/goblin-army-http.db")

    val futures = goblins.map { name =>
      Future {
        name -> ArmGoblinHttpSaga(name, store)
      }
    }
    val results = Await.result(Future.sequence(futures), 60.seconds)

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

    println("\n=== Stopping goblin supply services ===")
    GoblinHttpServiceRunner.stopAll(servers)
    println("=== Done ===")
