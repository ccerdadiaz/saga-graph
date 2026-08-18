package sagagraph.examples.goblin.http

import _root_.sagagraph.*
import _root_.sagagraph.store.sqlite.SqliteWalStore
import _root_.sagagraph.examples.goblin.GoblinEquipment
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.*

// ---------------------------------------------------------------------------
// GoblinArmyHttpDemo — arms 5 goblins using real HTTP services
//
// Starts all goblin supply services, runs the sagas concurrently,
// then stops the services.
//
// Ports: 8080 (W&M), 8081 (Smithy), 8082 (Rags), 8083 (Cobblery), 8084 (Portrait)
// ---------------------------------------------------------------------------
object GoblinArmyHttpDemo:

  def main(args: Array[String]): Unit =
    given ExecutionContext = ExecutionContext.global

    println("=== Starting goblin supply services ===")
    val servers = GoblinHttpServiceRunner.startAll()
    Thread.sleep(500) // give services time to start

    // ---------------------------------------------------------------------------
    // UI output — belongs to the demo presentation layer, not to business logging
    // ---------------------------------------------------------------------------
    println("=== DARK LORD'S ARMY RECRUITMENT — Operation: Ready for Battle (HTTP) ===")
    println("Recruits: Grishnakh, Ugluk, Muzgash, Lagduf, Snaga")
    println(s"Catalog: ${GoblinHttpClient.availableWeapons().mkString(", ")} | ${GoblinHttpClient.availableUniforms("S").mkString(", ")} | ${GoblinHttpClient.availableBoots(7).mkString(", ")}")
    println("=" * 60)

    val goblins = List("Grishnakh", "Ugluk", "Muzgash", "Lagduf", "Snaga")
    val store   = SqliteWalStore("examples/target/goblin-army-http.db")

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
          weaponId  = GoblinHttpClient.availableWeapons().headOption.getOrElse("none"),
          uniformId = GoblinHttpClient.availableUniforms(uniformSize).headOption.getOrElse("none"),
          bootsId   = GoblinHttpClient.availableBoots(bootsSize).headOption
        )

        name -> ArmGoblinHttpSaga(name, equipment, store)
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
