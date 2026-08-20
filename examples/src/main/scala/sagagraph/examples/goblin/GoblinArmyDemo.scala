package sagagraph.examples.goblin

import sagagraph.*
import sagagraph.store.sqlite.SqliteWalStore
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.*

// ---------------------------------------------------------------------------
// GoblinArmyDemo — arms goblins concurrently with limited resources
//
// Usage:
//   sbt "examples/runMain sagagraph.examples.goblin.GoblinArmyDemo"
//   sbt "examples/runMain sagagraph.examples.goblin.GoblinArmyDemo 50 0.1"
//
// Args:
//   goblinCount  — number of goblins to recruit (default: 5)
//   failureRate  — fraction of compensations that fail deliberately [0.0..1.0]
//                  0.0 = no induced failures (default)
//                  0.1 = ~10% of compensations fail deterministically
//
// With default args: standard demo — 5 goblins, no induced failures
// With volume args:  correctness test — detects WAL inconsistencies under load
//
// ZombieHunter runs autonomously during execution — recovers any saga that
// dies mid-flight, including those with induced compensation failures.
// ---------------------------------------------------------------------------
object GoblinArmyDemo:

  def main(args: Array[String]): Unit =
    given ExecutionContext = ExecutionContext.global

    val goblinCount = args.headOption.flatMap(_.toIntOption).getOrElse(5)
    val failureRate = args.lift(1).flatMap(_.toDoubleOption).getOrElse(0.0)

    val log = Slf4jLogger("sagagraph.examples.goblin.GoblinArmyDemo")
    val store = SqliteWalStore("examples/target/goblin-army.db")

    // Scale stock with goblin count — enough to arm ~60%, rest compete and fail
    SmithyService.resetWithStock(goblinCount * 6 / 10)
    RagsAndStyleService.resetWithStock(goblinCount * 7 / 10)
    CobbleryService.resetWithStock(goblinCount * 4 / 10)

    // Generate goblin names
    val baseNames = List(
      "Grishnakh",
      "Ugluk",
      "Muzgash",
      "Lagduf",
      "Snaga",
      "Gorbag",
      "Shagrat",
      "Bolg",
      "Azog",
      "Gothmog",
      "Lurtz",
      "Grima",
      "Sharku",
      "Snaga",
      "Mauhur"
    )
    val goblins = (0 until goblinCount)
      .map(i => s"${baseNames(i % baseNames.size)}-${i + 1}")
      .toList

    // ---------------------------------------------------------------------------
    // UI output — belongs to the demo presentation layer, not to business logging
    // ---------------------------------------------------------------------------
    println(
      "=== DARK LORD'S ARMY RECRUITMENT — Operation: Ready for Battle ==="
    )
    println(s"Recruits: $goblinCount goblins")
    if failureRate > 0.0 then
      println(
        f"Induced failure rate: ${failureRate * 100}%.1f%% (deterministic, weaponId hash)"
      )
    println(s"Catalog: ${SmithyService.getAvailable().mkString(", ")} | ${RagsAndStyleService
        .getAvailable("S")
        .mkString(", ")} | ${CobbleryService.getAvailableAny().mkString(", ")}")
    println("=" * 60)

    log.info(s"Recruiting $goblinCount goblins — failureRate: $failureRate")

    // Start ZombieHunter — runs autonomously during recruitment
    val registry = CompensationRegistry()
      .register("destroyMeasurements", _ => Right(()))
      .register(
        "returnWeapon",
        args =>
          args
            .flatMap(a =>
              scala.util
                .Try(
                  SmithyService.return_(ujson.read(a)("weaponId").str)
                )
                .toOption
            )
            .getOrElse(Right(()))
      )
      .register(
        "returnUniform",
        args =>
          args
            .flatMap(a =>
              scala.util
                .Try(
                  RagsAndStyleService.return_(ujson.read(a)("uniformId").str)
                )
                .toOption
            )
            .getOrElse(Right(()))
      )
      .register(
        "returnBoots",
        args =>
          args
            .flatMap(a =>
              scala.util
                .Try(
                  CobbleryService.return_(ujson.read(a)("bootsId").str)
                )
                .toOption
            )
            .getOrElse(Right(()))
      )

    val hunter = ZombieHunter(store, registry)
      .withInterval(2.seconds)
      .withThreshold(20.seconds)
      .withLogger(log)
      .start()

    // -------------------------------------------------------------------------
    // Pre-saga — resolve resources before starting
    // -------------------------------------------------------------------------
    val futures = goblins.map { name =>
      Future {
        val goblinHeight = 120 + (name.length * 7) % 40
        val goblinWeight = 40 + (name.length * 3) % 30
        val uniformSize = if goblinHeight > 145 then "L" else "S"
        val bootsSize = (goblinWeight / 10) + 2

        val equipment = GoblinEquipment(
          weaponId = SmithyService.getAvailable().headOption.getOrElse("none"),
          uniformId = RagsAndStyleService
            .getAvailable(uniformSize)
            .headOption
            .getOrElse("none"),
          bootsId = CobbleryService.getAvailable(bootsSize).headOption
        )

        name -> ArmGoblinSaga(name, equipment, store, failureRate)
      }
    }
    val results = Await.result(Future.sequence(futures), 120.seconds)

    // Stop ZombieHunter — wait for current cycle to complete
    hunter.stop()

    // ---------------------------------------------------------------------------
    // UI output — belongs to the demo presentation layer, not to business logging
    // ---------------------------------------------------------------------------
    val completed = results.count(_._2 == SagaResult.Completed)
    val failed = results.count(_._2.isInstanceOf[SagaResult.Failed])

    println("\n" + "=" * 60)
    println("=== RECRUITMENT REPORT ===")
    if goblinCount <= 20 then
      results.foreach { case (name, result) =>
        val status = result match
          case SagaResult.Completed => "✓ ARMED AND READY"
          case SagaResult.Failed(e) => s"✗ FAILED — ${e.getMessage}"
        println(s"  $name: $status")
      }
    else
      println(s"  Armed and ready: $completed")
      println(s"  Failed:          $failed")
    println("=" * 60)
    println("The Dark Lord will be... partially satisfied.")

    // WAL correctness analysis
    println("\n" + "=" * 60)
    println("=== WAL CORRECTNESS ANALYSIS ===")
    printWalAnalysis(store)
    println("=" * 60)

    if goblinCount <= 20 then
      println("\n" + "=" * 60)
      println("=== EXECUTION TIMELINE ===")
      store.printTimeline()

    store.close()

  private def printWalAnalysis(store: SqliteWalStore): Unit =
    store.printSagaStats()
    store.printEntryStats()
