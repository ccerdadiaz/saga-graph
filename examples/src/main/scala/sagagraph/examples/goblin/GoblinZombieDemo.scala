package sagagraph.examples.goblin

import sagagraph.*
import sagagraph.store.sqlite.SqliteWalStore
import scala.concurrent.duration.*

// ---------------------------------------------------------------------------
// GoblinZombieDemo — demonstrates ZombieHunter autonomous recovery
//
// Scenario:
//   1. A goblin saga starts arming Grimfang
//   2. The process "crashes" after writing to the WAL — compensation never runs
//   3. ZombieHunter detects the zombie saga and compensates autonomously
//   4. WAL state is verified — saga marked Compensated
//
// The crash is simulated by writing WAL entries without executing actions.
// In production this would happen if the process dies between WAL write
// and action execution — the invariant that protects us.
// ---------------------------------------------------------------------------
object GoblinZombieDemo:

  def main(args: Array[String]): Unit =

    val log   = Slf4jLogger("sagagraph.examples.goblin.GoblinZombieDemo")
    val store = SqliteWalStore("examples/target/goblin-zombie.db")

    // -------------------------------------------------------------------------
    // UI output — belongs to the demo presentation layer, not to business logging
    // -------------------------------------------------------------------------
    println("=== DARK LORD'S ZOMBIE RECOVERY DEMO ===")
    println("Goblin: Grimfang")
    println("=" * 60)

    // -------------------------------------------------------------------------
    // Step 1 — Simulate a crashed saga
    // WAL entries written, process "dies" before executing any action
    // In production: JVM crash, OOM, power loss — WAL protects us
    // -------------------------------------------------------------------------
    val sagaId = SagaId.generate()
    log.info(s"Simulating process crash for saga ${sagaId.value.take(8)}")

    val measureEntry = WalEntry(
      stepName         = "measure-Grimfang",
      compensate       = () => Right(()),
      compensationRef  = Some("destroyMeasurements"),
      compensationArgs = Some("""{"goblin":"Grimfang"}""")
    )
    val weaponEntry = WalEntry(
      stepName         = "weapon-Grimfang",
      compensate       = () => Right(()),
      compensationRef  = Some("returnWeapon"),
      compensationArgs = Some("""{"weaponId":"sword-1"}""")
    )
    val uniformEntry = WalEntry(
      stepName         = "uniform-Grimfang",
      compensate       = () => Right(()),
      compensationRef  = Some("returnUniform"),
      compensationArgs = Some("""{"uniformId":"uniform-1"}""")
    )

    // Write WAL — this is what the engine would do before each action
    store.registerSaga(sagaId)
    store.append(sagaId, measureEntry)
    store.append(sagaId, weaponEntry)
    store.append(sagaId, uniformEntry)

    // *** CRASH SIMULATED HERE ***
    // In a real crash the process would die at this point.
    // The WAL has the compensation info — ZombieHunter will use it.
    log.info(s"Process crashed — saga ${sagaId.value.take(8)} is now a zombie")

    // -------------------------------------------------------------------------
    // Step 2 — Build compensation registry
    // In production this would be the same registry the engine uses
    // -------------------------------------------------------------------------
    val registry = CompensationRegistry()
      .register("destroyMeasurements", args =>
        log.info(s"[Recovery] Destroying measurement records — ${args.getOrElse("{}")}")
        Right(())
      )
      .register("returnWeapon", args =>
        log.info(s"[Recovery] Returning weapon to Smithy — ${args.getOrElse("{}")}")
        Right(())
      )
      .register("returnUniform", args =>
        log.info(s"[Recovery] Returning uniform to Rags & Style — ${args.getOrElse("{}")}")
        Right(())
      )

    // -------------------------------------------------------------------------
    // Step 3 — Start ZombieHunter autonomous process
    // Low threshold for demo — in production use minutes or hours
    // -------------------------------------------------------------------------
    log.info("Starting ZombieHunter — scanning every 2 seconds, threshold 1 second")
    val hunter = ZombieHunter(store, registry)
      .withInterval(2.seconds)
      .withThreshold(1.second)
      .start()

    // Wait for ZombieHunter to detect and recover the zombie
    Thread.sleep(5000)

    // -------------------------------------------------------------------------
    // Step 4 — Stop ZombieHunter and report
    // -------------------------------------------------------------------------
    log.info("Stopping ZombieHunter")
    hunter.stop()

    // -------------------------------------------------------------------------
    // UI output — belongs to the demo presentation layer, not to business logging
    // -------------------------------------------------------------------------
    println("\n" + "=" * 60)
    println("=== ZOMBIE RECOVERY REPORT ===")
    store.loadActionable(sagaId) match
      case Left(err) =>
        println(s"  ERROR reading WAL: ${err.getMessage}")
      case Right(pending) if pending.isEmpty =>
        println(s"  Saga ${sagaId.value.take(8)}: ✓ FULLY RECOVERED — no actionable entries remain")
      case Right(pending) =>
        println(s"  Saga ${sagaId.value.take(8)}: ✗ PARTIALLY RECOVERED — ${pending.size} entries still pending")
        pending.foreach(e => println(s"    - ${e.stepName}"))
    println("=" * 60)

    store.close()
