package sagagraph.examples.goblin

import sagagraph.*
import sagagraph.store.sqlite.SqliteWalStore
import sagagraph.CompArgs.given
import scala.concurrent.duration.*

// ---------------------------------------------------------------------------
// GoblinTimeoutDemo — demonstrates TTL and Unknown state
//
// Two goblins visit the Smithy. The Smithy is having a bad day.
//
// Grimfang:
//   measure  → Done ✓  (normal)
//   weapon   → Smithy responds in 200ms, TTL = 50ms → TIMEOUT → Unknown
//   ← engine compensates measure (Done) → Compensated
//   ← saga stays Compensating, weapon Unknown
//   ← ZombieHunter compensates weapon (Unknown) → Compensated
//   ← saga Compensated
//
// Bolg:
//   measure  → Done ✓
//   weapon   → Smithy responds in 30ms, TTL = 200ms → Done ✓ (slow but within TTL)
//   uniform  → Done ✓
//   boots    → Done ✓
//   → Completed ✓
//
// The same step — weapon acquisition — behaves differently depending on
// the relationship between service response time and configured TTL.
// Unknown and Left trigger the same compensation behavior.
// ---------------------------------------------------------------------------
object GoblinTimeoutDemo:

  // A slow Smithy — responds after a configurable delay
  // In production this would be a real service that is temporarily overloaded
  // Controlled delay without random latency — for precise TTL demonstration
  private def slowAcquire(weaponId: String, delay: Duration): Either[Throwable, Weapon] =
    Thread.sleep(delay.toMillis)
    SmithyService.acquireRaw(weaponId)

  def main(args: Array[String]): Unit =

    val log   = Slf4jLogger("sagagraph.examples.goblin.GoblinTimeoutDemo")
    val store = SqliteWalStore("examples/target/goblin-timeout.db")

    SmithyService.resetWithStock(2)        // sword-1, sword-2
    RagsAndStyleService.resetWithStock(2)  // uniform-1, uniform-2
    CobbleryService.resetWithStock(2)      // boots-1, boots-2

    println("=== THE TIMEOUT DEMO ===")
    println("Two goblins visit the Smithy. The Smithy is having a bad day.")
    println()
    println("=" * 60)

    // -------------------------------------------------------------------------
    // Goblin A — Grimfang
    // Smithy responds in 200ms but TTL is 50ms → timeout → Unknown
    // -------------------------------------------------------------------------
    println("\n--- GOBLIN A: Grimfang ---")
    println("Smithy response time: 200ms | Step TTL: 50ms → will timeout")
    println()

    val sagaIdA   = SagaId.generate()
    val weaponIdA = SmithyService.getAvailable().head

    val registry = CompensationRegistry()
      .register("destroyMeasurements", _ => Right(()))
      .register("returnWeapon", args =>
        val id = args.flatMap(a => scala.util.Try(ujson.read(a)("weaponId").str).toOption).getOrElse(weaponIdA)
        log.info(s"[ZombieHunter] Returning weapon $id — idempotent compensation")
        SmithyService.return_(id)
      )

    val zh = ZombieHunter(store, registry)
      .withInterval(2.seconds)
      .withThreshold(1.second)
      .withLogger(Slf4jLogger("sagagraph.examples.goblin.ZombieHunter"))
      .start()

    val resultA = SagaGraph()
      .step(
        name       = s"measure-Grimfang",
        action     = () =>
          log.debug("Requesting measurement — Grimfang")
          WeightsAndMeasuresService.measure("Grimfang").map(_ => ()),
        compensate = () =>
          log.info("Compensating — destroying measurement records for Grimfang")
          Right(()),
        ref  = Some("destroyMeasurements"),
        args = CompArgs("goblin" -> "Grimfang")
      )
      .step(
        name       = s"weapon-Grimfang",
        action     = () =>
          log.debug(s"Requesting weapon $weaponIdA — Grimfang (Smithy is slow today...)")
          slowAcquire(weaponIdA, 200.millis).map(_ => ()),  // 200ms response
        compensate = () =>
          log.info(s"Compensating — returning weapon $weaponIdA — Grimfang")
          SmithyService.return_(weaponIdA),
        ref  = Some("returnWeapon"),
        args = CompArgs("weaponId" -> weaponIdA),
        ttl  = 50.millis  // TTL too short → timeout → Unknown
      )
      .run(store, sagaId = sagaIdA, logger = log)

    println(s"\nGrimfang result: ${resultA match
      case SagaResult.Completed => "✓ ARMED AND READY"
      case SagaResult.Failed(e) => s"✗ FAILED — ${e.getMessage}"
    }")

    println("\nWaiting for ZombieHunter to recover Grimfang's saga...")
    Thread.sleep(5000)

    // -------------------------------------------------------------------------
    // Goblin B — Bolg
    // Smithy responds in 30ms, TTL is 200ms → Done ✓
    // -------------------------------------------------------------------------
    println("\n--- GOBLIN B: Bolg ---")
    println("Smithy response time: 30ms | Step TTL: 200ms → will complete")
    println()

    val weaponIdB  = SmithyService.getAvailable().head
    val uniformIdB = RagsAndStyleService.getAvailable("S").headOption.getOrElse("none")
    val bootsIdB   = CobbleryService.getAvailableAny().headOption

    val resultB = SagaGraph()
      .step(
        name       = "measure-Bolg",
        action     = () =>
          log.debug("Requesting measurement — Bolg")
          WeightsAndMeasuresService.measure("Bolg").map(_ => ()),
        compensate = () => Right(()),
        ref        = Some("destroyMeasurements"),
        args       = CompArgs("goblin" -> "Bolg")
      )
      .step(
        name       = "weapon-Bolg",
        action     = () =>
          log.debug(s"Requesting weapon $weaponIdB — Bolg (Smithy is still slow...)")
          slowAcquire(weaponIdB, 30.millis).map(_ => ()),  // 30ms response — within TTL
        compensate = () => SmithyService.return_(weaponIdB),
        ref        = Some("returnWeapon"),
        args       = CompArgs("weaponId" -> weaponIdB),
        ttl        = 200.millis  // generous TTL → completes ✓
      )
      .optional(
        name       = "boots-Bolg",
        action     = () =>
          bootsIdB match
            case None     => Left(OutOfStockException("Cobblery"))
            case Some(id) => CobbleryService.acquire(id).map(_ => ()),
        compensate = () => bootsIdB.map(CobbleryService.return_).getOrElse(Right(())),
        ref        = bootsIdB.map(_ => "returnBoots"),
        args       = CompArgs("bootsId" -> bootsIdB.getOrElse("none"))
      )
      .bestEffort(
        name   = "portrait-Bolg",
        action = () => PortraitService.sendToMother(Goblin("Bolg", 55, 130))
      )
      .run(store, logger = log)

    println(s"\nBolg result: ${resultB match
      case SagaResult.Completed => "✓ ARMED AND READY"
      case SagaResult.Failed(e) => s"✗ FAILED — ${e.getMessage}"
    }")

    Thread.sleep(2000)
    zh.stop()

    // -------------------------------------------------------------------------
    // Summary
    // -------------------------------------------------------------------------
    println("\n" + "=" * 60)
    println("=== WHAT JUST HAPPENED ===")
    println()
    println("  Grimfang's weapon step timed out after 50ms.")
    println("  The Smithy may or may not have processed the request.")
    println("  The step was marked Unknown — the engine does not know.")
    println("  The engine compensated measure-Grimfang (Done) → Compensated.")
    println("  ZombieHunter found weapon-Grimfang (Unknown) and compensated it.")
    println()
    println("  Bolg's weapon step took 30ms — within the 200ms TTL.")
    println("  Done. No timeout. No ZombieHunter. Business as usual.")
    println()
    println("  Unknown and Left trigger the same compensation behavior.")
    println("  The engine does not distinguish — it compensates either way.")
    println("=" * 60)

    println("\n=== EXECUTION TIMELINE ===")
    store.printTimeline()

    println("\n=== WAL CORRECTNESS ANALYSIS ===")
    store.printSagaStats()
    store.printEntryStats()

    store.close()
