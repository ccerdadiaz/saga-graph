package sagagraph.examples.goblin.http

import org.eclipse.jetty.server.Server

// ---------------------------------------------------------------------------
// GoblinHttpServiceRunner — starts all goblin supply services
//
// Ports:
//   8080 — Weights & Measures  (GET /goblin/measure)
//   8081 — Smithy              (POST /weapon/acquire, POST /weapon/return)
//   8082 — Rags & Style        (POST /uniform/acquire, POST /uniform/return)
//   8083 — Cobblery            (POST /boots/acquire, POST /boots/return)
//   8084 — Portrait            (POST /portrait/send)
//
// Services are independent — they know nothing about sagas.
// Can be started standalone or embedded in GoblinArmyHttpDemo.
// ---------------------------------------------------------------------------
object GoblinHttpServiceRunner:

  def startAll(): List[Server] =
    List(
      WeightsAndMeasuresHttpService.start(8080),
      SmithyHttpService.start(8081),
      RagsAndStyleHttpService.start(8082),
      CobbleryHttpService.start(8083),
      PortraitHttpService.start(8084)
    )

  def stopAll(servers: List[Server]): Unit =
    servers.foreach(_.stop())

  def main(args: Array[String]): Unit =
    val servers = startAll()
    println("=== Goblin Supply Services running ===")
    println("  8080 — Weights & Measures")
    println("  8081 — Smithy")
    println("  8082 — Rags & Style")
    println("  8083 — Cobblery")
    println("  8084 — Portrait")
    println("Press ENTER to stop...")
    scala.io.StdIn.readLine()
    stopAll(servers)
    println("=== Services stopped ===")
