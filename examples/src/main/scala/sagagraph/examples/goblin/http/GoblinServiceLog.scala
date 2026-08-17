package sagagraph.examples.goblin.http

import org.slf4j.LoggerFactory

// ---------------------------------------------------------------------------
// GoblinServiceLog — shared logger for all goblin HTTP services
//
// In production each service would have its own logging infrastructure.
// Here we use a single logger routed to services.log to demonstrate
// the separation between the saga orchestration universe and the
// remote services universe — without simulating N separate configurations.
// ---------------------------------------------------------------------------
object GoblinServiceLog:
  val remote = LoggerFactory.getLogger("GoblinServices")
