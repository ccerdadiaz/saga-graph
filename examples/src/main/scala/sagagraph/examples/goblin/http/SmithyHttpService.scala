package sagagraph.examples.goblin.http

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import upickle.default.*
import GoblinServiceLog.remote

// ---------------------------------------------------------------------------
// SmithyHttpService — HTTP server exposing the Smithy & Kitchenware service
//
// Endpoints:
//   GET  /weapon/available          → ["sword-1","sword-3"]
//   POST /weapon/acquire  {"weaponId":"sword-1"}
//                         → 200 {"id":"sword-1","label":"heavy short sword"}
//                         → 409 {"error":"Not available"}
//   POST /weapon/return   {"weaponId":"sword-1"}
//                         → 200 {"status":"returned"}
//
// The compensation endpoint (/weapon/return) was added to support saga-graph
// adoption — existing services would need a similar endpoint.
// ---------------------------------------------------------------------------
object SmithyHttpService:

  private case class WeaponEntry(id: String, label: String, var available: Boolean = true)

  private val catalog = List(
    WeaponEntry("sword-1", "heavy short sword"),
    WeaponEntry("sword-2", "standard short sword"),
    WeaponEntry("sword-3", "heavy short sword")
  )

  // Returns available IDs in random order — simulates service-side selection policy
  def getAvailable(): List[String] =
    synchronized { scala.util.Random.shuffle(catalog.filter(_.available).map(_.id)) }

  def start(port: Int = 8081): Server =
    val server  = Server(port)
    val context = ServletContextHandler()
    context.setContextPath("/")
    server.setHandler(context)
    context.addServlet(ServletHolder(AvailableServlet()), "/weapon/available")
    context.addServlet(ServletHolder(AcquireServlet()),   "/weapon/acquire")
    context.addServlet(ServletHolder(ReturnServlet()),    "/weapon/return")
    server.start()
    server

  class AvailableServlet extends HttpServlet:
    override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit =
      res.setContentType("application/json")
      res.setStatus(200)
      res.getWriter.write(write(getAvailable()))

  case class AcquireRequest(weaponId: String)          derives ReadWriter
  case class WeaponResponse(id: String, label: String) derives ReadWriter
  case class ReturnRequest(weaponId: String)            derives ReadWriter
  case class ReturnResponse(status: String)             derives ReadWriter
  case class ErrorResponse(error: String)               derives ReadWriter

  class AcquireServlet extends HttpServlet:
    override def doPost(req: HttpServletRequest, res: HttpServletResponse): Unit =
      val body    = req.getReader.lines().toArray.mkString
      val request = read[AcquireRequest](body)
      synchronized:
        catalog.find(w => w.id == request.weaponId && w.available) match
          case Some(w) =>
            w.available = false
            remote.info(s"[Smithy] ${request.weaponId} acquired. Available: ${getAvailable().mkString(", ")}.")
            res.setContentType("application/json")
            res.setStatus(200)
            res.getWriter.write(write(WeaponResponse(w.id, w.label)))
          case None =>
            remote.info(s"[Smithy] ${request.weaponId} — not available. The forge is cold.")
            res.setContentType("application/json")
            res.setStatus(409)
            res.getWriter.write(write(ErrorResponse("Not available")))

  class ReturnServlet extends HttpServlet:
    override def doPost(req: HttpServletRequest, res: HttpServletResponse): Unit =
      val body    = req.getReader.lines().toArray.mkString
      val request = read[ReturnRequest](body)
      synchronized:
        catalog.find(_.id == request.weaponId).foreach(_.available = true)
        remote.info(s"[Smithy] ${request.weaponId} returned and available for another request. Available: ${getAvailable().mkString(", ")}.")
      res.setContentType("application/json")
      res.setStatus(200)
      res.getWriter.write(write(ReturnResponse("returned")))
