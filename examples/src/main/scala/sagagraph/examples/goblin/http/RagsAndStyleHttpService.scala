package sagagraph.examples.goblin.http

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import upickle.default.*
import GoblinServiceLog.remote

// ---------------------------------------------------------------------------
// RagsAndStyleHttpService — HTTP server exposing the Rags & Style service
//
// Endpoints:
//   GET  /uniform/available?size=S   → ["uniform-1","uniform-3"]
//   POST /uniform/acquire  {"uniformId":"uniform-1"}
//                          → 200 {"id":"uniform-1","size":"S","color":"Dark Army Green™"}
//                          → 409 {"error":"Not available"}
//   POST /uniform/return   {"uniformId":"uniform-1"}
//                          → 200 {"status":"returned"}
//
// The compensation endpoint (/uniform/return) was added to support saga-graph
// adoption — existing services would need a similar endpoint.
// ---------------------------------------------------------------------------
object RagsAndStyleHttpService:

  private case class UniformEntry(id: String, size: String, var available: Boolean = true)

  private val catalog = List(
    UniformEntry("uniform-1", "S"),
    UniformEntry("uniform-2", "L"),
    UniformEntry("uniform-3", "S"),
    UniformEntry("uniform-4", "L")
  )

  // Returns available IDs for given size in random order
  def getAvailable(size: String): List[String] =
    synchronized { scala.util.Random.shuffle(catalog.filter(u => u.available && u.size == size).map(_.id)) }

  def start(port: Int = 8082): Server =
    val server  = Server(port)
    val context = ServletContextHandler()
    context.setContextPath("/")
    server.setHandler(context)
    context.addServlet(ServletHolder(AvailableServlet()), "/uniform/available")
    context.addServlet(ServletHolder(AcquireServlet()),   "/uniform/acquire")
    context.addServlet(ServletHolder(ReturnServlet()),    "/uniform/return")
    server.start()
    server

  class AvailableServlet extends HttpServlet:
    override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit =
      val size = Option(req.getParameter("size")).getOrElse("")
      res.setContentType("application/json")
      res.setStatus(200)
      res.getWriter.write(write(getAvailable(size)))

  case class AcquireRequest(uniformId: String)                           derives ReadWriter
  case class UniformResponse(id: String, size: String, color: String)    derives ReadWriter
  case class ReturnRequest(uniformId: String)                            derives ReadWriter
  case class ReturnResponse(status: String)                              derives ReadWriter
  case class ErrorResponse(error: String)                                derives ReadWriter

  class AcquireServlet extends HttpServlet:
    override def doPost(req: HttpServletRequest, res: HttpServletResponse): Unit =
      val body    = req.getReader.lines().toArray.mkString
      val request = read[AcquireRequest](body)
      synchronized:
        catalog.find(u => u.id == request.uniformId && u.available) match
          case Some(u) =>
            u.available = false
            remote.info(s"[Rags & Style] ${request.uniformId} (size ${u.size}) acquired.")
            res.setContentType("application/json")
            res.setStatus(200)
            res.getWriter.write(write(UniformResponse(u.id, u.size, "Dark Army Green™")))
          case None =>
            remote.info(s"[Rags & Style] ${request.uniformId} — not available. Naked goblins are undignified.")
            res.setContentType("application/json")
            res.setStatus(409)
            res.getWriter.write(write(ErrorResponse("Not available")))

  class ReturnServlet extends HttpServlet:
    override def doPost(req: HttpServletRequest, res: HttpServletResponse): Unit =
      val body    = req.getReader.lines().toArray.mkString
      val request = read[ReturnRequest](body)
      synchronized:
        catalog.find(_.id == request.uniformId).foreach(_.available = true)
        remote.info(s"[Rags & Style] ${request.uniformId} returned and available for another request.")
      res.setContentType("application/json")
      res.setStatus(200)
      res.getWriter.write(write(ReturnResponse("returned")))
