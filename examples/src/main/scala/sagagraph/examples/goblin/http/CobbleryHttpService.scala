package sagagraph.examples.goblin.http

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import upickle.default.*
import GoblinServiceLog.remote

// ---------------------------------------------------------------------------
// CobbleryHttpService — HTTP server exposing the Cobblery service
//
// Endpoints:
//   GET  /boots/available?size=7    → ["boots-1"]
//   POST /boots/acquire  {"bootsId":"boots-1"}
//                        → 200 {"id":"boots-1","bootSize":7}
//                        → 409 {"error":"Not available"}
//   POST /boots/return   {"bootsId":"boots-1"}
//                        → 200 {"status":"returned"}
//
// The compensation endpoint (/boots/return) was added to support saga-graph
// adoption — existing services would need a similar endpoint.
// ---------------------------------------------------------------------------
object CobbleryHttpService:

  private case class BootsEntry(id: String, bootSize: Int, var available: Boolean = true)

  private val catalog = List(
    BootsEntry("boots-1", 7),
    BootsEntry("boots-2", 8)
  )

  def getAvailable(size: Int): List[String] =
    synchronized { scala.util.Random.shuffle(catalog.filter(b => b.available && b.bootSize == size).map(_.id)) }

  def getAvailableAny(): List[String] =
    synchronized { scala.util.Random.shuffle(catalog.filter(_.available).map(_.id)) }

  def start(port: Int = 8083): Server =
    val server  = Server(port)
    val context = ServletContextHandler()
    context.setContextPath("/")
    server.setHandler(context)
    context.addServlet(ServletHolder(AvailableServlet()), "/boots/available")
    context.addServlet(ServletHolder(AcquireServlet()),   "/boots/acquire")
    context.addServlet(ServletHolder(ReturnServlet()),    "/boots/return")
    server.start()
    server

  class AvailableServlet extends HttpServlet:
    override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit =
      val size = Option(req.getParameter("size")).flatMap(_.toIntOption).getOrElse(0)
      val ids  = if size > 0 then getAvailable(size) else getAvailableAny()
      res.setContentType("application/json")
      res.setStatus(200)
      res.getWriter.write(write(ids))

  case class AcquireRequest(bootsId: String)          derives ReadWriter
  case class BootsResponse(id: String, bootSize: Int) derives ReadWriter
  case class ReturnRequest(bootsId: String)            derives ReadWriter
  case class ReturnResponse(status: String)            derives ReadWriter
  case class ErrorResponse(error: String)              derives ReadWriter

  class AcquireServlet extends HttpServlet:
    override def doPost(req: HttpServletRequest, res: HttpServletResponse): Unit =
      val body    = req.getReader.lines().toArray.mkString
      val request = read[AcquireRequest](body)
      synchronized:
        catalog.find(b => b.id == request.bootsId && b.available) match
          case Some(b) =>
            b.available = false
            remote.info(s"[Cobblery] ${request.bootsId} (size ${b.bootSize}) acquired.")
            res.setContentType("application/json")
            res.setStatus(200)
            res.getWriter.write(write(BootsResponse(b.id, b.bootSize)))
          case None =>
            remote.info(s"[Cobblery] ${request.bootsId} — not available. Barefoot it is.")
            res.setContentType("application/json")
            res.setStatus(409)
            res.getWriter.write(write(ErrorResponse("Not available")))

  class ReturnServlet extends HttpServlet:
    override def doPost(req: HttpServletRequest, res: HttpServletResponse): Unit =
      val body    = req.getReader.lines().toArray.mkString
      val request = read[ReturnRequest](body)
      synchronized:
        catalog.find(_.id == request.bootsId).foreach(_.available = true)
        remote.info(s"[Cobblery] ${request.bootsId} returned and available for another request.")
      res.setContentType("application/json")
      res.setStatus(200)
      res.getWriter.write(write(ReturnResponse("returned")))
