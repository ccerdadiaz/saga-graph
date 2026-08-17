package sagagraph.examples.goblin.http

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import java.util.concurrent.atomic.AtomicInteger
import upickle.default.*
import GoblinServiceLog.remote

// ---------------------------------------------------------------------------
// CobbleryHttpService — HTTP server exposing the Cobblery service
//
// Endpoints:
//   POST /boots/acquire  {"name":"Grishnakh","weightKg":67}
//                        → 200 {"size":8}
//                        → 409 {"error":"Out of stock"}
//
//   POST /boots/return   {"name":"Grishnakh"}
//                        → 200 {"status":"returned","stock":N}
//
// The compensation endpoint (/boots/return) was added to support saga-graph
// adoption — existing services would need a similar endpoint.
// ---------------------------------------------------------------------------
object CobbleryHttpService:

  private val stock = AtomicInteger(2)

  case class AcquireRequest(name: String, weightKg: Int) derives ReadWriter
  case class BootsResponse(size: Int)                    derives ReadWriter
  case class ReturnRequest(name: String)                 derives ReadWriter
  case class ReturnResponse(status: String, stock: Int)  derives ReadWriter
  case class ErrorResponse(error: String)                derives ReadWriter

  def start(port: Int = 8083): Server =
    val server  = Server(port)
    val context = ServletContextHandler()
    context.setContextPath("/")
    server.setHandler(context)
    context.addServlet(ServletHolder(AcquireServlet()), "/boots/acquire")
    context.addServlet(ServletHolder(ReturnServlet()),  "/boots/return")
    server.start()
    server

  def reset(initialStock: Int = 2): Unit = stock.set(initialStock)

  class AcquireServlet extends HttpServlet:
    override def doPost(req: HttpServletRequest, res: HttpServletResponse): Unit =
      val body      = req.getReader.lines().toArray.mkString
      val request   = read[AcquireRequest](body)
      val remaining = stock.decrementAndGet()
      if remaining >= 0 then
        val size = (request.weightKg / 10) + 2
        remote.info(s"[Cobblery] ${request.name} gets boots size $size. Stock: $remaining remaining.")
        res.setContentType("application/json")
        res.setStatus(200)
        res.getWriter.write(write(BootsResponse(size)))
      else
        stock.incrementAndGet()
        remote.info(s"[Cobblery] ${request.name} — OUT OF STOCK. Barefoot it is.")
        res.setContentType("application/json")
        res.setStatus(409)
        res.getWriter.write(write(ErrorResponse("Out of stock")))

  class ReturnServlet extends HttpServlet:
    override def doPost(req: HttpServletRequest, res: HttpServletResponse): Unit =
      val body    = req.getReader.lines().toArray.mkString
      val request = read[ReturnRequest](body)
      val current = stock.incrementAndGet()
      remote.info(s"[Cobblery] ${request.name}'s boots returned and available for another request. Stock: $current available.")
      res.setContentType("application/json")
      res.setStatus(200)
      res.getWriter.write(write(ReturnResponse("returned", current)))
