package sagagraph.examples.goblin.http

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import java.util.concurrent.atomic.AtomicInteger
import upickle.default.*
import GoblinServiceLog.remote

// ---------------------------------------------------------------------------
// SmithyHttpService — HTTP server exposing the Smithy & Kitchenware service
//
// Endpoints:
//   POST /weapon/acquire  {"name":"Grishnakh","weightKg":67}
//                         → 200 {"kind":"short sword","size":"heavy"}
//                         → 409 {"error":"Out of stock"}
//
//   POST /weapon/return   {"name":"Grishnakh"}
//                         → 200 {"status":"returned","stock":N}
//
// This service knows nothing about sagas — it manages its own stock.
// The compensation endpoint (/weapon/return) was added to support saga-graph
// adoption — existing services would need a similar endpoint.
// ---------------------------------------------------------------------------
object SmithyHttpService:

  private val stock = AtomicInteger(3)

  case class AcquireRequest(name: String, weightKg: Int) derives ReadWriter
  case class WeaponResponse(kind: String, size: String)  derives ReadWriter
  case class ReturnRequest(name: String)                 derives ReadWriter
  case class ReturnResponse(status: String, stock: Int)  derives ReadWriter
  case class ErrorResponse(error: String)                derives ReadWriter

  def start(port: Int = 8081): Server =
    val server  = Server(port)
    val context = ServletContextHandler()
    context.setContextPath("/")
    server.setHandler(context)
    context.addServlet(ServletHolder(AcquireServlet()), "/weapon/acquire")
    context.addServlet(ServletHolder(ReturnServlet()),  "/weapon/return")
    server.start()
    server

  def reset(initialStock: Int = 3): Unit = stock.set(initialStock)

  class AcquireServlet extends HttpServlet:
    override def doPost(req: HttpServletRequest, res: HttpServletResponse): Unit =
      val body      = req.getReader.lines().toArray.mkString
      val request   = read[AcquireRequest](body)
      val remaining = stock.decrementAndGet()
      if remaining >= 0 then
        val size = if request.weightKg > 55 then "heavy" else "standard"
        remote.info(s"[Smithy] ${request.name} equipped with $size short sword. Stock: $remaining remaining.")
        res.setContentType("application/json")
        res.setStatus(200)
        res.getWriter.write(write(WeaponResponse("short sword", size)))
      else
        stock.incrementAndGet()
        remote.info(s"[Smithy] ${request.name} — OUT OF STOCK. The forge is cold.")
        res.setContentType("application/json")
        res.setStatus(409)
        res.getWriter.write(write(ErrorResponse("Out of stock")))

  class ReturnServlet extends HttpServlet:
    override def doPost(req: HttpServletRequest, res: HttpServletResponse): Unit =
      val body    = req.getReader.lines().toArray.mkString
      val request = read[ReturnRequest](body)
      val current = stock.incrementAndGet()
      remote.info(s"[Smithy] ${request.name}'s short sword returned and available for another request. Stock: $current available.")
      res.setContentType("application/json")
      res.setStatus(200)
      res.getWriter.write(write(ReturnResponse("returned", current)))
