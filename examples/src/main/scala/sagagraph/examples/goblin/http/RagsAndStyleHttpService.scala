package sagagraph.examples.goblin.http

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import jakarta.servlet.http.{
  HttpServlet,
  HttpServletRequest,
  HttpServletResponse
}
import java.util.concurrent.atomic.AtomicInteger
import upickle.default.*

// ---------------------------------------------------------------------------
// RagsAndStyleHttpService — HTTP server exposing the Rags & Style service
//
// Endpoints:
//   POST /uniform/acquire  {"name":"Grishnakh","heightCm":143}
//                          → 200 {"size":"S","color":"Dark Army Green™"}
//                          → 409 {"error":"Out of stock"}
//
//   POST /uniform/return   {"name":"Grishnakh"}
//                          → 200 {"status":"returned","stock":N}
//
// The compensation endpoint (/uniform/return) was added to support saga-graph
// adoption — existing services would need a similar endpoint.
// ---------------------------------------------------------------------------
object RagsAndStyleHttpService:

  private val stock = AtomicInteger(4)

  case class AcquireRequest(name: String, heightCm: Int) derives ReadWriter
  case class UniformResponse(size: String, color: String) derives ReadWriter
  case class ReturnRequest(name: String) derives ReadWriter
  case class ReturnResponse(status: String, stock: Int) derives ReadWriter
  case class ErrorResponse(error: String) derives ReadWriter

  def start(port: Int = 8082): Server =
    val server = Server(port)
    val context = ServletContextHandler()
    context.setContextPath("/")
    server.setHandler(context)

    context.addServlet(ServletHolder(AcquireServlet()), "/uniform/acquire")
    context.addServlet(ServletHolder(ReturnServlet()), "/uniform/return")

    server.start()
    server

  def reset(initialStock: Int = 4): Unit = stock.set(initialStock)

  class AcquireServlet extends HttpServlet:
    override def doPost(
        req: HttpServletRequest,
        res: HttpServletResponse
    ): Unit =
      val body = req.getReader.lines().toArray.mkString
      val request = read[AcquireRequest](body)
      val remaining = stock.decrementAndGet()
      if remaining >= 0 then
        val size = if request.heightCm > 145 then "L" else "S"
        res.setContentType("application/json")
        res.setStatus(200)
        res.getWriter.write(write(UniformResponse(size, "Dark Army Green™")))
      else
        stock.incrementAndGet()
        res.setContentType("application/json")
        res.setStatus(409)
        res.getWriter.write(write(ErrorResponse("Out of stock")))

  class ReturnServlet extends HttpServlet:
    override def doPost(
        req: HttpServletRequest,
        res: HttpServletResponse
    ): Unit =
      val body = req.getReader.lines().toArray.mkString
      val request = read[ReturnRequest](body)
      val current = stock.incrementAndGet()
      res.setContentType("application/json")
      res.setStatus(200)
      res.getWriter.write(write(ReturnResponse("returned", current)))
