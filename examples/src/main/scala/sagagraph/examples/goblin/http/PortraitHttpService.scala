package sagagraph.examples.goblin.http

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import upickle.default.*
import GoblinServiceLog.remote

// ---------------------------------------------------------------------------
// PortraitHttpService — HTTP server exposing the Portrait service
//
// Endpoints:
//   POST /portrait/send  {"name":"Grishnakh"}
//                        → 503 {"error":"Postal raven lost. Mother will never know."}
//
// Always fails — the Dark Lord's postal system is suboptimal.
// BestEffort step — saga continues regardless of response.
// ---------------------------------------------------------------------------
object PortraitHttpService:

  case class SendRequest(name: String)    derives ReadWriter
  case class ErrorResponse(error: String) derives ReadWriter

  def start(port: Int = 8084): Server =
    val server  = Server(port)
    val context = ServletContextHandler()
    context.setContextPath("/")
    server.setHandler(context)
    context.addServlet(ServletHolder(SendServlet()), "/portrait/send")
    server.start()
    server

  class SendServlet extends HttpServlet:
    override def doPost(req: HttpServletRequest, res: HttpServletResponse): Unit =
      val body    = req.getReader.lines().toArray.mkString
      val request = read[SendRequest](body)
      remote.warn(s"[Portrait] Postal raven lost for ${request.name}. Mother will never know.")
      res.setContentType("application/json")
      res.setStatus(503)
      res.getWriter.write(write(ErrorResponse("Postal raven lost. Mother will never know.")))
