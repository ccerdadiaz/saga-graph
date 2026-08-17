package sagagraph.examples.goblin.http

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import jakarta.servlet.http.{
  HttpServlet,
  HttpServletRequest,
  HttpServletResponse
}
import upickle.default.*

// ---------------------------------------------------------------------------
// WeightsAndMeasuresHttpService — HTTP server exposing the W&M service
//
// Endpoints:
//   POST /goblin/measure  {"name":"Grishnakh"}
//                         → 200 {"name":"Grishnakh","weightKg":67,"heightCm":143}
//
// Always available — no stock limit.
// The Dark Lord insists on proper documentation.
// ---------------------------------------------------------------------------
object WeightsAndMeasuresHttpService:

  case class MeasureRequest(name: String) derives ReadWriter
  case class GoblinResponse(name: String, weightKg: Int, heightCm: Int)
      derives ReadWriter

  def start(port: Int = 8080): Server =
    val server = Server(port)
    val context = ServletContextHandler()
    context.setContextPath("/")
    server.setHandler(context)

    context.addServlet(ServletHolder(MeasureServlet()), "/goblin/measure")

    server.start()
    server

  class MeasureServlet extends HttpServlet:
    override def doPost(
        req: HttpServletRequest,
        res: HttpServletResponse
    ): Unit =
      val body = req.getReader.lines().toArray.mkString
      val request = read[MeasureRequest](body)
      val weight = 40 + (request.name.length * 3) % 30
      val height = 120 + (request.name.length * 7) % 40
      res.setContentType("application/json")
      res.setStatus(200)
      res.getWriter.write(write(GoblinResponse(request.name, weight, height)))
