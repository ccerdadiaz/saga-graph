package sagagraph.examples.goblin.http

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers
import upickle.default.*
import _root_.sagagraph.examples.goblin.{Goblin, Weapon, Uniform, Boot, OutOfStockException, GoblinEquipment}

// ---------------------------------------------------------------------------
// GoblinHttpClient — HTTP client for goblin supply services
//
// Uses java.net.http.HttpClient (JDK 11+) — zero external dependencies.
// Translates HTTP responses into Either[Throwable, T] for saga consumption.
//
// The saga engine sees only () => Either[Throwable, Unit] — it knows nothing
// about HTTP, JSON, or service internals.
// ---------------------------------------------------------------------------
object GoblinHttpClient:

  private val client = HttpClient.newHttpClient()

  private def post(url: String, body: String): HttpResponse[String] =
    val request = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .header("Content-Type", "application/json")
      .POST(BodyPublishers.ofString(body))
      .build()
    client.send(request, BodyHandlers.ofString())

  private def get(url: String): HttpResponse[String] =
    val request = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .GET()
      .build()
    client.send(request, BodyHandlers.ofString())

  // -------------------------------------------------------------------------
  // Pre-saga — resolve available resources before starting
  // Returns available IDs in random order — service applies its own policy
  // Caller applies additional business criteria to choose from the list
  // -------------------------------------------------------------------------
  def availableWeapons(): List[String] =
    try read[List[String]](get("http://localhost:8081/weapon/available").body())
    catch case _ => List.empty

  def availableUniforms(size: String): List[String] =
    try read[List[String]](get(s"http://localhost:8082/uniform/available?size=$size").body())
    catch case _ => List.empty

  def availableBoots(size: Int): List[String] =
    try read[List[String]](get(s"http://localhost:8083/boots/available?size=$size").body())
    catch case _ => List.empty

  // -------------------------------------------------------------------------
  // Weights & Measures
  // -------------------------------------------------------------------------
  case class GoblinResponse(name: String, weightKg: Int, heightCm: Int) derives ReadWriter

  def measure(name: String): Either[Throwable, Goblin] =
    try
      val res = post("http://localhost:8080/goblin/measure", s"""{"name":"$name"}""")
      if res.statusCode() == 200 then
        val g = read[GoblinResponse](res.body())
        Right(Goblin(g.name, g.weightKg, g.heightCm))
      else
        Left(Exception(s"Weights & Measures failed: ${res.statusCode()}"))
    catch case e: Throwable => Left(e)

  // -------------------------------------------------------------------------
  // Smithy
  // -------------------------------------------------------------------------
  case class WeaponResponse(id: String, label: String) derives ReadWriter

  def acquireWeapon(weaponId: String): Either[Throwable, Weapon] =
    try
      val res = post("http://localhost:8081/weapon/acquire",
        s"""{"weaponId":"$weaponId"}""")
      if res.statusCode() == 200 then
        val w = read[WeaponResponse](res.body())
        Right(Weapon(w.id, w.label))
      else
        Left(OutOfStockException("Smithy"))
    catch case e: Throwable => Left(e)

  def returnWeapon(weaponId: String): Either[Throwable, Unit] =
    try
      val res = post("http://localhost:8081/weapon/return",
        s"""{"weaponId":"$weaponId"}""")
      if res.statusCode() == 200 then Right(())
      else Left(Exception(s"Smithy return failed: ${res.statusCode()}"))
    catch case e: Throwable => Left(e)

  // -------------------------------------------------------------------------
  // Rags & Style
  // -------------------------------------------------------------------------
  case class UniformResponse(id: String, size: String, color: String) derives ReadWriter

  def acquireUniform(uniformId: String): Either[Throwable, Uniform] =
    try
      val res = post("http://localhost:8082/uniform/acquire",
        s"""{"uniformId":"$uniformId"}""")
      if res.statusCode() == 200 then
        val u = read[UniformResponse](res.body())
        Right(Uniform(u.id, u.size, u.color))
      else
        Left(OutOfStockException("Rags & Style"))
    catch case e: Throwable => Left(e)

  def returnUniform(uniformId: String): Either[Throwable, Unit] =
    try
      val res = post("http://localhost:8082/uniform/return",
        s"""{"uniformId":"$uniformId"}""")
      if res.statusCode() == 200 then Right(())
      else Left(Exception(s"Rags & Style return failed: ${res.statusCode()}"))
    catch case e: Throwable => Left(e)

  // -------------------------------------------------------------------------
  // Cobblery
  // -------------------------------------------------------------------------
  case class BootsResponse(id: String, bootSize: Int) derives ReadWriter

  def acquireBoots(bootsId: String): Either[Throwable, Boot] =
    try
      val res = post("http://localhost:8083/boots/acquire",
        s"""{"bootsId":"$bootsId"}""")
      if res.statusCode() == 200 then
        val b = read[BootsResponse](res.body())
        Right(Boot(b.id, b.bootSize))
      else
        Left(OutOfStockException("Cobblery"))
    catch case e: Throwable => Left(e)

  def returnBoots(bootsId: String): Either[Throwable, Unit] =
    try
      val res = post("http://localhost:8083/boots/return",
        s"""{"bootsId":"$bootsId"}""")
      if res.statusCode() == 200 then Right(())
      else Left(Exception(s"Cobblery return failed: ${res.statusCode()}"))
    catch case e: Throwable => Left(e)

  // -------------------------------------------------------------------------
  // Portrait
  // -------------------------------------------------------------------------
  def sendPortrait(goblin: Goblin): Either[Throwable, Unit] =
    try
      val res = post("http://localhost:8084/portrait/send", s"""{"name":"${goblin.name}"}""")
      if res.statusCode() == 200 then Right(())
      else Left(Exception("Postal raven lost"))
    catch case e: Throwable => Left(e)
