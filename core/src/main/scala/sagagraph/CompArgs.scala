package sagagraph

import scala.annotation.targetName

// ---------------------------------------------------------------------------
// CompArgs — minimal JSON-like argument builder for compensation payloads
//
// No external dependencies. Produces a JSON string that can be persisted
// in the WAL and parsed on recovery.
//
// Usage:
//   CompArgs("id" -> 455654, "tenant" -> "acme", "active" -> true)
//   // produces: {"id":455654,"tenant":"acme","active":true}
//
// Supported value types: String, Int, Long, Boolean, Double
// For complex types, use the raw string constructor: CompArgs.raw("{...}")
// ---------------------------------------------------------------------------
case class CompArgs(args: Map[String, CompArgs.Value]):

  def toJson: String =
    val entries = args.map: (k, v) =>
      s""""$k":${v.render}"""
    s"{${entries.mkString(",")}}"

  override def toString: String = toJson

object CompArgs:

  // Supported value types
  enum Value:
    case Str(v: String)
    case Num(v: Long)
    case Dec(v: Double)
    case Bool(v: Boolean)
    case Null

    def render: String = this match
      case Str(v)  => s""""$v""""
      case Num(v)  => v.toString
      case Dec(v)  => v.toString
      case Bool(v) => v.toString
      case Null    => "null"

  // Conversion implicits — the user passes plain values
  given Conversion[String, Value] = Value.Str(_)
  given Conversion[Int, Value] = v => Value.Num(v.toLong)
  given Conversion[Long, Value] = Value.Num(_)
  given Conversion[Double, Value] = Value.Dec(_)
  given Conversion[Boolean, Value] = Value.Bool(_)

  // Primary constructor — varargs of key-value pairs
  def apply(pairs: (String, Value)*): CompArgs =
    CompArgs(pairs.toMap)

  // Convenience overload — accepts plain String values without explicit conversion
  @targetName("applyStrings")
  def apply(pairs: (String, String)*): CompArgs =
    CompArgs(pairs.map((k, v) => k -> Value.Str(v)).toMap)

  // Raw string — escape hatch for complex payloads
  def raw(json: String): String = json

  // Empty — for steps with no meaningful payload
  val empty: CompArgs = CompArgs(Map.empty)
