package sagagraph.examples.goblin

import com.typesafe.scalalogging.Logger
import sagagraph.SagaLogger

// ---------------------------------------------------------------------------
// Slf4jLogger — SagaLogger implementation using scala-logging + logback
//
// Wraps a scala-logging Logger to implement the SagaLogger trait.
// Log format is configured in logback.xml.
// ---------------------------------------------------------------------------
class Slf4jLogger(underlying: Logger) extends SagaLogger:
  def debug(msg: => String): Unit = underlying.debug(msg)
  def info(msg: => String): Unit = underlying.info(msg)
  def warn(msg: => String): Unit = underlying.warn(msg)

object Slf4jLogger:
  def apply(name: String): Slf4jLogger =
    new Slf4jLogger(Logger(name))
