package sagagraph.examples.goblin

import com.typesafe.scalalogging.Logger
import sagagraph.SagaLogger

// ---------------------------------------------------------------------------
// Slf4jLogger — SagaLogger implementation using scala-logging + logback
//
// Wraps a scala-logging Logger to implement the SagaLogger trait.
// Log format is configured in logback.xml.
// ---------------------------------------------------------------------------
import sagagraph.SagaContext

class Slf4jLogger(underlying: Logger) extends SagaLogger:

  private def prefix =
    SagaContext.current
      .map(id => s"[${id.value.take(8)}]")
      .getOrElse("[-]")

  def debug(msg: => String): Unit = underlying.debug(s"$prefix $msg")
  def info(msg: => String): Unit = underlying.info(s"$prefix $msg")
  def warn(msg: => String): Unit = underlying.warn(s"$prefix $msg")

object Slf4jLogger:
  def apply(name: String): Slf4jLogger =
    new Slf4jLogger(Logger(name))
