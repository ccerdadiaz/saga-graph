package sagagraph

// ---------------------------------------------------------------------------
// SagaLogger — pluggable logging abstraction for the saga engine
//
// Implementations:
//   - NoOpLogger    — silent, for tests (default)
//   - PrintlnLogger — stdout, for development
//   - Slf4jLogger   — in store-sqlite or examples, with scala-logging
//
// The engine logs at three levels:
//   - warn  — compensation failures, unexpected states
//   - info  — saga lifecycle (started, completed, compensated)
//   - debug — step-level detail (action started, WAL written)
// ---------------------------------------------------------------------------
trait SagaLogger:
  def debug(msg: => String): Unit
  def info(msg: => String): Unit
  def warn(msg: => String): Unit

object SagaLogger:

  val noOp: SagaLogger = new SagaLogger:
    def debug(msg: => String): Unit = ()
    def info(msg: => String): Unit = ()
    def warn(msg: => String): Unit = ()

  val stdout: SagaLogger = new SagaLogger:
    private def prefix =
      SagaContext.current
        .map(id => s"[${id.value.take(8)}]")
        .getOrElse("[-]")

    def debug(msg: => String): Unit = Predef.println(s"[DEBUG] $prefix $msg")
    def info(msg: => String): Unit = Predef.println(s"[INFO]  $prefix $msg")
    def warn(msg: => String): Unit = Predef.println(s"[WARN]  $prefix $msg")
