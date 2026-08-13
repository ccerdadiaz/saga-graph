package sagagraph

// ---------------------------------------------------------------------------
// CompensationRegistry — maps compensation_ref names to executable functions
//
// The registry bridges the WAL (which stores ref + args as strings) and the
// runtime (which needs a callable function to perform the compensation).
//
// Usage:
//   val registry = CompensationRegistry()
//     .register("deleteReservation", args => reservationService.delete(args))
//     .register("rollbackPhysical",  args => physicalService.rollback(args))
// ---------------------------------------------------------------------------
class CompensationRegistry:
    private val handlers = scala.collection.mutable.Map.empty[String, String => Either[Throwable, Unit]]

    def register(
        ref:     String,
        handler: String => Either[Throwable, Unit]  
    ): CompensationRegistry =
        handlers(ref) = handler
        this

    def resolve(ref: String): Option[String => Either[Throwable, Unit]] =
        handlers.get(ref)