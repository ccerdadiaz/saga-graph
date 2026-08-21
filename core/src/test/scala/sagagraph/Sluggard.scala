package sagagraph

import scala.concurrent.duration.*

// ---------------------------------------------------------------------------
// Sluggard — a service that takes its time, and may or may not deliver
//
// Used in tests to simulate slow services, timeouts, and UNKNOWN states.
// "Go to the ant, thou sluggard; consider her ways, and be wise."
//                                                    — Proverbs 6:6
//
// Usage:
//   Sluggard.action(200.millis)                    — slow but succeeds
//   Sluggard.action(200.millis, Left(boom))        — slow and fails
//   Sluggard.action(200.millis) with ttl=50.millis — times out → Unknown
// ---------------------------------------------------------------------------
object Sluggard:

  def action(
      delay:  Duration,
      result: Either[Throwable, Unit] = Right(())
  ): () => Either[Throwable, Unit] =
    () => { Thread.sleep(delay.toMillis); result }

  def boom(delay: Duration = 0.millis): () => Either[Throwable, Unit] =
    action(delay, Left(Exception("Sluggard failed to deliver")))

  def diligent(delay: Duration = 0.millis): () => Either[Throwable, Unit] =
    action(delay, Right(()))
