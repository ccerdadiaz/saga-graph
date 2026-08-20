package sagagraph.store.sqlite

import sagagraph.*
import java.sql.{Connection, DriverManager}

// ---------------------------------------------------------------------------
// SqliteWalStore — reference implementation of WalStore using SQLite
//
// - One .db file, no server, no configuration
// - compensate closures are NOT persisted (they live in the engine)
//   compensation_ref + compensation_args are stored for ZombieHunter recovery
//
// NOTE: SQLite is single-writer by design — synchronized serializes concurrent
// access. Production-grade stores (PostgreSQL, Oracle) handle concurrency
// natively and do not require this workaround.
// ---------------------------------------------------------------------------
class SqliteWalStore(dbPath: String) extends WalStore:

  private val conn: Connection =
    Class.forName("org.sqlite.JDBC")
    val c = DriverManager.getConnection(s"jdbc:sqlite:$dbPath")
    c.setAutoCommit(true)
    val pragmaSt = c.createStatement()
    pragmaSt.execute("PRAGMA journal_mode=WAL")
    pragmaSt.execute("PRAGMA busy_timeout=5000")
    pragmaSt.close()
    initSchema(c)
    c

  // -------------------------------------------------------------------------
  // Steps
  // -------------------------------------------------------------------------

  def append(sagaId: SagaId, entry: WalEntry): Either[Throwable, Unit] =
    synchronized:
      try
        val sql = """
          INSERT OR IGNORE INTO wal_entries
            (saga_id, step_name, status, compensation_ref, compensation_args, created_at)
          VALUES (?, ?, 'Registered', ?, ?, ?)
        """
        val ps = conn.prepareStatement(sql)
        ps.setString(1, sagaId.value)
        ps.setString(2, entry.stepName)
        ps.setString(3, entry.compensationRef.orNull)
        ps.setString(4, entry.compensationArgs.orNull)
        ps.setLong(5, System.currentTimeMillis())
        ps.executeUpdate()
        ps.close()
        Right(())
      catch case e: Throwable => Left(e)

  def markRunning(sagaId: SagaId, stepName: String): Either[Throwable, Unit] =
    synchronized:
      try
        val sql = """
          UPDATE wal_entries SET status = 'Running', started_at = ?
          WHERE  saga_id = ? AND step_name = ?
        """
        val ps = conn.prepareStatement(sql)
        ps.setLong(1, System.currentTimeMillis())
        ps.setString(2, sagaId.value)
        ps.setString(3, stepName)
        ps.executeUpdate()
        ps.close()
        Right(())
      catch case e: Throwable => Left(e)

  def markDone(sagaId: SagaId, stepName: String): Either[Throwable, Unit] =
    synchronized:
      try
        val sql = """
          UPDATE wal_entries SET status = 'Done', finished_at = ?
          WHERE  saga_id = ? AND step_name = ?
        """
        val ps = conn.prepareStatement(sql)
        ps.setLong(1, System.currentTimeMillis())
        ps.setString(2, sagaId.value)
        ps.setString(3, stepName)
        ps.executeUpdate()
        ps.close()
        Right(())
      catch case e: Throwable => Left(e)

  def markFailed(sagaId: SagaId, stepName: String): Either[Throwable, Unit] =
    synchronized:
      try
        val sql = """
          UPDATE wal_entries SET status = 'Failed', finished_at = ?
          WHERE  saga_id = ? AND step_name = ?
        """
        val ps = conn.prepareStatement(sql)
        ps.setLong(1, System.currentTimeMillis())
        ps.setString(2, sagaId.value)
        ps.setString(3, stepName)
        ps.executeUpdate()
        ps.close()
        Right(())
      catch case e: Throwable => Left(e)

  def markCompensated(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, Unit] =
    synchronized:
      updateEntryStatus(sagaId, stepName, "Compensated")

  def markCompensationFailed(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, Unit] =
    synchronized:
      updateEntryStatus(sagaId, stepName, "CompensationFailed")

  def markHumanIntervention(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, Unit] =
    synchronized:
      updateEntryStatus(sagaId, stepName, "HumanIntervention")

  def getStatus(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, WalEntry.Status] =
    synchronized:
      try
        val sql = """
          SELECT status FROM wal_entries
          WHERE  saga_id = ? AND step_name = ?
        """
        val ps = conn.prepareStatement(sql)
        ps.setString(1, sagaId.value)
        ps.setString(2, stepName)
        val rs = ps.executeQuery()
        val result =
          if rs.next() then
            rs.getString("status") match
              case "Registered"  => Right(WalEntry.Status.Registered)
              case "Running"     => Right(WalEntry.Status.Running)
              case "Done"        => Right(WalEntry.Status.Done)
              case "Failed"      => Right(WalEntry.Status.Failed)
              case "Compensated" => Right(WalEntry.Status.Compensated)
              case "CompensationFailed" =>
                Right(WalEntry.Status.CompensationFailed)
              case "HumanIntervention" =>
                Right(WalEntry.Status.HumanIntervention)
              case unknown => Left(Exception(s"Unknown status: $unknown"))
          else Left(Exception(s"Step '$stepName' not found for saga $sagaId"))
        rs.close()
        ps.close()
        result
      catch case e: Throwable => Left(e)

  // Returns Registered and CompensationFailed entries — actionable by ZombieHunter
  def loadActionable(sagaId: SagaId): Either[Throwable, List[WalEntry]] =
    synchronized:
      try
        val sql = """
          SELECT step_name, compensation_ref, compensation_args
          FROM   wal_entries
          WHERE  saga_id = ? AND status IN ('Registered', 'CompensationFailed')
          ORDER  BY created_at DESC
        """
        val ps = conn.prepareStatement(sql)
        ps.setString(1, sagaId.value)
        val rs = ps.executeQuery()
        val buffer = scala.collection.mutable.ListBuffer.empty[WalEntry]
        while rs.next() do
          buffer += WalEntry(
            stepName = rs.getString("step_name"),
            compensate = () => Right(()),
            compensationRef = Option(rs.getString("compensation_ref")),
            compensationArgs = Option(rs.getString("compensation_args"))
          )
        rs.close()
        ps.close()
        Right(buffer.toList)
      catch case e: Throwable => Left(e)

  // -------------------------------------------------------------------------
  // Sagas
  // -------------------------------------------------------------------------

  def registerSaga(sagaId: SagaId): Either[Throwable, Unit] =
    synchronized:
      try
        val sql = """
          INSERT OR IGNORE INTO sagas (saga_id, status, created_at)
          VALUES (?, 'Running', ?)
        """
        val ps = conn.prepareStatement(sql)
        ps.setString(1, sagaId.value)
        ps.setLong(2, System.currentTimeMillis())
        ps.executeUpdate()
        ps.close()
        Right(())
      catch case e: Throwable => Left(e)

  def markSagaCompleted(sagaId: SagaId): Either[Throwable, Unit] =
    synchronized:
      updateSagaStatus(sagaId, "Completed")

  def markSagaCompensating(sagaId: SagaId): Either[Throwable, Unit] =
    synchronized:
      updateSagaStatus(sagaId, "Compensating")

  def markSagaCompensated(sagaId: SagaId): Either[Throwable, Unit] =
    synchronized:
      updateSagaStatus(sagaId, "Compensated")

  def markSagaFailed(
      sagaId: SagaId,
      cause: Throwable
  ): Either[Throwable, Unit] =
    synchronized:
      updateSagaStatus(sagaId, "Failed")

  def findZombies(olderThanMs: Long): Either[Throwable, List[SagaId]] =
    synchronized:
      try
        val threshold = System.currentTimeMillis() - olderThanMs
        // Running or TTL — sagas not in terminal state beyond threshold
        // TODO: per-step TTL — currently all sagas use the same global threshold.
        // Future: consider step-level timeouts and per-saga TTL configuration.
        // ZombieHunter should also detect Running steps beyond their individual TTL.
        val sql = """
          SELECT saga_id FROM sagas
          WHERE  status NOT IN ('Completed', 'Compensated', 'Failed')
          AND    created_at < ?
        """
        val ps = conn.prepareStatement(sql)
        ps.setLong(1, threshold)
        val rs = ps.executeQuery()
        val buffer = scala.collection.mutable.ListBuffer.empty[SagaId]
        while rs.next() do buffer += SagaId(rs.getString("saga_id"))
        rs.close()
        ps.close()
        Right(buffer.toList)
      catch case e: Throwable => Left(e)

  // -------------------------------------------------------------------------
  // Diagnostic — prints execution timeline with parallelism detection
  // -------------------------------------------------------------------------
  def printTimeline(): Unit =
    synchronized:
      val sql = """
        SELECT
          saga_id,
          step_name,
          started_at  - MIN(started_at) OVER (PARTITION BY saga_id) AS started_ms,
          finished_at - MIN(started_at) OVER (PARTITION BY saga_id) AS finished_ms,
          finished_at - started_at AS duration_ms,
          CASE
            WHEN EXISTS (
              SELECT 1 FROM wal_entries other
              WHERE  other.saga_id     = wal_entries.saga_id
              AND    other.step_name  != wal_entries.step_name
              AND    other.started_at  < wal_entries.finished_at
              AND    other.finished_at > wal_entries.started_at
            )
            THEN 'YES ←'
            ELSE 'no'
          END AS parallel
        FROM  wal_entries
        WHERE started_at IS NOT NULL
        ORDER BY saga_id, started_at ASC
      """
      val st = conn.createStatement()
      val rs = st.executeQuery(sql)
      var lastSaga = ""
      while rs.next() do
        val sagaId = rs.getString("saga_id")
        val stepName = rs.getString("step_name")
        val started = rs.getLong("started_ms").toString.padTo(10, ' ')
        val finished = rs.getLong("finished_ms").toString.padTo(10, ' ')
        val duration = rs.getLong("duration_ms").toString.padTo(8, ' ')
        val parallel = rs.getString("parallel")
        if sagaId != lastSaga then
          println(s"\n  saga: $sagaId")
          println(
            s"  ${"step".padTo(25, ' ')} ${"start(ms)".padTo(10, ' ')} ${"end(ms)"
                .padTo(10, ' ')} ${"dur(ms)".padTo(8, ' ')} parallel"
          )
          println(s"  ${"-" * 65}")
          lastSaga = sagaId
        println(
          s"  ${stepName.padTo(25, ' ')} $started $finished $duration $parallel"
        )
      rs.close()
      st.close()

  def close(): Unit = conn.close()

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  private def updateEntryStatus(
      sagaId: SagaId,
      stepName: String,
      status: String
  ): Either[Throwable, Unit] =
    try
      val sql = """
        UPDATE wal_entries SET status = ?
        WHERE  saga_id = ? AND step_name = ?
      """
      val ps = conn.prepareStatement(sql)
      ps.setString(1, status)
      ps.setString(2, sagaId.value)
      ps.setString(3, stepName)
      ps.executeUpdate()
      ps.close()
      Right(())
    catch case e: Throwable => Left(e)

  private def updateSagaStatus(
      sagaId: SagaId,
      status: String
  ): Either[Throwable, Unit] =
    try
      val sql = "UPDATE sagas SET status = ? WHERE saga_id = ?"
      val ps = conn.prepareStatement(sql)
      ps.setString(1, status)
      ps.setString(2, sagaId.value)
      ps.executeUpdate()
      ps.close()
      Right(())
    catch case e: Throwable => Left(e)

  private def initSchema(c: Connection): Unit =
    val st = c.createStatement()
    st.executeUpdate("""
      CREATE TABLE IF NOT EXISTS sagas (
        saga_id    TEXT    PRIMARY KEY,
        status     TEXT    NOT NULL DEFAULT 'Running',
        created_at INTEGER NOT NULL
      )
    """)
    st.executeUpdate("""
      CREATE TABLE IF NOT EXISTS wal_entries (
        saga_id           TEXT    NOT NULL,
        step_name         TEXT    NOT NULL,
        status            TEXT    NOT NULL DEFAULT 'Registered',
        compensation_ref  TEXT,
        compensation_args TEXT,
        created_at        INTEGER NOT NULL,
        started_at        INTEGER,
        finished_at       INTEGER,
        PRIMARY KEY (saga_id, step_name),
        FOREIGN KEY (saga_id) REFERENCES sagas(saga_id)
      )
    """)
    st.close()
