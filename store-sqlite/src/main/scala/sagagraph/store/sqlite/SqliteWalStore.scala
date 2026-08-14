package sagagraph.store.sqlite

import sagagraph.*
import java.sql.{Connection, DriverManager}

class SqliteWalStore(dbPath: String) extends WalStore:

  private val conn: Connection =
    Class.forName("org.sqlite.JDBC")
    val c = DriverManager.getConnection(s"jdbc:sqlite:$dbPath")
    c.setAutoCommit(true)
    initSchema(c)
    c

  def append(sagaId: SagaId, entry: WalEntry): Either[Throwable, Unit] =
    try
      ensureSaga(sagaId)
      val sql = """
        INSERT OR IGNORE INTO wal_entries
          (saga_id, step_name, status, compensation_ref, compensation_args, created_at)
        VALUES (?, ?, 'Pending', ?, ?, ?)
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

  def loadPending(sagaId: SagaId): Either[Throwable, List[WalEntry]] =
    try
      val sql = """
        SELECT step_name, compensation_ref, compensation_args
        FROM   wal_entries
        WHERE  saga_id = ? AND status = 'Pending'
        ORDER  BY created_at DESC
      """
      val ps = conn.prepareStatement(sql)
      ps.setString(1, sagaId.value)
      val rs = ps.executeQuery()
      val entries = Iterator
        .continually(rs)
        .takeWhile(_.next())
        .map { rs =>
          WalEntry(
            stepName = rs.getString("step_name"),
            compensate = () => Right(()),
            compensationRef = Option(rs.getString("compensation_ref")),
            compensationArgs = Option(rs.getString("compensation_args"))
          )
        }
        .toList
      rs.close()
      ps.close()
      Right(entries)
    catch case e: Throwable => Left(e)

  def markCompensated(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, Unit] =
    updateEntryStatus(sagaId, stepName, "Compensated")

  def complete(sagaId: SagaId): Either[Throwable, Unit] =
    try
      val sql = "UPDATE sagas SET status = 'Completed' WHERE saga_id = ?"
      val ps = conn.prepareStatement(sql)
      ps.setString(1, sagaId.value)
      ps.executeUpdate()
      ps.close()
      Right(())
    catch case e: Throwable => Left(e)

  def findZombies(olderThanMs: Long): Either[Throwable, List[SagaId]] =
    try
      val threshold = System.currentTimeMillis() - olderThanMs
      val sql = """
        SELECT saga_id FROM sagas
        WHERE status NOT IN ('Completed', 'Compensated') AND created_at < ?
      """
      val ps = conn.prepareStatement(sql)
      ps.setLong(1, threshold)
      val rs = ps.executeQuery()
      val ids = Iterator
        .continually(rs)
        .takeWhile(_.next())
        .map(rs => SagaId(rs.getString("saga_id")))
        .toList
      rs.close()
      ps.close()
      Right(ids)
    catch case e: Throwable => Left(e)

  private def ensureSaga(sagaId: SagaId): Unit =
    val sql = """
      INSERT OR IGNORE INTO sagas (saga_id, status, created_at)
      VALUES (?, 'Running', ?)
    """
    val ps = conn.prepareStatement(sql)
    ps.setString(1, sagaId.value)
    ps.setLong(2, System.currentTimeMillis())
    ps.executeUpdate()
    ps.close()

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
        saga_id          TEXT    NOT NULL,
        step_name        TEXT    NOT NULL,
        status           TEXT    NOT NULL DEFAULT 'Pending',
        compensation_ref  TEXT,
        compensation_args TEXT,
        created_at       INTEGER NOT NULL,
        PRIMARY KEY (saga_id, step_name),
        FOREIGN KEY (saga_id) REFERENCES sagas(saga_id)
      )
    """)
    st.close()

  def markCompensationFailed(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, Unit] =
    updateEntryStatus(sagaId, stepName, "CompensationFailed")

  def markCompensated(sagaId: SagaId): Either[Throwable, Unit] =
    try
      val sql = "UPDATE sagas SET status = 'Compensated' WHERE saga_id = ?"
      val ps = conn.prepareStatement(sql)
      ps.setString(1, sagaId.value)
      ps.executeUpdate()
      ps.close()
      Right(())
    catch case e: Throwable => Left(e)

  def markActionFailed(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, Unit] =
    updateEntryStatus(sagaId, stepName, "ActionFailed")

  def getStatus(
      sagaId: SagaId,
      stepName: String
  ): Either[Throwable, WalEntry.Status] =
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
            case "Pending"      => Right(WalEntry.Status.Pending)
            case "ActionFailed" => Right(WalEntry.Status.ActionFailed)
            case "Compensated"  => Right(WalEntry.Status.Compensated)
            case "CompensationFailed" =>
              Right(WalEntry.Status.CompensationFailed)
            case unknown => Left(Exception(s"Unknown status: $unknown"))
        else Left(Exception(s"Step '$stepName' not found for saga $sagaId"))
      rs.close()
      ps.close()
      result
    catch case e: Throwable => Left(e)
