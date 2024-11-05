package com.cdm.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.sql.Connection._
import com.cdm._
import com.cdm.log.Logg
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

/**
  * Created by Devaraj Jonnadula on 5/9/2017.
  */

case class BatchHandler(batchId: String, handleFailedTrans: Boolean = false, private val con: Connection, private val statement: PreparedStatement) extends Logg{

  import Converters._

  private var batchSize: Long = 0L
  private var batchData: ListBuffer[String] = if (handleFailedTrans) new ListBuffer() else null

  def addData(data: Array[String]): Unit = {
    data.view.zipWithIndex foreach { case (rowData, rowId) =>
      statement setString(rowId, rowData)
      if (handleFailedTrans) batchData += rowData
      batchSize = inc(batchSize)
    }
    logWarnings(con)
  }

  def rollBackBatch(): Unit = {
    tryAndThrow(con.rollback(), error(_: Throwable), Some(s"Cannot RollBack Batch $batchId"))
    logWarnings(con)
  }

  def commitBatch(): Unit = {
    tryAndThrow(con.commit(), error(_: Throwable), Some(s"Cannot RollBack Batch $batchId"))
    logWarnings(con)
  }

  def sendPersistRequest(): Boolean = {
    if (batchSize > Int.MaxValue) isBatchSucceeded(tryAndThrow(statement.executeLargeBatch(), error(_: Throwable), Some(s"Cannot Send Batched Data to Target Database for $batchId")))
    else isBatchSucceeded(tryAndThrow(statement.executeBatch(), error(_: Throwable), Some(s"Cannot Send Batched Data to Target Database for $batchId")))
  }

  def sendFailedRowsInLastTransaction(): Boolean = {
    require(!handleFailedTrans, s"This Batch Handler Cannot deal with Failed Transactions. Enable handleFailedTrans to Achieve this")
    batchData.view.zipWithIndex foreach { case (rowData, rowId) =>
      statement setString(rowId, rowData)
    }
    sendPersistRequest()
  }

  private def isBatchSucceeded[T](updateCountFromBatch: Array[T]): Boolean = {
    if (updateCountFromBatch == null) {
      error(s" null update count was returned! for Batch $batchId")
      logWarnings(con)
    } else if (updateCountFromBatch.length == batchSize) {
      info(s"Batch Succeeded for $batchId")
      logWarnings(con)
      return true
    } else {
      updateCountFromBatch.foreach {
        case smallBatchRowId: Int =>
          if (smallBatchRowId > 0) transForRowSucceeded(smallBatchRowId)
          else warn(s"Batch Update for row $smallBatchRowId failed: expected 1 , got $smallBatchRowId")
        case largeBatchRowId: Long =>
          if (largeBatchRowId > 0) transForRowSucceeded(largeBatchRowId.toInt)
          else warn(s"Batch Update for row $largeBatchRowId failed: expected 1 , got $largeBatchRowId")
      }
    }
    false
  }

  private def transForRowSucceeded(id: Int): Unit = {
    if (handleFailedTrans) {
      batchData remove id
      batchSize = dec(batchSize)
    }
  }

}

abstract class JdbcHandler(private val jdbcConnector: JdbcConnector) extends Logg with AutoCloseable {

  import Converters._

  @volatile private var statement: Statement = _

  def selectFromTable(query: String, uniqueKey: String): Map[String, Map[String, String]] = {

    if (statement == null) getConnection
    val resultSet = statement.executeQuery(query)
    val meta = resultSet.getMetaData
    val out = resultSet.lazyStream.map(rs => rs.getString(uniqueKey) -> Range(1, meta.getColumnCount).map(columnIndex => meta.getColumnName(columnIndex) -> rs.getString(columnIndex)).toMap).toMap
    logWarnings(statement.getConnection)
    out
  }

  def selectFromTable(query: String): ResultSet = {
    if (statement == null) getConnection
    val resultSet = statement.executeQuery(query)
    logWarnings(statement.getConnection)
    resultSet
  }


  def prepareBatch(query: String, batchId: String, handleFailedTrans: Boolean = false): BatchHandler = {
    val con = getConnection
    val stmt = con.prepareStatement(query)
    logWarnings(con)
    BatchHandler(batchId, handleFailedTrans, con, stmt)
  }


  def createTable(newTable: String): Unit = {
    if (statement == null) getConnection
    statement.executeQuery(newTable)
    logWarnings(statement.getConnection)
  }

  def dropTable(table: String): Unit = {
    if (statement == null) getConnection
    statement.executeQuery(table)
    logWarnings(statement.getConnection)
  }


  private def getConnection: Connection = {
    val con = jdbcConnector.currentConnection
    con.clearWarnings()
    con.setAutoCommit(false)
    con.setTransactionIsolation(TRANSACTION_READ_UNCOMMITTED)
    statement = con.createStatement()
    debug(s"Cleared Sql Warnings for $con")
    con
  }


  override def close(): Unit = {
    closeResource(statement)
  }

}

private[jdbc] object Converters extends Logg{

  implicit class ResultSetStream(rs: ResultSet) {
    def lazyStream: Stream[ResultSet] = {
      new Iterator[ResultSet] {
        def hasNext: Boolean = rs.next()

        def next(): ResultSet = rs
      }.toStream
    }
  }

  implicit class ExtractSingleColumn(rs: ResultSet) {
    def asSeq: Seq[String] = {
      new Iterator[String] {
        def hasNext: Boolean = rs.next()

        def next(): String = rs.getString(1)
      }.toSeq
    }
  }

  implicit def logWarnings(con: Connection): Unit = {
    var warning = con.getWarnings
    while (warning != null) {
      warn(s"*** SQLWarning caught *** SQL State = ${warning.getSQLState} Error Code = ${warning.getErrorCode}", warning.fillInStackTrace())
      warning = warning.getNextWarning
    }
    con clearWarnings()
  }

}
