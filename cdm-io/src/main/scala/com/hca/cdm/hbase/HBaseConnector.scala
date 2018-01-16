
package com.hca.cdm.hbase

import java.io.IOException
import java.util.concurrent.TimeUnit
import com.hca.cdm.log.Logg
import com.hca.cdm._
import com.hca.cdm.utils.RetryHandler
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import scala.collection.concurrent.TrieMap
import scala.language.postfixOps
import collection.JavaConverters._
import com.hca.cdm.auth.LoginRenewer.performAction

/**
  * Created by Devaraj Jonnadula on 8/23/2017.
  */
private[cdm] class HBaseConnector(conf: Configuration, nameSpace: String = "hl7") extends Logg with AutoCloseable {
  self =>
  private val connection = performAction(asFunc(ConnectionFactory.createConnection(conf)))
  private val mutatorStore = new TrieMap[String, BatchOperator]()
  private lazy val regionReplication = tryAndReturnDefaultValue0(lookUpProp("hbase.regions.replication").toInt, 1)
  registerHook(newThread(s"$nameSpace-sHook-$connection", runnable(close())))

  def getTable(tableName: String): Table = connection.getTable(TableName.valueOf(nameSpace, tableName))

  def getRegionLocator(tableName: TableName): RegionLocator = connection.getRegionLocator(tableName)

  def isClosed: Boolean = connection.isClosed

  def getAdmin: Admin = connection.getAdmin

  def close(): Unit = {
    mutatorStore.foreach(_._2.close())
    closeResource(connection)
  }

  def getBatchOperator(table: String, batchSize: Int): BatchOperator = synchronized {
    mutatorStore.getOrElseUpdate(table, new BatchOperator(nameSpace, table, connection, batchSize))
  }

  def stillAlive: Boolean = {
    !connection.isClosed && !connection.isAborted
  }

  def createTable(tableName: String, props: Option[Map[String, String]] = None, families: List[HColumnDescriptor] = Nil): Unit = {
    val admin = getAdmin
    val table = TableName.valueOf(nameSpace, tableName)
    if (!admin.tableExists(table)) {
      val tableDesc =
        new HTableDescriptor(table)
          .setRegionReplication(regionReplication)
          .setRegionMemstoreReplication(true)
      tableDesc.setCompactionEnabled(true)
      tableDesc.setDurability(Durability.FSYNC_WAL)
      families.foreach(tableDesc.addFamily)
      tableDesc.setConfiguration("hbase.regionserver.storefile.refresh.period", "500")
      props.foreach(_.foreach { case (k, v) => tableDesc.setConfiguration(k, v) })
      tryAndGoNextAction0(admin.createTable(tableDesc), closeResource(admin))
    } else {
      closeResource(admin)
    }
  }

  def batchOperators(tables: Set[String], batchSize: Int): Map[String, BatchOperator] = synchronized {
    tables.map { table => table -> getBatchOperator(table, batchSize) } toMap
  }

  def findBatch[T](request: List[(String, Get)], responseHandler: (Result) => T): List[(String, List[T])] = {
    request.groupBy(_._1).map {
      case (table, req) =>
        val Table = getTable(table)
        (table, tryAndGoNextAction0(Table.get(req.map(_._2).asJava).map(responseHandler).toList, closeResource(Table)))
    }.toList
  }

  def findRow[T](table: String, request: Get, responseHandler: (Result) => T): T = {
    val Table = getTable(table)
    tryAndGoNextAction(asFunc(responseHandler(Table.get(request))), closeResource(Table))
  }
}

private[cdm] class BatchOperator(nameSpace: String, table: String, connection: Connection, batchSize: Int = 1000) extends Logg {
  require(valid(table) && !table.trim.isEmpty, s"Cannot Operate on Table $table")
  @volatile private var batched: Long = 0
  private val batchRunner = newDaemonScheduler(s"$table-BatchRunner-$connection")
  batchRunner scheduleAtFixedRate(newThread(s"$table-BatchTask", runnable(asFunc(runBatch()))), 1, 1, TimeUnit.SECONDS)
  private val mutator = connection.getBufferedMutator(TableName.valueOf(nameSpace, table))

  @throws[IOException]
  def mutate(op: Mutation): Unit = {
    if (supportedMutation(op)) {
      tryAndThrow(mutator mutate op, error(_: Throwable))
      batched = inc(batched)
      submitBatch()
    }
  }

  @throws[IOException]
  def mutateMulti(ops: Traversable[Mutation]): Unit = {
    ops foreach mutate
  }

  def submitBatch(withRetry: Boolean = false): Unit = {
    if (batched >= batchSize) {
      if (withRetry) {
        if (new RetryHandler().retryOperation(asFunc(tryAndThrow(mutator flush(), error(_: Throwable))))) reset()
      }
      else {
        tryAndThrow(mutator flush(), error(_: Throwable))
        reset()
      }

    }
  }

  private def runBatch(): Unit = {
    if (batched > 0) {
      if (tryAndLogErrorMes(mutator flush(), error(_: Throwable))) reset()
    }
  }

  private def reset(): Unit = {
    batched = 0
  }

  def close(): Unit = {
    tryAndLogErrorMes(mutator flush(), error(_: Throwable))
    batchRunner shutdownNow()
    batchRunner awaitTermination(1, TimeUnit.HOURS)
    closeResource(mutator)
  }

  private def supportedMutation(mut: Mutation): Boolean = {
    mut.isInstanceOf[Put] | mut.isInstanceOf[Delete]
  }

  def getTable: String = table

}


object HBaseConnector extends Logg {
  private val lock = new Object()
  private var ins: HBaseConnector = _

  def stop(): Unit = {
    closeResource(ins)
    ins = null
  }

  def apply(conf: Configuration, nameSpace: String): HBaseConnector = {
    def createIfNotExist = new (() => HBaseConnector) {
      override def apply(): HBaseConnector = new HBaseConnector(conf, nameSpace)
    }

    createInstance(createIfNotExist)

  }

  def apply(nameSpace: String, tables: Set[String], batchSize: Int): Map[String, BatchOperator] = {
    def createIfNotExist = new (() => HBaseConnector) {
      override def apply(): HBaseConnector = new HBaseConnector(hadoop.hadoopConf, nameSpace)
    }

    createInstance(createIfNotExist).batchOperators(tables, batchSize)


  }

  def apply(nameSpace: String = "hl7"): HBaseConnector = {
    def createIfNotExist = new (() => HBaseConnector) {

      override def apply(): HBaseConnector = new HBaseConnector(hadoop.hadoopConf, nameSpace)
    }

    createInstance(createIfNotExist)
  }

  private def createInstance(createIfNotExist: () => HBaseConnector): HBaseConnector = {
    lock.synchronized(
      if (ins == null) {
        ins = createIfNotExist()
        info(s"Created instance for $this handler $ins")
        ins
      } else {
        ins
      })
  }
}




