
package com.hca.cdm.hbase

import java.io.IOException
import java.util.Properties
import scala.collection.JavaConverters._
import com.hca.cdm.log.Logg
import com.hca.cdm._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import scala.collection.concurrent.TrieMap

/**
  * Created by Devaraj Jonnadula on 8/23/2017.
  */
private[cdm] class HBaseConnector(conf: Configuration, nameSpace: String = "hl7") extends Logg {

  private val connection = ConnectionFactory.createConnection(conf)
  private val mutatorStore = new TrieMap[String, BatchOperator]()

  def getTable(tableName: String): Table = connection.getTable(TableName.valueOf(nameSpace,tableName))

  def getRegionLocator(tableName: TableName): RegionLocator = connection.getRegionLocator(tableName)

  def isClosed: Boolean = connection.isClosed

  def getAdmin: Admin = connection.getAdmin

  def close(): Unit = {
    mutatorStore.foreach(_._2.close())
    closeResource(connection)
  }

  def getBatchOperator(table: String): BatchOperator = {
    mutatorStore.getOrElseUpdate(table, new BatchOperator(table))
  }

  def stillAlive: Boolean = {
    connection.isClosed && connection.isAborted
  }

  def createTable(tableName: String, props: Option[Properties] = None, families: List[HColumnDescriptor] = Nil): Unit = {
    val admin = getAdmin
    val table = TableName.valueOf(nameSpace, tableName)
    if (!admin.tableExists(table)) {
      val tableDesc =
        new HTableDescriptor(table)
          .setRegionReplication(3)
          .setRegionMemstoreReplication(true)
      tableDesc.setCompactionEnabled(true)
      tableDesc.setRegionMemstoreReplication(true)
      tableDesc.setDurability(Durability.SYNC_WAL)
      families.foreach(tableDesc.addFamily)
      props.foreach(_.asScala.foreach { case (k, v) => tableDesc.setConfiguration(k, v) })
      tryAndGoNextAction(asFunc(admin.createTable(tableDesc)), asFunc(admin.close()))
    } else {
      closeResource(admin)
    }
  }


  class BatchOperator(table: String, batchSize: Int = 1000) {
    @volatile private var batched: Int = 0
    private val mutator = connection.getBufferedMutator(TableName.valueOf(nameSpace,table))

    @throws[IOException]
    def mutate(op: Mutation): Unit = {
      if (supportedMutation(op)) {
        tryAndThrow(mutator mutate op, error(_: Throwable))
        batched += 1
      }
      if (batched >= batchSize) {
        submitBatch()
        batched = 0
      }
    }

    @throws[IOException]
    def mutateMulti(ops: Traversable[Mutation]): Unit = {
      ops foreach mutate
    }

    def submitBatch(): Unit = {
      tryAndThrow(mutator flush(), error(_: Throwable))
    }

    def close(): Unit = {
      tryAndLogErrorMes(mutator flush(), error(_: Throwable))
      closeResource(mutator)
    }

    private def supportedMutation(mut: Mutation): Boolean = {
      mut.isInstanceOf[Put] | mut.isInstanceOf[Delete]
    }

  }

}

object HBaseConnector extends  Logg{
  private val lock = new Object()
  private var ins: HBaseConnector = _

  def apply(conf: Configuration, nameSpace: String): HBaseConnector = {
    def createIfNotExist = new (() => HBaseConnector) {
      override def apply(): HBaseConnector = new HBaseConnector(conf,nameSpace)
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




