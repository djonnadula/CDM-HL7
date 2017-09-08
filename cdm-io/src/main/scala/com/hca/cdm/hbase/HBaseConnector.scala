
package com.hca.cdm.hbase
import java.io.IOException
import java.util.Properties
import scala.collection.JavaConverters._
import com.hca.cdm.log.Logg
import com.hca.cdm._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants._
import org.apache.hadoop.hbase.{HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.ipc.RpcControllerFactory
import scala.collection.concurrent.TrieMap

/**
  * Created by Devaraj Jonnadula on 8/23/2017.
  */
object HBaseConnector extends Logg {

  private[cdm] val cache = new TrieMap[Key, HConnection]()

  def getConnection(conf: Configuration): HConnection = {
    cache.getOrElseUpdate(Key(conf), HConnection(ConnectionFactory.createConnection(conf)))
  }

  private case class Key(conf: Configuration) {
    val keyProp =
      List(ZOOKEEPER_QUORUM, ZOOKEEPER_ZNODE_PARENT, ZOOKEEPER_CLIENT_PORT, ZOOKEEPER_RECOVERABLE_WAITTIME, HBASE_CLIENT_PAUSE, HBASE_CLIENT_RETRIES_NUMBER,
        HBASE_RPC_TIMEOUT_KEY, HBASE_META_SCANNER_CACHING, HBASE_CLIENT_INSTANCE_ID, RPC_CODEC_CONF_KEY, USE_META_REPLICAS, RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY)

    val props: List[String] = keyProp.map(prop => conf.get(prop, EMPTYSTR))

    override def hashCode(): Int = {
      props.hashCode()

    }

    override def equals(obj: scala.Any): Boolean = {
      props.equals(obj)
    }
  }



  private case class HConnection(private val connection: Connection) {

    require(valid(connection) && stillAlive, "Invalid Connection")
    registerHook(newThread(s"Shook$connection", runnable({
      mutatorStore.values.foreach {
        _.close()
      }
      close()
    })))
    private val mutatorStore = new TrieMap[String, BatchOperator]()

    def getTable(tableName: TableName): Table = connection.getTable(tableName)

    def getRegionLocator(tableName: TableName): RegionLocator = connection.getRegionLocator(tableName)

    def isClosed: Boolean = connection.isClosed

    def getAdmin: Admin = connection.getAdmin

    def close(): Unit = closeResource(connection)

    def getBatchOperator(table: String): BatchOperator = {
      mutatorStore.getOrElseUpdate(table, BatchOperator(table))
    }

    def stillAlive: Boolean = {
      connection.isClosed && connection.isAborted
    }

    def createTable(tableName: String, props: Option[Properties] = None): Unit = {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      tableDesc.setCompactionEnabled(true)
      tableDesc.setDurability(Durability.SYNC_WAL)
      tableDesc.setRegionMemstoreReplication(true)
      props.foreach(_.asScala.foreach { case (k, v) => tableDesc.setConfiguration(k, v) })
      val admin = getAdmin
      tryAndGoNextAction(asFunc(admin.createTable(tableDesc)), asFunc(admin.close()))
    }


    private case class BatchOperator(table: String) {
      private val mutator = connection.getBufferedMutator(TableName.valueOf(table))

      @throws[IOException]
      def mutate(op: Mutation): Unit = {
        if (supportedMutation(op)) tryAndThrow(mutator mutate op, error(_: Throwable))
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
        mut match {
          case Put | Delete => true
          case _ => false
        }

      }

    }

  }

}

