package com.hca.cdm.hive

import com.hca.cdm.log.Logg
import java.sql._
import com.hca.cdm._
import com.hca.cdm.auth.LoginRenewer._
import com.hca.cdm.exception.CdmException
import org.apache.commons.dbcp.BasicDataSource
import org.apache.hadoop.hive.conf.HiveConf
import scala.collection.JavaConverters._

/**
  * Created by Devaraj Jonnadula on 2/15/2017.
  */
class HiveConnector extends Logg with AutoCloseable {
  private lazy val driverName = "org.apache.hive.jdbc.HiveDriver"
  private var dataSource: BasicDataSource = _
  private var client: Connection = _
  private var discoveryFromZk = false

  @throws[CdmException]
  def hiveJDBCClient(config: HiveConf, initialConn: Int = 5): Unit = {
    config.getAllProperties.asScala.foreach(x => debug(x._1 + " :: " + x._2))
    dataSource = new BasicDataSource()
    dataSource.setDriverClassName(driverName)
    dataSource.setInitialSize(initialConn)
    dataSource.setUrl(connectURL(config, config.get("hive.server2.support.dynamic.service.discovery")))
    client = performAction(dataSource.getConnection)
    sHook()
  }


  @throws[CdmException]
  def sql(query: String, primaryKey: String, skipFields: Set[String] = Set.empty): Map[String, Map[String, String]] = {
    tryAndThrow(resultSetMapping(client.createStatement().executeQuery(query), primaryKey), error(_: Throwable)).map {
      case (tab, mapping) =>
        skipFields.foreach(field => mapping.filterKeys(_ != field))
        tab -> mapping
    }
  }

  @throws[CdmException]
  def sql(query: String): Unit = {
    tryAndThrow(client.createStatement().executeQuery(query), debug(_: Throwable))

  }

  def multiQueries(sql: Seq[String]): Unit = {
    val statement = client.createStatement()
    sql.foreach(query => tryAndLogErrorMes(statement.executeQuery(query), debug(_: Throwable)))
  }


  @throws[CdmException]
  def tables(dbName: String): Seq[String] = {
    import Converters._
    val statement = client.createStatement()
    tryAndLogErrorMes(statement.executeQuery(s"use $dbName"), debug(_: Throwable))
    tryAndThrow(statement.executeQuery("show tables").asSeq, error(_: Throwable))
  }

  private def resultSetMapping(resultSet: ResultSet, primaryKey: String): Map[String, Map[String, String]] = {
    import Converters._
    val meta = resultSet.getMetaData
    resultSet.lazyStream.map(rs => {
      rs.getString(primaryKey) -> Range(1, meta.getColumnCount).map(columnIndex => {
        val column = meta.getColumnName(columnIndex)
        column.substring(column.indexOf(".") + 1) -> rs.getString(columnIndex)
      }).toMap
    }).toMap
  }

  private def sHook(): Unit = {
    registerHook(newThread(s"${this.getClass}-SHook", runnable(closeResource(client))))
  }

  private def skipReg(skipPattern: List[String]) = {
    skipPattern.map(_.r)
  }

  private def connectURL(config: HiveConf, hive2WithPort: String, discoveryEnabled: Boolean = false): String = {
    if (discoveryEnabled) s"jdbc:hive2://${config.get("hive.zookeeper.quorum")}/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=${config.get("hive.zookeeper.namespace")};principal=${config.get("hive.server2.authentication.kerberos.principal", "hive/_HOST@HCA.CORPAD.NET")}"
    else s"jdbc:hive2://${config.get("hive2.hosts", "xrdcldbdm010001.unix.medcity.net:10000")}/default;principal=${config.get("hive.server2.authentication.kerberos.principal", "hive/_HOST@HCA.CORPAD.NET")}"
  }

  private[this] object Converters {

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

  }

  override def close(): Unit = closeResource(client)
}


