package com.hca.cdm.jdbc

import java.sql.Connection
import java.util.Properties
import com.hca.cdm.exception.CdmJDBCException
import com.hca.cdm.jdbc.JdbcSources._
import com.hca.cdm.log.Logg
import com.hca.cdm._
import org.apache.commons.dbcp.BasicDataSource

/**
  * Created by Devaraj Jonnadula on 5/2/2017.
  */
class JdbcConnector(source: RDBMSSource, config: Properties, initConnections: Int = 5) extends Logg with AutoCloseable {

  private var dataSource: BasicDataSource = _
  private var client: Connection = _
  source match {
    case TERADATA =>
      dataSource = new BasicDataSource()
      dataSource.setDriverClassName("com.teradata.jdbc.TeraDriver")
      dataSource.setInitialSize(initConnections)
      dataSource.setUrl(JDBCURL.TERADATA(config.get("teradata.host").asInstanceOf[String], config.getOrDefault("teradata.fastload", "false").asInstanceOf[String].toBoolean,
        config.get("teradata.database").asInstanceOf[String], "UTF8", initConnections))
    case OTHER | DB2 | _ => throw new CdmJDBCException(s"Data Source of $source is not yet supported")
  }

  def getConnection(user: String, password: String): Connection = synchronized {
    dataSource.setUsername(user)
    dataSource.setPassword(password)
    client = dataSource.getConnection()
    client.setAutoCommit(false)
    client
  }

  def currentConnection: Connection = {
    getConnection(config.getProperty("user"), config.getProperty("pass"))
  }

  private[jdbc] object JDBCURL extends Enumeration {
    def TERADATA(host: String, fastLoadEnabled: Boolean, dataBase: String, charset: String = "UTF8", minimumSessions: Int, ldapLogin: Boolean = false): String = {
      if (fastLoadEnabled) s"jdbc:teradata://$host/${ldapEnabled(ldapLogin)}TMODE=ANSI,CHARSET=$charset,TYPE=FASTLOAD,SESSIONS=$minimumSessions,DATABASE=$dataBase,FINALIZE_AUTO_CLOSE=ON,PREP_SUPPORT=ON,LOG=INFO"
      else s"jdbc:teradata://$host/${ldapEnabled(ldapLogin)}TMODE=ANSI,CHARSET=$charset,SESSIONS=$minimumSessions,DATABASE=$dataBase,FINALIZE_AUTO_CLOSE=ON,PREP_SUPPORT=ON,LOG=INFO"

    }
  }

  private def ldapEnabled(ldap: Boolean): String = if (ldap) "LOGMECH=LDAP," else EMPTYSTR

  override def close(): Unit = {
    if (valid(dataSource)) dataSource.close()
    closeResource(client)
  }
}
