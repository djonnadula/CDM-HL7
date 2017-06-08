package com.hca.cdm.sqlserver

import java.sql.Connection

import com.hca.cdm._
import com.microsoft.sqlserver.jdbc._

/**
  * Created by dof7475 on 6/6/2017.
  */
object SqlServerConnector {
  val dataSource = new SQLServerDataSource()

  def getConnection(dbName: String, dbURL: String, dbUser: String, dbPassword: String) : Connection = {
    dataSource.setDatabaseName(dbName)
    dataSource.setURL(dbURL)
    dataSource.setUser(dbUser)
    dataSource.setPassword(dbPassword)
    dataSource.getConnection(dbUser, dbPassword)
  }

}

