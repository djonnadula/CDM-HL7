package com.hca.cdm.jdbc

/**
  * Created by Devaraj Jonnadula on 5/2/2017.
  */
object JdbcSources extends Enumeration {

  type RDBMSSource = Value
  val TERADATA = Value("TERADATA")
  val DB2 = Value("DB2")
  val OTHER = Value("OTHER")

}
