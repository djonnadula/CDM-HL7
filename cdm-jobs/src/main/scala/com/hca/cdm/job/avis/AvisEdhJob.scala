package com.hca.cdm.job.avis

import com.hca.cdm._
import com.hca.cdm.auth.LoginRenewer
import com.hca.cdm.hadoop.HadoopConfig
import com.hca.cdm.log.Logg
import com.hca.cdm.{propFile, reload}
import org.apache.log4j.PropertyConfigurator.configure
import java.sql.Connection
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import com.hca.cdm.exception.CdmException
import com.hca.cdm.jdbc.JdbcSources.TERADATA
import com.hca.cdm.jdbc.JdbcConnector
import com.hca.cdm.utils.RetryHandler
import collection.JavaConverters._
import scala.language.postfixOps
import com.hca.cdm.utils.DateConstants._
import com.hca.cdm.notification.EVENT_TIME
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource


/**
  * Created by Devaraj Jonnadula on 10/25/2017.
  *
  */
object AvisEdhJob extends Logg with App {

  self =>
  configure(currThread.getContextClassLoader.getResource("cdm-log4j.properties"))
  private val config_file = args(0)
  propFile = config_file
  reload(config_file)
  private val config = HadoopConfig.loadConfig(lookUpProp("hadoop.config.files"))
  LoginRenewer.loginFromKeyTab(lookUpProp("keytab"), lookUpProp("principal"), Some(config))
  private val fs = LoginRenewer.performAction(asFunc(FileSystem get config))
  private val jdbcConnector = new JdbcConnector(TERADATA, loadConfig(config_file))
  private val connection = jdbcConnector getConnection(lookUpProp("teradata.load.user"), loadCommand(lookUpProp("teradata.load.password")))
  private val statement = connection createStatement()
  registerHook(newThread(s"SHook-${self.getClass.getSimpleName}${lookUpProp("app")}", runnable({
    shutDown()
    closeResource(fs)
    info("Hadoop File System shutdown completed")
  })))
  private var batchOffset = EMPTYSTR
  private val inDateFormat = new SimpleDateFormat(HL7_DEFAULT)
  private val outDateFormat = new SimpleDateFormat(DATE_PATTERN_YYYY_MM_DD)
  private val transactionDateFormat = DateTimeFormatter.ofPattern(DATE_PATTERN_YYYY_MM_DD)
  private val transDate = "2017-11-01"
  if (isEtlCompleted) {
    doJob()
  } else {
    info(s"ETL Job has not completed so far $EVENT_TIME will try with Retry Policy")

    def retryOp() = {
      if (!isEtlCompleted) throw new CdmException(s"ETL Job has not completed so far $EVENT_TIME")
      else doJob()
    }

    tryAndLogErrorMes(new RetryHandler(4, 60000).retryOperation(retryOp), info(_: Throwable), Some(s"ETL Job has not completed so far exiting $EVENT_TIME"))
  }

  private def doJob(): Unit = {
    info(s"Last Batch Offset found $batchOffset")
    if (loadHadoopStaging()) {
      truncateEdwStaging()
      loadEdwStaging()
    }
    edwLoadComplete()
  }

  private def loadEdwStaging(): Unit = {
    kInit()
    val commands = new ListBuffer[String]
    commands += lookUpProp("sqoop.script")
    commands += batchOffset
    if (executeScript(commands.asJava)) info(s"loadEdwStaging completed for $commands")
  }

  private def loadHadoopStaging(): Boolean = {
    kInit()
    var transactionDate = LocalDate.parse(outDateFormat.format(inDateFormat.parse(batchOffset))).minusDays(1).format(transactionDateFormat)
    if (transactionDate == EMPTYSTR) transactionDate = self.transDate
    val commands = new ListBuffer[String]
    commands += lookUpProp("beeline.script")
    commands += transactionDate
    commands += batchOffset
    if (executeScript(commands.asJava)) {
      info(s"loadHadoopStaging completed for $commands ")
      return true
    }
    false

  }

  private def isEtlCompleted: Boolean = {
    val resultSet = statement executeQuery lookUpProp("avis.teradata.staging.check")
    if (valid(resultSet) && resultSet.next()) {
      batchOffset = resultSet.getString(1)
      closeResource(resultSet)
      return true
    }
    closeResource(resultSet)
    false
  }

  private def edwLoadComplete(): Unit = {
    val jdbcConnector = new JdbcConnector(TERADATA, loadConfig(config_file), 1)
    val connection = jdbcConnector getConnection(lookUpProp("teradata.load.user"), loadCommand(lookUpProp("teradata.load.password")))
    val statement = connection prepareStatement lookUpProp("avis.teradata.load.staging.complete")
    var currentLoadOffset = currentBatchOffset(connection)
    if (currentLoadOffset == EMPTYSTR || currentLoadOffset == null) currentLoadOffset = batchOffset
    statement.setString(1, currentLoadOffset)
    val status = statement.executeUpdate()
    if (status >= 0) info(s"Batch completed with status $status and Batch Offset $currentLoadOffset")
    commit(connection)
    closeResource(statement)
    closeResource(connection)
    closeResource(jdbcConnector)
  }

  private def truncateEdwStaging(): Boolean = {
    if (tryAndReturnDefaultValue(asFunc(statement.executeUpdate(lookUpProp("avis.teradata.staging.truncate"))), -1) >= 0) {
      info(s"avis teradata staging truncate completed ${lookUpProp("avis.teradata.staging.truncate")}")
      commit(connection)
      shutDown()
      return true
    }
    false
  }

  private def commit(con: Connection): Unit = {
    con commit()
  }

  private def currentBatchOffset(connection: Connection): String = {
    val resultSet = connection.createStatement() executeQuery lookUpProp("avis.teradata.staging.batch.maxoffset")
    if (valid(resultSet) && resultSet.next()) {
      resultSet getString 1
    } else transDate
  }

  private def shutDown(): Unit = {
    closeResource(statement)
    closeResource(connection)
    closeResource(jdbcConnector)
    info("Teradata Connection shutdown completed")
  }

  private def kInit(): Unit = {
    val commands = new ListBuffer[String]
    commands += "kinit"
    commands += "-k"
    commands += "-t"
    commands += lookUpProp("keytab")
    commands += lookUpProp("principal")
    if (executeScript(commands.asJava)) info(s"kInit completed for $commands")
  }

  private def loadCommand(commandFile: String): String = {
    if (lookUpProp("hl7.env") == "PROD") {
      new BufferedSource(fs.open(new Path(commandFile))).getLines().map(_.trim()).mkString(EMPTYSTR)
    }
    else s"${readFile(commandFile).getLines().map(_.trim()).mkString(EMPTYSTR)}"
  }
}
