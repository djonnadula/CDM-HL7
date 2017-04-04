package com.hca.cdm.job.regression

import java.util.Date
import com.hca.cdm.log.Logg
import com.hca.cdm._
import com.hca.cdm.auth.LoginRenewer._
import com.hca.cdm.hive.{HiveConnector => dataFetcher}
import com.hca.cdm.hadoop.HadoopConfig._
import com.hca.cdm.notification.TaskState._
import com.hca.cdm.utils.DateConstants._
import com.hca.cdm.utils.DateUtil._
import com.hca.cdm.utils.ExecutionPool
import org.apache.log4j.PropertyConfigurator._
import com.hca.cdm.notification.{sendMail => mail}
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.concurrent.duration.Duration.{Inf => waitTillTaskCompletes}
import scala.concurrent.{Await, Future => async}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * Created by Devaraj Jonnadula on 2/15/2017.
  */
object SegmentsValidation extends Logg with App {

  configure(currThread.getContextClassLoader.getResource("cdm-log4j.properties"))
  reload(args(0))
  private val config = loadConfig(lookUpProp("hl7.config.files"))
  private val hiveCfg = hiveConf(config)
  private val client = new dataFetcher
  loginFromKeyTab(lookUpProp("hl7.auth.keytab"), lookUpProp("hl7.auth.principal"), Some(config))
  private val executor = new ExecutionPool
  private val noDataInQA = "No Data in QA"
  private val noDataInReg = "No Data in Reg"
  private val noTableInReg = "No Table Exist for Segment in Regression Database"
  private val rowsmatchingForId = "Rows Matching for Id's"
  private val ETL_TIME = "etl_firstinsert_datetime"
  private val MSG_TYPE = "message_type"
  private val builder = new StringBuilder
  private val append = builder append (_: Any)
  private val tdData = " <td width=100 style=font-size:1em; line-height:1.2em; font-family:Courier;>"
  private val tdDataEnd = "</td>"
  runJob()


  private def runJob(): Unit = {
    try {
      config.set("hive2.hosts", lookUpProp("cdm.hive.hosts"))
      client.hiveJDBCClient(hiveCfg)
      val sourceOfTruth = DataSource(lookUpProp("hl7.regDataSource"), lookUpProp("hl7.segments.controlId"))
      sourceOfTruth.addPartitionsIfNotExist()
      val segmentsFromReg = sourceOfTruth.segmentsInTables
      val sourceToCompare = DataSource(lookUpProp("hl7.qaDataSource"), lookUpProp("hl7.segments.controlId"))
      sourceToCompare.addPartitionsIfNotExist()
      val segmentsFromQA = sourceToCompare.segmentsInTables
      generateReport(
        segmentsFromQA.flatMap({ case (qaSeg, qaRow) =>
          if (segmentsFromReg isDefinedAt qaSeg) {
            val regDataForTable = segmentsFromReg(qaSeg)
            qaRow.flatMap({ case (qaControlId, qaColumns) =>
              if (regDataForTable isDefinedAt qaControlId) {
                val columnsMisMatching = new mutable.ListBuffer[Column]()
                val columnsDontExist = new mutable.ListBuffer[Column]()
                val regData = regDataForTable(qaControlId)
                var msgType = EMPTYSTR
                qaColumns.filter(_._1 != ETL_TIME) foreach {
                  case (qaColumn, qaData) =>
                    if (qaColumn == MSG_TYPE && (regData isDefinedAt MSG_TYPE)) msgType = qaData
                    if (regData isDefinedAt qaColumn) {
                      if (qaData != regData(qaColumn)) columnsMisMatching += Column(qaColumn, qaData, regData(qaColumn))
                    } else columnsDontExist += Column(qaColumn, qaData, noDataInReg)
                }
                if (columnsMisMatching.isEmpty && columnsDontExist.isEmpty) {
                  List(ComparedData(qaSeg, qaControlId, EMPTYSTR,
                    if (valid(columnsMisMatching)) Some(columnsMisMatching.toList) else None,
                    if (valid(columnsDontExist)) Some(columnsDontExist.toList) else None)) ::: List(ComparedData(qaSeg, qaControlId, rowsmatchingForId, None, None, msgType))
                } else {
                  List(ComparedData(qaSeg, qaControlId, EMPTYSTR,
                    if (valid(columnsMisMatching)) Some(columnsMisMatching.toList) else None,
                    if (valid(columnsDontExist)) Some(columnsDontExist.toList) else None))
                }
              } else List(ComparedData(qaSeg, qaControlId, noDataInReg))
            })
          } else List(ComparedData(qaSeg, EMPTYSTR, noTableInReg))
        }).groupBy(_.segment).map(seg => seg._1 -> seg._2.toList.sortBy(_.segment)))
    } catch {
      case t: Throwable =>
        error(s"Cannot Run Job for $this", t)
    } finally {
      executor.printStats()
      executor.shutDown()
      closeResource(client)
      info(s"Shutdown Completed for $this")
    }


  }

  private def tryForTaskExe[T](action: async[T]): Try[T] = Try(Await result(action, waitTillTaskCompletes))

  private[this] case class DataSource(source: String, uniqueId: String) {

    val tables: Seq[String] = client.tables(source).filter(table => {
      !(table.contains("hl7_all_proc_rejected") | table.contains("hl7_audit_data"))
    })

    def addPartitionsIfNotExist(): Unit = {
      client.multiQueries(tables.map(table => s"MSCK REPAIR TABLE $table"))
    }

    def segmentsInTables: Map[String, Map[String, Map[String, String]]] = {
      import executor.poolContext
      info(s"Tables from Data Source $source")
      tables.foreach(info(_))
      info("---------------------------------------------------------------------------")
      tables.map(table => {
        async {
          table.substring(table.indexOf("_") + 1, table.lastIndexOf("_")) -> client.sql(generateQuery(table), uniqueId)
        }(poolContext)
      }).map(tryForTaskExe).map {
        case Success(data) => data._1 -> data._2
        case Failure(t) =>
          error(s"Cannot fetch Data for Table", t)
          null
      }.filter(_ != null).toMap
    }

    private def generateQuery(table: String): String = {
      s"select * from $source.$table"
    }
  }

  private[this] case class TableData(segment: String, rows: Map[String, List[(String, String)]])

  private[this] case class ComparedData(segment: String, controlId: String, message: String, columnsMisMatching: Option[List[Column]] = None,
                                        columnsDontExist: Option[List[Column]] = None, messageType: String = EMPTYSTR)

  private[this] case class Column(name: String, data: String, expected: String, msgType: String = EMPTYSTR)


  private[this] def generateReport(data: Map[String, List[ComparedData]]) {
    ListMap(data.toSeq.sortBy(_._1): _*).foreach({ case (segment, results) =>
      if (valid(results)) {
        results.groupBy(_.message).foreach({ case (message, result) =>
          message match {
            case `rowsmatchingForId` =>
              append(s"<div style=color:#00b258><h4>QA Segments for ${segment.toUpperCase()} Which Match with Regression Source as Follows  </h4>")
              append("<table cellspacing=0 cellpadding=10 border=1 style=font-size:1em; line-height:1.2em; font-family:Courier;><tr>")
              append(s"<th width=30 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:Courier;>Message Type</th>")
              append(s"<th width=30 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:Courier;>Control Id</th>")
              append("</tr>")
              result.foreach({ column =>
                append("<tr>" + tdData + column.messageType + tdDataEnd +
                  tdData + column.controlId + tdDataEnd +
                  "</tr>")
              })
              append("</table>")
              append("</div>")
            case `noTableInReg` =>
              append(s"<div style=color:#FF4500><h3>No Table Exist for ${segment.toUpperCase} in Regression Data Base</h3><br/>")
            case `noDataInReg` =>
              append(s"<div style=color:#FF4500><h3>No Data Exist for ${segment.toUpperCase} & Controld Id's ${result.map(_.controlId).mkString(" :: ")} in Regression Data Base to Compare Against QA</h3><br/>")
            case `EMPTYSTR` =>
              if (resultsNotMatched(result)) {
                append(s"<div style=color:#708090><h3>Mismatch Results for Segment ${segment.toUpperCase} from QA to Regression Data Base as follows </h3>")
                val misMatch = result.filter(_.columnsMisMatching.isDefined)
                if (valid(misMatch)) {
                  append("<h4 style=color:#FF0000> Columns which Don't Match are as follows </h4>")
                  append("<div><table cellspacing=0 cellpadding=10 border=1 style=font-size:1em; line-height:1.2em; font-family:Courier;><tr>")
                  append(s"<th width=30 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:Courier;>Control Id</th>")
                  append(s"<th width=30 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:Courier;>Attribute</th>")
                  append(s"<th width=30 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:Courier;>QA output</th>")
                  append(s"<th width=30 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:Courier;>Expected Output</th>")
                  append("</tr>")
                  misMatch.foreach(row => handleColumnReport(row.columnsMisMatching.get, row.controlId))
                  append("</table>")
                  append("</div> ")
                }
                val dontExist = result.filter(_.columnsDontExist.isDefined)
                if (valid(dontExist)) {
                  append("<div style=color:#FF0000><h4> Columns which Don't Exist in Regression Source as follows  </h4>")
                  append("<table cellspacing=0 cellpadding=10 border=1 style=font-size:1em; line-height:1.2em; font-family:Courier;><tr>")
                  append(s"<th width=30 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:Courier;>Control Id</th>")
                  append(s"<th width=30 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:Courier;>Attribute</th>")
                  append(s"<th width=30 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:Courier;>QA output</th>")
                  append(s"<th width=30 style=font-weight:bold; font-size:1em; line-height:1.2em; font-family:Courier;>Expected Output</th>")
                  append("</tr>")
                  dontExist.foreach(row => handleColumnReport(row.columnsDontExist.get, row.controlId))
                  append("</table>")
                  append("</div> ")
                }
              }
          }
        })
      } else {
        append(s"<div style=color:#0000FF><h3>${segment.toUpperCase} has no Data to Compare </h3></div>")
      }
    })
    mail("{encrypt} Regression Test Results for HL7 Segments Ran on " + dateToString(new Date().toInstant.atZone(sys_ZoneId).toLocalDateTime, DATE_WITH_TIMESTAMP), builder.result(), NORMAL, statsReport = true)
  }

  private def handleColumnReport(columns: List[Column], controlId: String): Unit = {
    columns.foreach({ column =>
      append("<tr>" + tdData + controlId + tdDataEnd +
        tdData + column.name + tdDataEnd +
        tdData + column.data + tdDataEnd +
        tdData + column.expected + tdDataEnd +
        "</tr>")
    })
  }

  private def resultsNotMatched(data: List[ComparedData]): Boolean = {
    data.foreach(x => if (x.columnsDontExist.isDefined || x.columnsMisMatching.isDefined) return true)
    false
  }

}


