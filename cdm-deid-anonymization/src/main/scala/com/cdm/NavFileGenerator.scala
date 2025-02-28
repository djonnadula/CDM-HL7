

package com.cdm

import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Date
import com.cdm.auth.LoginRenewer
import com.cdm.hadoop.HadoopConfig
import com.cdm.log.Logg
import org.apache.log4j.PropertyConfigurator.configure
import com.cdm.hl7.constants.HL7Constants._
import DataManipulations._
import com.cdm.hbase._
import com.cdm.hl7.enrichment.OffHeapConfig
import com.cdm.utils.DateConstants.HL7_ORG
import org.joda.time.DateTime
import com.cdm.Patterns._
import com.cdm.utils.{ExecutionPool, RetryHandler}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await.result
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.{global => executionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future => async}

/**
  * Created by Devaraj Jonnadula on 3/1/2018.
  */
object NavFileGenerator extends Logg with App {

  self =>
  configure(currThread.getContextClassLoader.getResource("cdm-log4j.properties"))
  reload(args(0))
  private val deIdCfg = Config(lookUpProp("hl7.did.repo")).getCfg
  private val navHeader = new ListBuffer[String]
  private val hl7Mappings = {
    val temp = new mutable.LinkedHashMap[String, DataManipulations.Value]
    readFile(lookUpProp("navq.mappings")).getLines().takeWhile(valid(_)).map(temp => temp split(COMMA, -1)).takeWhile(valid(_)).foreach { x =>
      temp += x(1) -> tryAndReturnDefaultValue0(withName(x(2)), NONE)
      navHeader += x(0)
    }
    temp
  }
  private val outBound = hl7Mappings.zipWithIndex.map { case (k, idx) => idx -> (k._1, EMPTYSTR) }
  private val deIdFields = hl7Mappings.filter(_._2 == DE_ID).keySet.toSet
  private val config = HadoopConfig.loadConfig(tryAndReturnDefaultValue0(lookUpProp("hadoop.config.files").split("\\;", -1).toSeq, Seq[String]()))
  config.set("hbase.client.scanner.timeout.period", "50000000")
  LoginRenewer.loginFromKeyTab(lookUpProp("keytab"), lookUpProp("principal"), Some(config))
  private val connector = HBaseConnector(config, lookUpProp("cdm.hl7.hbase.namespace"))
  private val navqWriter = new BufferedWriter(new FileWriter(new File(lookUpProp("navq.file.in") + "-out")))
  private var transIdsWriter: BufferedWriter = _
  private val pool = new ExecutionPool
  registerHook(newThread(s"SHook-${self.getClass.getSimpleName}${lookUpProp("app")}", runnable({shutDown()})))
  if (lookUpProp("ref").toBoolean) {
    transIdsWriter = new BufferedWriter(new FileWriter(new File(lookUpProp("trans.ids.dir") + FS + "De-Identified-Control-Ids-Mappings.txt")))
    generateRefFile()
  }
  processNavFile()


  private def generateRefFile(): Unit = {
    val deIdCfg = Config(lookUpProp("hl7.ref.did.repo")).getCfg
    val orgCfg = Config(lookUpProp("hl7.ref.org.repo")).getCfg
    writeMsg("Original-Facility|Original-Control-Id|De-Id-Facility|De-Id-Control-Id", transIdsWriter)
    val familyQualifiers = new mutable.HashMap[String, Set[String]]()
    familyQualifiers += deIdCfg.identifier -> deIdCfg.fetchKeyAttributes
    familyQualifiers += orgCfg.identifier -> orgCfg.fetchKeyAttributes
    val actions = new ListBuffer[async[Unit]]
    val size = tryAndReturnDefaultValue0(lookUpProp("batchsize").toInt,50000)
    val max = tryAndReturnDefaultValue0(lookUpProp("tablesize").toInt,Int.MaxValue)
    for (range <- 0 until(max, size)) {
      actions += async {
        HUtils.fetchFamilyQualifiers(deIdCfg.repo, familyQualifiers.toMap, size, range)(connector) foreach {
          x =>
            if (x(orgCfg.identifier).forall { case (_, v) => v != EMPTYSTR } && x(deIdCfg.identifier).forall { case (_, v) => v != EMPTYSTR })
              writeMsg(s"${x(orgCfg.identifier).values.mkString(PIPE_DELIMITED_STR)}$PIPE_DELIMITED_STR${x(deIdCfg.identifier).values.mkString(PIPE_DELIMITED_STR)}", transIdsWriter)
        }
        transIdsWriter.flush()
      }(executionContext)
    }
    actions.foreach { a =>
      tryAndReturnThrow(result(a, Duration.Inf)) match {
        case Left(_) =>
          info(s"Succeeded for $a")
        case Right(t) =>
          error(t)
          info(s"Failed for $a trying again")
          RetryHandler(4, 1000, asFunc({
            result(a, Duration.Inf)
          }), asFunc(error(s"Failed for $a after trying max")))
      }
    }
  }

  private def processNavFile(): Unit = {
    writeMsg(navHeader.mkString(PIPE_DELIMITED_STR), navqWriter)
    val in = readFile(lookUpProp("navq.file.in")).getLines().map(readMsg).map(struct => deIdCfg.fetchKey(struct) -> struct).toMap
    val deIdentified = getDeIdData(deIdCfg, in.keys.toList)
    in foreach { case (key, layout) =>
      if (deIdentified.isDefinedAt(key) && deIdentified(key).nonEmpty) {
        hl7Mappings.foreach {
          case (enrichField, op) =>
            op match {
              case DE_ID =>
                layout update(enrichField, deIdentified(key).getOrElse(enrichField, EMPTYSTR))
              case ANONYMIZE | DEFAULT =>
                layout update(enrichField, EMPTYSTR)
              case DATE =>
                layout update(enrichField, handleDates(layout(enrichField)))
              case NONE =>
            }
        }
        writeMsg(layout.values.mkString(PIPE_DELIMITED_STR), navqWriter)
      }
    }
  }

  private def handleDates(date: String): String = {
    if (valid(date) && date != EMPTYSTR) {
      var formatter = getFormatter(date)
      Try(formatter.parse(date).getTime) match {
        case Success(x) =>
          return formatter.format(alterDate(new org.joda.time.DateTime(x)))
        case Failure(_) =>
          def tryAgain = {
            val dt = date.substring(0, 8)
            formatter = getFormatter(dt)
            val dat = formatter.parse(dt)
            formatter.format(new org.joda.time.DateTime(dat.getTime))
          }

          return tryAndReturnDefaultValue0(tryAgain, default = date)

      }
    }
    date
  }

  private def getFormatter(date: String): SimpleDateFormat = {
    val length = date.length
    if (date contains "/") {
      return new SimpleDateFormat(format7)
    }
    if (length == 8) new SimpleDateFormat(format2)
    else if (length > 8 && length <= HL7_ORG.length) new SimpleDateFormat(format1)
    else if (length == format3.length) new SimpleDateFormat(format3)
    else new SimpleDateFormat(format2)
  }

  private def alterDate(date: DateTime): Date = {
    date.minusDays(1).minusHours(6).minusMinutes(14).plusSeconds(44).toDate
  }

  private def getDeIdData(cfg: OffHeapConfig, keys: List[String]): Map[String, mutable.Map[String, String]] = {
    HUtils.getRows(cfg.repo, cfg.identifier, keys, deIdFields)(connector).map {
      case (k, v) => k -> v.map(x => x._1 -> new String(x._2, UTF8)).filter(_._2 != EMPTYSTR)
    }
  }

  private def readMsg(msg: String): mutable.LinkedHashMap[String, String] = {
    val out = outBound.clone()
    msg.split(s"\\$PIPE_DELIMITED_STR", -1).zipWithIndex.foreach { case (data, idx) => if (out.isDefinedAt(idx)) out.update(idx, (out(idx)._1, data)) }
    out.map(_._2)
  }

  private def shutDown(): Unit = {
    closeResource(connector)
    if (valid(navqWriter)) navqWriter.flush()
    if (valid(transIdsWriter)) transIdsWriter.flush()
    closeResource(navqWriter)
    closeResource(transIdsWriter)
    info(s"$self shutdown completed")
  }


  private def writeMsg(msg: String, writer: BufferedWriter): Unit = synchronized {
    if (valid(msg) && valid(writer)) {
      writer.write(msg)
      writer.newLine()
    }
  }


}


