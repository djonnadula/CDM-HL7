/*
package com.hca.cdm

import java.io.{BufferedWriter, File, FileWriter}

import com.hca.cdm.auth.LoginRenewer
import com.hca.cdm.hadoop.HadoopConfig
import com.hca.cdm.log.Logg
import org.apache.log4j.PropertyConfigurator.configure
import com.hca.cdm.hl7.constants.HL7Constants._
import DataManipulations._
import com.hca.cdm.hbase._
import com.hca.cdm.hl7.enrichment.OffHeapConfig

import scala.collection.mutable


/**
  * Created by Devaraj Jonnadula on 3/1/2018.
  */
object NavFileGenerator extends Logg with App with DataManipulator {

  self =>
  configure(currThread.getContextClassLoader.getResource("cdm-log4j.properties"))
  reload(args(0))
  private val deIdCfg = Config(lookUpProp("hl7.did.repo")).getCfg
  private val orgCfg = Config(lookUpProp("hl7.org.repo")).getCfg
  private val navqMappings = readFile(lookUpProp("navq.mappings")).getLines().takeWhile(valid(_)).map(temp => temp split(COMMA, -1)).takeWhile(valid(_))
  private val hl7Mappings = navqMappings.map(x => x(1) -> tryAndReturnDefaultValue0(withName(x(2)), NONE)).toMap
  private val outBound = {
    val temp = new mutable.LinkedHashMap[Int, (String, String)]
    navqMappings.zipWithIndex.foreach { case (k,idx) => temp +=  idx -> (k(1),EMPTYSTR) }
    temp
  }
  private val deIdFields = hl7Mappings.filter(_._2 == DE_ID).keySet
  private val config = HadoopConfig.loadConfig(tryAndReturnDefaultValue0(lookUpProp("hadoop.config.files").split("\\;", -1).toSeq, Seq[String]()))
  LoginRenewer.loginFromKeyTab(lookUpProp("keytab"), lookUpProp("principal"), Some(config))
  private val connector = HBaseConnector(config, lookUpProp("cdm.hl7.hbase.namespace"))
  private val navqWriter = new BufferedWriter(new FileWriter(new File(lookUpProp("navq.file.dir")+FS+"iNAVQ-De-Identified.txt")))
  private val transIdsWriter = new BufferedWriter(new FileWriter(new File(lookUpProp("trans.ids.dir")+FS+"De-Identified-Control-Ids-Mappings.txt")))
  registerHook(newThread(s"SHook-${self.getClass.getSimpleName}${lookUpProp("app")}", runnable({
    shutDown()
    info(s"$self shutdown hook completed")
  })))

  transIdsWriter.write("Original-Control-Id,De-Id-Control-Id")



  private def processNavFile() : Unit ={
    readFile(lookUpProp("navq.file.in")).getLines().zipWithIndex.foreach{case(data,idx) =>
        if(idx >0){
          val layout = readMsg(data)
          val deIdentified = getDeIdData(deIdCfg,layout)
          hl7Mappings.foreach {
            case (enrichField, op) =>
              op match {
                case DE_ID =>
                  if ((deIdentified isDefinedAt enrichField) && deIdentified(enrichField) != EMPTYSTR) {
                  } else if ((deIdentified isDefinedAt enrichField) && deIdentified(enrichField) == EMPTYSTR) {
                  } else if (!(deIdentified isDefinedAt enrichField)) {
                  } else {
                  }
                case ANONYMIZE | DEFAULT =>
                  layout update(enrichField, EMPTYSTR)
                case DATE =>
                  handleDates()
                /*  val modDate = handleDates(layout(enrichField))
                  val formattedDt = getAlternateDateFrmt(modDate._2)
                  layout update(enrichField, modDate._1)
                  hl7Mod = handleText(hl7Mod, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)
                  obs_note = handleText(obs_note, enrichField, org(enrichField), layout(enrichField))(facility, sourceSystem)*/
                case NONE =>
              }
          }
        }else {
         writeMsg(navqMappings.map(x => x(1)).mkString(COMMA),navqWriter)
        }
    }
  }

  private def getDeIdData( cfg : OffHeapConfig, data : mutable.LinkedHashMap[String, String]): mutable.Map[String, String] = {
    HUtils.getRow(cfg.repo, cfg.identifier, cfg.fetchKey(data), deIdFields)(connector).map { case (k, v) => k -> new String(v, UTF8) }
  }

  private def readMsg(msg : String) : mutable.LinkedHashMap[String, String] ={
    val out = outBound.clone()
    msg.split(COMMA,-1).zipWithIndex.foreach{case (data,idx) => if(out.isDefinedAt(idx)) out.update(idx, (out(idx)._1,data))}
    out.map(_._2)

  }
  private def shutDown(): Unit = {
    closeResource(connector)
    if(valid(navqWriter)) navqWriter.flush()
    if(valid(transIdsWriter)) transIdsWriter.flush()
    closeResource(navqWriter)
    closeResource(transIdsWriter)
    info(s"$self shutdown completed")
  }


  private def writeMsg(msg : String , writer: BufferedWriter) : Unit={
    if(valid(msg) && valid(writer)) {
      writer.write(msg)
      writer.newLine()
    }
  }


}
*/
