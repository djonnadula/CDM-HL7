


package com.hca.cdm.hl7.parser

import com.hca.cdm.Models.MSGMeta
import com.hca.cdm._
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.audit._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.{withName => hl7}
import com.hca.cdm.hl7.model._
import com.hca.cdm.log.Logg
import org.apache.log4j.PropertyConfigurator._

import scala.util.{Failure, Success, Try}

/**
  * Created by Devaraj Jonnadula on 8/24/2016.
  */
object HL7LocalRunner extends App with Logg {

  configure(currThread.getContextClassLoader.getResource("local-log4j.properties"))
  reload(null,Some(currThread.getContextClassLoader.getResourceAsStream("Hl7LocalConfig.properties")))
  val msgType = hl7("ORU")
    // hl7(args(0))
  private val msgs = EMPTYSTR
  private val messageTypes = lookUpProp("hl7.messages.type") split COMMA
  private val hl7MsgMeta = messageTypes.map(mtyp => hl7(mtyp) -> getMsgTypeMeta(hl7(mtyp), lookUpProp(mtyp + ".kafka.source"))) toMap
  private val templatesMapping = loadTemplate(lookUpProp("hl7.template"))
  private val segmentsMapping = applySegmentsToAll(loadSegments(lookUpProp("hl7.segments")), messageTypes)
  private val modelsForHl7 = hl7MsgMeta.map(msgType => msgType._1 -> segmentsForHl7Type(msgType._1, segmentsMapping(msgType._1.toString)))
  private val registeredSegmentsForHl7 = modelsForHl7.mapValues(_.models.keySet)
  private val hl7Parsers = hl7MsgMeta map (hl7 => hl7._1 -> new HL7Parser(hl7._1, templatesMapping))
  private val jsonAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, jsonStage)(EMPTYSTR, _: MSGMeta)))
  private val segmentsAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, segmentStage)(_: String, _: MSGMeta)))
  private val adhocAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, adhocStage)(_: String, _: MSGMeta)))
  private val allSegmentsInHl7Auditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, segmentsInHL7)(_: String, _: MSGMeta)))
  private val segmentsHandler = modelsForHl7 map (hl7 => hl7._1 -> new DataModelHandler(hl7._2, registeredSegmentsForHl7(hl7._1), segmentsAuditor(hl7._1),
    allSegmentsInHl7Auditor(hl7._1), adhocAuditor(hl7._1)))
  Try(hl7Parsers(msgType).transformHL7(msgs, reject) rec) match {
    case Success(map) =>
      map match {
        case Left(out) =>
          info(out._1)
          segmentsHandler(msgType).handleSegments(outio, reject, audit, adhocDestination)(out._2, out._3)
        case Right(t) =>
          error(t);
      }
    case Failure(t) =>
      error(t)
  }

  private def outio(k: String, v: String) = {
    info(k)
  }

  private def reject(k: String, v: String) = {
    info(k)
  }

  private def audit(k: String, v: String) = {
    info(k)
  }

  private def adhocDestination(k: String, v: String, dest: String) = {
    info(k)
  }

}





