package com.hca.cdm.hl7.parser.tests

import com.hca.cdm.Models.MSGMeta
import com.hca.cdm._
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.audit._
import com.hca.cdm.hl7.constants.HL7Types.{withName => hl7, _}
import com.hca.cdm.hl7.model._
import com.hca.cdm.hl7.parser.HL7Parser
import com.hca.cdm.log.Logg
import com.hca.cdm.mq.publisher.{MQAcker => TLMAcknowledger}
import scala.util.{Failure, Success, Try}

/**
  * Created by Peter James on 2/16/2017.
  *
  * HL7 Unit Test Setup
  */
class HL7ParserTestSetup(msgType: HL7) extends Logg {

  HL7ParserTestUtils.loadProperties()
  val res = ""
  val messageTypes = lookUpProp("hl7.messages.type") split ","
  val templatesMapping = loadTemplate(lookUpProp("hl7.template"))
  val segmentsMapping = applySegmentsToAll(loadSegments(lookUpProp("hl7.segments")), messageTypes)
  val hl7MsgMeta = messageTypes.map(mtyp => hl7(mtyp) -> getMsgTypeMeta(hl7(mtyp), mtyp + ".kafka.source")) toMap
  val modelsForHl7 = hl7MsgMeta.map(msgType => msgType._1 -> segmentsForHl7Type(msgType._1, segmentsMapping(msgType._1.toString)))
  val registeredSegmentsForHl7 = modelsForHl7.mapValues(_.models.keySet)
  val hl7Parsers = hl7MsgMeta map (hl7 => hl7._1 -> new HL7Parser(hl7._1, templatesMapping))
  val segmentsAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, segmentStage)(_: String, _: MSGMeta)))
  val adhocAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, adhocStage)(_: String, _: MSGMeta)))
  val allSegmentsInHl7Auditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, segmentsInHL7)(_: String, _: MSGMeta)))
  val segmentsHandler = modelsForHl7 map (hl7 => hl7._1 -> new DataModelHandler(hl7._2, registeredSegmentsForHl7(hl7._1), segmentsAuditor(hl7._1),
    allSegmentsInHl7Auditor(hl7._1), adhocAuditor(hl7._1), null))
  var tlmAckIO: (String, String) => Unit = null
  TLMAcknowledger("test", "test")(lookUpProp("mq.hosts"), lookUpProp("mq.manager"), lookUpProp("mq.channel"), lookUpProp("mq.destination.queues"))
  tlmAckIO = TLMAcknowledger.ackMessage(_: String, _: String)
  
  def parse(message: String): String = {
    Try(hl7Parsers(msgType).transformHL7(message, reject) rec) match {
      case Success(map) =>
        map match {
          case Left(out) =>
            info("json: " + out._1)
            segmentsHandler(msgType).handleSegments(outio, templateerrorReject, audit, adhocDestination, Some(tlmAckIO),null)(out._2, message, out._3)
            out._1
          case Right(t) =>
            error(t.toString)
            ""
        }
      case Failure(t) =>
        error(t)
        ""
    }
  }

  def outio(k: String, v: String) = {
  }

  def reject(k: AnyRef, v: String) = {
  }

  def audit(k: String, v: String) = {
  }

  def adhocDestination(k: String, v: String, dest: String) = {
  }

  private def templateerrorReject(k: AnyRef, v: String) = {
    info("reject: " + k + " " + v)
  }
}
