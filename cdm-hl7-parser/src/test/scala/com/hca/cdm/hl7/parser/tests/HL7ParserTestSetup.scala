package com.hca.cdm.hl7.parser.tests

import com.hca.cdm.Models.MSGMeta
import com.hca.cdm._
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.audit._
import com.hca.cdm.hl7.constants.HL7Types.{withName => hl7, _}
import com.hca.cdm.hl7.model._
import com.hca.cdm.hl7.parser.HL7Parser

import scala.util.{Success, Try}

/**
  * Created by Peter James on 2/16/2017.
  *
  * HL7 Unit Test Setup
  */
class HL7ParserTestSetup(msgType: HL7) {

    def loadProperties(properties: String): Unit = {
        reload(null, Some(currThread.getContextClassLoader.getResourceAsStream(properties)))
    }
    loadProperties("Hl7TestConfig.properties")
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
        allSegmentsInHl7Auditor(hl7._1), adhocAuditor(hl7._1),null))

    def parse(message: String): String = {
        Try( hl7Parsers(msgType).transformHL7(message, reject) rec ) match {
            case Success(map) =>
                map match {
                    case Left(out) =>
                        segmentsHandler(msgType).handleSegments(outio, reject, audit, adhocDestination)(out._2,null, out._3)
                        info(out._1)
                        out._1
                    case Right(t) =>
                        t.toString
                }
        }
    }

    def outio(k: String, v: String) = {
        info("outio: " + k)
    }

    def reject(k: String, v: String) = {
        info("reject: " + k)
    }

    def audit(k: String, v: String) = {
        info("audit: " + k)
    }

    def adhocDestination(k: String, v: String, dest: String) = {
        info("adhocDestination: " + k)
    }
}
