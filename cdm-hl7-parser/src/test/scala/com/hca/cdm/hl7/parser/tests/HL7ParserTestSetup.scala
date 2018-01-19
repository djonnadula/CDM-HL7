package com.hca.cdm.hl7.parser.tests

import com.hca.cdm._
import com.hca.cdm.hl7.constants.HL7Types.{withName => hl7, _}
import com.hca.cdm.hl7.model._
import com.hca.cdm.hl7.parser.HL7Parser
import com.hca.cdm.log.Logg

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
  val hl7Parsers = hl7MsgMeta map (hl7 => hl7._1 -> new HL7Parser(hl7._1, templatesMapping))
  
  def parse(message: String): String = {
    Try(hl7Parsers(msgType).transformHL7(message, reject) rec) match {
      case Success(map) =>
        map match {
          case Left(out) =>
            out._1._1
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
