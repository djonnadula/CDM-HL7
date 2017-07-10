package com.hca.cdm.hl7.parser

import com.hca.cdm.Models.MSGMeta
import com.hca.cdm._
import com.hca.cdm.hl7.model._
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.audit._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.{withName => hl7}
import com.hca.cdm.log.Logg
import org.apache.log4j.PropertyConfigurator._

import scala.util.{Failure, Success, Try}

/**
  * Created by Devaraj Jonnadula on 8/24/2016.
  */
object HL7LocalRunner extends App with Logg {

  configure(currThread.getContextClassLoader.getResource("local-log4j.properties"))
  reload(null,Some(currThread.getContextClassLoader.getResourceAsStream("Hl7LocalConfig.properties")))
  val msgType = hl7("ADT")
    // hl7(args(0))
  private val msgs = "MSH|^~\\&||COCHM|||201703310757||ADT^A31|MT_COCHM_ADT_TNAGTMRI.1.7325521|P|2.1\nEVN|A31|201703310757||E\nPID|1||Q002023752|Q95587|GUPTON^BRANDY^LEIGH^^^|ANGELA|19900517|F|GUPTON^BRANDY^^^^|W|2704 HWY 47 NORTH^^WHITE BLUFF^TN^37187^USA^^^DICKSO.TN||615-972-6793|615-446-8000|ENG|S|NON|Q95587|412-65-5356\nIN1|1|TC.AMERIC||TNCARE AMERICHOICE|PO BOX 5220^STE 200^KINGSTON^NY^12402-5220||800-690-1606|MT01|TNCARE AMERICHOICE|||20081001|20110516|||GUPTON^BRANDY^L^^^|01|||||||||||||||||||JD3794405\nIN1|2|TC.AMGRP||TNCARE AMERIGROUP|PO BOX 61010^TN CLAIMS^VIRGINIA BEACH^VA^23466-1010||800-454-3730|||||||||GUPTON^BRANDY^L^^^|01|||||||||||||||||||712927939\nIN1|3|TC.BC||TNCARE BLUECARE|1 CAMERON HILL CIRCLE^SUITE 0002^CHATTANOOGA^TN^37402-0002||800-276-1978|125000||||20150101||||GUPTON^BRANDY^^^^|01|||||||||||||||||||ZECM12714949\nIN1|4|TC.UHCCMPL||TNCARE UHC COMMUNITY PLAN|PO BOX 5220^^KINGSTON^NY^12402-5220||800-690-1606|MT05||||20110601||||GUPTON^BRANDY^L^^^|01|||||||||||||||||||JD3794405\nIN1|5|UNINSURED||UNINSURED DISCOUNT PLAN|552 METROPLEX DR^^NASHVILLE^TN^37211||615-886-5660|||||||||GUPTON^BRANDY^LEIGH^^^|01|||||||||||||||||||412655356\n"
    //"MSH|^~\\&||COCXG|||201701231800||RDE^O01|MT_COCXG_RDE_XGPHAORD.1.10910266|P|2.2\nPID|||J000466169|J429090|KERR^BURGRTT^ULANE^^^||19499517|||||||||||J00073669669\nPV1|I||J.PACUH^J.801^A||||LAZJE^Lazarus^Jeffrey^J MD"

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
  private val tlmAuditor = tlmAckMsg("test", applicationReceiving, HDFS, _: String)(_: MSGMeta)
  private val segmentsHandler = modelsForHl7 map (hl7 => hl7._1 -> new DataModelHandler(hl7._2, registeredSegmentsForHl7(hl7._1), segmentsAuditor(hl7._1),
    allSegmentsInHl7Auditor(hl7._1), adhocAuditor(hl7._1), tlmAckMsg(hl7._1.toString, applicationReceiving, HDFS, _: String)(_: MSGMeta)))

  Try(hl7Parsers(msgType).transformHL7(msgs, reject) rec) match {
    case Success(map) =>
      map match {
        case Left(out) =>
          info("json: " + out._1)
          segmentsHandler(msgType).handleSegments(outio, reject, audit, adhocDestination)(out._2, out._3)
        case Right(t) =>
          error(t);
      }
    case Failure(t) =>
      error(t)
  }

  private def outio(k: String, v: String) = {
    info("outio: " + k)
  }

  private def reject(k: String, v: String) = {
    info("reject: " + k)
  }

  private def audit(k: String, v: String) = {
    info("audit: " + k)
  }

  private def adhocDestination(k: String, v: String, dest: String) = {
    info("adhocDestiation: " + k)
  }

}





