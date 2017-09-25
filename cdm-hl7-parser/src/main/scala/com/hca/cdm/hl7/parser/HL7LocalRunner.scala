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
import com.hca.cdm.mq.publisher.{MQAcker => TLMAcknowledger}
/**
  * Created by Devaraj Jonnadula on 8/24/2016.
  */
object HL7LocalRunner extends App with Logg {

  configure(currThread.getContextClassLoader.getResource("local-log4j.properties"))
  reload(null,Some(currThread.getContextClassLoader.getResourceAsStream("Hl7LocalConfig.properties")))
//  private val messageTypesR = lookUpProp("hl7.messages.type") split COMMA
//  private val hl7MsgMetaR = messageTypesR.map(mtyp => mtyp -> getReceiverMeta(hl7(mtyp), lookUpProp(s"$mtyp.wsmq.source"), lookUpProp(s"$mtyp.kafka"))).toMap
//  private val hl7QueueMappingR = hl7MsgMetaR.flatMap(x => x._2.wsmq.map(que => que -> x._1))
//  private val hl7KafkaOutR = hl7MsgMetaR.map(x => x._1 -> x._2.kafka)
//  private val hl7QueuesR = hl7MsgMetaR.flatMap(_._2.wsmq).toSet
//  private val tlmAuditorR = hl7QueueMappingR.map (x =>  x._2 -> (tlmAckMsg(x._1, applicationSending, WSMQ, HDFS)(_: MSGMeta)))

//  private val messageTypesR = lookUpProp("hl7.messages.type") split COMMA
//  private val hl7MsgMetaR = messageTypesR.map(mtyp => mtyp -> getReceiverMeta(hl7(mtyp), lookUpProp(s"$mtyp.wsmq.source"), lookUpProp(s"$mtyp.kafka"))).toMap
//  private val hl7QueueMappingR = hl7MsgMetaR.map(x => x._2.wsmq -> x._1)
//  private val hl7KafkaOutR = hl7MsgMetaR.map(x => x._1 -> x._2.kafka)
//  private val hl7QueuesR = hl7MsgMetaR.map(_._2.wsmq).toSet
//  private val tlmAuditorR = hl7MsgMetaR map (x => x._2.wsmq -> (tlmAckMsg(x._1, applicationSending, WSMQ, HDFS)(_: MSGMeta)))
  val msgType = hl7("ORU")
    // hl7(args(0))
  private val msgs =
    "MSH|^~\\&||COCXG|||201701231800||RDE^O01|MT_COCXG_RDE_XGPHAORD.1.10910266|P|2.2\n" +
      "PID|||J000466169|J429090|KERR^BURGRTT^ULANE^^^||19499517|||||||||||J00073669669\n" +
      "PV1|I||J.PACUH^J.801^A||||LAZJE^Lazarus^Jeffrey^J MD"

  private val messageTypes = lookUpProp("hl7.messages.type") split COMMA
  private val hl7MsgMeta = messageTypes.map(mtyp => hl7(mtyp) -> getMsgTypeMeta(hl7(mtyp), lookUpProp(mtyp + ".kafka.source"))) toMap
  private val templatesMapping = loadTemplate(lookUpProp("hl7.template"))
  private val segmentsMapping = applySegmentsToAll(loadSegments(lookUpProp("hl7.segments")) ++ loadSegments(lookUpProp("hl7.adhoc-segments")), messageTypes)
  private val modelsForHl7 = hl7MsgMeta.map(msgType => msgType._1 -> segmentsForHl7Type(msgType._1, segmentsMapping(msgType._1.toString)))
  private val registeredSegmentsForHl7 = modelsForHl7.mapValues(_.models.keySet)
  private val hl7Parsers = hl7MsgMeta map (hl7 => hl7._1 -> new HL7Parser(hl7._1, templatesMapping))
  private val segmentsAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, segmentStage)(_: String, _: MSGMeta)))
  private val adhocAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, adhocStage)(_: String, _: MSGMeta)))
  private val allSegmentsInHl7Auditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, segmentsInHL7)(_: String, _: MSGMeta)))
  private val tlmAuditor = tlmAckMsg("test", applicationReceiving, HDFS, _: String)(_: MSGMeta)
  private val segmentsHandler = modelsForHl7 map (hl7 => hl7._1 -> new DataModelHandler(hl7._2, registeredSegmentsForHl7(hl7._1), segmentsAuditor(hl7._1),
    allSegmentsInHl7Auditor(hl7._1), adhocAuditor(hl7._1), tlmAckMsg(hl7._1.toString, applicationReceiving, HDFS, _: String)(_: MSGMeta)))
  var tlmAckIO: (String, String) => Unit = null
  modelsForHl7.filter(_._1.toString == "MDM")
    // .filter(_._2.models.filter(_._1.contains("ADHOC")))
    TLMAcknowledger("test", "test")(lookUpProp("mq.hosts"), lookUpProp("mq.manager"), lookUpProp("mq.channel"), lookUpProp("mq.destination.queues"))
    tlmAckIO = TLMAcknowledger.ackMessage(_: String, _: String)

  Try(hl7Parsers(msgType).transformHL7(msgs, reject) rec) match {
    case Success(map) =>
      map match {
        case Left(out) =>
          info("json: " + out._1)
          segmentsHandler(msgType).handleSegments(outio, templateerrorReject, audit, adhocDestination,Some(tlmAckIO),null)(out._2, msgs, out._3)
        case Right(t) =>
          error(t);
      }
    case Failure(t) =>
      error(t)
  }

  private def outio(k: String, v: String) = {
   // info("outio: " + k)
  }

  private def reject(k: AnyRef, v: String) = {
    info("reject: " + k)
  }
  private def templateerrorReject(k: AnyRef, v: String) = {
    info("reject: " + k + " " + v)
  }
  private def audit(k: String, v: String) = {
   // info("audit: " + k)
  }

  private def adhocDestination(k: String, v: String, dest: String) = {
    info("adhocDestiation: " + k)
  }

}





