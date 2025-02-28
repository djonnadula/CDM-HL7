package com.cdm.hl7.model

import com.cdm._
import com.cdm.hl7.audit.AuditConstants.segmentsInHL7
import com.cdm.hl7.audit._
import com.cdm.hl7.constants.HL7Constants._
import com.cdm.hl7.model.SegmentsState._
import com.cdm.log.Logg

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.{global => executionContext}
import scala.concurrent.duration.Duration.{Inf => waitTillTaskCompletes}
import scala.concurrent.{Await, Future => async}
import scala.util.{Failure, Success, Try}
import AuditConstants._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.cdm.Models.MSGMeta

import scala.collection.mutable.ListBuffer
import scala.language.postfixOps

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  *
  * Handler executes Registered Segments Asynchronously
  */
class DataModelHandler(hl7Segments: Hl7Segments, allSegmentsForHl7: Set[String],
                       segmentsAuditor: (String, MSGMeta) => String, segmentsInHl7Auditor: (String, MSGMeta) => String,
                       adhocAuditor: (String, MSGMeta) => String, tlmAuditor: (String, MSGMeta) => String)
  extends SegmentsHandler with Logg {

  private val hl7 = hl7Segments.msgType.toString
  info(
    hl7 + " " +
      """ Handler Registered For Modeling ::
      ____
    HL / /    DATA
      / /
     / /
    /_/
      " Segments Registered with Handler :: """ + hl7Segments.models.keys.mkString(caret))
  logIdent = s"$hl7-Model Handler  "
  private lazy val sizeCheck = checkSize(lookUpProp("hl7.message.max").toInt)(_, _)
  private lazy val fromJson = new ObjectMapper().registerModule(DefaultScalaModule).readValue(_: String, classOf[mutable.Map[String, String]])

  private case class Segment(seg: String, apply: (mapType, String) => Hl7SegmentTrans, adhoc: Boolean, dest: Option[DestinationSystem], auditKey: String, headerKey: String, tlmAckApplication: String = EMPTYSTR)

  private val metrics = new TrieMap[String, Long]
  private val dataModeler = DataModeler(hl7Segments.msgType)
  private val segRef = {
    val temp = new mutable.ArrayBuffer[Segment]
    hl7Segments.models.foreach { case (seg, models) =>
      models.foreach { model =>
        temp += Segment(seg, dataModeler.applyModel(seg, model)(_: mapType, _: String), model.adhoc.isDefined, if (model.adhoc.isDefined) Some(model.adhoc.get.destination) else None,
          if (model.adhoc.isDefined) seg substring ((seg indexOf COLON) + 1) else EMPTYSTR,
          if (model.adhoc.isDefined) seg.replaceAll(COLON, "-") else EMPTYSTR,
          if (model.adhoc.isDefined) model.adhoc.get.ackApplication else EMPTYSTR)
        SegmentsState.values.foreach { state =>
          metrics += s"$hl7$COLON$seg$COLON$state" -> 0L
        }
      }
    }
    temp
  }
  private lazy val nonAdhocSegments = segRef map (seg => if (!seg.adhoc) seg.seg else EMPTYSTR) filter (_ != EMPTYSTR)

  private def runModel(io: (String, String) => Unit, rejectIO: (AnyRef, String) => Unit, auditIO: (String, String) => Unit,
                       adhocIO: (String, String, String) => Unit, tlmAckIO: Option[(String, String) => Unit] = None,
                       hBaseIO: Map[String, (String, String, mutable.Map[String, String],Boolean) => Unit])
                      (data: mapType, rawHl7: String, meta: MSGMeta): Unit = {
    val tlmAckMessages = if (tlmAckIO isDefined) new ListBuffer[(String, String)] else null
    segRef map (seg => seg -> run(seg, data, rawHl7)) foreach { case (segment, transaction) => tryForTaskExe(transaction) match {
      case Success(tranCompleted) =>
        tranCompleted.trans match {
          case Left(out) =>
            out foreach { case (rec, t) => if (rec ne null) {
              rec match {
                case `skippedStr` =>
                  updateMetrics(segment.seg, SKIPPED)
                  val msg = rejectMsg(hl7, segment.seg, meta, skippedStr, data)
                  sizeCheck(msg, segment.seg)
                  tryAndLogThr(rejectIO(msg, header(hl7, rejectStage, Left(meta))), s"$hl7$COLON${segment.seg}-rejectIO-skippedSegment", error(_: Throwable))
                  debug(s"Segment Skipped :: $msg")
                case `filteredStr` =>
                  updateMetrics(segment.seg, FILTERED)
                // This Check Added After Discussing this Log is not Required as of now So commenting.
                /*  val msg = rejectMsg(hl7, segment.seg, meta, filteredStr, null)
                 sizeCheck(msg, segment.seg)
                 tryAndLogThr(rejectIO(msg, header(hl7, rejectStage, Left(meta))), s"$hl7$COLON${segment.seg}-rejectIO-filteredSegment", error(_: Throwable))
                 debug(s"Segment Filtered :: $msg") */
                case `partialRejectStr` =>
                  updateMetrics(segment.seg, PARTIAL_REJECT)
                  val msg = rejectMsg(hl7, segment.seg, meta, partialRejectStr, data, t)
                  sizeCheck(msg, segment.seg)
                  tryAndLogThr(rejectIO(msg, header(hl7, rejectStage, Left(meta))), s"$hl7$COLON${segment.seg}-rejectIO-partialRejectSegment", error(_: Throwable))
                case _ =>
                  sizeCheck(rec, segment.seg)
                  if (segment.adhoc) {
                    def adhocFailed(): Unit = {
                      tryAndLogThr(rejectIO(rejectMsg(hl7, segment.seg, meta, " Writing Adhoc request Data to OUT Failed ", data),
                        header(hl7, rejectStage, Left(meta)))
                        , s"$hl7$COLON${segment.seg}-adhocIO-rejectIO", error(_: Throwable))
                      updateMetrics(segment.seg, FAILED)
                    }

                    if (segment.tlmAckApplication != EMPTYSTR && (tlmAckIO isDefined)) {
                      tlmAckMessages += Pair(tlmAuditor(segment.tlmAckApplication, meta), segment.tlmAckApplication)
                    }
                    if (segment.dest.get.system == Destinations.WSMQ && (tlmAckIO isDefined)) {
                      tlmAckMessages += Pair(rec, segment.headerKey)
                      if (tryAndLogThr(auditIO(adhocAuditor(segment.auditKey, meta), header(hl7, auditHeader, Left(meta))),
                        s"$hl7$COLON${segment.seg}-auditIO-adhocAuditor", error(_: Throwable))) {
                        updateMetrics(segment.seg, PROCESSED)
                      } else adhocFailed()
                    }
                    else if (segment.dest.get.system == Destinations.HBASE) {
                      val jsonKV = fromJson(rec)
                      val key = segment.dest.get.offHeapConfig.get.key.foldLeft(EMPTYSTR)((a, b) => a + jsonKV.getOrElse(b, EMPTYSTR))
                      if (tryAndLogThr(hBaseIO(segment.dest.get.route).apply(segment.dest.get.offHeapConfig.get.family, key, jsonKV,true),
                        s"$hl7$COLON${segment.seg}-adhocIO", error(_: Throwable))) {
                        if (tryAndLogThr(auditIO(adhocAuditor(segment.auditKey, meta), header(hl7, auditHeader, Left(meta))),
                          s"$hl7$COLON${segment.seg}-auditIO-adhocAuditor", error(_: Throwable))) {
                          updateMetrics(segment.seg, PROCESSED)
                        } else adhocFailed()
                      } else adhocFailed()
                    } else {
                      if (tryAndLogThr(adhocIO(rec, header(hl7, segment.headerKey, Left(meta)), segment.dest.get.route),
                        s"$hl7$COLON${segment.seg}-adhocIO", error(_: Throwable))) {
                        if (tryAndLogThr(auditIO(adhocAuditor(segment.auditKey, meta), header(hl7, auditHeader, Left(meta))),
                          s"$hl7$COLON${segment.seg}-auditIO-adhocAuditor", error(_: Throwable))) {
                          updateMetrics(segment.seg, PROCESSED)
                        } else adhocFailed()
                      } else adhocFailed()
                    }
                  }
                  else {
                    if (tryAndLogThr(io(rec, header(hl7, s"$segmentsInHL7&${segment.seg}", Left(meta))), s"$hl7$COLON${segment.seg}-segmentsIO", error(_: Throwable))) {
                      if (tryAndLogThr(auditIO(segmentsAuditor(segment.seg, meta), header(hl7, auditHeader, Left(meta))),
                        s"$hl7$COLON${segment.seg}-auditIO-segmentsAuditor", error(_: Throwable))) {
                        updateMetrics(segment.seg, PROCESSED)
                      }
                    } else {
                      tryAndLogThr(rejectIO(rejectMsg(hl7, segment.seg, meta, " Writing Data to OUT Failed ", data),
                        header(hl7, rejectStage, Left(meta))),
                        s"$hl7$COLON${segment.seg}-segmentsIO-rejectIO", error(_: Throwable))
                      updateMetrics(segment.seg, FAILED)
                    }
                  }
              }
            } else {
              updateMetrics(segment.seg, FAILED)
              val msg = rejectMsg(hl7, segment.seg, meta, t.getMessage, data, t)
              sizeCheck(msg, segment.seg)
              tryAndLogThr(rejectIO(msg, header(hl7, rejectStage, Left(meta))), s"$hl7$COLON${segment.seg}-rejectIO-FAILED", error(_: Throwable))
              error(t.getMessage + " :: " + msg, t)
            }
            }
          case Right(det) =>
            det match {
              case `notValidStr` => updateMetrics(segment.seg, INVALID)
                val msg = rejectMsg(hl7, segment.seg, meta,
                  s" Invalid Input or Meta Data Required to process Doesn't Exist. Expected message Type $hl7 but Received message ${hl7Type(data)}", data)
                sizeCheck(msg, segment.seg)
                tryAndLogThr(rejectIO(msg, header(hl7, rejectStage, Left(meta))), s"$hl7$COLON${segment.seg}-rejectIO-$notValidStr", error(_: Throwable))
                error(s"Invalid Input Came :: $msg")
              case `NA` => updateMetrics(segment.seg, NOTAPPLICABLE)
              case _ =>
            }
        }
      case Failure(t) =>
        updateMetrics(segment.seg, FAILED)
        val msg = rejectMsg(hl7, segment.seg, meta, t.getMessage, data, t)
        sizeCheck(msg, segment.seg)
        tryAndLogThr(rejectIO(msg, header(hl7, rejectStage, Left(meta))), s"$hl7$COLON${segment.seg}-rejectIO-${t.getMessage}", error(_: Throwable))
        error(s"Applying $hl7 Model For Segment ${segment.seg} Failed ${t.getMessage} :: $msg", t)
    }
    }
    if (segRef.nonEmpty) tryAndLogThr(auditIO(segmentsInHl7Auditor(segmentsInMsg(allSegmentsForHl7, data), meta), header(hl7, auditHeader, Left(meta))),
      s"$hl7$COLON nonAdhocSegments-auditIO-segmentsInHl7Auditor", error(_: Throwable))
    if (tlmAckIO isDefined) {
      tlmAckMessages.foreach(msg => tlmAckIO.get(msg._1, msg._2))
      tlmAckIO.get(tlmAuditor(segmentsInHL7, meta), segmentsInHL7)
    }
  }

  private def run(segment: Segment, data: mapType, rawHl7: String) =
    async {
      segment apply(data, rawHl7)
    }(executionContext)


  private def tryForTaskExe[T](action: async[T]): Try[T] = Try(Await result(action, waitTillTaskCompletes))

  private def updateMetrics(seg: String, state: SegState): Unit = {
    val key = s"$hl7$COLON$seg$COLON$state"
    metrics get key match {
      case Some(stat) => metrics update(key, inc(stat))
      case _ => throw new DataModelException("Cannot Update Metrics Key not Found." + logIdent + " This should not Happen :: " + seg)
    }
  }


  override def handleSegments(io: (String, String) => Unit, rejectIO: (AnyRef, String) => Unit, auditIO: (String, String) => Unit,
                              adhocIO: (String, String, String) => Unit, tlmAckIO: Option[(String, String) => Unit] = None,
                              hBaseIO: Map[String, (String, String, mutable.Map[String, String],Boolean) => Unit])(data: mapType, rawHl7: String, meta: MSGMeta): Unit =
    runModel(io, rejectIO, auditIO, adhocIO, tlmAckIO, hBaseIO)(data, rawHl7, meta): Unit


  override def metricsRegistry: TrieMap[String, Long] = metrics


  override def resetMetrics: Boolean = {
    metrics.synchronized {
      metrics.transform({ case (_, met) => if (met != 0L) 0L else met })
    }
    true
  }


  override def toString: String = s"DataModelHandler($hl7, $metrics, $dataModeler)"

  private def overSizeMsgFound(seg: String): Unit = updateMetrics(seg, OVERSIZED)

  private def checkSize(threshold: Int)(data: AnyRef, seg: String): Unit = if (!IOCanHandle(data, threshold)) overSizeMsgFound(seg)

}
