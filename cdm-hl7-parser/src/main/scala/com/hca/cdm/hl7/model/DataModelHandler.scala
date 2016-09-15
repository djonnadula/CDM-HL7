package com.hca.cdm.hl7.model

import java.util.concurrent.{RejectedExecutionHandler, ThreadPoolExecutor, TimeUnit}

import com.hca.cdm._
import com.hca.cdm.hl7.audit._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.model.SegmentsState._
import com.hca.cdm.log.Logg

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future => async}
import scala.util.{Failure, Success}

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
class DataModelHandler(hl7Segments: Hl7Segments, allSegmentsForHl7: Set[String], segmentsAuditor: (String, MSGMeta) => String, segmentsInHl7Auditor: (String, MSGMeta) => String,
                       adhocAuditor: (String, MSGMeta) => String)
                      (io: (String, String) => Unit, rejectIO: (String, String) => Unit, auditIO: (String, String) => Unit, adhocIO: (String, String, String) => Unit)
  extends Logg with SegmentsHandler {

  private val hl7 = hl7Segments.msgType.toString
  outStream.println(
    hl7 + " " +
      """ Handler Registered For Modeling ::
      ____
    HL / /    DATA
      / /
     / /
    /_/
      """)
  logIdent = hl7 + " Model Handler "
  private val pool = {
    val t = newDaemonCachedThreadPool(logIdent)
    t.setMaximumPoolSize(60)
    t.setRejectedExecutionHandler(new PoolFullHandler)
    t
  }
  private implicit val ec = ExecutionContext.fromExecutor(pool, logEx)

  private case class Segment(seg: String, apply: (mapType) => Hl7SegmentTrans, adhoc: Boolean, dest: String = EMPTYSTR, auditKey: String)

  private lazy val metrics = new mutable.HashMap[String, mutable.HashMap[SegState, Long]]
  private lazy val dataModeler = DataModeler(hl7Segments.msgType)
  private lazy val segRef = {
    val temp = new mutable.ArrayBuffer[Segment]
    hl7Segments.models.foreach({ case (seg, models) =>
      metrics += hl7 + COLON + seg -> initSegStateWithZero
      models.foreach(model => {
        temp += Segment(seg, dataModeler.applyModel(seg, model)(_: mapType), model.adhoc.isDefined, if (model.adhoc.isDefined) model.adhoc.get.dest else EMPTYSTR,
          if (model.adhoc.isDefined) seg substring ((seg indexOf COLON) + 1) else EMPTYSTR)
      })
    })
    temp
  }

  private def logEx(t: Throwable): Unit = error("Unable to Execute Task ", t)

  private def runModel(data: mapType, meta: MSGMeta): Unit = {
    segRef.foreach(segment => {
      run(segment, data) onComplete {
        case Success(transaction) => transaction.trans match {
          case Left(out) =>
            out foreach { case (rec, t) => rec ne null match {
              case true =>
                rec match {
                  case `skippedStr` =>
                    updateMetrics(segment.seg, SKIPPED)
                    val msg = rejectMsg(hl7, segment.seg, meta, skippedStr, data)
                    rejectIO(msg, hl7 + COLON + segment.seg)
                    error("Segment Skipped :: " + msg)
                  case _ =>
                    segment.adhoc match {
                      case true => if (tryAndLogErrorMes(adhocIO(rec, hl7 + COLON + segment.seg, segment.dest), error(_: String))) {
                        auditIO(adhocAuditor(segment.auditKey, meta), hl7 + COLON + segment.seg)
                        updateMetrics(segment.seg, PROCESSED)
                      } else {
                        rejectIO(rejectMsg(hl7, segment.seg, meta, " Writing Data to OUT Failed ", data), hl7 + COLON + segment.seg)
                        updateMetrics(segment.seg, FAILED)
                      }
                      case _ =>
                        if (tryAndLogErrorMes(io(rec, hl7 + COLON + segment.seg), error(_: String))) {
                          auditIO(segmentsAuditor(segment.seg, meta), hl7 + COLON + segment.seg)
                          updateMetrics(segment.seg, PROCESSED)
                        } else {
                          rejectIO(rejectMsg(hl7, segment.seg, meta, " Writing Data to OUT Failed ", data), hl7 + COLON + segment.seg)
                          updateMetrics(segment.seg, FAILED)
                        }
                    }
                }
              case _ =>
                updateMetrics(segment.seg, FAILED)
                val msg = rejectMsg(hl7, segment.seg, meta, " FAILED ", data, t)
                rejectIO(msg, hl7 + COLON + segment.seg)
                error(" Data Segmentation Failed  :: " + msg, t)
            }
            }
          case Right(det) =>
            det match {
              case `notValidStr` => updateMetrics(segment.seg, INVALID)
                val msg = rejectMsg(hl7, segment.seg, meta, " Invalid Input or Meta Data Required to process Doesn't Exist", data)
                rejectIO(msg, hl7 + COLON + segment.seg)
                error("Invalid Input Came :: " + msg)
              case `NA` => updateMetrics(segment.seg, NOTAPPLICABLE)
              case _ =>
            }
        }
        case Failure(t) =>
          updateMetrics(segment.seg, FAILED)
          val msg = rejectMsg(hl7, segment.seg, meta, " Running Task FAILED ", data, t)
          rejectIO(msg, hl7 + COLON + segment.seg)
          error("Running Task FAILED :: " + msg, t)
      }
    })
    if (segRef.nonEmpty) auditIO(segmentsInHl7Auditor(segmentsInMsg(allSegmentsForHl7, data), meta), hl7)
  }

  private def run(segment: Segment, data: mapType) =
    async {
      segment apply data
    }(ec)


  private def updateMetrics(seg: String, state: SegState) = {
    metrics.get(hl7 + COLON + seg) match {
      case Some(stat) => stat update(state, inc(stat(state)))
      case _ => throw new DataModelException("Cannot Update Metrics Key not Found. This should not Happen :: " + seg)
    }
  }


  override def handleSegments(data: mapType, meta: MSGMeta): Unit = runModel(data, meta)

  override def metricsRegistry: mutable.HashMap[String, mutable.HashMap[SegState, Long]] = this.metrics


  override def resetMetrics: Boolean = {
    this.metrics.synchronized {
      this.metrics.foreach(seg => seg._2.transform((k, v) => if (v != 0L) 0L else v))
    }
    true
  }


  override def shutDown(): Unit = {
    pool.shutdown()
    pool.awaitTermination(10, TimeUnit.MINUTES)
    info(" Shutdown Completed Gracefully for " + logIdent)
  }

  override def printStats(): Unit = {
    info("Total Tasks Completed So far        :: " + pool.getCompletedTaskCount)
    info("Total Tasks Currently Executing     :: " + pool.getActiveCount)
    info("Total Tasks Scheduled for Execution :: " + pool.getTaskCount)
  }

  private class PoolFullHandler extends RejectedExecutionHandler {

    private val failureTracker = new mutable.HashMap[Runnable, Int]()

    override def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor): Unit = {
      debug(" Task Cannot Execute and Trying to Run again from Pool :: " + executor)
      executor.purge()
      if (!failureTracker.isDefinedAt(r)) failureTracker += r -> 1
      else failureTracker(r) match {
        case x => if (x < 10) {
          failureTracker update(r, x + 1)
          sleep(500)
          executor.submit(r)
        }
        else if (x > 20) failureTracker remove r
        else {
          sleep(1500)
          executor.submit(r)
          failureTracker update(r, x + 1)
        }
      }

    }
  }

  override def tasksPending: Boolean = pool.synchronized {
    pool.getActiveCount == 0 & pool.getTaskCount == 0
  }

  override def canExecuteMore: Boolean = pool.synchronized {
    !pool.isShutdown
  }
}
