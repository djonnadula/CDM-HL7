package com.hca.cdm.hl7.model

import java.util.concurrent.{RejectedExecutionHandler, ThreadPoolExecutor, TimeUnit}

import com.hca.cdm._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.model.SegmentsState.SegState
import com.hca.cdm.log.Logg

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future => async}
import scala.util.{Failure, Success, Try}

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
class DataModelHandler(hl7Segments: Hl7Segments, io: (String, String) => Unit, rejectIO: (String, String) => Unit)
  extends Logg with SegmentsHandler {
  logIdent = hl7Segments.msgType.toString + " Model Handle "
  private val pool = {
    val t = newDaemonCachedThreadPool(logIdent)
    t.setMaximumPoolSize(60)
    t.setRejectedExecutionHandler(new PoolFullHandler)
    t
  }
  private implicit val ec = ExecutionContext.fromExecutor(pool, logEx)

  private lazy val metrics = new mutable.HashMap[String, mutable.HashMap[SegState, Long]]
  private lazy val dataModeler = DataModeler(hl7Segments.msgType)
  private lazy val segRef = {
    val temp = new mutable.ArrayBuffer[(String, (mapType) => Traversable[Hl7SegmentTrans])]()
    hl7Segments.models.foreach({ case (seg, models) =>
      metrics += seg -> initSegStateWithZero
      models.foreach(model => {
        temp += (seg, dataModeler.applyModel(seg, model)(_: mapType))
      })
    })
    temp
  }

  private def logEx(t: Throwable): Unit = error("Unable to Execute Task ", t)

  private def runModel(data: mapType): Unit = {
    segRef.foreach(seg => {
      run(seg, data) onComplete {
        case Success(trans) => trans.foreach(tOut => {
          tOut.seg match {
            case Left(out) =>
              if (tryAndLogErrorMes(io(out, hl7Segments.msgType.toString + ":" + seg._1), error(_: String))) updateMetrics(seg._1, SegmentsState.PROCESSED)
            case Right(det) => det._1 match {
              case Some(notValid) =>
                notValid match {
                  case x: String => if (x == skippedStr) updateMetrics(seg._1, SegmentsState.SKIPPED)
                  else if (x == notValidStr) updateMetrics(seg._1, SegmentsState.INVALID)
                  case _ =>
                }
              case None => updateMetrics(seg._1, SegmentsState.FAILED)
                error(" Data Segmentation Failed for segment :: " + seg._1, det._2)

            }
          }
        })

        case Failure(t) =>
          rejectIO(seg._1, t.getStackTrace.mkString(","))
          t.printStackTrace()
          error("Running Task for  Data Segmentation Failed for segment :: " + seg._2, t)
      }
    })

  }

  private def run(in: (String, (mapType) => Traversable[Hl7SegmentTrans]), data: mapType) =
    async {
      in._2(data)
    }(ec)


  private def updateMetrics(seg: String, state: SegState) = {
    metrics.get(seg) match {
      case Some(stat) => stat update(state, inc(stat(state)))
      case _ =>
    }
  }


  override def handleSegments(data: mapType): Unit = runModel(data)

  override def metricsRegistry: mutable.HashMap[String, mutable.HashMap[SegState, Long]] = this.metrics


  override def resetMetrics: Boolean = {
    this.metrics.synchronized {
      this.metrics.foreach(seg => seg._2.transform((k, v) => if (v != 0L) 0L else v))
    }
    true
  }


  override def shutDown(): Unit = {
    pool.shutdown()
    pool.awaitTermination(5, TimeUnit.MINUTES)
    info(" Shutdown Completed Gracefully for " + logIdent)
  }

  override def printStats(): Unit = {
    info("Total Tasks Completed So far        :: " + pool.getCompletedTaskCount)
    info("Total Tasks Currently Executing     :: " + pool.getActiveCount)
    info("Total Tasks Scheduled for Execution :: " + pool.getTaskCount)
  }

  private class PoolFullHandler extends RejectedExecutionHandler {
    override def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor): Unit = {
      debug(" Task Cannot Execute and Trying to Run again from Pool :: " + executor)
      executor.purge()
      sleep(300)
      executor.submit(r)
    }
  }

}
