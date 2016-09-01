package com.hca.cdm.job

import com.hca.cdm._
import com.hca.cdm.hl7.constants.HL7Types.{HL7, withName => hl7}
import com.hca.cdm.hl7.model.{DataModelHandler, _}
import com.hca.cdm.hl7.parser.HL7Parser
import com.hca.cdm.kafka.config.HL7ConsumerConfig.{createConfig => conf}
import com.hca.cdm.kafka.producer.{KafkaProducerHandler => KProducer}
import com.hca.cdm.log.Logg
import com.hca.cdm.spark.config.{Hl7SparkUtil => sp}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
  * Created by Devaraj Jonnadula on 8/19/2016.
  */
object HL7Job extends App with Logg {

  reload()
  private val app = loopUpProp("hl7.app")
  private val appHome = loopUpProp("hl7.app.home")
  private val rejectedTopic = loopUpProp("hl7.reject")
  private val segmentsReject = loopUpProp("hl7.segment.reject")
  private val hl7JsonTopic = loopUpProp("hl7.json")
  private val segTopic = loopUpProp("hl7.segment")
  private val consumerGroup = loopUpProp("hl7.group")
  private val batchCycle = loopUpProp("hl7.batch.interval")
  private val defaultPar = loopUpProp("hl7.spark.default.parallelism")
  private val checkPointHome = loopUpProp("hl7.checkpoint")
  private val hl7MsgMeta = loopUpProp("hl7.messages.type").split(",").toList.map(mtyp => getMsgTypeMeta(hl7(mtyp)))
  private val templatesMapping = hl7MsgMeta.map(msgType => msgType.msgType -> loadTemplate(msgType)).toMap
  private val modelsForHl7 = hl7MsgMeta.map(msgType => msgType.msgType -> segmentsForHl7Type(msgType.msgType, loadSegments(msgType))).toMap
  private val hl7_spark_master = loopUpProp("hl7.spark.master")
  private val hl7IO = KProducer(hl7JsonTopic)
  private val segmentsIO = KProducer(segTopic)
  private val hl7Json = hl7IO.writeData(_: Any, _: Any, hl7JsonTopic)
  private val hl7JsonRejIO = hl7IO.writeData(_: Any, _: Any, rejectedTopic)
  private val hl7SegIO = segmentsIO.writeData(_: Any, _: Any, segTopic)
  private val hl7SegRejIO = segmentsIO.writeData(_: Any, _: Any, segmentsReject)
  private var sparkConf: SparkConf = _

  private var sparkStrCtx: StreamingContext = _

  private val checkPointInterval = Seconds(5)
  private val kafkaConsumerProp = conf(consumerGroup).asScala
  private val hl7Parsers = templatesMapping.map(hl7 => hl7._1 -> new HL7Parser(hl7._2))
  private val segmentsModeler = modelsForHl7.map(x => x._1 -> new DataModelHandler(x._2, hl7SegIO, hl7SegRejIO))
  implicit val ec = ExecutionContext.fromExecutor(newDaemonCachedThreadPool(getClass.getName))
  private val segmentsMetrics = {
    val temp = new mutable.HashMap[HL7, SegmentsHandler]
    segmentsModeler.foreach(x => temp += x)
    temp
  }


  def runJobsAsync() = {

    sparkConf = sp.getConf(app, defaultPar)
    sparkConf.setMaster(hl7_spark_master)
    sparkStrCtx = sp.getStreamingContext(batchCycle, sparkConf)
    registerHook(newThread(app + consumerGroup + "SparkCtx SHook", runnable({
      sparkStrCtx.stop(stopSparkContext = true, stopGracefully = false)
      info(Thread.currentThread().getName + " Shutdown Completed for HL7 Jobs")
    })))
    val trackers = new ListBuffer[Future[MsgTypeMeta]]

    hl7MsgMeta.foreach(hl7Meta => {
      trackers += runJob(hl7Meta, hl7Meta.msgType.toString)
    })
    sparkStrCtx.start()
    sparkStrCtx.awaitTermination()

    trackers.foreach(track => {
      track onComplete {
        case Success(meta) => info(" Job Succeeded for HL7 Message Types :: " + meta.msgType.toString)
        case Failure(t) => error(" Job Failed for HL7 Message Type ", t)
      }
    })
    shutdown(sparkStrCtx)
    info("Shutdown Completed in runJob")
    closeResource(hl7IO)
    closeResource(segmentsIO)
  }


  private def runJob(hl7Meta: MsgTypeMeta, hl7Str: String) = {
    Future {
      try {
        val streamLine = sp.stream(sparkStrCtx, kafkaConsumerProp.toMap, hl7Meta.kafka)
        streamLine.foreachRDD(rdd => {
          rdd.foreachPartitionAsync(hlsItr => {
            hlsItr.foreach(hl7 => {
              hl7Parsers(hl7Meta.msgType).transformHL7(hl7._2).rec match {
                case Left(out) => hl7Json(out._1, hl7Str)
                  segmentsModeler(hl7Meta.msgType).handleSegments(out._2)
                case Right(t) => hl7JsonRejIO(hl7._2, hl7Str + ":" + t.toString)
              }
            })
          })
        })

      } catch {
        case t: Throwable => error(" HL7 Process failed for Message Type :: " + hl7Meta.msgType, t)
      } finally {
      }
      hl7Meta
    }(ec)

  }

  private def shutdown(sparkStrCtx: StreamingContext) = {
    if (sparkStrCtx != null) {
      sparkStrCtx.stop(stopSparkContext = false, stopGracefully = true)
    }
  }

  private def getMsgTypeMeta(msgType: HL7) = {
    val msgTypeStr = msgType.toString
    MsgTypeMeta(msgType, appHome, loopUpProp(msgTypeStr + ".template"), loopUpProp(msgTypeStr + ".segments"),
      loopUpProp(msgTypeStr + ".kafka.source"))
  }

  runJobsAsync()

  /*

      /*private def registerSegmentsMetric(context: SparkContext, handler: SegmentsHandler) = {
        handler.metricsRegistry.map(met => {
          met._2.map(state => {
           val c = context.accumulable(0L, met._1 + ":" + state._1.toString)
          })
        })
      }*/




    }
  */

}
