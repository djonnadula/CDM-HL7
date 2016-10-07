package com.hca.cdm.job

import java.lang.System.{getenv => fromEnv}

import com.hca.cdm._
import com.hca.cdm.hadoop._
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.audit._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.{HL7, UNKNOWN, withName => hl7}
import com.hca.cdm.hl7.model._
import com.hca.cdm.hl7.parser.HL7Parser
import com.hca.cdm.job.exception.UnknownMessageTypeException
import com.hca.cdm.kafka.config.HL7ConsumerConfig.{createConfig => consumerConf}
import com.hca.cdm.kafka.config.HL7ProducerConfig.{createConfig => producerConf}
import com.hca.cdm.kafka.producer.{KafkaProducerHandler => KProducer}
import com.hca.cdm.kafka.util.TopicUtil.{createTopicIfNotExist => createTopic}
import com.hca.cdm.log.Logg
import com.hca.cdm.spark.{Hl7SparkUtil => sparkUtil}
import com.hca.cdm.utils.DateUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.AccumulatorParam.LongAccumulatorParam
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Accumulator, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * Created by Devaraj Jonnadula on 8/19/2016.
  */
object HL7Job extends Logg with App {

  private val config_file = args(0)
  private val fileSystem = FileSystem.get(new Configuration())
  private val appHomeDir = fileSystem.getHomeDirectory.toString
  private val stagingDir = fromEnv("SPARK_YARN_STAGING_DIR")
  private val it = fileSystem.listFiles(new Path(appHomeDir + FS + stagingDir), true)
  while (it.hasNext) info("File uploaded For Job :: " + it.next().getPath.toString)
  reload(config_file, Some(fileSystem.open(new Path(appHomeDir + FS + stagingDir + FS + config_file))))
  private val defaultPar = lookUpProp("hl7.spark.default.parallelism")
  private val batchCycle = lookUpProp("hl7.batch.interval").toInt
  printConfig()

  // ******************************************************** Spark Part ***********************************************
  private val rddLifeTime = Seconds(batchCycle + (batchCycle / 2))
  private val rddCheckPointInterval = Seconds(batchCycle)
  private val storage = StorageLevel.MEMORY_AND_DISK_SER_2
  private val sparkConf = sparkUtil getConf(lookUpProp("hl7.app"), defaultPar)
  private val sparkStrCtx: StreamingContext = sparkUtil getStreamingContext(batchCycle, sparkConf)
  private val context: SparkContext = sparkStrCtx.sparkContext
  context setLogLevel "INFO"
  context setJobDescription lookUpProp("job.desc")
  // ******************************************************** Job Part ***********************************************
  private var sHook: Thread = _
  private val app = lookUpProp("hl7.app")
  private val consumerGroup = lookUpProp("hl7.group")
  private val kafkaConsumerProp = (consumerConf(consumerGroup) asScala) toMap
  private val rejectedTopic = lookUpProp("hl7.reject")
  private val hl7JsonTopic = lookUpProp("hl7.json")
  private val segTopic = lookUpProp("hl7.segment")
  private val auditTopic = lookUpProp("hl7.audit")
  private val kafkaProducerConf = producerConf(hl7JsonTopic)
  private val messageTypes = lookUpProp("hl7.messages.type") split COMMA
  private val hl7MsgMeta = messageTypes.map(mtyp => hl7(mtyp) -> getMsgTypeMeta(hl7(mtyp), lookUpProp(mtyp + ".kafka.source"))) toMap
  private val topicsToSubscribe = hl7MsgMeta.map(hl7Type => hl7Type._2.kafka) toSet
  private val hl7TypesMapping = hl7MsgMeta.map(hl7Type => hl7Type._1.toString -> hl7Type._1)
  private val templatesMapping = loadTemplate(lookUpProp("hl7.template"))
  private val segmentsMapping = loadSegments(lookUpProp("hl7.segments"))
  private val modelsForHl7 = hl7MsgMeta.map(msgType => msgType._1 -> segmentsForHl7Type(msgType._1, segmentsMapping(msgType._1.toString)))
  private val registeredSegmentsForHl7 = modelsForHl7.mapValues(_.models.keySet)
  private val hl7Parsers = hl7MsgMeta map (hl7 => hl7._1 -> new HL7Parser(hl7._1, templatesMapping))
  private val jsonAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, jsonStage)(EMPTYSTR, _: MSGMeta)))
  private val segmentsAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, segmentStage)(_: String, _: MSGMeta)))
  private val adhocAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, adhocStage)(_: String, _: MSGMeta)))
  private val allSegmentsInHl7Auditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, segmentsInHL7)(_: String, _: MSGMeta)))
  private val segmentsHandler = modelsForHl7 map (hl7 => hl7._1 -> new DataModelHandler(hl7._2, registeredSegmentsForHl7(hl7._1), segmentsAuditor(hl7._1),
    allSegmentsInHl7Auditor(hl7._1), adhocAuditor(hl7._1)))
  private var segmentsAccumulators: TrieMap[String, Accumulator[Long]] = registerSegmentsMetric()
  private val segmentsDriverMetrics = driverSegmentsMetric()
  private var parserAccumulators: TrieMap[String, Accumulator[Long]] = registerParserMetric()
  private val parserDriverMetrics = driverParserMetric()
  initialise()

  private def initialise(): Unit = {
    info("Job Initialisation Started on :: " + DateUtil.currentTimeStamp)
    context setCheckpointDir lookUpProp("hl7.checkpoint")
    //sparkStrCtx checkpoint lookUpProp("hl7.checkpoint")
    info("Checkpoint Created :: " + context.getCheckpointDir)
    sparkStrCtx remember rddLifeTime
    sparkStrCtx.sparkContext addSparkListener new MetricsListener
    modelsForHl7.values foreach (segment => segment.models.values.foreach(models => models.foreach(model => {
      if (model.adhoc isDefined) createTopic(model.adhoc.get dest)
    })))
    createTopic(hl7JsonTopic, segmentPartitions = false)
    createTopic(segTopic, segmentPartitions = false)
    createTopic(auditTopic, segmentPartitions = false)
    createTopic(rejectedTopic, segmentPartitions = false)
    sHook = newThread(app + " " + consumerGroup + "SparkCtx SHook", runnable({
      close()
      info(currThread.getName + " Shutdown HOOK Completed for " + app)
    }))
    registerHook(sHook)
    info(" Initialisation Done. Opening Streams from Kafka")
    runJob()
  }

  private def runJob(): Unit = {
    val streamLine = sparkUtil stream(sparkStrCtx, kafkaConsumerProp, topicsToSubscribe)
    info("Kafka Stream Was Opened Successfully with ID :: " + streamLine.id)
    streamLine checkpoint rddCheckPointInterval foreachRDD (rdd => {
      rdd checkpoint()
      info("RDD Checkpoint Succeeded :: " + rdd.id + " :: " + rdd.isCheckpointed)
      //  rdd  persist storage
      rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach(range => info("Got an RDD from Topic :: "
        + range.topic + " , partition :: " + range.partition + " Offsets From :: " + range.fromOffset + " To :: " + range.untilOffset))
      info("Got RDD with Partitions :: " + rdd.partitions.length + " Executing Asynchronously Each of Them.")
      val parserS = hl7Parsers
      val segHandlers = segmentsHandler
      val jsonAudits = jsonAuditor
      val jsonOut = hl7JsonTopic
      val rejectOut = rejectedTopic
      val segOut = segTopic
      val auditOut = auditTopic
      val prodConf = kafkaProducerConf
      val hl7TypesMappings = hl7TypesMapping
      val segmentsAccumulators = this.segmentsAccumulators
      val parserAccumulators = this.parserAccumulators
      val maxMessageSize = lookUpProp("hl7.message.max") toInt
      val jsonOverSized = lookUpProp("hl7.direct.json")
      val segOverSized = lookUpProp("hl7.direct.segment")
      val rejectOverSized = lookUpProp("hl7.direct.reject")
      val adhocOverSized = lookUpProp("hl7.direct.adhoc")
      rdd foreachPartitionAsync (dataItr => {
        dataItr nonEmpty match {
          case true =>
            val kafkaOut = KProducer(multiDest = true)(prodConf)
            val hl7JsonIO = kafkaOut.writeData(_: String, _: String, jsonOut)(maxMessageSize, overSizedHandle(jsonStage, rdd.context, jsonOverSized)(_: AnyRef))
            val hl7RejIO = kafkaOut.writeData(_: String, _: String, rejectOut)(maxMessageSize, overSizedHandle(rejectStage, rdd.context, rejectOverSized)(_: AnyRef))
            val hl7SegIO = kafkaOut.writeData(_: String, _: String, segOut)(maxMessageSize, overSizedHandle(segmentStage, rdd.context, segOverSized)(_: AnyRef))
            val auditIO = kafkaOut.writeData(_: String, _: String, auditOut)(maxMessageSize, null)
            val adhocIO = kafkaOut.writeData(_: String, _: String, _: String)(maxMessageSize, overSizedHandle(adhocStage, rdd.context, adhocOverSized)(_: AnyRef))
            dataItr foreach (hl7 => {
              var msgType: HL7 = UNKNOWN
              try {
                val header = if (hl7._1 != null & hl7._1!= EMPTYSTR) hl7TypesMappings get (hl7._1 substring(0, hl7._1 indexOf COLON)) else None
                header match {
                  case Some(msgTyp) => msgType = msgTyp
                  case None => trace("Header Not Found in raw HL7 :: " + hl7._1 + " Trying To Find In registered HL7 Types")
                    val delim = if (hl7._2 contains "\r\n") "\r\n" else "\n"
                    val rawSplit = hl7._2 split delim
                    val reqMsgType = valid(rawSplit) match {
                      case true => val temp = PIPER split rawSplit(0)
                        valid(temp, 8) match {
                          case true => val hl7Type = temp(8) split("\\" + repeat, -1)
                            if (valid(hl7Type)) hl7Type(0)
                            else EMPTYSTR
                          case _ => EMPTYSTR
                        }
                      case _ => EMPTYSTR
                    }
                    if (reqMsgType != EMPTYSTR) msgType = if (hl7TypesMappings isDefinedAt reqMsgType) hl7TypesMappings(reqMsgType) else UNKNOWN
                    else hl7TypesMappings foreach (x => if (hl7._2 contains x._1) msgType = x._2)
                    trace("Able To Find Which Type of HL7 From Message :: " + msgType)
                    if (msgType == UNKNOWN) throw new UnknownMessageTypeException(hl7TypesMappings.keySet.mkString + " Registered Handlers Cannot Deal With Message Came Into Process :: " + msgType)
                }
                val hl7Str = msgType.toString
                val segHandlerIO = segHandlers(msgType).handleSegments(hl7SegIO, hl7RejIO, auditIO, adhocIO)(_, _)
                Try(parserS(msgType) transformHL7 hl7._2 rec) match {
                  case Success(data) => data match {
                    case Left(out) =>
                      segHandlerIO(out._2, out._3)
                      if (tryAndLogThr(hl7JsonIO(out._1, hl7Str + COLON + jsonStage), hl7Str + COLON + hl7JsonIOFun, error(_: Throwable))) {
                        tryAndLogThr(auditIO(jsonAudits(msgType)(out._3), hl7Str + COLON + jsonStage), hl7Str + COLON + hl7JsonAuditIOFun, error(_: Throwable))
                      }
                      else {
                        val msg = rejectMsg(hl7Str, jsonStage, out._3, " Writing Data to OUT Failed ", out._2)
                        tryAndLogThr(hl7RejIO(msg, hl7Str + COLON + jsonStage), hl7Str + COLON + "hl7RejIO-rejectMsg", error(_: Throwable))
                        error("Sending JSON to Kafka Failed :: " + msg)
                      }
                    case Right(t) =>
                      val msg = rejectRawMsg(hl7Str, jsonStage, hl7._2, " Running Task FAILED :: " + t.getMessage, t)
                      tryAndLogThr(hl7RejIO(msg, hl7Str + COLON + jsonStage), hl7Str + COLON + "hl7RejIO-rejectRawMsg" + t.getMessage, error(_: Throwable))
                      error("Parsing Raw HL7 Failed :: " + msg)
                  }
                  case Failure(t) =>
                    val msg = rejectRawMsg(hl7Str, jsonStage, hl7._2, t.getMessage, t)
                    tryAndLogThr(hl7RejIO(msg, hl7Str + COLON + jsonStage), hl7Str + COLON + "rejectRawMsg-" + t.getMessage, error(_: Throwable))
                    error("Parsing Raw HL7 Failed :: " + msg)
                }
              } catch {
                case t: Throwable => error(" Processing HL7 failed for Message  :: ", t)
                  val msg = rejectRawMsg(msgType.toString, jsonStage, hl7._2, " Handlers Cannot Deal With Message Came Into Process  :: " + t.getMessage, t)
                  tryAndLogThr(hl7RejIO(msg, msgType.toString + COLON + jsonStage), msgType.toString + COLON + "rejectRawMsg-" + t.getMessage, error(_: Throwable))
              }
            })
            this.segmentsAccumulators = segmentsAccumulators
            segHandlers.values foreach (handler => collectSegmentMetrics(handler metricsRegistry))
            this.parserAccumulators = parserAccumulators
            parserS.values foreach (parser => collectParserMetrics(parser metricsRegistry))
            closeResource(kafkaOut)
          case _ => info("Partition was Empty For RDD So skipping :: " + dataItr)
        }
      })
    })
    try {
      sparkStrCtx start()
      info("Started Spark Context Execution :: " + DateUtil.currentTimeStamp)
      sparkStrCtx awaitTermination()
    } catch {
      case t: Throwable => error("Spark Context Starting Failed ", t)
    } finally {
      close()
    }
  }

  private def close() = {
    sparkUtil shutdown sparkStrCtx
    info("sparkStrCtx Shutdown Completed for " + app)
    context stop()
    defaultSleep()
    info("Metrics For Segments Data Modeling :: ")
    segmentsDriverMetrics foreach (x => info(x._1 + " : " + x._2))
    println("*******************************************************************************************************")
    info("Metrics For Segments Data Modeling :: ")
    segmentsDriverMetrics foreach (x => info(x._1 + " : " + x._2))
    println("*******************************************************************************************************")
    fileSystem close()
    info("Shutdown Completed for System " + app)
    println("*****************************************END***********************************************************")
    unregister(sHook)
  }


  private def registerSegmentsMetric() = {
    val temp = new TrieMap[String, Accumulator[Long]]
    segmentsHandler.foreach(handler => handler._2.metricsRegistry.foreach({ case (k, v) => temp += k -> sparkStrCtx.sparkContext.accumulator(v, k)(LongAccumulatorParam) }))
    temp
  }

  private def registerParserMetric() = {
    val temp = new TrieMap[String, Accumulator[Long]]
    hl7Parsers.foreach(parser => parser._2.metricsRegistry.foreach({ case (k, v) => temp += k -> sparkStrCtx.sparkContext.accumulator(v, k)(LongAccumulatorParam) }))
    temp
  }

  private def driverSegmentsMetric() = {
    val temp = new TrieMap[String, Long]
    segmentsHandler.foreach(handler => temp ++= handler._2.metricsRegistry)
    temp
  }

  private def driverParserMetric() = {
    val temp = new TrieMap[String, Long]
    hl7Parsers.foreach(handler => temp ++= handler._2.metricsRegistry)
    temp
  }


  private def collectSegmentMetrics(taskMetrics: TrieMap[String, Long]) = segmentsAccumulators.synchronized {
    taskMetrics.foreach({ case (k, metric) => if (metric > 0L) segmentsAccumulators(k) += metric })
  }

  private def collectParserMetrics(taskMetrics: TrieMap[String, Long]) = parserAccumulators.synchronized {
    taskMetrics.foreach({ case (k, metric) => if (metric > 0L) parserAccumulators(k) += metric })
  }

  class MetricsListener extends SparkListener with StreamingListener with Logg {
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      super.onStageCompleted(stageCompleted)
      stageCompleted.stageInfo.failureReason match {
        case Some(x) => error(app + " Stage failed Due to  " + x)
        case _ =>
          stageCompleted.stageInfo.accumulables.foreach(acc => {
            val key = acc._2.name
            val reporter = (segmentsDriverMetrics isDefinedAt key, parserDriverMetrics isDefinedAt key) match {
              case (true, _) => segmentsDriverMetrics
              case (_, true) => parserDriverMetrics
              case _ => throw new RuntimeException("Accumulator Metric Should Exist :: " + key)
            }
            reporter update(key, inc(reporter(key), acc._2.value toLong))
          })
      }
    }

  }


}
