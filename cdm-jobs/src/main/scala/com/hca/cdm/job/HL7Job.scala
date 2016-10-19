package com.hca.cdm.job

import java.lang.System.{getenv => fromEnv}
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}
import java.util.concurrent.TimeUnit
import java.util.Date
import com.hca.cdm._
import com.hca.cdm.notification.{EVENT_TIME, sendMail => mail}
import com.hca.cdm.hadoop._
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.audit._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.{HL7, UNKNOWN, withName => hl7}
import com.hca.cdm.hl7.exception.UnknownMessageTypeException
import com.hca.cdm.hl7.model._
import com.hca.cdm.hl7.parser.HL7Parser
import com.hca.cdm.job.report.StatsReporter
import com.hca.cdm.kafka.config.HL7ConsumerConfig.{createConfig => consumerConf}
import com.hca.cdm.kafka.config.HL7ProducerConfig.{createConfig => producerConf}
import com.hca.cdm.kafka.producer.{KafkaProducerHandler => KProducer}
import com.hca.cdm.kafka.util.TopicUtil.{createTopicIfNotExist => createTopic}
import com.hca.cdm.log.Logg
import com.hca.cdm.notification.TaskState._
import com.hca.cdm.notification._
import com.hca.cdm.spark.{Hl7SparkUtil => sparkUtil}
import com.hca.cdm.utils.DateUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.AccumulatorParam.LongAccumulatorParam
import org.apache.spark.scheduler._
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Accumulator, FutureAction, SparkContext}
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * Created by Devaraj Jonnadula on 8/19/2016.
  *
  * Job which Assembles Functions to run on Spark Container for Transforming Raw HL7 to Output Formats Json...
  * and Breaks Data Into Segments level and Special Cases.
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
  private val sparkConf = sparkUtil getConf(lookUpProp("hl7.app"), defaultPar)
  private val sparkStrCtx: StreamingContext = sparkUtil getStreamingContext(batchCycle, sparkConf)
  private val context: SparkContext = sparkStrCtx.sparkContext
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
  private val maxMessageSize = lookUpProp("hl7.message.max") toInt
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
  private val monitorHandler = newDaemonScheduler(app + " Monitor-Pool")
  initialise()

  private def initialise(): Unit = {
    info("Job Initialisation Started on :: " + new Date())
    context setCheckpointDir lookUpProp("hl7.checkpoint")
    sparkStrCtx checkpoint lookUpProp("hl7.checkpoint")
    info("Checkpoint Created :: " + context.getCheckpointDir)
    //sparkStrCtx remember rddLifeTime
    sparkStrCtx.sparkContext addSparkListener new MetricsListener
    modelsForHl7.values foreach (segment => segment.models.values.foreach(models => models.foreach(model => {
      if (model.adhoc isDefined) createTopic(model.adhoc.get dest)
    })))
    createTopic(hl7JsonTopic, segmentPartitions = false)
    createTopic(segTopic, segmentPartitions = false)
    createTopic(auditTopic, segmentPartitions = false)
    createTopic(rejectedTopic, segmentPartitions = false)
    monitorHandler scheduleAtFixedRate(new StatsReporter(app), initDelay + 2, 86400, TimeUnit.SECONDS)
    sHook = newThread(app + " " + consumerGroup + "SparkCtx SHook", runnable({
      close()
      info(currThread.getName + " Shutdown HOOK Completed for " + app)
    }))
    registerHook(sHook)
    info(" Initialisation Done. Opening Streams from Kafka")
    runJob()
  }

  /**
    * Main Job Execution
    *
    * Executes Each RDD Partitions Asynchronously.
    */
  private def runJob(): Unit = {
    val streamLine = sparkUtil stream(sparkStrCtx, kafkaConsumerProp, topicsToSubscribe)
    info("Kafka Stream Was Opened Successfully with ID :: " + streamLine.id)
    streamLine checkpoint rddCheckPointInterval foreachRDD (rdd => {
      rdd checkpoint()
      info("RDD Checkpoint Succeeded :: " + rdd.id + " :: " + rdd.isCheckpointed)
      var messagesInRDD = 0L
      rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach(range => {
        info("Got an RDD from Topic :: "
          + range.topic + " , partition :: " + range.partition + " messages Count :: " + range.count + " Offsets From :: "
          + range.fromOffset + " To :: " + range.untilOffset)
        messagesInRDD = inc(messagesInRDD, range.count())
      })
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
      val maxMessageSize = this.maxMessageSize
      val jsonOverSized = OverSizeHandler(jsonStage, lookUpProp("hl7.direct.json"))
      val segOverSized = OverSizeHandler(segmentStage, lookUpProp("hl7.direct.segment"))
      val rejectOverSized = OverSizeHandler(rejectStage, lookUpProp("hl7.direct.reject"))
      val adhocOverSized = OverSizeHandler(adhocStage, lookUpProp("hl7.direct.adhoc"))
      val sizeCheck = checkSize(maxMessageSize)(_, _)
      val tracker = new ListBuffer[FutureAction[Unit]]
      tracker += rdd foreachPartitionAsync (dataItr => {
        dataItr nonEmpty match {
          case true =>
            val kafkaOut = KProducer(multiDest = true)(prodConf)
            val hl7JsonIO = kafkaOut.writeData(_: String, _: String, jsonOut)(maxMessageSize, jsonOverSized)
            val hl7RejIO = kafkaOut.writeData(_: String, _: String, rejectOut)(maxMessageSize, rejectOverSized)
            val hl7SegIO = kafkaOut.writeData(_: String, _: String, segOut)(maxMessageSize, segOverSized)
            val auditIO = kafkaOut.writeData(_: String, _: String, auditOut)(maxMessageSize)
            val adhocIO = kafkaOut.writeData(_: String, _: String, _: String)(maxMessageSize, adhocOverSized)
            dataItr foreach (hl7 => {
              var msgType: HL7 = UNKNOWN
              try {
                msgType = if (hl7._1 != null && hl7._1 != EMPTYSTR) hl7TypesMappings getOrElse(hl7._1 substring(0, hl7._1 indexOf COLON), UNKNOWN) else UNKNOWN
                msgType != UNKNOWN match {
                  case true =>
                  case _ => info("Message Type Header Not Found in raw HL7 :: " + hl7._1 + " Trying To Find In registered HL7 Types")
                    val delim = if (hl7._2 contains "\r\n") "\r\n" else "\n"
                    val rawSplit = hl7._2 split delim
                    val reqMsgType = valid(rawSplit) match {
                      case true => val temp = PIPER split rawSplit(0)
                        valid(temp, 8) match {
                          case true =>
                            val hl7Type = temp(8) contains repeat match {
                              case true => (temp(8) split("\\" + repeat, -1)) (0)
                              case _ => temp(8)
                            }
                            if (valid(hl7Type)) hl7Type
                            else EMPTYSTR
                          case _ => EMPTYSTR
                        }
                      case _ => EMPTYSTR
                    }
                    if (reqMsgType != EMPTYSTR) msgType = hl7TypesMappings getOrElse(reqMsgType, UNKNOWN)
                    if (msgType == UNKNOWN) throw new UnknownMessageTypeException(hl7TypesMappings.keySet.mkString(repeat) +
                      " Registered Handlers Cannot Deal With Message Came Into Process :: " + reqMsgType + " for " + hl7._2)
                    else info("Able To Find Which Type of HL7 From Message :: " + msgType)
                }
                val hl7Str = msgType.toString
                val segHandlerIO = segHandlers(msgType).handleSegments(hl7SegIO, hl7RejIO, auditIO, adhocIO)(_, _)
                Try(parserS(msgType) transformHL7 hl7._2 rec) match {
                  case Success(data) => data match {
                    case Left(out) =>
                      segHandlerIO(out._2, out._3)
                      sizeCheck(out._1, parserS(msgType))
                      if (tryAndLogThr(hl7JsonIO(out._1, hl7Str + COLON + jsonStage), hl7Str + COLON + hl7JsonIOFun, error(_: Throwable))) {
                        tryAndLogThr(auditIO(jsonAudits(msgType)(out._3), hl7Str + COLON + jsonStage), hl7Str + COLON + hl7JsonAuditIOFun, error(_: Throwable))
                      }
                      else {
                        val msg = rejectMsg(hl7Str, jsonStage, out._3, " Writing Data to OUT Failed ", out._2)
                        tryAndLogThr(hl7RejIO(msg, hl7Str + COLON + jsonStage), hl7Str + COLON + "hl7RejIO-rejectMsg", error(_: Throwable))
                        error("Sending JSON to Kafka Failed :: " + msg)
                      }
                    case Right(t) =>
                      val msg = rejectRawMsg(hl7Str, jsonStage, hl7._2, t.getMessage, t)
                      if (msgType != UNKNOWN) sizeCheck(msg, parserS(msgType))
                      tryAndLogThr(hl7RejIO(msg, hl7Str + COLON + jsonStage), hl7Str + COLON + "hl7RejIO-rejectRawMsg" + t.getMessage, error(_: Throwable))
                      error("Parsing Raw HL7 Failed :: " + msg)
                  }
                  case Failure(t) =>
                    val msg = rejectRawMsg(hl7Str, jsonStage, hl7._2, t.getMessage, t)
                    if (msgType != UNKNOWN) sizeCheck(msg, parserS(msgType))
                    tryAndLogThr(hl7RejIO(msg, hl7Str + COLON + jsonStage), hl7Str + COLON + "rejectRawMsg-" + t.getMessage, error(_: Throwable))
                    error("Parsing Raw HL7 Failed :: " + msg)
                }
              } catch {
                case t: Throwable => error(" Processing HL7 failed for Message  :: ", t)
                  val msg = rejectRawMsg(msgType.toString, jsonStage, hl7._2, t.getMessage, t)
                  if (msgType != UNKNOWN) sizeCheck(msg, parserS(msgType))
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
      tracker.foreach(_.get())
      info("Processing Completed for RDD :: " + rdd.id + " Messages Count :: " + messagesInRDD)
    })
    startStreams()
  }

  /**
    * Starts Spark Streaming
    */
  private def startStreams() = {
    try {
      sparkStrCtx start()
      info("Started Spark Streaming Context Execution :: " + DateUtil.currentTimeStamp)
      sparkStrCtx awaitTermination()
    } catch {
      case t: Throwable => error("Spark Context Starting Failed ", t)
    } finally {
      close()
    }
  }

  /**
    * Close All Resources
    */
  private def close() = {
    sparkUtil shutdown sparkStrCtx
    info("sparkStrCtx Shutdown Completed for " + app)
    context stop()
    defaultSleep()
    info("Metrics For Segments Data Modeling :: ")
    segmentsDriverMetrics foreach (x => info(x._1 + " : " + x._2))
    println("*******************************************************************************************************")
    info("Metrics For Hl7 Messages :: ")
    parserDriverMetrics foreach (x => info(x._1 + " : " + x._2))
    println("*******************************************************************************************************")
    monitorHandler shutdown()
    monitorHandler awaitTermination(1, TimeUnit.HOURS)
    fileSystem close()
    info("Shutdown Completed for System " + app)
    println("*****************************************END***********************************************************")
    unregister(sHook)
  }

  /**
    * Seconds from currents point to next Day starting
    */
  private def initDelay = LocalDateTime.now().until(LocalDate.now().plusDays(1).atStartOfDay(), ChronoUnit.SECONDS)

  private def checkSize(threshold: Int)(data: AnyRef, parser: HL7Parser) = if (!IOCanHandle(data, threshold)) parser.overSizeMsgFound()

  /**
    * Creates Accumulator for HL7 Segments Tasks
    */
  private def registerSegmentsMetric() = {
    val temp = new TrieMap[String, Accumulator[Long]]
    segmentsHandler.foreach(handler => handler._2.metricsRegistry.foreach({ case (k, v) => temp += k -> sparkStrCtx.sparkContext.accumulator(v, k)(LongAccumulatorParam) }))
    temp
  }

  /**
    * Reset Metrics for Past Day when report is Generated at EOD
    */
  def resetMetrics(): Unit = {
    parserDriverMetrics.synchronized {
      parserDriverMetrics.transform((k, v) => if (v > 0L) 0L else v)
    }
    segmentsDriverMetrics.synchronized {
      segmentsDriverMetrics.transform((k, v) => if (v > 0L) 0L else v)
    }
    parserAccumulators.synchronized {
      parserAccumulators.transform((k, v) => if (v.value > 0L) {
        v.value_=(0L)
        v
      } else v)
    }
    segmentsAccumulators.synchronized {
      segmentsAccumulators.transform((k, v) => if (v.value > 0L) {
        v.value_=(0L)
        v
      } else v)
    }
  }

  /**
    * Parsing Metrics Collected from all Jobs
    */
  def parserMetrics: Map[String, Long] = parserDriverMetrics.synchronized {
    parserDriverMetrics toMap
  }

  /**
    * Segments Metrics Collected from all Jobs
    */
  def segmentMetrics: Map[String, Long] = segmentsDriverMetrics.synchronized {
    segmentsDriverMetrics toMap
  }

  /**
    * Creates Accumulator for Parsing Tasks
    */
  private def registerParserMetric() = {
    val temp = new TrieMap[String, Accumulator[Long]]
    hl7Parsers.foreach(parser => parser._2.metricsRegistry.foreach({ case (k, v) => temp += k -> sparkStrCtx.sparkContext.accumulator(v, k)(LongAccumulatorParam) }))
    temp
  }

  /**
    * Creates Metrics Registry for Segments Jobs that can be Collected at Driver
    */

  private def driverSegmentsMetric() = {
    val temp = new TrieMap[String, Long]
    segmentsHandler.foreach(handler => temp ++= handler._2.metricsRegistry)
    temp
  }

  /**
    * Creates Metrics Registry for Parsing Jobs that can be Collected at Driver
    */
  private def driverParserMetric() = {
    val temp = new TrieMap[String, Long]
    hl7Parsers.foreach(handler => temp ++= handler._2.metricsRegistry)
    temp
  }

  /**
    * Collects Segments Metrics from Tasks Completed in Driver
    */
  private def collectSegmentMetrics(taskMetrics: TrieMap[String, Long]) = segmentsAccumulators.synchronized {
    taskMetrics.foreach({ case (k, metric) =>
      if (metric > 0L) segmentsAccumulators(k) += metric
    })
  }

  /**
    * Collects Parser Metrics from Tasks Completed in Driver
    */
  private def collectParserMetrics(taskMetrics: TrieMap[String, Long]) = parserAccumulators.synchronized {
    taskMetrics.foreach({ case (k, metric) => if (metric > 0L) parserAccumulators(k) += metric })
  }

  /**
    * Listener For Spark Events
    *
    */
  private class MetricsListener extends SparkListener with StreamingListener with Logg {
    val stageTracker = new TrieMap[Int,StageInfo]()
    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      super.onStageSubmitted(stageSubmitted)
      stageTracker += stageSubmitted.stageInfo.stageId -> stageSubmitted.stageInfo
      if(stageTracker.size > 20) {
        if(context.requestExecutors(2)) stageTracker.clear()
      }

    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      super.onStageCompleted(stageCompleted)
      stageCompleted.stageInfo.failureReason match {
        case Some(x) => error(app + " Stage failed Due to  " + x)
        case _ =>
          stageTracker.remove(stageCompleted.stageInfo.stageId)
          stageCompleted.stageInfo.accumulables.foreach(acc => {
            val key = acc._2.name
            val reporter = (segmentsDriverMetrics isDefinedAt key, parserDriverMetrics isDefinedAt key) match {
              case (true, _) => segmentsDriverMetrics
              case (_, true) => parserDriverMetrics
              case _ => throw new RuntimeException("Accumulator Metric Should Exist :: " + key)
            }
            reporter update(key, acc._2.value toLong)
          })
      }
    }

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      super.onApplicationEnd(applicationEnd)
      mail("{encrypt} " + app + " with Job ID " + context.applicationId + " Ended",
        app + " Job in Critical State. This Should Not Happen for this Application. Some one has to Check Immediately What was happening with Job ID :: " + context.applicationId + " \n\n" + EVENT_TIME
        , CRITICAL)
    }
  }


}

