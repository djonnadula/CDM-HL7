package com.hca.cdm.job

import java.lang.System.{getenv => fromEnv}
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.Date
import java.util.concurrent.atomic.AtomicBoolean
import LocalDateTime._
import com.hca.cdm.Models.MSGMeta
import com.hca.cdm.io.IOConstants._
import com.hca.cdm._
import com.hca.cdm.notification.{EVENT_TIME, sendMail => mail}
import com.hca.cdm.hadoop._
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.audit._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.{HL7, IPLORU, ORU, UNKNOWN, withName => hl7}
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
import com.hca.cdm.spark.{Hl7SparkUtil => sparkUtil}
import com.hca.cdm.utils.RetryHandler
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.AccumulatorParam.LongAccumulatorParam
import org.apache.spark.scheduler._
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{Accumulator, FutureAction}
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import org.apache.spark.deploy.SparkHadoopUtil.{get => hdpUtil}

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
  private val maxPartitions = defaultPar.toInt * lookUpProp("hl7.spark.dynamicAllocation.maxExecutors").toInt
  private val batchDuration = sparkUtil batchCycle(lookUpProp("hl7.batch.time.unit"), batchCycle)
  printConfig()


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
  private val segmentsMapping = applySegmentsToAll(loadSegments(lookUpProp("hl7.segments")), messageTypes)
  private val modelsForHl7 = hl7MsgMeta.map(msgType => msgType._1 -> segmentsForHl7Type(msgType._1, segmentsMapping(msgType._1.toString)))
  private val registeredSegmentsForHl7 = modelsForHl7.mapValues(_.models.keySet)
  private val hl7Parsers = hl7MsgMeta map (hl7 => hl7._1 -> new HL7Parser(hl7._1, templatesMapping))
  private val jsonAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, jsonStage)(EMPTYSTR, _: MSGMeta)))
  private val segmentsAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, segmentStage)(_: String, _: MSGMeta)))
  private val adhocAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, adhocStage)(_: String, _: MSGMeta)))
  private val allSegmentsInHl7Auditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, segmentsInHL7)(_: String, _: MSGMeta)))
  private val segmentsHandler = modelsForHl7 map (hl7 => hl7._1 -> new DataModelHandler(hl7._2, registeredSegmentsForHl7(hl7._1), segmentsAuditor(hl7._1),
    allSegmentsInHl7Auditor(hl7._1), adhocAuditor(hl7._1)))
  private val ensureStageCompleted = new AtomicBoolean(false)
  private var runningStage: StageInfo = _
  private val jsonOverSized = OverSizeHandler(jsonStage, lookUpProp("hl7.direct.json"))
  private val segOverSized = OverSizeHandler(segmentStage, lookUpProp("hl7.direct.segment"))
  private val rejectOverSized = OverSizeHandler(rejectStage, lookUpProp("hl7.direct.reject"))
  private val adhocOverSized = OverSizeHandler(adhocStage, lookUpProp("hl7.direct.adhoc"))
  private val sourceHl7Mapping = hl7MsgMeta.map(hl7 => hl7._2.kafka -> hl7._1)
  private val msgTypeFreq = {
    val temp = new TrieMap[String, (HL7, Long)]
    hl7MsgMeta.foreach({ case (k, v) => temp += v.kafka -> (k, 0L) })
    temp
  }
  private var segmentsAccumulators: TrieMap[String, Accumulator[Long]] = _
  private var segmentsDriverMetrics: TrieMap[String, Long] = _
  private var parserAccumulators: TrieMap[String, Accumulator[Long]] = _
  private var parserDriverMetrics: TrieMap[String, Long] = _
  private var monitorHandler: ScheduledExecutorService = _
  private val lowFrequencyHL7 = {
    val temp = new TrieMap[HL7, Int]
    lookUpProp("hl7.messages.low.frequency").split(COMMA).foreach(x => {
      temp += hl7(x) -> 0
    })
    temp
  }

  // ******************************************************** Spark Part ***********************************************
  private val checkPoint = lookUpProp("hl7.checkpoint")
  private val sparkConf = sparkUtil.getConf(lookUpProp("hl7.app"), defaultPar)
  private val hdpConf = hdpUtil.conf
  private val restoreFromChk = new AtomicBoolean(true)
  private val newCtxIfNotExist = new (() => StreamingContext) {
    override def apply(): StreamingContext = {
      val ctx = sparkUtil createStreamingContext(sparkConf, batchDuration)
      restoreFromChk set false
      ctx
    }
  }
  private val sparkStrCtx: StreamingContext = sparkUtil streamingContext(checkPoint, newCtxIfNotExist)
  sparkStrCtx.sparkContext setJobDescription lookUpProp("job.desc")
  initialise(sparkStrCtx)
  startStreams()

  private def initialise(sparkStrCtx: StreamingContext): Unit = {
    info("Job Initialisation Started on :: " + new Date())
    modelsForHl7.values foreach (segment => segment.models.values.foreach(models => models.foreach(model => {
      if (model.adhoc isDefined) createTopic(model.adhoc.get dest)
    })))
    createTopic(hl7JsonTopic, segmentPartitions = false)
    createTopic(segTopic, segmentPartitions = false)
    createTopic(auditTopic, segmentPartitions = false)
    createTopic(rejectedTopic, segmentPartitions = false)
    sparkStrCtx.sparkContext addSparkListener new MetricsListener(sparkStrCtx)
    segmentsAccumulators = registerSegmentsMetric(sparkStrCtx)
    segmentsDriverMetrics = driverSegmentsMetric()
    parserAccumulators = registerParserMetric(sparkStrCtx)
    parserDriverMetrics = driverParserMetric()
    restoreMetrics()
    monitorHandler = newDaemonScheduler(app + "-Monitor-Pool")
    monitorHandler scheduleAtFixedRate(new StatsReporter(app), initDelay + 2, 86400, TimeUnit.SECONDS)
    monitorHandler scheduleAtFixedRate(new DataFlowMonitor(sparkStrCtx, 10), 610, 600, TimeUnit.SECONDS)
    sparkUtil addHook persistParserMetrics
    sparkUtil addHook persistSegmentMetrics
    sHook = newThread(s"$app-SparkCtx SHook", runnable({
      close()
      shutDown()
      info(s"${currThread.getName}  Shutdown HOOK Completed for " + app)
    }))
    registerHook(sHook)
    info("Initialisation Done. Running Job")
    if (!restoreFromChk.get()) runJob(sparkStrCtx)
  }

  /**
    * Main Job Execution
    *
    * Executes Each RDD Partitions Asynchronously.
    */
  private def runJob(sparkStrCtx: StreamingContext): Unit = {
    val streamLine = sparkUtil stream(sparkStrCtx, kafkaConsumerProp, topicsToSubscribe)
    info(s"Kafka Stream Was Opened Successfully with ID :: ${streamLine.id}")
    streamLine foreachRDD (rdd => {
      var messagesInRDD = 0L
      rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach(range => {
        debug("Got RDD " + rdd.id + " from Topic :: "
          + range.topic + " , partition :: " + range.partition + " messages Count :: " + range.count + " Offsets From :: "
          + range.fromOffset + " To :: " + range.untilOffset)
        messagesInRDD = inc(messagesInRDD, range.count())
        this.msgTypeFreq update(range.topic, (sourceHl7Mapping(range.topic), inc(this.msgTypeFreq(range.topic)._2, range.count())))
      })
      if (messagesInRDD > 0L) {
        info(s"Got RDD ${rdd.id} with Partitions :: " + rdd.partitions.length + " and Messages Cnt:: " + messagesInRDD + " Executing Asynchronously Each of Them.")
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
        val jsonOverSized = this.jsonOverSized
        val segOverSized = this.segOverSized
        val rejectOverSized = this.rejectOverSized
        val adhocOverSized = this.adhocOverSized
        val sizeCheck = checkSize(maxMessageSize)(_, _)
        val confFile = config_file
        val tracker = new ListBuffer[FutureAction[Unit]]
        tracker += rdd foreachPartitionAsync (dataItr => {
          dataItr nonEmpty match {
            case true =>
              propFile = confFile
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
                    case _ =>
                      info("Message Type Header Not Found in raw HL7 :: " + hl7._1 + " Trying To Find In registered HL7 Types")
                      val delim = if (hl7._2 contains "\r\n") "\r\n" else "\n"
                      val rawSplit = hl7._2 split delim
                      val reqMsgType = valid(rawSplit) match {
                        case true => val temp = PIPER split rawSplit(0)
                          valid(temp, 9) match {
                            case true =>
                              val hl7Type = temp(8) contains caret match {
                                case true => (temp(8) split("\\" + caret, -1)) (0)
                                case _ => temp(8)
                              }
                              if (valid(hl7Type)) (hl7Type, temp(9))
                              else (EMPTYSTR, EMPTYSTR)
                            case _ => (EMPTYSTR, EMPTYSTR)
                          }
                        case _ => (EMPTYSTR, EMPTYSTR)
                      }
                      if (reqMsgType._1 != EMPTYSTR) msgType = hl7TypesMappings getOrElse(reqMsgType._1, UNKNOWN) match {
                        case ORU =>
                          if (valid(reqMsgType._2) && reqMsgType._2.startsWith("IP")) IPLORU else ORU
                        case any => any
                      }
                      if (msgType == UNKNOWN) throw new UnknownMessageTypeException(hl7TypesMappings.keySet.mkString(caret) +
                        s"Registered Handlers Cannot Deal With Message Came Into Process :: $reqMsgType  for ${hl7._2}")
                      else info("Able To Find Which Type of HL7 From Message :: " + msgType)
                  }
                  val hl7Str = msgType.toString
                  val segHandlerIO = segHandlers(msgType).handleSegments(hl7SegIO, hl7RejIO, auditIO, adhocIO)(_, _)
                  Try(parserS(msgType) transformHL7(hl7._2, hl7RejIO) rec) match {
                    case Success(data) => data match {
                      case Left(out) =>
                        segHandlerIO(out._2, out._3)
                        sizeCheck(out._1, parserS(msgType))
                        if (tryAndLogThr(hl7JsonIO(out._1, header(hl7Str, jsonStage, Left(out._3))), s"$hl7Str$COLON$hl7JsonIOFun", error(_: Throwable))) {
                          tryAndLogThr(auditIO(jsonAudits(msgType)(out._3), header(hl7Str, auditHeader, Left(out._3))), s"$hl7Str$COLON$hl7JsonAuditIOFun", error(_: Throwable))
                        }
                        else {
                          val msg = rejectMsg(hl7Str, jsonStage, out._3, " Writing Data to OUT Failed ", out._2)
                          tryAndLogThr(hl7RejIO(msg, header(hl7Str, rejectStage, Left(out._3))), s"$hl7Str$COLON hl7RejIO-rejectMsg", error(_: Throwable))
                          error("Sending JSON to Kafka Failed :: " + msg)
                        }
                      case Right(t) =>
                        val msg = rejectRawMsg(hl7Str, jsonStage, hl7._2, t.getMessage, t, stackTrace = false)
                        if (msgType != UNKNOWN) sizeCheck(msg, parserS(msgType))
                        tryAndLogThr(hl7RejIO(msg, header(hl7Str, rejectStage, Right(hl7._2))), s"$hl7Str$COLON hl7RejIO-rejectRawMsg${t.getMessage}", error(_: Throwable))
                        debug(msg, t)
                    }
                    case Failure(t) =>
                      val msg = rejectRawMsg(hl7Str, jsonStage, hl7._2, t.getMessage, t, stackTrace = false)
                      if (msgType != UNKNOWN) sizeCheck(msg, parserS(msgType))
                      tryAndLogThr(hl7RejIO(msg, header(hl7Str, rejectStage, Right(hl7._2))), s"$hl7Str$COLON rejectRawMsg-${t.getMessage}", error(_: Throwable))
                      error(msg)
                  }
                } catch {
                  case t: Throwable => error(s" Processing HL7 failed for Message with header ${hl7._1} & body \n ${hl7._2} \n with error message ${t.getMessage}", t)
                    val msg = rejectRawMsg(msgType.toString, jsonStage, hl7._2, t.getMessage, t)
                    if (msgType != UNKNOWN) sizeCheck(msg, parserS(msgType))
                    tryAndLogThr(hl7RejIO(msg, header(msgType.toString, rejectStage, Right(hl7._2))), s"${msgType.toString}$COLON rejectRawMsg-${t.getMessage}", error(_: Throwable))
                }
              })
              this.segmentsAccumulators = segmentsAccumulators
              segHandlers.values foreach (handler => collectSegmentMetrics(handler metricsRegistry))
              this.parserAccumulators = parserAccumulators
              parserS.values foreach (parser => collectParserMetrics(parser metricsRegistry))
            case _ => info(s"Partition was Empty For RDD ${rdd.id} So skipping :: $dataItr")
          }
        })
        tracker.foreach(_.get())
        info(s"Processing Completed for RDD :: ${rdd.id} Messages Count :: $messagesInRDD")
      } else info(s"Batch was Empty So Skipping RDD :: ${rdd.id}")
    })
  }

  /**
    * Starts Spark Streaming
    */
  private def startStreams() = {
    try {
      sparkStrCtx start()
      info(s"Started Spark Streaming Context Execution :: ${new Date()}")
      sparkStrCtx awaitTermination()
    } catch {
      case t: Throwable => error("Spark Context Starting Failed ", t)
    } finally {
      shutDown()
      close()
    }
  }

  private def shutDown() : Unit ={
     sparkUtil shutdownEverything sparkStrCtx
     closeResource(fileSystem)
  }
  /**
    * Close All Resources
    */
  private def close() = {
    info(s"Shutdown Invoked for $app")
    monitorHandler shutdown()
    monitorHandler awaitTermination(1, TimeUnit.HOURS)
    info("Metrics For Segments Data Modeling :: ")
    segmentsDriverMetrics foreach (x => info(x._1 + " : " + x._2))
    info("*******************************************************************************************************")
    info("Metrics For Hl7 Messages :: ")
    parserDriverMetrics foreach (x => info(x._1 + " : " + x._2))
    info("*******************************************************************************************************")
    info(s"Shutdown Completed for $app")
    info("*****************************************END***********************************************************")
  }

  /**
    * Seconds from currents point to next Day starting
    */
  private def initDelay = now(sys_ZoneId) until(LocalDate.now(sys_ZoneId).plusDays(1).atStartOfDay(), ChronoUnit.SECONDS)


  private def checkSize(threshold: Int)(data: AnyRef, parser: HL7Parser) = if (!IOCanHandle(data, threshold)) parser.overSizeMsgFound()


  /**
    * Creates Accumulator for HL7 Segments Tasks
    */
  private def registerSegmentsMetric(sparkStrCtx: StreamingContext) = {
    val temp = new TrieMap[String, Accumulator[Long]]
    segmentsHandler.foreach(handler => handler._2.metricsRegistry.foreach({ case (k, v) => temp += k -> sparkStrCtx.sparkContext.accumulator(v, k)(LongAccumulatorParam) }))
    info(s"Accumulators are Registered for Segments")
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
  private def registerParserMetric(sparkStrCtx: StreamingContext) = {
    val temp = new TrieMap[String, Accumulator[Long]]
    hl7Parsers.foreach(parser => parser._2.metricsRegistry.foreach({ case (k, v) => temp += k -> sparkStrCtx.sparkContext.accumulator(v, k)(LongAccumulatorParam) }))
    info(s"Accumulators are Registered for Parsers")
    temp
  }

  /**
    * Creates Metrics Registry for Segments Jobs that can be Collected at Driver
    */

  private def driverSegmentsMetric() = {
    val temp = new TrieMap[String, Long]
    segmentsHandler.foreach(handler => temp ++= handler._2.metricsRegistry)
    info(s"Driver metrics Registered for Segments")
    temp
  }

  /**
    * Creates Metrics Registry for Parsing Jobs that can be Collected at Driver
    */
  private def driverParserMetric() = {
    val temp = new TrieMap[String, Long]
    hl7Parsers.foreach(handler => temp ++= handler._2.metricsRegistry)
    info(s"Driver metrics Registered for Parsers")
    temp
  }

  /**
    * Collects Segments Metrics from Tasks Completed in Driver
    */
  private def collectSegmentMetrics(taskMetrics: TrieMap[String, Long], taskUpdate: Boolean = true) = segmentsAccumulators.synchronized {
    val NA = SegmentsState.NOTAPPLICABLE.toString
    taskMetrics.foreach({ case (k, metric) => if (metric > 0L) taskUpdate match {
      case true =>
        if (!(k contains NA)) this.segmentsAccumulators(k) += metric
      case _ => this.segmentsAccumulators(k).value_=(metric)
    }
    })
  }

  /**
    * Collects Parser Metrics from Tasks Completed in Driver
    */
  private def collectParserMetrics(taskMetrics: TrieMap[String, Long], taskUpdate: Boolean = true) = parserAccumulators.synchronized {
    taskMetrics.foreach({ case (k, metric) => if (metric > 0L) taskUpdate match {
      case true => this.parserAccumulators(k) += metric
      case _ => this.parserAccumulators(k).value_=(metric)
    }
    })
  }

  def checkForStageToComplete(): Boolean = {
    ensureStageCompleted.get() match {
      case true =>
        true
      case _ =>
        val retry = RetryHandler(8, defaultWaitBetweenRetriesMS)
        while (retry.tryAgain()) {
          if (ensureStageCompleted.get()) return true
        }
        false
    }
  }


  private def persistParserMetrics(): Unit = {
    writeObject(parserDriverMetrics, "parser-metrics", lookUpProp("hl7.parser.metrics"), hdpConf)
    info("Successfully Persisted Parser Metrics ")
  }

  private def persistSegmentMetrics(): Unit = {
    writeObject(segmentsDriverMetrics, "segments-metrics", lookUpProp("hl7.segments.metrics"), hdpConf)
    info("Successfully Persisted Segment Metrics ")
  }


  private def restoreMetrics(): Unit = {
    info("Trying to Recover Metrics if Possible")
    val parserMetrics = readObject[TrieMap[String, Long]](lookUpProp("hl7.parser.metrics"), hdpConf)
    if (parserMetrics.isDefined) {
      parserMetrics.get.foreach({ case (k, metric) => if ((parserDriverMetrics isDefinedAt k) && metric > 0L) parserDriverMetrics update(k, metric) })
      collectParserMetrics(parserDriverMetrics, taskUpdate = false)
      info("Recovered Parser Metrics ")
    }
    val segmentMetrics = readObject[TrieMap[String, Long]](lookUpProp("hl7.segments.metrics"), hdpConf)
    if (segmentMetrics.isDefined) {
      segmentMetrics.get.foreach({ case (k, metric) => if ((segmentsDriverMetrics isDefinedAt k) && metric > 0L) segmentsDriverMetrics update(k, metric) })
      collectSegmentMetrics(segmentsDriverMetrics, taskUpdate = false)
      info("Recovered Segments Metrics ")
    }
  }

  private def persistMetrics(): Unit = {
    persistParserMetrics()
    persistSegmentMetrics()
  }

  /**
    * Listener For Spark Events
    *
    */
  private class MetricsListener(sparkStrCtx: StreamingContext) extends SparkListener {
    val stageTracker = new TrieMap[Int, StageInfo]()

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      super.onStageSubmitted(stageSubmitted)
      runningStage = stageSubmitted.stageInfo
      ensureStageCompleted set false
      stageTracker += stageSubmitted.stageInfo.stageId -> stageSubmitted.stageInfo
      debug(s"Total Stages so Far ${stageTracker.size}")
      if (stageTracker.size > 20) {
        if (sparkStrCtx.sparkContext.requestExecutors(2)) stageTracker.clear()
      }
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      super.onStageCompleted(stageCompleted)
      ensureStageCompleted compareAndSet(false, true)
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
  }

  private class DataFlowMonitor(sparkStrCtx: StreamingContext, timeInterval: Int) extends Runnable {
    val timeCheck = timeInterval * 60000L

    override def run(): Unit = {
      checkForStageToComplete()
      if (!(runningStage.completionTime.isEmpty && runningStage.submissionTime.isDefined && ((currMillis - runningStage.submissionTime.get) >= timeCheck))) {
        msgTypeFreq.transform({ case (k, v) =>
          lowFrequencyHL7 isDefinedAt v._1 match {
            case true =>
              if (lowFrequencyHL7(v._1) < 6) {
                lowFrequencyHL7 update(v._1, lowFrequencyHL7(v._1) + 1)
                v
              } else {
                if (v._2 <= 0) noDataAlert(v._1, k, timeInterval * lowFrequencyHL7(v._1))
                lowFrequencyHL7 update(v._1, 0)
                (v._1, 0L)
              }
            case _ =>
              if (v._2 <= 0) noDataAlert(v._1, k)
              (v._1, 0L)
          }
        })
      }
      else {
        error("Stage was not Completed. Running for Long Time with Id " + runningStage.stageId + " Attempt Made so far " + runningStage.attemptId)
        mail("{encrypt} " + app + " with Job ID " + sparkStrCtx.sparkContext.applicationId + " Running Long",
          app + " Batch was Running more than what it Should. Batch running with Stage Id :: " + runningStage.stageId + " and Attempt Made so far :: " + runningStage.attemptId +
            " . \nBatch Submitted Since " + new Date(runningStage.submissionTime.get) + "  has not Completed. Some one has to Check Immediately" +
            " What is happening with this Job. Maximum Execution time Expected :: " + batchCycle + " seconds" + "\n\n" + EVENT_TIME
          , CRITICAL)
      }
    }

    private def noDataAlert(hl7: HL7, whichSource: String, howLong: Int = timeInterval): Unit = {
      mail("{encrypt} " + app + " with Job ID " + sparkStrCtx.sparkContext.applicationId + " Not Receiving Data for Stream " + hl7,
        app + " Job has not Received any Data in last " + howLong + " minutes. Some one has to Check Immediately What is happening with Receiver Job for HL7 Message Type "
          + hl7 + ".\nSource for this Stream " + whichSource + "\n\n" + EVENT_TIME
        , CRITICAL)
    }
  }

}


