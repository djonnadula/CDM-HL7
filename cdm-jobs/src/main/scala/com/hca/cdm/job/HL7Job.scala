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
import com.hca.cdm.auth.LoginRenewer
import com.hca.cdm.auth.LoginRenewer.loginFromKeyTab
import com.hca.cdm.notification.{EVENT_TIME, sendMail => mail}
import com.hca.cdm.hadoop._
import com.hca.cdm.hl7.EnrichCacheManager
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.audit._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.{HL7, IPLORU, ORU, UNKNOWN, withName => whichHL7}
import com.hca.cdm.hl7.exception.UnknownMessageTypeException
import com.hca.cdm.hl7.model._
import com.hca.cdm.hl7.parser.HL7Parser
import com.hca.cdm.hl7.validation.NotValidHl7Exception
import com.hca.cdm.job.report.StatsReporter
import com.hca.cdm.kfka.config.HL7ConsumerConfig.{createConfig => consumerConf}
import com.hca.cdm.kfka.config.HL7ProducerConfig.{createConfig => producerConf}
import com.hca.cdm.kfka.producer.{KafkaProducerHandler => KProducer}
import com.hca.cdm.kfka.util.TopicUtil.{createTopicIfNotExist => createTopic}
import com.hca.cdm.log.Logg
import com.hca.cdm.mq.publisher.{MQAcker => TLMAcknowledger}
import com.hca.cdm.notification.TaskState._
import com.hca.cdm.spark.{Hl7SparkUtil => sparkUtil}
import com.hca.cdm.utils.RetryHandler
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.AccumulatorParam.LongAccumulatorParam
import org.apache.spark.scheduler._
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.StreamingContext
import org.apache.spark._
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import scala.collection.mutable
import TimeUnit._
import com.hca.cdm.hbase.{HBaseConnector, HUtils}
import com.hca.cdm.kfka.util.OffsetManager
import org.apache.hadoop.io.{Writable, Text}
import org.apache.spark.rdd.RDD

/**
  * Created by Devaraj Jonnadula on 8/19/2016.
  *
  * Job which Assembles Functions to run on Spark Container for Transforming Raw HL7 to Output Formats Json...
  * and Breaks Data Into Segments level and Special Cases.
  */
object HL7Job extends Logg with App {

  self =>
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
  private val monitorInterval = lookUpProp("hl7.monitor.interval").toInt
  private val iscAlertInterval = lookUpProp("hl7.alert.isc.interval").toInt
  private val iscMonitoringEnabled = iscAlertInterval > 0
  private var sHook: Thread = _
  private val app = lookUpProp("hl7.app")
  private val consumerGroup = lookUpProp("hl7.group")
  private val kafkaConsumerProp = (consumerConf(consumerGroup) asScala) toMap
  private val rejectedTopic = lookUpProp("hl7.reject")
  private val hl7JsonTopic = {
    val temp = lookUpProp("hl7.json").split(COMMA)
    (tryAndReturnDefaultValue0(temp(0), EMPTYSTR), tryAndReturnDefaultValue0(temp(1), EMPTYSTR))
  }
  private val segTopic = lookUpProp("hl7.segment")
  private val auditTopic = lookUpProp("hl7.audit")
  private val maxMessageSize = lookUpProp("hl7.message.max") toInt
  private val kafkaProducerConf = producerConf()
  private val messageTypes = lookUpProp("hl7.messages.type") split COMMA
  private val hl7MsgMeta = messageTypes.map(mtyp => whichHL7(mtyp) -> getMsgTypeMeta(whichHL7(mtyp), lookUpProp(mtyp + ".kafka.source"))) toMap
  private val topicsToSubscribe = hl7MsgMeta.map(hl7Type => hl7Type._2.kafka) toSet
  private val hl7TypesMapping = hl7MsgMeta.map(hl7Type => hl7Type._1.toString -> hl7Type._1)
  private val templatesMapping = loadTemplate(lookUpProp("hl7.template"))
  private val segmentsMapping = applySegmentsToAll(loadSegments(lookUpProp("hl7.segments")) ++ loadSegments(lookUpProp("hl7.adhoc-segments")), messageTypes)
  private val modelsForHl7 = hl7MsgMeta.map(msgType => msgType._1 -> segmentsForHl7Type(msgType._1, segmentsMapping(msgType._1.toString)))
  private val registeredSegmentsForHl7 = modelsForHl7.mapValues(_.models.keySet)
  private val hl7Parsers = hl7MsgMeta map (hl7 => hl7._1 -> new HL7Parser(hl7._1, templatesMapping))
  private val jsonAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, jsonStage)(EMPTYSTR, _: MSGMeta)))
  private val segmentsAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, segmentStage)(_: String, _: MSGMeta)))
  private val adhocAuditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, adhocStage)(_: String, _: MSGMeta)))
  private val allSegmentsInHl7Auditor = hl7MsgMeta map (msgType => msgType._1 -> (auditMsg(msgType._1.toString, segmentsInHL7)(_: String, _: MSGMeta)))
  private val segmentsHandler = modelsForHl7 map (hl7 => hl7._1 -> new DataModelHandler(hl7._2, registeredSegmentsForHl7(hl7._1), segmentsAuditor(hl7._1),
    allSegmentsInHl7Auditor(hl7._1), adhocAuditor(hl7._1), tlmAckMsg(hl7._1.toString, applicationReceiving, HDFS, _: String)(_: MSGMeta)))
  private val ensureStageCompleted = new AtomicBoolean(false)
  @volatile private var runningStage: StageInfo = _
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
      temp += whichHL7(x) -> 0
    })
    temp
  }
  private val ackQueue = enabled(lookUpProp("mq.destination.queues"))
  private val inputSource: InputSource.Source = tryAndReturnDefaultValue(asFunc(InputSource.withName(lookUpProp("hl7.data.source"))), InputSource.KAFKA)
  if (inputSource == InputSource.HDFS) require(messageTypes.size == 1, "When Source is Configured as HDFS only one Message type should be active")
  private val isKafkaSource: Boolean = inputSource == InputSource.KAFKA

  // ******************************************************** Spark Part ***********************************************
  private val checkpointEnabled = isCheckPointEnabled
  private val restoreFromChk = new AtomicBoolean(checkpointEnabled)
  private val checkPoint = lookUpProp("hl7.checkpoint")
  private val sparkConf = sparkUtil.getConf(lookUpProp("hl7.app"), defaultPar)
  private val hdpConf = hadoop.hadoopConf
  private val hTables = new mutable.HashSet[String]
  private var offSetManager: OffsetManager = _
  private val appManagesOffset: Boolean = isKafkaSource && !checkpointEnabled && tryAndReturnDefaultValue0(lookUpProp("hl7.checkpoint.external.enable").toBoolean, true)
  private val sparkManagesOffsets: Boolean = checkpointEnabled && isKafkaSource
  loginFromKeyTab(sparkConf.get("spark.yarn.keytab"), sparkConf.get("spark.yarn.principal"), Some(hdpConf))
  LoginRenewer.scheduleRenewal(master = true, namesNodes = EMPTYSTR, conf = Some(hdpConf))

  private def newCtxIfNotExist = new (() => StreamingContext) {
    override def apply(): StreamingContext = {
      val ctx = sparkUtil createStreamingContext(sparkConf, batchDuration)
      info(s"New Context Created $ctx")
      restoreFromChk set false
      ctx
    }
  }

  private val sparkStrCtx: StreamingContext = if (checkpointEnabled) sparkUtil streamingContext(checkPoint, newCtxIfNotExist) else sparkUtil createStreamingContext(sparkConf, batchDuration)
  sparkStrCtx.sparkContext setJobDescription lookUpProp("job.desc")
  initialise(sparkStrCtx)
  startStreams()

  private def initialise(sparkStrCtx: StreamingContext): Unit = {
    info("Job Initialisation Started on :: " + new Date())
    hl7JsonTopic.productIterator.foreach { tpc => if (tpc.asInstanceOf[String] != EMPTYSTR) createTopic(tpc.asInstanceOf[String], segmentPartitions = false) }
    if (segTopic != EMPTYSTR) createTopic(segTopic, segmentPartitions = false)
    if (auditTopic != EMPTYSTR) createTopic(auditTopic, segmentPartitions = false)
    if (rejectedTopic != EMPTYSTR) createTopic(rejectedTopic, segmentPartitions = false)
    modelsForHl7.values foreach { segment =>
      segment.models.values.foreach { models =>
        models.foreach { model =>
          if (model.adhoc isDefined) {
            EnrichCacheManager().getEnRicher(model.adhoc.get.transformer).offHeapDataEnRicher.repos.foreach(hTables += _)
            model.adhoc.get.destination.system match {
              case Destinations.KAFKA =>
                createTopic(model.adhoc.get.destination.route)
              case Destinations.HBASE =>
                hTables += model.adhoc.get.destination.route
                LoginRenewer.performAction(asFunc(HBaseConnector(hdpConf, lookUpProp("cdm.hl7.hbase.namespace")).createTable(model.adhoc.get.destination.route, None,
                  List(HUtils.createFamily(model.adhoc.get.destination.offHeapConfig.get.family)))))
              case _ =>
            }
          }
        }
      }
    }
    HBaseConnector stop()
    if (appManagesOffset) offSetManager = new OffsetManager(lookUpProp("cdm.hl7.hbase.namespace"), lookUpProp("cdm.hl7.hbase.app.state.store"), app, hdpConf)
    sparkStrCtx.sparkContext addSparkListener new MetricsListener(sparkStrCtx)
    segmentsAccumulators = registerSegmentsMetric(sparkStrCtx)
    segmentsDriverMetrics = driverSegmentsMetric()
    parserAccumulators = registerParserMetric(sparkStrCtx)
    parserDriverMetrics = driverParserMetric()
    restoreMetrics()
    monitorHandler = newDaemonCachedScheduler(s"$app-Monitor-Pool", 3)
    if (tryAndReturnDefaultValue0(lookUpProp("cdm.stats.generate").toBoolean, false)) {
      monitorHandler scheduleAtFixedRate(new StatsReporter(app), initDelay + 2, 86400, TimeUnit.SECONDS)
    }
    if (monitorInterval > 0) {
      monitorHandler scheduleAtFixedRate(new DataFlowMonitor(sparkStrCtx, monitorInterval), minToSec(monitorInterval) + 2, minToSec(monitorInterval), TimeUnit.SECONDS)
    }
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
    val streamLine = if (appManagesOffset) {
      sparkUtil stream(sparkStrCtx, kafkaConsumerProp, offSetManager.appStarted(topicsToSubscribe, kafkaConsumerProp))
    } else if (sparkManagesOffsets) {
      sparkUtil stream(sparkStrCtx, kafkaConsumerProp, topicsToSubscribe)
    } else {
      val rdds = new mutable.Queue[RDD[(Writable, Text)]]
      dataDirectories.foreach(dir => rdds += sparkStrCtx.sparkContext.sequenceFile[Writable, Text](dir, maxPartitions))
      sparkStrCtx queueStream rdds
    }
    info(s"$inputSource Stream Was Opened Successfully with ID :: ${streamLine.id}")
    streamLine foreachRDD (rdd => {
      var messagesInRDD = 0L
      if (isKafkaSource) {
        rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach(range => {
          debug("Got RDD " + rdd.id + " from Topic :: "
            + range.topic + " , partition :: " + range.partition + " messages Count :: " + range.count + " Offsets From :: "
            + range.fromOffset + " To :: " + range.untilOffset)
          messagesInRDD = inc(messagesInRDD, range.count())
          self.msgTypeFreq update(range.topic, (sourceHl7Mapping(range.topic), inc(self.msgTypeFreq(range.topic)._2, range.count())))
        })
      } else messagesInRDD = rdd.count()
      if (messagesInRDD > 0L) {
        info(s"Got RDD ${rdd.id} with Partitions :: " + rdd.partitions.length + " and Messages Cnt:: " + messagesInRDD + " Executing Asynchronously Each of Them.")
        val hl7s = self.messageTypes
        val parserS = self.hl7Parsers
        val segHandlers = self.segmentsHandler
        val jsonAudits = self.jsonAuditor
        val jsonOut = self.hl7JsonTopic
        val rejectOut = self.rejectedTopic
        val segOut = self.segTopic
        val auditOut = self.auditTopic
        val prodConf = self.kafkaProducerConf
        val hl7TypesMappings = self.hl7TypesMapping
        val segmentsAccumulators = self.segmentsAccumulators
        val parserAccumulators = self.parserAccumulators
        val maxMessageSize = self.maxMessageSize
        val jsonOverSized = self.jsonOverSized
        val segOverSized = self.segOverSized
        val rejectOverSized = self.rejectOverSized
        val adhocOverSized = self.adhocOverSized
        val sizeCheck = checkSize(maxMessageSize)(_, _)
        val confFile = self.config_file
        val tlmAckQueue = self.ackQueue
        val appName = self.app
        val inputSource = self.inputSource
        val hTables = self.hTables.toSet
        val tracker = new ListBuffer[FutureAction[Unit]]
        tracker += rdd foreachPartitionAsync (dataItr => {
          if (dataItr nonEmpty) {
            propFile = confFile
            LoginRenewer.scheduleRenewal()
            val kafkaOut = KProducer()(prodConf)
            val hl7JsonIO = kafkaOut.write(_: Product, _: String, jsonOut)(maxMessageSize, jsonOverSized)
            val hl7RejIO = kafkaOut.writeData(_: AnyRef, _: String, rejectOut)(maxMessageSize, rejectOverSized)
            val hl7SegIO = kafkaOut.writeData(_: String, _: String, segOut)(maxMessageSize, segOverSized)
            val auditIO = kafkaOut.writeData(_: String, _: String, auditOut)(maxMessageSize)
            val adhocIO = kafkaOut.writeData(_: String, _: String, _: String)(maxMessageSize, adhocOverSized)
            val hBaseWriter = HBaseConnector(lookUpProp("cdm.hl7.hbase.namespace"), hTables, lookUpProp("cdm.hl7.hbase.batch.write.size").toInt).
              map { case (dest, operator) => dest -> (HUtils.sendRequest(operator)(_: String, _: String, _: mutable.Map[String, String],_:Boolean)) }
            EnrichCacheManager(lookUpProp("hl7.adhoc-segments"), hl7s, offHeapHandlers(HBaseConnector(lookUpProp("cdm.hl7.hbase.namespace")), hBaseWriter))
            var tlmAckIO: (String, String) => Unit = null
            if (tlmAckQueue.isDefined) {
              TLMAcknowledger(appName, appName)(lookUpProp("mq.hosts"), lookUpProp("mq.manager"), lookUpProp("mq.channel"), lookUpProp("mq.destination.queues"), 3)
              tlmAckIO = TLMAcknowledger.ackMessage(_: String, _: String)
            }
            val ackTlm = (meta: MSGMeta, hl7Str: String) => if (tlmAckQueue isDefined) tlmAckIO(tlmAckMsg(hl7Str, applicationReceiving, HDFS, jsonStage)(meta), jsonStage)
            InputSource.convert(inputSource, hl7s.head, dataItr) foreach { case (hl7Key, hl7) =>
              var msgType: HL7 = UNKNOWN
              try {
                msgType = if (hl7Key != null && hl7Key != EMPTYSTR) hl7TypesMappings getOrElse(hl7Key substring(0, hl7Key indexOf COLON), UNKNOWN) else UNKNOWN
                if (msgType == UNKNOWN) {
                  info(s"Message Type Header Not Found in raw HL7 :: $hl7Key Trying To Find In registered HL7 Types")
                  val delim = if (hl7 contains "\r\n") "\r\n" else "\n"
                  val rawSplit = hl7 split delim
                  val reqMsgType = if (valid(rawSplit)) {
                    val temp = PIPER split rawSplit(0)
                    if (valid(temp, 9)) {
                      val hl7Type = if (temp(8) contains caret) (temp(8) split("\\" + caret, -1)) (0)
                      else temp(8)
                      if (valid(hl7Type)) (hl7Type, temp(9))
                      else (EMPTYSTR, EMPTYSTR)
                    } else (EMPTYSTR, EMPTYSTR)
                  } else (EMPTYSTR, EMPTYSTR)
                  if (reqMsgType._1 != EMPTYSTR) msgType = hl7TypesMappings getOrElse(reqMsgType._1, UNKNOWN) match {
                    case ORU =>
                      if (valid(reqMsgType._2) && reqMsgType._2.startsWith("IP")) IPLORU else ORU
                    case any => any
                  }
                  if (msgType == UNKNOWN) throw new UnknownMessageTypeException(hl7TypesMappings.keySet.mkString(caret) +
                    s" Registered Handlers Cannot Deal With Message Came Into Process :: $reqMsgType  for $hl7")
                  else info("Able To Find Which Type of HL7 From Message :: " + msgType)
                }
                val hl7Str = msgType.toString
                val segHandlerIO = segHandlers(msgType).handleSegments(hl7SegIO, hl7RejIO, auditIO, adhocIO,
                  if (tlmAckQueue isDefined) Some(tlmAckIO(_: String, _: String)) else None, hBaseWriter)(_, _, _)
                Try(parserS(msgType) transformHL7(hl7, hl7RejIO) rec) match {
                  case Success(data) => data match {
                    case Left(out) =>
                      ackTlm(out._3, hl7Str)
                      segHandlerIO(out._2, hl7, out._3)
                      sizeCheck(out._1._1, parserS(msgType))
                      sizeCheck(out._1._2, parserS(msgType))
                      if (tryAndLogThr(hl7JsonIO(out._1, header(hl7Str, jsonStage, Left(out._3))), s"$hl7Str$COLON$hl7JsonIOFun", error(_: Throwable))) {
                        tryAndLogThr(auditIO(jsonAudits(msgType)(out._3), header(hl7Str, auditHeader, Left(out._3))), s"$hl7Str$COLON$hl7JsonAuditIOFun", error(_: Throwable))
                      }
                      else {
                        val msg = rejectMsg(hl7Str, jsonStage, out._3, " Writing Data to OUT Failed ", out._2)
                        tryAndLogThr(hl7RejIO(msg, header(hl7Str, rejectStage, Left(out._3))), s"$hl7Str$COLON hl7RejIO-rejectMsg", error(_: Throwable))
                        error("Sending JSON to Kafka Failed :: " + msg)
                      }
                    case Right(t) =>
                      ackTlm(metaFromRaw(hl7), hl7Str)
                      val msg = rejectRawMsg(hl7Str, jsonStage, hl7, t.getMessage, t)
                      if (msgType != UNKNOWN) sizeCheck(msg, parserS(msgType))
                      tryAndLogThr(hl7RejIO(msg, header(hl7Str, rejectStage, Right(hl7))), s"$hl7Str$COLON hl7RejIO-rejectRawMsg${t.getMessage}", error(_: Throwable))
                      debug(msg, t)
                  }
                  case Failure(t) =>
                    ackTlm(metaFromRaw(hl7), hl7Str)
                    val msg = rejectRawMsg(hl7Str, jsonStage, hl7, t.getMessage, t)
                    if (msgType != UNKNOWN) sizeCheck(msg, parserS(msgType))
                    tryAndLogThr(hl7RejIO(msg, header(hl7Str, rejectStage, Right(hl7))), s"$hl7Str$COLON rejectRawMsg-${t.getMessage}", error(_: Throwable))
                    if (!t.isInstanceOf[NotValidHl7Exception]) error(msg)
                }
              } catch {
                case t: Throwable => error(s" Processing HL7 failed for Message with header $hl7Key & body \n $hl7 \n with error message ${t.getMessage}", t)
                  ackTlm(metaFromRaw(hl7), msgType.toString)
                  val msg = rejectRawMsg(msgType.toString, jsonStage, hl7, t.getMessage, t)
                  if (msgType != UNKNOWN) sizeCheck(msg, parserS(msgType))
                  tryAndLogThr(hl7RejIO(msg, header(msgType.toString, rejectStage, Right(hl7))), s"${msgType.toString}$COLON rejectRawMsg-${t.getMessage}", error(_: Throwable))
              }
            }
            self.segmentsAccumulators = segmentsAccumulators
            segHandlers.values foreach (handler => collectSegmentMetrics(handler metricsRegistry))
            self.parserAccumulators = parserAccumulators
            parserS.values foreach (parser => collectParserMetrics(parser metricsRegistry))
          } else info(s"Partition was Empty For RDD ${rdd.id} So skipping :: $dataItr")
        })
        tryAndLogErrorMes(tracker.foreach(_.get()), error(_: Throwable))
        info(s"Processing Completed for RDD :: ${rdd.id} Messages Count :: $messagesInRDD")
      } else info(s"Batch was Empty So Skipping RDD :: ${rdd.id}")
      if (self.appManagesOffset) self.offSetManager.batchCompleted(rdd)
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
      case t: Throwable =>
        if (!t.isInstanceOf[InterruptedException]) {
          error("Spark Context Starting Failed will try with Retry Policy", t)
          val retry = RetryHandler()

          def retryStart(): Unit = {
            sparkStrCtx start()
            info(s"Started Spark Streaming Context Execution :: ${new Date()}")
            sparkStrCtx awaitTermination()
          }

          tryAndLogErrorMes(retry.retryOperation(retryStart), error(_: Throwable), Some(s"Cannot Start sparkStrCtx for $app After Retries ${retry.triesMadeSoFar()}"))
        }
    } finally {
      shutDown()
      close()
    }
  }

  private def shutDown(): Unit = {
    sparkUtil shutdownEverything sparkStrCtx
    closeResource(fileSystem)
  }

  /**
    * Close All Resources
    */
  private def close(): Unit = {
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


  private def checkSize(threshold: Int)(data: AnyRef, parser: HL7Parser): Unit = if (!IOCanHandle(data, threshold)) parser.overSizeMsgFound()


  private def minToSec(min: Int): Long = {
    min * 60L
  }

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
      parserDriverMetrics.transform((_, v) => if (v > 0L) 0L else v)
    }
    segmentsDriverMetrics.synchronized {
      segmentsDriverMetrics.transform((_, v) => if (v > 0L) 0L else v)
    }
    parserAccumulators.synchronized {
      parserAccumulators.transform((_, v) => if (v.value > 0L) {
        v.value_=(0L)
        v
      } else v)
    }
    segmentsAccumulators.synchronized {
      segmentsAccumulators.transform((_, v) => if (v.value > 0L) {
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
  private def collectSegmentMetrics(taskMetrics: TrieMap[String, Long], taskUpdate: Boolean = true): Unit = segmentsAccumulators.synchronized {
    val NA = SegmentsState.NOTAPPLICABLE.toString
    taskMetrics.foreach({ case (k, metric) => if (metric > 0L) if (taskUpdate) {
      if (!(k contains NA)) this.segmentsAccumulators(k) += metric
    } else {
      this.segmentsAccumulators(k).value_=(metric)
    }
    })
  }

  /**
    * Collects Parser Metrics from Tasks Completed in Driver
    */
  private def collectParserMetrics(taskMetrics: TrieMap[String, Long], taskUpdate: Boolean = true): Unit = parserAccumulators.synchronized {
    taskMetrics.foreach({ case (k, metric) => if (metric > 0L) if (taskUpdate) {
      this.parserAccumulators(k) += metric
    } else {
      this.parserAccumulators(k).value_=(metric)
    }
    })
  }

  def checkForStageToComplete(): Boolean = {
    if (ensureStageCompleted.get()) {
      true
    } else {
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

  private def dataDirectories: ListBuffer[String] = {
    val dirs = lookUpProp("hl7.data.directories")
    val dateRanges = tryAndReturnDefaultValue(asFunc(lookUpProp("hl7.data.dates")), EMPTYSTR).split(COMMA, -1)
    val dataDirs = new ListBuffer[String]
    dateRanges.foreach { dates =>
      if (dates contains "between") {
        val from = LocalDate.parse(dates substring(0, dates.indexOf("between")))
        val to = LocalDate.parse(dates substring (dates.indexOf("between") + "between".length))
        Iterator.iterate(from)(_.plusDays(1)).takeWhile(!_.isAfter(to)).foreach{Dt =>
          info(s"$dirs$Dt")
          dataDirs += s"$dirs$Dt"}
      }else if (dates contains "greaterThan") {
        val from = LocalDate.parse(dates substring(dates.indexOf("greaterThan")+ "greaterThan".length))
        val to = LocalDate.now()
        Iterator.iterate(from)(_.plusDays(1)).takeWhile(!_.isAfter(to)).foreach{Dt =>
          info(s"$dirs$Dt")
          info(s"${fileSystem.exists(new Path(s"$dirs$Dt"))}")
          dataDirs += s"$dirs$Dt"}
      } else if (dates == "*") {
        fileSystem.listStatus(new Path(dirs)).foreach { fs =>
          info(s"$dirs - ${fs.getPath}")
          dataDirs += s"${fs.getPath}"
        }
      }
      else dataDirs += s"$dirs$dates"
    }
    if (dateRanges isEmpty) dataDirs += dirs
    dataDirs.filter(path => fileSystem.exists(new Path(path)))
  }

  private def isCheckPointEnabled: Boolean = {
    val appChk = lookUpProp("hl7.spark.checkpoint.enable").toBoolean
    if (appChk) {
      inputSource match {
        case InputSource.HDFS =>
          warn(s"Check pointing is not supported for source $inputSource")
          false
        case InputSource.KAFKA => true
      }
    }
    else false
  }


  private def offHeapHandlers(hBaseConnector: HBaseConnector, hBaseWriter: Map[String, (String, String, mutable.Map[String, String],Boolean) => Unit]): ((String, String, String, Set[String]) => mutable.Map[String, Array[Byte]],
    (String, String, Set[String],Int, String,String) => Map[Int, mutable.Map[String, Array[Byte]]],
    Map[String, (String, String, mutable.Map[String, String],Boolean) => Unit]) = synchronized {
    (HUtils.getRow(_: String, _: String, _: String, _: Set[String])(hBaseConnector),
      HUtils.getRandom(_: String, _: String, _: Set[String],_:Int,_:String,_:String)(operator = hBaseConnector), hBaseWriter)
  }

  /**
    * Listener For Spark Events
    *
    */
  private class MetricsListener(sparkStrCtx: StreamingContext) extends SparkListener {
    private val stageTracker = new TrieMap[Int, StageInfo]()
    private val stagesSubmitted = new mutable.Queue[StageInfo]
    private var firstStage = true

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      super.onStageSubmitted(stageSubmitted)
      if (firstStage) {
        runningStage = stageSubmitted.stageInfo
        firstStage = false
      }
      stagesSubmitted += stageSubmitted.stageInfo
      ensureStageCompleted set false
      stageTracker += stageSubmitted.stageInfo.stageId -> stageSubmitted.stageInfo
      debug(s"Total Stages so Far ${stageTracker.size}")
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      super.onStageCompleted(stageCompleted)
      if (runningStage.completionTime isDefined) runningStage = if (stagesSubmitted.nonEmpty) stagesSubmitted.dequeue() else null
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
    val timeCheck: Long = MILLISECONDS.convert(timeInterval, MINUTES)
    val lowFrequencyHl7AlertInterval: Int = lookUpProp("hl7.low.frequency.interval").toInt
    val iscMsgAlertFreq: TrieMap[HL7, Int] = {
      val temp = new TrieMap[HL7, Int]()
      hl7MsgMeta.foreach(hl7 => temp += hl7._1 -> 0)
      temp
    }

    override def run(): Unit = {
      checkForStageToComplete()
      if (!runningLong) {
        msgTypeFreq.transform({ case (hl7, rate) =>
          if (lowFrequencyHL7 isDefinedAt rate._1) {
            if (lowFrequencyHL7(rate._1) < lowFrequencyHl7AlertInterval) {
              lowFrequencyHL7 update(rate._1, lowFrequencyHL7(rate._1) + 1)
              rate
            } else {
              if (rate._2 <= 0 && iscMonitoringEnabled) noDataAlertForISC(rate._1, hl7, timeInterval * (lowFrequencyHL7(rate._1) + 1))
              else if (rate._2 <= 0) noDataAlert(rate._1, hl7, timeInterval * (lowFrequencyHL7(rate._1) + 1))
              lowFrequencyHL7 update(rate._1, 0)
              (rate._1, 0L)
            }
          } else {
            if (rate._2 <= 0) {
              if (iscMonitoringEnabled) {
                IscAlertCheck(rate._1, hl7, timeInterval * iscAlertInterval)
              } else noDataAlert(rate._1, hl7)
            }
            (rate._1, 0L)
          }
        })
      }
      else {
        warn("Stage was not Completed. Running for Long Time with Id " + runningStage.stageId + " Attempt Made so far " + runningStage.attemptId)
        mail("{encrypt} " + app + " with Job ID " + sparkStrCtx.sparkContext.applicationId + " Running Long",
          app + " Batch was Running more than what it Should. Batch running with Stage Id :: " + runningStage.stageId + " and Attempt Made so far :: " + runningStage.attemptId +
            " . \nBatch Submitted Since " + new Date(runningStage.submissionTime.get) + "  has not Completed. Some one has to Check " +
            " What is happening with this Job. Maximum Execution time Expected :: " + SECONDS.convert(batchDuration.milliseconds, MILLISECONDS) + " seconds" + "\n\n" + EVENT_TIME
          , CRITICAL)
      }
    }

    private def runningLong: Boolean = {
      valid(runningStage) && runningStage.completionTime.isEmpty && runningStage.submissionTime.isDefined &&
        ((currMillis - runningStage.submissionTime.get) >= timeCheck)
    }

    private def IscAlertCheck(hl7: HL7, source: String, howLong: Int): Unit = {
      if (iscMsgAlertFreq(hl7) < iscAlertInterval) {
        iscMsgAlertFreq update(hl7, iscMsgAlertFreq(hl7) + 1)
        noDataAlert(hl7, source)
      }
      else {
        noDataAlertForISC(hl7, source, howLong)
        iscMsgAlertFreq update(hl7, 0)
      }
    }

    private def noDataAlert(hl7: HL7, whichSource: String, howLong: Int = timeInterval): Unit = {
      mail("{encrypt} " + app + " with Job ID " + sparkStrCtx.sparkContext.applicationId + " Not Receiving Data for HL7 Stream " + hl7,
        app + " Job has not Received any Data in last " + howLong + " minutes. Some one has to Check What is happening with Receiver Job for HL7 Message Type "
          + hl7 + ".\nSource for this Stream " + whichSource + "\n\n" + EVENT_TIME
        , CRITICAL)
    }

    private def noDataAlertForISC(hl7: HL7, whichSource: String, howLong: Int = timeInterval): Unit = {
      mail("{encrypt} " + app + " with Job ID " + sparkStrCtx.sparkContext.applicationId + " Not Receiving Data for HL7 Stream " + hl7,
        "Production Control,\n\n   " +
          app + " Job has not Received any Data in last " + howLong + " minutes. Please contact On Call person from CDM-BD Group and " +
          "notify about this Event to Check what is happening with Receiver Job for HL7 Message Type " + hl7 + ".\n Source for this Stream " + whichSource +
          "\n\n   Thanks,\n   CDM-BD Team  " +
          "\n\n" + EVENT_TIME
        , CRITICAL, statsReport = false, lookUpProp("monitoring.notify.group").split(COMMA))
    }


    override def toString: String = s"DataFlowMonitor(timeCheck=$timeCheck, lowFrequencyHl7AlertInterval=$lowFrequencyHl7AlertInterval, iscMsgAlertFreq=$iscMsgAlertFreq)"
  }


  private[this] object InputSource extends Enumeration with Logg {
    type Source = Value
    val KAFKA: InputSource.Value = Value("KAFKA")
    val HDFS: InputSource.Value = Value("HDFS")

    def convert(source: Source, msgType: String, dataItr: Iterator[(Object, Object)]): Iterator[(String, String)] = {
      source match {
        case InputSource.KAFKA => dataItr.asInstanceOf[Iterator[(String, String)]]
        case InputSource.HDFS =>
          val msgHeader = s"$msgType$COLON"
          dataItr map { case (_, hl7) => (msgHeader, hl7.toString.replaceAll("[\r\n]+", "\r\n")) }
      }
    }


    private case class FileBasedRdd(dir: String, sparkCtx: SparkContext, maxPartitions: Int) extends RDD[(Writable, Text)](sparkCtx, Nil) {
      self =>
      info(s"FileBase Rdd initiated for $dir with Max Partitions $maxPartitions & $sparkCtx")
      self.setName(dir)
      private lazy val data: RDD[(Writable, Text)] = sparkCtx.sequenceFile[Writable, Text](dir, maxPartitions)

      override def compute(split: Partition, context: TaskContext): Iterator[(Writable, Text)] = {
        data.iterator(split, context)
      }

      override protected def getPartitions: Array[Partition] = {
        val temp = new Array[Partition](maxPartitions)
        for (part <- 0 until maxPartitions) {
          temp(part) = new Partition {
            override def index: Int = {
              part
            }
          }
        }
        temp
      }
    }

  }

}



