package com.hca.cdm.job.psg.aco

import java.lang.System.{getenv => fromEnv}
import java.net.InetSocketAddress
import java.util.Date

import akka.actor.ActorSystem
import akka.util.ByteString
import com.hca.cdm._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.job.psg.aco.PsgAcoAdtJobUtils._
import com.hca.cdm.kafka.config.HL7ConsumerConfig.{createConfig => consumerConf}
import com.hca.cdm.kafka.config.HL7ProducerConfig._
import com.hca.cdm.log.Logg
import com.hca.cdm.spark.{Hl7SparkUtil => sparkUtil}
import com.hca.cdm.tcp.AkkaTcpClient
import com.hca.cdm.tcp.AkkaTcpClient.{Ping, SendMessage}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.HasOffsetRanges

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

/**
  * Created by dof7475 on 6/1/2017.
  */
object PsgAcoAdtJob extends Logg with App {

  self =>
  private lazy val config_file = args(0)
  info("config_file: " + config_file)
  propFile = config_file
  reload(propFile)
  private lazy val app = lookUpProp("hl7.app")
  private lazy val defaultPar = lookUpProp("hl7.spark.default.parallelism")
  private lazy val sparkConf = sparkUtil.getConf(lookUpProp("hl7.app"), defaultPar)
  private lazy val batchCycle = lookUpProp("hl7.batch.interval").toInt
  private lazy val maxPartitions = defaultPar.toInt * lookUpProp("hl7.spark.dynamicAllocation.maxExecutors").toInt
  private lazy val batchDuration = sparkUtil batchCycle(lookUpProp("hl7.batch.time.unit"), batchCycle)
  private lazy val consumerGroup = lookUpProp("PSGACOADT.group")
  private lazy val checkpointEnable = lookUpProp("PSGACOADT.spark.checkpoint.enable").toBoolean
  private lazy val checkPoint = lookUpProp("PSGACOADT.checkpoint")
  private lazy val topicsToSubscribe = Set(lookUpProp("PSGACOADT.kafka.source"))
  private lazy val kafkaConsumerProp = (consumerConf(consumerGroup) asScala) toMap
  private lazy val fileSystem = FileSystem.get(new Configuration())
  private lazy val appHomeDir = fileSystem.getHomeDirectory.toString
  private lazy val stagingDir = fromEnv("SPARK_YARN_STAGING_DIR")
  private lazy val adtTypes = lookUpProp("PSGACOADT.adt.types").split(",")
  private lazy val insuranceFileLocation = new Path(lookUpProp("PSGACOADT.insurance.file.location"))
  private lazy val facFileLocation = new Path(lookUpProp("PSGACOADT.fac.file.location"))
  private lazy val insuranceArray = PsgAcoAdtJobUtils.readFile(insuranceFileLocation, fileSystem)
  private lazy val facArray = PsgAcoAdtJobUtils.readFile(facFileLocation, fileSystem)
  private lazy val outputFile = new Path(lookUpProp("PSGACOADT.output.file.location"))
  private lazy val mqQueue = enabled(lookUpProp("mq.queueResponse"))
  private lazy val insuranceNameMatcher = lookUpProp("PSGACOADT.insurance.name.match").split(",")
  private lazy val auditTopic = lookUpProp("hl7.audit")
  private lazy val maxMessageSize = lookUpProp("hl7.message.max") toInt
  private lazy val kafkaProducerConf = createConfig(auditTopic)
  private lazy val cloverleafAddr = lookUpProp("PSGACOADT.cloverleaf.addr")
  private lazy val cloverleafPort = lookUpProp("PSGACOADT.cloverleaf.port").toInt
  private lazy val begin_of_message: Char = 0x0b
  private lazy val end_of_segment: Char = 0x1c
  private lazy val end_of_message: Char = 0x0d
  private lazy val testMessage = "MSH|^~\\&||COCSZ|||201702072335||ADT^A08|MT_COCSZ_ADT_SZGTADM.1.32377292|P|2.1\r" +
    "EVN|A08|201702072335|||R.SZ.GDE^ESTILLORE^GERELI^D^^^000D\r" +
    "PID|1||D002403639|D1676444|JONES^ZAYQUAN^A^^^||19970109|M|^^^^^|B|999 UNKNOWN ADDRESS^^LAS VEGAS^NV^89104^USA^^^CLARK||(702)999-9999|(702)999-9999|ENG|S|NON|D00115310916|\r" +
    "NK1|1|UNKNOWN^UNKNOW^^^^|SP|999 UNKNOWN ADDRESS^^LAS VEGAS^NV^89104^USA^^^CLARK|(702)999-9999\r" +
    "NK1|2|UNKNOWN^UNKNOW^^^^|SP|999 UNKNOWN ADDRESS^^LAS VEGAS^NV^89104^USA^^^CLARK|(702)999-9999\r" +
    "PV1|1|E|D.ER^^|EM|||EMEGA^Emery^Garrett^J^^^MD|.SELF^REFERRED^SELF^^^^|.NO PCP^PHYSICIAN^NO^PRIMARY OR FAMILY^^^|ERS||||PR|AMB|N||ER||99|||||||||||||||||||COCSZ|FRENCH FRY IN THROAT, ABCD WNL, ACUITY 3|REG|||201702072215\r" +
    "AL1|1|DA|F001900388^No Known Allergies^No Known Allergies|U||20170207\r" +
    "ACC|20170207^|11\r" +
    "GT1|1||JONES^ZAYQUAN^A^^^||999 UNKNOWN ADDRESS^^LAS VEGAS^NV^89104^USA^^^CLARK|(702)999-9999||19970109|M||SA||||UNKNOWN|UNKNOWNN^^LAS VEGAS^NV^89148|(702)999-9999|||N\r" +
    "GT1|2||^^^^^||^^^^^^^^|||||||||||^^^^\r" +
    "IN1|1|MEDNVPA||MEDICAID PENDING|.^^.^NV^.^USA||.|99999|NONE|||||||JONES^ZAYQUAN^A^^^|01|19970109||||||||||||||||||777777777|||||||M\r" +
    "IN1|2|CHAX050||CHARITY PENDING|.^^.^NV^.^USA||.|99999|NONE|||||||JONES^ZAYQUAN^A^^^|01|19970109||||||||||||||||||777777777|||||||M\r" +
    "IN1|3|UNINSURED||UNINSURED DISCOUNT PLAN|.^^.^NV^.^USA||.|99999|NONE|||||||JONES^ZAYQUAN^A^^^|01|19970109||||||||||||||||||777777777|||||||M\r" +
    "ZCD|1|ETHNICITY^ETHNICITY^2\r" +
    "ZCD|2|ZSS.DEPREQ^DEPOSIT REQ?^N\r" +
    "ZCD|3|ZSS.ESTCHG^EST PT DUE^200.00\r" +
    "ZCD|4|ZSS.UNABLE^Comment if FULL amt requested NOT collected^PAY AT FAC\r" +
    "ZIN|1|SP|MEDICAID PENDING|N||||N|||||MEDNVPA\r" +
    "ZIN|2|SP|SELF PAY|N||||N|||||CHAX050\r" +
    "ZIN|3|SP|UNINSURED DISCOUNT PLAN|N||||N|||||UNINSURED\r" +
    "ZCS|UNK|UNKNOWN^^LAS VEGAS^NV^89148|N|NONE|NONE|01541\r"

  private var sparkStrCtx: StreamingContext = initContext
  printConfig()
  startStreams()

  private def newCtxIfNotExist = new (() => StreamingContext) {
    override def apply(): StreamingContext = {
      val ctx = sparkUtil createStreamingContext(sparkConf, batchDuration)
      info(s"New Checkpoint Created for $app $ctx")
      runJob(ctx)
      ctx
    }
  }

  private def initContext: StreamingContext = {
    sparkStrCtx = if (checkpointEnable) sparkUtil streamingContext(checkPoint, newCtxIfNotExist) else sparkUtil createStreamingContext(sparkConf, batchDuration)
    sparkStrCtx.sparkContext setJobDescription lookUpProp("job.desc")
    if (!checkpointEnable) runJob(sparkStrCtx)
    // createTopicIfNotExist(auditTopic, segmentPartitions = false)
    sparkStrCtx
  }

  private def runJob(sparkStrCtx: StreamingContext): Unit = {
    val streamLine = sparkUtil stream(sparkStrCtx, kafkaConsumerProp, topicsToSubscribe)
    info("kafkaConsumerProp: " + kafkaConsumerProp)
    info("subscribed topics: " + topicsToSubscribe.mkString(","))
    info(s"Kafka Stream Was Opened Successfully with ID :: ${streamLine.id}")


//    val tcpListener = actorSys.actorOf(AkkaTcpListener.props(), "listenerActor")
//    val msg = new StringBuilder()
//    msg.append(begin_of_message).append(testMessage).append(end_of_segment).append(end_of_message)
//    val tcpActor = actorSys.actorOf(AkkaTcpClient.props(new InetSocketAddress(cloverleafAddr, cloverleafPort), msg.toString()), "tcpActor")
//    info("tcpActor.path: " + tcpActor.path)
//    tcpListener ! Write(ByteString(msg.toString()))
    streamLine foreachRDD (rdd => {
      val confFile = config_file
      var messagesInRDD = 0L
      rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach(range => {
        debug("Got RDD " + rdd.id + " from Topic :: "
          + range.topic + " , partition :: " + range.partition + " messages Count :: " + range.count + " Offsets From :: "
          + range.fromOffset + " To :: " + range.untilOffset)
        messagesInRDD = inc(messagesInRDD, range.count())
      })
      if (messagesInRDD > 0L) {
        info(s"Got RDD ${rdd.id} with Partitions :: " + rdd.partitions.length + " and Messages Cnt:: " + messagesInRDD + " Executing Asynchronously Each of Them.")
        rdd foreachPartitionAsync (dataItr => {
          if (dataItr.nonEmpty) {
            propFile = confFile
            val actorSys = ActorSystem.create("PSGActorSystem")
            val tcpActor = actorSys.actorOf(AkkaTcpClient.props(new InetSocketAddress(cloverleafAddr, cloverleafPort)), "tcpActor")
//            val tcpManager = actorSys.actorSelection("akka://PSGActorSystem/system/IO-TCP")

            info("tcpActor.path: " + tcpActor.path.toString)
//            val auditOut = auditTopic
//            val maxMessageSize = self.maxMessageSize
//            val prodConf = kafkaProducerConf
//            val kafkaOut = KafkaProducerHandler()(prodConf)
//            val auditIO = kafkaOut.writeData(_: String, _: String, auditOut)(maxMessageSize)
            val message = dataItr.next()._2
            val delim = if (message contains "\r\n") "\r\n" else "\n"
            trySplit(message, delim) match {
              case Success(splitted) =>

                val msh = segment(splitted, MSH)
                val pv1 = segment(splitted, PV1)
                val primaryIn1 = segment(splitted, PRIMARY_IN1)
                msh.foreach(seg => debug("MSH: " + seg))
                pv1.foreach(seg => debug("PV1: " + seg))
                primaryIn1.foreach(seg => debug("IN1: " + seg))
                if (eventTypeMatch(msh, adtTypes)) {
                  info(s"Message event type matches")
//                  tcpManager ! Connect(new InetSocketAddress(cloverleafAddr, cloverleafPort))
//                  tcpActor ! Connected(new InetSocketAddress(cloverleafAddr, cloverleafPort), null)
                  tcpActor ! Ping("ping")


                  if (singleFieldMatch(msh, facArray, "\\|", 3)) {
                    info("Message facility type matches")
                    val msg1 = new StringBuilder()
                    msg1.append(begin_of_message).append(testMessage).append(end_of_segment).append(end_of_message)

                    info("connecting from job")
//                    tcpManager ! Connect(new InetSocketAddress(cloverleafAddr, cloverleafPort))
                    tcpActor ! SendMessage(ByteString(msg1.toString()))

                    if (singleFieldMatch(primaryIn1, insuranceArray, "\\|", 36) &&
                      (stringMatcher(primaryIn1, insuranceNameMatcher, "\\|", 4) ||
                      stringMatcher(primaryIn1, insuranceNameMatcher, "\\|", 9))) {
                      info(s"Patient primary insurance Id is present and name matches")

                      // PID SSN removal
                      val pidSegment = segment(splitted, PID)
                      val ssn = getField(pidSegment, "\\|", 19)
                      val newPid = removeField(pidSegment, "\\|", 19)
                      debug(s"newPid: $newPid")
                      splitted.update(splitted.indexWhere(segment => segment.startsWith(PID)), newPid)

                      // GT1 SSN removal
                      val newGT1s = new ArrayBuffer[String]
                      val gt1Segment = segment(splitted, GT1)
                      gt1Segment.foreach(seg => {
                        val splitSeg = seg.split("\\|")
                        if (splitSeg(12).nonEmpty) {
                          splitSeg.update(12, "")
                          newGT1s += splitSeg.mkString("|")
                        } else {
                          info(s"Segment contains no value at index: 12")
                          newGT1s += seg
                        }
                      })

                      for (gt1 <- newGT1s) {
                        try {
                          splitted.update(splitted.indexWhere(segment => {
                            val index = GT1 + "|" + (newGT1s.indexOf(gt1) + 1)
                            val ind = segment.startsWith(index)
                            ind
                          }), gt1)
                        } catch {
                          case e: Exception => error(e)
                        }
                      }

                      val cleanMessage = splitted.mkString("\r")

                      // Search entire message for SSN
                      val finalMessage = cleanMessage.replace(ssn, "")
                      debug(s"Old message: $message")
                      debug(s"Message to send: $finalMessage")
                      if (mqQueue.isDefined) {
                        try{
//                          val msg = new StringBuilder()
//                          msg.append(begin_of_message).append(finalMessage).append(end_of_segment).append(end_of_message)
//                          tcpActor ! ByteString(msg.toString())

//                          val promise = Promise[String]()
//                          val akkaProps = Props(classOf[AkkaTcpClient], new InetSocketAddress(cloverleafAddr, cloverleafPort), promise)
//                          val sys = ActorSystem.create("PSGActorSystem")
//                          val tcpActor = sys.actorOf(akkaProps)
//                          promise.future.map { data =>
//                            tcpActor ! "close"
//                            info("closed tcpActor")
//                          }
//                          MQAcker(app, app)(lookUpProp("mq.hosts"), lookUpProp("mq.manager"), lookUpProp("mq.channel"), mqQueue.get)
//                          MQAcker.ackMessage(finalMessage, "PSG-ACO-ADT")

                          // tryAndLogThr(auditIO(jsonAudits(msgType)(out._3), header(hl7Str, auditHeader, Left(out._3))), s"ADT-PSG-ACO", error(_: Throwable))
                        } catch {
                          case e: Exception => error(e)
                        }
                      }
                    }
                  }
                }
              case Failure(t) => error(s"Failed to split message: $t")
            }
          }
        })
      }
    })
  }

  private def startStreams() = {
    try {
      sparkStrCtx start()
      info(s"Started Spark Streaming Context Execution :: ${new Date()}")
      sparkStrCtx awaitTermination()
    } catch {
      case t: Throwable => error("Spark Context Starting Failed will try with Retry Policy", t)

    } finally {
      info("finally")
      shutDown()
    }
  }

  private def shutDown(): Unit = {
    sparkUtil shutdownEverything sparkStrCtx
    closeResource(fileSystem)
  }

}