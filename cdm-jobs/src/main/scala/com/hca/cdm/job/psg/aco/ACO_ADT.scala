package com.hca.cdm.job.psg.aco

import com.hca.cdm._
import com.hca.cdm.job.SparkTool
import com.hca.cdm.log.Logg
import com.hca.cdm.mq.publisher.MQAcker
import com.hca.cdm.spark.{Hl7SparkUtil => sparkUtil}
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConverters._

/**
  * Created by dof7475 on 6/1/2017.
  */
object ACO_ADT extends Logg with App with SparkTool {

  private val config_file = args(0)
  propFile = config_file

  printConfig()
  startStreams()

  override def runJob(sparkStrCtx: StreamingContext): Unit = {
    val streamLine = sparkUtil stream(sparkStrCtx, kafkaConsumerProp, topicsToSubscribe)
    info("kafkaConsumerProp: " + kafkaConsumerProp)
    info("subscribed topics: " + topicsToSubscribe.mkString(","))
    info(s"Kafka Stream Was Opened Successfully with ID :: ${streamLine.id}")
    streamLine foreachRDD (rdd => {
      info("in rdd zone")
      var messagesInRDD = 0L
      rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach(range => {
        debug("Got RDD " + rdd.id + " from Topic :: "
          + range.topic + " , partition :: " + range.partition + " messages Count :: " + range.count + " Offsets From :: "
          + range.fromOffset + " To :: " + range.untilOffset)
        messagesInRDD = inc(messagesInRDD, range.count())
      })
      if (messagesInRDD > 0L) {
        val wsmqQueue = mqQueue
        val appName = app
        info(s"Got RDD ${rdd.id} with Partitions :: " + rdd.partitions.length + " and Messages Cnt:: " + messagesInRDD + " Executing Asynchronously Each of Them.")
        info(s"rdd: ${rdd.toString()}")
        rdd foreachPartitionAsync (dataItr => {
          if (dataItr.nonEmpty) {
            val hl7Json = dataItr.next()._2
            info("hl7Json: " + hl7Json)
            var wsmqIO: (String) => Unit = null
            if (wsmqQueue.isDefined) {
              MQAcker(appName, appName, wsmqQueue.get)(lookUpProp("mq.hosts"), lookUpProp("mq.manager"), lookUpProp("mq.channel"), numberOfIns = 2)
              MQAcker.ackMessage(hl7Json)
            }
            //val ackTlm = (meta: MSGMeta, hl7Str: String) => if (wsmqQueue.isDefined) wsmqIO(tlmAckMsg(hl7Str, applicationReceiving, HDFS, jsonStage)(meta))
          }
        })
      }
    })
  }


}