package com.hca.cdm.kfka.util

import com.hca.cdm._
import com.hca.cdm.hbase.{HBaseConnector, HUtils}
import com.hca.cdm.log.Logg
import com.hca.cdm.utils.RetryHandler
import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Get, Put, Result}
import org.apache.hadoop.hbase.util.Bytes._
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.HColumnDescriptor._
import org.apache.hadoop.hbase.HConstants._
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps

/**
  * Created by Devaraj Jonnadula on 9/16/2017.
  */
private[cdm] class OffsetManager(storeNameSpace: String, store: String, appAsRow: String, conf: Configuration) extends Logg {
  private lazy val storeHandler = HBaseConnector(conf, storeNameSpace)
  private lazy val offsetFamily: String = "kfk_off"
  private lazy val topicOffsetSplit = "-"
  private lazy val allAttributes = Set.empty[String]

  def batchCompleted[T](batch: RDD[T]): Unit = {
    require(batch.isInstanceOf[HasOffsetRanges], "Currently Only Kafka Impl is supported")
    val table = storeHandler.getTable(store)
    val req = new Put(toBytes(appAsRow))
    val familyBytes = toBytes(offsetFamily)
    batch.asInstanceOf[HasOffsetRanges].offsetRanges.map { range =>
      TopicAndPartition(range.topic, range.partition) -> PartitionOffset(range.fromOffset, range.untilOffset)
    }.foreach { case (tp, off) =>
      req.addImmutable(familyBytes, toBytes(storeTopicPart(tp)), toBytes(off.untilOffset))
    }
    info(s"Sending Persist Request for Batch completed with id ${batch.id} ${batch.asInstanceOf[HasOffsetRanges].offsetRanges.mkString(";")}")
    tryAndGoNextAction0(new RetryHandler().retryOperation(asFunc(table.put(req))), closeResource(table))
  }

  def appStarted(topics: Set[String], kafkaParams: Map[String, String]): Map[TopicAndPartition, Long] = {
    val storeProps = Map(MIN_VERSIONS -> "0", VERSIONS -> "100")
    storeHandler.createTable(store, Some(storeProps), List(HUtils.createFamily(offsetFamily)))
    val response = HUtils.getRow(store, offsetFamily, appAsRow, allAttributes)(storeHandler)
    var out: Map[TopicAndPartition, Long] = null
    if (valid(response) && response.nonEmpty) {
      out = response.map { case (k, v) => topicPartitionFromStore(k) -> toLong(v) }.toMap
      info(s"Recovering App State from $appAsRow $out")
      if (valid(out)) {
        val topicsNotFound = topics diff out.map(_._1.topic).toSet
        if (topicsNotFound nonEmpty) {
          info(s"Offset Store does not hold Topics metadata will fetch from brokers for topics $topicsNotFound")
          out = out ++ offsetsFromBrokers(topicsNotFound, kafkaParams)
          info(s"Offset Store does not hold Topics metadata fetched Topic Partition info from brokers for topics $out")
        }
        out filterKeys (tp => topics contains tp.topic)
      } else {
        offsetsFromBrokers(topics, kafkaParams)
      }
    } else {
      info(s"Looks Like First Time App is Connecting to Offsets Repo, so Fetching Offsets from Broker for $appAsRow")
      out = offsetsFromBrokers(topics, kafkaParams)
      info(s"Offsets Fetched from Brokers $out")
      out
    }
  }

  private def offsetsFromBrokers(topics: Set[String], kafkaParams: Map[String, String]): Map[TopicAndPartition, Long] = {
    val cluster = tryAndThrow(currThread.getContextClassLoader.loadClass("org.apache.spark.streaming.kafka.KafkaCluster")
      .getConstructor(classOf[Map[String, String]]).newInstance(kafkaParams).asInstanceOf[Object]
      , error(_: Throwable), Some("Cannot load Kafka CLuster Impl for fetching Topic Metadata"))
    MethodUtils.invokeMethod(KafkaUtils, "getFromOffsets", cluster, kafkaParams, topics)
      .asInstanceOf[Map[TopicAndPartition, Long]]
  }

  private case class PartitionOffset(fromOffset: Long, untilOffset: Long)


  private def storeTopicPart(topicPart: TopicAndPartition): String = {
    s"${topicPart.topic}$topicOffsetSplit${topicPart.partition}"
  }

  private def topicPartitionFromStore(value: String): TopicAndPartition = {
    val temp = value.split(topicOffsetSplit)
    TopicAndPartition(temp(0), temp(1).toInt)
  }


}
