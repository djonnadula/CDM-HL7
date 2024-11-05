package com.cdm.kfka.util

import com.cdm.exception.CDMKafkaException
import com.cdm.io.IOConstants._
import com.cdm.log.Logg
import kafka.admin.{AdminUtils => admin}
import kafka.utils.{ZkUtils => zk}
import org.I0Itec.zkclient.{ZkClient => zkClient}

/**
  * Created by Devaraj Jonnadula on 8/19/2016.
  *
  * Utility for dealing with Kafka Topics
  */
object TopicUtil extends Logg {


  def createTopicIfNotExist(topic: String, segmentPartitions: Boolean = true): Boolean = synchronized {
    info("Creating Topic :: " + topic + " with Partitions Config :: "
      + (if (segmentPartitions) defaultSegmentPartitions else defaultHL7Partitions))
    var success: Boolean = false
    val zkUtils = createZkUtil
    try {
      (checkTopicExists(zkUtils, topic), segmentPartitions) match {
        case (true, _) => success = true
        case (false, true) => admin.createTopic(zkUtils, topic, defaultSegmentPartitions, defaultReplication)
          success = true
        case (false, false) => admin.createTopic(zkUtils, topic, defaultHL7Partitions, defaultReplication)
          success = true
      }
    } catch {
      case e: Exception => throw new CDMKafkaException("Unable to Crete Topic :  " + topic, e)
    } finally zkUtils.close
    success

  }

  def changePartitions(topic: String, newPartitions: Int): Int = synchronized {
    val zkUtil = createZkUtil
    try {
      if (checkTopicExists(zkUtil, topic)) {
        admin.addPartitions(zkUtil, topic, newPartitions)
        return newPartitions
      }
    } catch {
      case e: Exception => throw new CDMKafkaException("Unable to Delete Topic : " + topic, e)
    } finally zkUtil.close

    -1
  }

  def deleteTopic(topic: String): Boolean = synchronized {
    var success: Boolean = false
    val zkUtil = createZkUtil
    try {
      if (checkTopicExists(zkUtil, topic)) {
        admin.deleteTopic(zkUtil, topic)
        success = true
      }
    } catch {
      case e: Exception => throw new CDMKafkaException("Unable to Delete Topic : " + topic, e)
    } finally zkUtil.close

    success

  }

  private def checkTopicExists(zkUtil: zk, topic: String): Boolean = admin.topicExists(zkUtil, topic)


  def topicExists(topic: String): Boolean = synchronized {
    val zkUtil = createZkUtil
    var topicExists = false
    try {
      topicExists = admin.topicExists(zkUtil, topic)
    } catch {
      case e: Exception => throw new CDMKafkaException("Unable To check if Topic Exists for Topic ::  " + topic, e)
    } finally zkUtil.close
    topicExists
  }

  def createZkClient(zkUrl: String, sessionTimeout: Int, connectionTimeout: Int): zkClient = synchronized {
    zk.createZkClient(zkUrl, sessionTimeout, connectionTimeout)
  }

  private def createZkUtil = zk(zkHosts, zkSessionTimeout, zkConnectionTimeout, isZkSecurityEnabled = false)

  def topicPartitions(topics: Seq[String]): Map[String, Int] = {
    val zkUtil = createZkUtil
    var partitionsMapping: Map[String, Int] = Map()
    try {
      partitionsMapping = zkUtil.getPartitionsForTopics(topics).map({ case (k, v) => k -> v.max }).toMap
    } catch {
      case e: Exception => throw new CDMKafkaException("Unable To check  Topic Partitions for Topics ::  " + topics, e)
    } finally zkUtil.close
    partitionsMapping
  }
}
