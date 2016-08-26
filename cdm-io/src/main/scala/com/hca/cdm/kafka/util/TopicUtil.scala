package com.hca.cdm.kafka.util

import com.hca.cdm.exception.CDMKafkaException
import com.hca.cdm.io.IOConstants._
import kafka.admin.{AdminUtils => admin}
import kafka.utils.{ZkUtils => zk}
import org.I0Itec.zkclient.{ZkClient => zkClient}

/**
  * Created by Devaraj Jonnadula on 8/19/2016.
  */
object TopicUtil {


  def createTopicIfNotExist(topic: String, segmentPartitions: Boolean = false): Boolean = {
    var success: Boolean = false
    val zkUtils = zk(zkHosts, zkSessionTimeout, zkConnectionTimeout, isZkSecurityEnabled = false)
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

  def changePartitions(topic: String, newPartitions: Int): Int = {
    val zkUtil = zk(zkHosts, zkSessionTimeout, zkConnectionTimeout, isZkSecurityEnabled = false)
    try {
      checkTopicExists(zkUtil, topic) match {
        case true => admin.addPartitions(zkUtil, topic, newPartitions)
          return newPartitions
        case _ =>
      }
    } catch {
      case e: Exception => throw new CDMKafkaException("Unable to Delete Topic : " + topic, e)
    } finally zkUtil.close

    -1
  }

  def deleteTopic(topic: String): Boolean = {
    var success: Boolean = false
    val zkUtil = zk(zkHosts, zkSessionTimeout, zkConnectionTimeout, isZkSecurityEnabled = false)
    try {
      checkTopicExists(zkUtil, topic) match {
        case true => admin.deleteTopic(zkUtil, topic)
          success = true
        case _ =>
      }
    } catch {
      case e: Exception => throw new CDMKafkaException("Unable to Delete Topic : " + topic, e)
    } finally zkUtil.close

    success

  }

  private def checkTopicExists(zkUtil: zk, topic: String): Boolean = admin.topicExists(zkUtil, topic)


  def topicExists(topic: String): Boolean = {
    val zkUtil = zk(zkHosts, zkSessionTimeout, zkConnectionTimeout, isZkSecurityEnabled = false)
    var topicExists = false
    try {
      topicExists = admin.topicExists(zkUtil, topic)
    } catch {
      case e: Exception => throw new CDMKafkaException("Unable To check if Topic Exists for Topic ::  " + topic, e)
    } finally zkUtil.close
    topicExists
  }

  def createZkClient(zkUrl: String, sessionTimeout: Int, connectionTimeout: Int): zkClient = zk.createZkClient(zkUrl, sessionTimeout, connectionTimeout)

}
