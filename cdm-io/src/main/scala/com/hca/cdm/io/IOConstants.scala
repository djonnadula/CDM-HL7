package com.hca.cdm.io

import java.net.InetAddress

import com.hca.cdm._

import scala.util.Random

/**
  * Created by Devaraj Jonnadula on 8/19/2016.
  * Constants for Kafka
  */
object IOConstants {


  private val random = new Random
  lazy val zkHosts: String = lookUpProp("hl7.zkHosts")
  lazy val kafkaHosts: String = lookUpProp("hl7.kafka.brokers")
  val DEFAULT_RETRIES: Int = 30
  val defaultSegmentPartitions: Int = 6
  val defaultHL7Partitions: Int = 6
  val zkSessionTimeout: Int = 6000
  val zkConnectionTimeout: Int = 10000
  val defaultReplication: Int = 3
  val producerBatch: String = "25000"
  val producerAck: String = "all"
  val defaultDeSerializer: String = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
  val defaultSerializer: String = "org.apache.kafka.common.serialization.ByteArraySerializer"
  val defaultPartitioner: String = "org.apache.kafka.clients.producer.internals.DefaultPartitioner"
  val defaultCompression: String = "none"
  val lz4Compression: String = "lz4"
  val defaultProducerLinger: String = "200"
  val blockOnbufferFull = "60000"
  val metadataFetchTimeout: String = "3000"
  val defaultSendRetries: Int = 10
  val defaultHeartBeat: String = "20000"
  val defaultRequestTimeOut: String = "120000"
  val defaultHeartBeatTimeOut: String = "20000"
  val producerBufferMemory: String = (128 * 1024 * 1024).toString
  val sendBuffer: String = "1048576"
  val receiveBuffer: String = "1048576"
  val checkCRS: String = "false"
  val defaultRetries: Int = 30
  val defaultWaitBetweenRetriesMS: Long = 1000
  val DEFAULT_WAIT_MS: Long = 1000
  val defaultProducerRetries: String = "2"
  val defaultConsumerSessionTimeOut = "30000"
  val defaultConsumerAutoCommit = "false"
  lazy val defaultOffsetReset: String = lookUpProp("hl7.topicReset")
  val fetchBytes: String = (5 * 1024 * 1024).toString
  val fetchMinBytes: String = "100"
  val fetchMinWait: String = "3000"
  val maxRecSize = "5242880"
  val requestMaxSize: String = Int.MaxValue.toString

  def defaultClientId: String = "CDM-HL7-" + InetAddress.getLocalHost.getHostName + "-" + random.nextLong


}
