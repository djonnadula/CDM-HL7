package com.hca.cdm.kafka.config

import java.util.{Properties => prop}

import org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
import com.hca.cdm.io.IOConstants._
import com.hca.cdm.{asFunc, lookUpProp, tryAndReturnDefaultValue}
import org.apache.kafka.clients.consumer.ConsumerConfig._

/**
  * Created by Devaraj Jonnadula on 8/17/2016.
  *
  * Config for Kafka Consumer
  */
object HL7ConsumerConfig {


  def createConfig(jobConsumer: String): prop = {
    val prop = new prop
    prop.put(GROUP_ID_CONFIG, jobConsumer)
    prop.put(BOOTSTRAP_SERVERS_CONFIG, kafkaHosts)
    prop.put(KEY_DESERIALIZER_CLASS_CONFIG, defaultDeSerializer)
    prop.put(VALUE_DESERIALIZER_CLASS_CONFIG, defaultDeSerializer)
    prop.put(HEARTBEAT_INTERVAL_MS_CONFIG, defaultHeartBeat)
    prop.put(SESSION_TIMEOUT_MS_CONFIG, defaultConsumerSessionTimeOut)
    prop.put(REQUEST_TIMEOUT_MS_CONFIG, defaultRequestTimeOut)
    prop.put(HEARTBEAT_INTERVAL_MS_CONFIG, defaultHeartBeatTimeOut)
    prop.put(ENABLE_AUTO_COMMIT_CONFIG, defaultConsumerAutoCommit)
    prop.put(AUTO_OFFSET_RESET_CONFIG, defaultOffsetReset)
    prop.put(CHECK_CRCS_CONFIG, checkCRS)
    prop.put(MAX_PARTITION_FETCH_BYTES_CONFIG, fetchBytes)
    prop.put(RECEIVE_BUFFER_CONFIG, receiveBuffer)
    prop.put(SEND_BUFFER_CONFIG, sendBuffer)
    prop.put(FETCH_MIN_BYTES_CONFIG, fetchMinBytes)
    prop.put(FETCH_MAX_WAIT_MS_CONFIG, fetchMinWait)
    prop.put(CLIENT_ID_CONFIG, defaultClientId + jobConsumer)
    prop.put("fetch.message.max.bytes", fetchBytes)
    if (tryAndReturnDefaultValue(asFunc(lookUpProp("sasl.enabled").toBoolean), false) == true) {
      prop.put(SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
    }
    prop
  }
}
