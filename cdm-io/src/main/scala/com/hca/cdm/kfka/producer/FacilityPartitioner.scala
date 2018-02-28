package com.hca.cdm.kfka.producer

import java.util
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import scala.util.Random
import com.hca.cdm._
import com.hca.cdm.log.Logg

/**
  * Created by Devaraj Jonnadula on 1/25/2018.
  */
private[cdm] class FacilityPartitioner extends Partitioner with Logg {
  private lazy val randomPart = new Random
  private val maxPart: Int = 6

  def partition(topic: String, key: scala.Any, keyBytes: Array[Byte], value: scala.Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    val split = new String(keyBytes) split("\\:", -1)
    if (valid(split)) {
      tryAndReturnDefaultValue0(split(3), EMPTYSTR) match {
        case fac: String =>
          val topicPart: Int = tryAndReturnDefaultValue0(cluster.partitionCountForTopic(topic), randomPart.nextInt(maxPart))
          var facPart = fac.hashCode % topicPart
          if (facPart < 0) facPart = randomPart.nextInt(maxPart)
          debug(s"$fac partition $facPart")
          facPart
        case `EMPTYSTR` => randomPart(cluster, topic)
      }
    } else randomPart(cluster, topic)
  }


  private def randomPart(cluster: Cluster, topic: String): Int = {
    tryAndReturnDefaultValue0(randomPart.nextInt(cluster.partitionCountForTopic(topic) % maxPart), randomPart.nextInt(maxPart))
  }

  def close(): Unit = {}

  def configure(configs: util.Map[String, _]): Unit = {}
}
