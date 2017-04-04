package com.hca.cdm.hl7.model

import com.hca.cdm.Models.MSGMeta
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
trait SegmentsHandler extends Serializable {


  def handleSegments(io: (String, String) => Unit, rejectIO: (String, String) => Unit, auditIO: (String, String) => Unit,
                     adhocIO: (String, String, String) => Unit, tlmAckIO: Option[(String) => Unit] = None)(data: mutable.LinkedHashMap[String, Any], meta: MSGMeta): Unit


  def metricsRegistry: TrieMap[String, Long]

  def resetMetrics: Boolean

  // def collectMetrics(driverMetrics: TrieMap[String, mutable.HashMap[SegState, Long]]): TrieMap[String, mutable.HashMap[SegState, Long]]


}
