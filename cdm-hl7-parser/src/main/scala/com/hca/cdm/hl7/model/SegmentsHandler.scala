package com.hca.cdm.hl7.model

import com.hca.cdm.Models.MSGMeta
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
trait SegmentsHandler extends Serializable {


  def handleSegments(io: (String, String) => Unit, rejectIO: (AnyRef, String) => Unit, auditIO: (String, String) => Unit,
                     adhocIO: (String, String, String) => Unit, tlmAckIO: Option[(String, String) => Unit] = None,
                     hBaseIO: Map[String, (String, String, mutable.Map[String,String],Boolean) => Unit])
                     (data: mutable.LinkedHashMap[String, Any], rawHl7: String, meta: MSGMeta): Unit


  def metricsRegistry: TrieMap[String, Long]

  def resetMetrics: Boolean

  // def collectMetrics(driverMetrics: TrieMap[String, mutable.HashMap[SegState, Long]]): TrieMap[String, mutable.HashMap[SegState, Long]]

}
