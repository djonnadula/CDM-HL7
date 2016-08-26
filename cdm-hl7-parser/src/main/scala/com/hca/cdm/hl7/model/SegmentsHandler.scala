package com.hca.cdm.hl7.model

import com.hca.cdm.hl7.model.SegmentsState.SegState

import scala.collection.mutable

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
trait SegmentsHandler {


  def handleSegments(data: mutable.LinkedHashMap[String, Any]): Unit

  def metricsRegistry: mutable.HashMap[String, mutable.HashMap[SegState, Long]]

  def resetMetrics: Boolean

  def shutDown : Unit

}
