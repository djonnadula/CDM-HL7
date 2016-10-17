package com.hca.cdm.io

import com.hca.cdm.hadoop.OverSizeHandler

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
trait DataWriter {

  def writeData(data: AnyRef, header: AnyRef, topic: String)(sizeThreshold: Int, overSizeHandler: OverSizeHandler)

  def getTotalWritten(topic: String): Long

  def getTotalWritten: Map[String, Long]

  def close()
}
