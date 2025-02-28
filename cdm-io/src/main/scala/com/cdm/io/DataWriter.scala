package com.cdm.io

import com.cdm.hadoop.OverSizeHandler

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  *
  * Common Functions IO should Implement
  */
trait DataWriter {

  def writeData(data: AnyRef, header: AnyRef, topic: String)(sizeThreshold: Int, overSizeHandler: OverSizeHandler)

  def getTotalWritten(topic: String): Long

  def getTotalWritten: Map[String, Long]

  def close()
}
