package com.hca.cdm.io

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
trait DataWriter {

  def writeData(data: AnyRef, header: AnyRef, topic: String)(sizeThreshold: Int, overSizeHandler: (AnyRef) => Unit)

  def getTotalWritten(topic: String): Long

  def getTotalWritten: Map[String, Long]

  def close()
}
