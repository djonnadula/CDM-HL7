package com.hca.cdm.io


/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
trait DataWriter {

  def writeData(data: Any, header: Any, topic: String)

  def getTotalWritten(topic :String) : Long

  def getTotalWritten : Map[String,Long]

  def close()
}
