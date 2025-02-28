package com.cdm.io

import java.util.function.Consumer


/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  *
  * Common Functions IO should Implement
  */
trait DataReader[ T ] {


  def rawData: Option[ Array[ Byte ] ]

  def getIterator: Iterator[ T ]

  def forEach(action: Consumer[ T ])

  def nextRecord: Option[ T ]

  def hasData: Boolean

  def getTotalRead : Long

  def close()


}
