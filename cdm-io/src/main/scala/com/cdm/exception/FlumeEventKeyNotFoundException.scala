package com.cdm.exception

/**
  * Created by Devaraj Jonnadula on 11/3/2016.
  */
class FlumeEventKeyNotFoundException(message: String, t: Throwable) extends RuntimeException(message, t) {

  def this(message: String) = this(message, null)

  def this(t: Throwable) = this("", t)

}
