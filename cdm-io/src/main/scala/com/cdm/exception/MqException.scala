package com.cdm.exception

/**
  * Created by Devaraj Jonnadula on 11/13/2016.
  */
class MqException(message: String, t: Throwable) extends RuntimeException(message, t) {

  def this(message: String) = this(message, null)

  def this(t: Throwable) = this("", t)

}
