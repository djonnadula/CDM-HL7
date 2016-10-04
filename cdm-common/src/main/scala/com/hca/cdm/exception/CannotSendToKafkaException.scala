package com.hca.cdm.exception

/**
  * Created by Devaraj Jonnadula on 9/26/2016.
  */
class CannotSendToKafkaException(message: String, t: Throwable) extends RuntimeException(message, t) {

  def this(message: String) = this(message, null)

  def this(t: Throwable) = this(t.getMessage, t)


}

