package com.hca.cdm.exception

/**
  * Created by Devaraj Jonnadula on 2/24/2017.
  *
  *
  * Thrown When Operation Fails After Trying for Max Tries
  */
class OperationFailedAfterMaxTriesException(message: String, t: Throwable) extends RuntimeException(message, t) {

  def this(message: String) = this(message, null)

  def this(t: Throwable) = this("", t)

}