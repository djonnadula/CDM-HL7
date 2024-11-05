package com.cdm.exception

/**
  * Created by Devaraj Jonnadula on 5/2/2017.
  */
class CdmJDBCException(message: String, t: Throwable) extends RuntimeException(message, t) {

  def this(message: String) = this(message, null)

  def this(t: Throwable) = this(t.getMessage, t)


}
