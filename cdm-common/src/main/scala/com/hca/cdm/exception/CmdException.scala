package com.hca.cdm.exception

/**
  * Created by Devaraj Jonnadula on 8/15/2016.
  *
  * Generic CDM  exception thrown at Runtime
  */

class CmdException(message: String, t: Throwable) extends RuntimeException(message, t) {

  def this(message: String) = this(message, null)

  def this(t: Throwable) = this("", t)

}
