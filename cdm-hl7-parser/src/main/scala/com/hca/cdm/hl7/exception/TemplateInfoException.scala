package com.hca.cdm.hl7.exception

/**
  * Created by Devaraj Jonnadula on 10/12/2016.
  */
class TemplateInfoException(message: String, t: Throwable) extends RuntimeException {

  def this(message: String) = this(message, null)

  def this(t: Throwable) = this("", t)


}

