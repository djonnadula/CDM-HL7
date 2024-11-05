package com.cdm.hl7.exception

/**
  * Created by Devaraj Jonnadula on 1/9/2017.
  *
  * Exception thrown at Runtime if Template has invalid Format
  */
class InvalidTemplateFormatException (message: String, t: Throwable) extends RuntimeException(message,t) {

  def this(message: String) = this(message, null)

  def this(t: Throwable) = this("", t)


}